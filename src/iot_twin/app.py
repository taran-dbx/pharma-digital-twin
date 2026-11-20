"""IoT Digital Twin — FastAPI application.

Slim factory: lifespan, routers, and static file mounts.
All route handlers and business logic live in routes/ and services/.
"""

from __future__ import annotations

import asyncio
import logging
import os
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from iot_twin.core.config import INDUSTRIES, UPDATE_RATE_HZ
from iot_twin.core.paths import SPA_DIST_DIR, SPA_INDEX, STATIC_DIR
from iot_twin.core.shutdown import install_sigterm_handler, shutdown
from iot_twin.core.state import AppState
from iot_twin.routes import create_api_router
from iot_twin.routes.industries_admin import start_industry_runtime_refresh
from iot_twin.services.alerts.alert_snapshot_sync import start_alert_snapshot_sync
from iot_twin.services.deployment.deployment_service import (
    RuntimeOAuthCredentialError,
    sync_runtime_oauth_secret,
)
from iot_twin.services.simulator_service import run_simulator
from iot_twin.services.usage_tracking import UsageTracker, UsageTrackingMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("iot_digital_twin")

# Usage tracking (dbdemos-tracker): a no-op until started; opt out with
# USAGE_TRACKING_ENABLED=false. See services/usage_tracking.py.
usage_tracker = UsageTracker.from_env()


async def start_usage_tracking() -> None:
    """Start usage tracking and record the app start; never raises."""
    if await usage_tracker.start():
        usage_tracker.track_app_start()


async def publish_runtime_oauth_secret() -> None:
    """Copy the app SP secret into the UC secret that streaming jobs read.

    A secret rotation restarts the app; running consumers wait for this copy.
    """
    try:
        result = await sync_runtime_oauth_secret()
        logger.info("App SP OAuth secret published to the UC secret (%s)", result.get("status"))
    except RuntimeOAuthCredentialError as exc:
        logger.info("Skipping UC secret refresh: %s", exc)
    except Exception as exc:
        logger.warning("UC secret refresh failed: %s", exc)


# ---------------------------------------------------------------------------
# Lifespan — startup and shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(application: FastAPI):
    state = AppState(industries=INDUSTRIES, update_rate_hz=UPDATE_RATE_HZ)
    application.state.app_state = state

    install_sigterm_handler(state)

    logger.info("=== IoT Digital Twin starting ===")
    logger.info("PGHOST=%s", os.getenv("PGHOST", "(not set)"))
    logger.info("LAKEBASE_ENDPOINT=%s", os.getenv("LAKEBASE_ENDPOINT", "(not set)"))
    logger.info("ZEROBUS_SERVER_ENDPOINT=%s", os.getenv("ZEROBUS_SERVER_ENDPOINT", "(not set)"))
    logger.info("DATABRICKS_CLIENT_ID=%s", os.getenv("DATABRICKS_CLIENT_ID", "(not set)"))
    logger.info("INDUSTRIES=%s", INDUSTRIES)

    # --- Single Lakebase connection (iot-app-backend, OAuth) ---
    # One AppBackendConnectionManager serves both state.db and state.app_backend_db
    # so existing callsites on either field keep working without modification.
    if os.getenv("PGHOST"):
        try:
            from iot_twin.models import app_schema
            from iot_twin.services.db.app_backend_db import build_app_backend_manager

            app_db = build_app_backend_manager()
            await app_db.initialize()
            state.db = app_db
            state.app_backend_db = app_db
            state.db_available = True
            state.app_backend_available = True
            state.db_error = None
            state.app_backend_error = None
            logger.info("Lakebase connection pool ready (iot-app-backend)")
            await app_schema.ensure_app_schema(app_db)
            await app_db.ensure_pipelines_table()
            await app_db.ensure_user_settings_table()
            await app_db.ensure_source_bindings_table()
            logger.info("App-backend schema ensured")
        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Lakebase connection failed (app will run without DB): %s\n%s",
                exc,
                traceback.format_exc(),
            )
            state.db_available = False
            state.app_backend_available = False
            state.db_error = err_msg
            state.app_backend_error = err_msg
            state.db = None
            state.app_backend_db = None
    else:
        logger.info("PGHOST not set — Lakebase features disabled, WebSocket-only mode")
        state.db_available = False
        state.db_error = "PGHOST not set"
        state.app_backend_available = False
        state.app_backend_error = "PGHOST not set"

    secret_sync = asyncio.create_task(publish_runtime_oauth_secret())
    # Off the startup path: the workspace-id lookup must not delay boot.
    tracking_start = asyncio.create_task(start_usage_tracking())

    # --- Simulator background task ---
    state.simulator_task = asyncio.create_task(run_simulator(state))
    start_industry_runtime_refresh(state)
    start_alert_snapshot_sync(state)

    logger.info("=== IoT Digital Twin started (db=%s) ===", state.db_available)
    yield
    secret_sync.cancel()
    tracking_start.cancel()
    logger.info("=== IoT Digital Twin shutting down ===")
    await shutdown(state)
    logger.info("=== IoT Digital Twin stopped ===")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------
app = FastAPI(title="IoT Digital Twin", lifespan=lifespan)
app.add_middleware(UsageTrackingMiddleware, tracker=usage_tracker)

# Every router (API, setup, admin, validation, SPA root) is registered by
# create_api_router(), in a fixed order.
app.include_router(create_api_router())

# Static file mounts — must come after route registration
app.mount("/assets", StaticFiles(directory=SPA_DIST_DIR / "assets"), name="spa-assets")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
# Vite copies client/public/* to the dist root; serve brand SVGs from there so
# the SPA catch-all below doesn't swallow /brand/*.svg and return index.html.
if (SPA_DIST_DIR / "brand").is_dir():
    app.mount("/brand", StaticFiles(directory=SPA_DIST_DIR / "brand"), name="brand")

# SPA catch-all — must be the very last route


@app.get("/{path:path}")
async def spa_fallback(path: str):
    """Catch-all for client-side routes — serve the React SPA entry point."""
    return FileResponse(SPA_INDEX)
