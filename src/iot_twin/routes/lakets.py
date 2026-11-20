"""LakeTS-specific routes — sync readiness preflight etc."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from uuid import UUID

import asyncpg
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import cdf_reconciliation_service
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.lakets import lakets_service
from iot_twin.services.lakets.lakets_deploy_service import deploy_retention_job
from iot_twin.services.pipelines.pipeline_db import get_industry_lakebase, get_pipeline_lakebase

logger = logging.getLogger("iot_digital_twin.routes.lakets")

router = APIRouter()
_INDUSTRY_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
# Bound CDF / LakeTS probes so a hung industry Lakebase cannot pin the app
# worker until the reverse-proxy upstream timeout (~10m).
_LAKEBASE_PROBE_TIMEOUT_SECONDS = 8.0
_LAKEBASE_RESOLVE_TIMEOUT_SECONDS = 25.0
_LAKETS_DASHBOARD_TIMEOUT_SECONDS = 30.0


async def _probe_wait(coro, *, timeout: float = _LAKEBASE_PROBE_TIMEOUT_SECONDS):
    """Await ``coro`` with a deadline and fully reap cancellation."""
    return await asyncio.wait_for(coro, timeout=timeout)


_CDF_STATUS_SQL = """
SELECT t.status
FROM wal2delta.tables t
JOIN pg_class c ON c.oid = t.table_oid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
"""
_REGISTERED_CDF_STATUS_SQL = """
SELECT t.status, r.shadow_table_name AS shadow_table
FROM lakets._chronotable_registry r
LEFT JOIN pg_namespace n ON n.nspname = $1
LEFT JOIN pg_class c
  ON c.relnamespace = n.oid AND c.relname = r.shadow_table_name
LEFT JOIN wal2delta.tables t ON t.table_oid = c.oid
WHERE r.schema_name = 'public'
  AND r.table_name = 'sensor_readings'
  AND r.sync_enabled
  AND r.shadow_table_name IS NOT NULL
"""


def _unavailable_dashboard(reason: str) -> dict[str, dict[str, Any]]:
    return {
        section: {"value": None, "reason": reason} for section in lakets_service.DASHBOARD_SECTIONS
    }


async def _open_lakebase_conn(lb: dict) -> Any:
    """Open a direct asyncpg connection using the same resolution as provisioning.

    Mirrors ``services/provisioning_service.provision_lakebase_table`` — autoscale
    mode asks the Databricks SDK for a short-lived OAuth credential; direct mode
    uses plain creds from the body.
    """
    mode = lb.get("mode", "autoscale")
    database = lb.get("database", "databricks_postgres")

    if mode == "autoscale":
        project_id = lb.get("project_id", "")
        branch_id = lb.get("branch_id", "production")
        if not project_id:
            raise ValueError("project_id is required in autoscale mode")

        w = WorkspaceClient()
        parent = f"projects/{project_id}/branches/{branch_id}"
        endpoints = list(w.postgres.list_endpoints(parent=parent))
        if not endpoints:
            raise RuntimeError(f"No endpoints for {parent}")

        endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
        cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
        return await asyncpg.connect(
            host=endpoint.status.hosts.host,
            database=database,
            user=w.current_user.me().user_name,
            password=cred.token,
            port=5432,
            ssl="require",
            timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
            command_timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )

    return await asyncpg.connect(
        host=lb.get("host", ""),
        database=database,
        user=lb.get("user", ""),
        password=lb.get("password", ""),
        port=int(lb.get("port", 5432)),
        ssl="require",
        timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        command_timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
    )


async def _is_realtime_industry_deployment(
    state: AppState, pipeline_id: str, industry: str
) -> bool:
    """Bind an industry dashboard request to deployed realtime metadata."""
    db = getattr(state, "app_backend_db", None)
    if db is None:
        raise RuntimeError("App metadata database is unavailable")
    row = await db.fetchrow(
        """
        SELECT 1
        FROM pipeline_deployment pd
        JOIN pipeline_setup ps ON ps.id = pd.setup_id
        WHERE pd.id = $1::uuid
          AND ps.industry = $2
          AND pd.mode = 'realtime'
          AND pd.job_id IS NOT NULL
          AND pd.status NOT IN ('pending', 'deploying', 'failed')
        """,
        pipeline_id,
        industry,
    )
    return row is not None


@router.post("/api/lakets/sync/check")
async def check_sync(request: Request):
    """Probe a Lakebase for Lakehouse Sync readiness.

    Accepts the same ``lb`` body shape as ``/api/settings/provision``.
    Returns the readiness dict (booleans + a human-readable ``reason``).
    Never raises — connection errors are folded into the result dict.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    lb = body.get("lakebase") or body.get("lb") or body
    try:
        conn = await _open_lakebase_conn(lb)
    except Exception as exc:
        logger.warning("Sync check could not connect: %s", exc)
        return {
            "database_compatible": False,
            "wal2delta_available": False,
            "wal2delta_installed": False,
            "lakets_sync_functions_present": False,
            "ready_to_enable": False,
            "error": f"Connection failed: {exc}"[:200],
        }

    try:
        return await lakets_service.check_lakehouse_sync_readiness(conn)
    finally:
        await conn.close()


@router.get("/api/pipelines/{pipeline_id}/cdf-status")
async def get_cdf_status(
    pipeline_id: str,
    industry: str | None = Query(
        None,
        description="Industry id — resolves the deterministic iot-pipelines "
        "branch directly (industry-first dashboards have no legacy pipelines row).",
    ),
    state: AppState = Depends(get_app_state),
):
    """Report whether Lakebase CDF (Lakehouse Sync) is streaming for a pipeline.

    Queries the pipeline's Lakebase ``wal2delta.tables`` (the Lakehouse Sync
    bookkeeping schema). A row status of ``STREAMING`` means CDF is actively
    replicating; ``SNAPSHOTTING`` means the initial backfill is in progress.
    If the schema/table is missing or the query errors, we fail closed to
    ``status="NONE"`` with a human-readable ``reason``.

    ``hot_latest_at`` is the latest ``sensor_readings.event_time`` in Lakebase.
    The query uses the time ordering rather than an expensive full-table row
    count. Cold-tier freshness is queried separately through DBSQL.
    """
    none_result = {
        "enabled": False,
        "streaming": False,
        "status": "NONE",
        "reason": "Lakebase Sync not enabled",
        "hot_latest_at": None,
    }

    if industry:
        try:
            industry = cdf_reconciliation_service.normalize_industry(industry)
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "Invalid industry"})
        try:
            UUID(pipeline_id)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid pipeline deployment id"},
            )
        try:
            is_bound = await _is_realtime_industry_deployment(state, pipeline_id, industry)
        except Exception as exc:
            logger.warning(
                "CDF deployment validation failed for %s: %s",
                pipeline_id,
                exc,
                exc_info=True,
            )
            return JSONResponse(
                status_code=503,
                content={"error": "Pipeline metadata unavailable"},
            )
        if not is_bound:
            return JSONResponse(
                status_code=404,
                content={"error": "Realtime industry deployment not found"},
            )

    try:
        # Industry-first pipelines resolve the deterministic iot-pipelines branch
        # by industry (no legacy pipelines/sink_config row); legacy pipelines fall
        # back to the per-pipeline lookup. Hard-bound so a hung pool never
        # stalls the dashboard proxy.
        lb = await _probe_wait(
            (
                get_industry_lakebase(state, industry)
                if industry
                else get_pipeline_lakebase(state, pipeline_id)
            ),
            timeout=_LAKEBASE_RESOLVE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        return {
            **none_result,
            "reason": (f"Lakebase probe timeout after {_LAKEBASE_RESOLVE_TIMEOUT_SECONDS:g}s"),
        }
    except Exception as exc:
        return {**none_result, "reason": f"Pipeline Lakebase not resolvable: {exc}"[:200]}

    if lb is None:
        return {**none_result, "reason": "Pipeline Lakebase not resolvable"}

    try:
        if industry:
            expected_shadow = cdf_reconciliation_service.shadow_table_name(industry)
            rows = await _probe_wait(
                lb.fetch(
                    _CDF_STATUS_SQL,
                    cdf_reconciliation_service.CDF_SHADOW_SCHEMA,
                    expected_shadow,
                ),
                timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
            )
        else:
            rows = await _probe_wait(
                lb.fetch(
                    _REGISTERED_CDF_STATUS_SQL,
                    cdf_reconciliation_service.CDF_SHADOW_SCHEMA,
                ),
                timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
            )
            expected_shadow = rows[0].get("shadow_table") if rows else None
    except TimeoutError:
        return {
            **none_result,
            "reason": (f"Lakebase Sync probe timeout after {_LAKEBASE_PROBE_TIMEOUT_SECONDS:g}s"),
        }
    except Exception as exc:
        logger.warning("cdf-status wal2delta probe failed for %s: %s", pipeline_id, exc)
        return {**none_result, "reason": f"Lakebase Sync not enabled: {exc}"[:200]}

    statuses = {str(r["status"]).upper() for r in rows if r["status"] is not None}

    if "STREAMING" in statuses:
        status = "STREAMING"
    elif "SNAPSHOTTING" in statuses:
        status = "SNAPSHOTTING"
    elif statuses:
        status = "UNKNOWN"
    else:
        missing = (
            f"{cdf_reconciliation_service.CDF_SHADOW_SCHEMA}.{expected_shadow}"
            if expected_shadow
            else "wal2delta tables"
        )
        return {**none_result, "reason": f"Lakebase Sync not enabled ({missing} absent)"}

    hot_latest_at = None
    try:
        hot_latest_at = await _probe_wait(
            lb.fetchval("SELECT MAX(event_time) FROM sensor_readings"),
            timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning("cdf-status latest hot timestamp timed out for %s", pipeline_id)
    except Exception as exc:
        logger.warning(
            "cdf-status latest hot timestamp failed for %s: %s",
            pipeline_id,
            exc,
        )

    return {
        "enabled": True,
        "streaming": status == "STREAMING",
        "status": status,
        "reason": (
            f"{cdf_reconciliation_service.CDF_SHADOW_SCHEMA}.{expected_shadow} "
            f"reports status={status}"
            if expected_shadow
            else f"wal2delta tables report status={status}"
        ),
        "hot_latest_at": hot_latest_at,
    }


@router.get("/api/pipelines/{pipeline_id}/lakets/dashboard")
async def get_lakets_dashboard(
    pipeline_id: str,
    industry: str | None = Query(
        None,
        description="Industry id for the deterministic industry Lakebase branch",
    ),
    state: AppState = Depends(get_app_state),
):
    """Return consolidated LakeTS observability for a pipeline Lakebase."""
    if industry:
        if not _INDUSTRY_ID_RE.fullmatch(industry) or industry not in CURATED_INDUSTRIES:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid industry"},
            )
        try:
            UUID(pipeline_id)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid pipeline deployment id"},
            )
        try:
            is_bound = await _is_realtime_industry_deployment(state, pipeline_id, industry)
        except Exception as exc:
            logger.warning(
                "Realtime deployment validation failed for %s: %s",
                pipeline_id,
                exc,
                exc_info=True,
            )
            return JSONResponse(
                status_code=503,
                content={"error": "Pipeline metadata unavailable"},
            )
        if not is_bound:
            return JSONResponse(
                status_code=404,
                content={"error": "Realtime industry deployment not found"},
            )

    try:
        lb = await _probe_wait(
            (
                get_industry_lakebase(state, industry)
                if industry
                else get_pipeline_lakebase(state, pipeline_id)
            ),
            timeout=_LAKEBASE_RESOLVE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "LakeTS dashboard Lakebase resolution timed out for %s after %.0fs",
            pipeline_id,
            _LAKEBASE_RESOLVE_TIMEOUT_SECONDS,
        )
        return _unavailable_dashboard("Pipeline Lakebase probe timed out")
    except Exception as exc:
        logger.warning(
            "LakeTS dashboard Lakebase resolution failed for %s: %s",
            pipeline_id,
            exc,
            exc_info=True,
        )
        return _unavailable_dashboard("Pipeline Lakebase not resolvable")

    if lb is None:
        return _unavailable_dashboard("Pipeline Lakebase not resolvable")
    try:
        return await _probe_wait(
            lakets_service.get_dashboard(lb),
            timeout=_LAKETS_DASHBOARD_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "LakeTS dashboard timed out for %s after %.0fs",
            pipeline_id,
            _LAKETS_DASHBOARD_TIMEOUT_SECONDS,
        )
        return _unavailable_dashboard("LakeTS dashboard timed out")
    except Exception as exc:
        logger.warning(
            "LakeTS dashboard failed for %s: %s",
            pipeline_id,
            exc,
            exc_info=True,
        )
        return _unavailable_dashboard("LakeTS dashboard unavailable")


@router.post("/api/lakets/retention/deploy")
async def deploy_retention(request: Request):
    """Upload the retention notebook and create/update a daily Databricks Job."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    lb = body.get("lakebase") or body.get("lb") or body
    project_id = (lb.get("project_id") or "").strip()
    if not project_id:
        return JSONResponse(
            status_code=400,
            content={"error": "lakebase.project_id is required"},
        )

    try:
        return deploy_retention_job(
            project_id=project_id,
            branch_id=(lb.get("branch_id") or "production").strip(),
            database=(lb.get("database") or "databricks_postgres").strip(),
        )
    except Exception as exc:
        logger.exception("Retention job deploy failed")
        return JSONResponse(status_code=500, content={"error": str(exc)[:500]})
