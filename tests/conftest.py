"""Shared fixtures for the IoT Digital Twin test suite."""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import AsyncGenerator
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Ensure ``src/`` is on sys.path so ``import iot_twin`` resolves even without
# ``pip install -e .`` (pytest's ``pythonpath`` setting does the same).
SRC_ROOT = str(Path(__file__).resolve().parent.parent / "src")
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

# Never send usage-tracking events from the test suite (the app reads this at
# import time). Tests that exercise tracking build their own UsageTracker.
os.environ.setdefault("USAGE_TRACKING_ENABLED", "false")

# ---------------------------------------------------------------------------
# Live-workspace opt-in
# ---------------------------------------------------------------------------
# Tests that talk to a real workspace are OFF unless PHARMA_TWIN_LIVE_TESTS=1,
# even when DATABRICKS_CONFIG_PROFILE points at a real workspace: this app
# shares workspaces with the upstream deployment, so a stray live call must be
# impossible by default. ``integration``-marked tests are skipped and any real
# ``WorkspaceClient`` construction raises (mocks are unaffected).
LIVE_TESTS_ENV = "PHARMA_TWIN_LIVE_TESTS"


def live_tests_enabled() -> bool:
    return os.getenv(LIVE_TESTS_ENV) == "1"


def require_live_tests() -> str:
    """Skip unless live tests are opted in; return the explicit CLI profile."""
    if not live_tests_enabled():
        pytest.skip(f"live workspace test: set {LIVE_TESTS_ENV}=1 to run")
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "").strip()
    if not profile:
        pytest.skip("live workspace test: set DATABRICKS_CONFIG_PROFILE explicitly")
    return profile


def pytest_collection_modifyitems(config, items):  # noqa: ARG001 - pytest hook signature
    if live_tests_enabled():
        return
    skip = pytest.mark.skip(reason=f"live workspace test: set {LIVE_TESTS_ENV}=1 to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


class LiveWorkspaceBlocked(RuntimeError):
    """Raised when a test constructs a real WorkspaceClient without the opt-in."""


@pytest.fixture(autouse=True)
def _block_live_workspace(monkeypatch):
    """Make real ``WorkspaceClient`` construction fail unless live tests are opted in."""
    if live_tests_enabled():
        yield
        return
    try:
        from databricks.sdk import WorkspaceClient
    except Exception:  # pragma: no cover - SDK stubbed / unavailable
        yield
        return
    if not isinstance(WorkspaceClient, type):  # stubbed SDK
        yield
        return

    def _blocked(self, *args, **kwargs):
        raise LiveWorkspaceBlocked(
            f"real WorkspaceClient constructed in a test; set {LIVE_TESTS_ENV}=1 for live tests"
        )

    monkeypatch.setattr(WorkspaceClient, "__init__", _blocked)
    yield


# ---------------------------------------------------------------------------
# Conditionally pre-mock databricks.sdk to prevent architecture-mismatch
# ImportError on hosts where the crypto/cffi packages are built for the wrong
# architecture (arm64 binaries run under Rosetta x86_64, or vice-versa).
#
# When the real SDK is importable (e.g. the .venv interpreter) we skip all
# mocking so submodules like databricks.sdk.service.* resolve correctly.
#
# When the real SDK is NOT importable we install lightweight stub mocks, but
# also register every submodule path that production code imports so they
# resolve as packages rather than causing "not a package" errors.
# ---------------------------------------------------------------------------
def _pre_mock_sdk() -> None:
    """Inject lightweight mocks only when the real SDK cannot be imported."""
    try:
        import databricks.sdk.service.jobs  # noqa: F401

        # Real SDK is available — do not override anything.
        return
    except Exception:
        pass

    # Real SDK is broken on this interpreter.  Register stubs for every module
    # path the production code imports so `from databricks.sdk.service.X import Y`
    # resolves to a MagicMock attribute rather than raising ModuleNotFoundError.
    _stub = MagicMock()
    _submodule_names = (
        "_cffi_backend",
        "cffi",
        "cryptography",
        "cryptography.exceptions",
        "cryptography.hazmat",
        "cryptography.hazmat.bindings",
        "cryptography.hazmat.bindings._rust",
        "google",
        "google.auth",
        "google.auth.crypt",
        "google.auth.crypt.es",
        "google.auth.iam",
        "google.auth.impersonated_credentials",
        "databricks",
        "databricks.sdk",
        "databricks.sdk.config",
        "databricks.sdk.core",
        "databricks.sdk.credentials_provider",
        "databricks.sdk.errors",
        "databricks.sdk.service",
        "databricks.sdk.service.apps",
        "databricks.sdk.service.catalog",
        "databricks.sdk.service.compute",
        "databricks.sdk.service.jobs",
        "databricks.sdk.service.postgres",
        "databricks.sdk.service.sql",
        "databricks.sdk.service.workspace",
    )
    for _name in _submodule_names:
        sys.modules.setdefault(_name, MagicMock())


_pre_mock_sdk()

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import list_alert_rules
from tests._fakes import fake_credential


@pytest.fixture()
def instant_alert_rules():
    """Catalog alert rules with their sustain duration removed.

    Route and plumbing tests post one reading and expect an alert; the
    duration debounce has its own engine-level tests.
    """

    def _rules(industry: str):
        return tuple(replace(rule, duration_s=0.0) for rule in list_alert_rules(industry))

    with patch("iot_twin.services.alerts.alert_engine.list_alert_rules", side_effect=_rules):
        yield


# ---------------------------------------------------------------------------
# AppState with a mocked database
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_db() -> MagicMock:
    """Return a mock LakebaseConnectionManager with async helpers."""
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchrow = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


@pytest.fixture()
def mock_app_state(mock_db: MagicMock) -> AppState:
    """Return an AppState with a mocked database layer."""
    state = AppState()
    state.db = mock_db
    state.db_available = True
    state.db_error = None
    state.industries = "pharmaceutical"
    return state


# ---------------------------------------------------------------------------
# FastAPI test client (httpx AsyncClient)
# ---------------------------------------------------------------------------
def _build_test_app(state: AppState):
    """Create a minimal FastAPI app wired to the given AppState."""
    from fastapi import FastAPI

    from iot_twin.routes.health import router as health_router

    test_app = FastAPI()
    test_app.state.app_state = state
    test_app.include_router(health_router)
    return test_app


@pytest_asyncio.fixture()
async def test_client(mock_app_state: AppState) -> AsyncGenerator[AsyncClient, None]:
    """Yield an httpx AsyncClient bound to the test app."""
    app = _build_test_app(mock_app_state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Databricks WorkspaceClient mock
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_ws_client() -> MagicMock:
    """Return a mocked Databricks WorkspaceClient."""
    ws = MagicMock()
    ws.clusters = MagicMock()
    ws.clusters.list = MagicMock(return_value=[])
    ws.jobs = MagicMock()
    ws.jobs.list = MagicMock(return_value=[])
    ws.warehouses = MagicMock()
    ws.warehouses.list = MagicMock(return_value=[])
    ws.current_user = MagicMock()
    ws.current_user.me = MagicMock(return_value=MagicMock(user_name="test@example.com"))
    ws.postgres = MagicMock()
    ws.postgres.generate_database_credential = MagicMock(
        return_value=MagicMock(token=fake_credential("token"))
    )
    return ws


# ---------------------------------------------------------------------------
# Integration fixture — live app-backend Lakebase project (opt-in only)
# ---------------------------------------------------------------------------
# The app-backend data layer (models/app_schema.py + services repos) is
# validated against this deployment's OWN app-backend Lakebase project
# (``resource_names.app_backend_project()``, overridable via
# APP_BACKEND_LAKEBASE_ENDPOINT, which must be an owned name). Each test runs in
# a throwaway schema (``test_phase1_<hex>``) dropped on teardown. Skipped unless
# PHARMA_TWIN_LIVE_TESTS=1 and an explicit DATABRICKS_CONFIG_PROFILE are set.
def app_backend_endpoint() -> str:
    from iot_twin.core import resource_names
    from iot_twin.core.config import assert_owned_name

    endpoint = os.getenv("APP_BACKEND_LAKEBASE_ENDPOINT", "").strip() or (
        f"projects/{resource_names.app_backend_project()}/branches/production/endpoints/primary"
    )
    return assert_owned_name(endpoint, what="app-backend test endpoint")


@pytest_asyncio.fixture()
async def app_backend_conn():
    """Yield an asyncpg connection to the owned app-backend project in a throwaway schema."""
    profile = require_live_tests()
    try:
        import asyncpg
        from databricks.sdk import WorkspaceClient
    except Exception as exc:  # pragma: no cover - deps missing
        pytest.skip(f"integration deps unavailable: {exc}")

    endpoint_name = app_backend_endpoint()
    try:
        w = WorkspaceClient(profile=profile)
        endpoint = w.postgres.get_endpoint(name=endpoint_name)
        host = endpoint.status.hosts.host
        user = w.current_user.me().user_name
        cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
        conn = await asyncpg.connect(
            host=host,
            database="databricks_postgres",
            user=user,
            password=cred.token,
            port=5432,
            ssl="require",
        )
    except Exception as exc:  # pragma: no cover - offline / no creds
        pytest.skip(f"app-backend {endpoint_name} not reachable: {exc}")

    schema = f"test_phase1_{uuid.uuid4().hex[:8]}"
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await conn.execute(f'SET search_path TO "{schema}"')
    try:
        yield conn
    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()


@pytest.fixture(autouse=True)
def _route_admin_by_default(request, monkeypatch):
    """Route tests run as an app admin unless marked ``non_admin``.

    Workspace-writing routes are gated by ``routes._admin.require_admin``. Only
    that module's reference is replaced, so tests of ``app_permissions.is_admin``
    itself are unaffected. Tests of the gate use ``@pytest.mark.non_admin``.
    """
    from types import SimpleNamespace

    from iot_twin.routes import _admin

    admin = "non_admin" not in request.keywords
    monkeypatch.setattr(_admin, "app_permissions", SimpleNamespace(is_admin=lambda _r: admin))
    yield


@pytest.fixture(autouse=True)
def _reset_genie_allowlist_cache():
    """Per-pipeline Genie allowlist verifications are process-cached; isolate tests."""
    from iot_twin.services import genie_service

    genie_service.invalidate_allowlist_cache()
    yield
    genie_service.invalidate_allowlist_cache()
