"""Every Lakebase / UC entry point refuses names outside the resource prefix.

Each guard must fire BEFORE any workspace client is built, so a foreign name
(e.g. an upstream ``iot-*`` project) can never be created, connected to,
granted on or deleted. Routes answer such requests with 403.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core import config
from iot_twin.core.auth import get_current_user
from iot_twin.core.config import NotOwnedError

ROOT = Path(__file__).resolve().parents[1]
FOREIGN = (
    "iot-lakets-demo",
    "iot-pipelines",
    "other-project",
    "pharma-twin/../x",
)


@pytest.fixture(autouse=True)
def _default_prefix(monkeypatch):
    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)


@pytest.fixture()
def no_client():
    """Fail the test if a WorkspaceClient is constructed."""
    with patch("databricks.sdk.WorkspaceClient") as ws:
        yield ws
    ws.assert_not_called()


# --- is_owned_name hardening ------------------------------------------------------
@pytest.mark.parametrize(
    "name",
    [
        " pharma-twin-x",
        "pharma-twin-x ",
        "pharma-twin-x\n",
        "pharma-twin/../iot-x",
        "projects/pharma-twin-b/branches/../../iot-pipelines",
        "pharma-twin..x",
        "pharma-twin-x;drop",
        "pharma-twin x",
        "PHARMA-TWIN-X",
        "pharma-twin-x'",
    ],
)
def test_padding_traversal_and_bad_chars_are_not_owned(name: str) -> None:
    assert not config.is_owned_name(name)


def test_assert_owned_name_returns_stripped_value() -> None:
    assert config.assert_owned_name("  pharma-twin-backend \n") == "pharma-twin-backend"


def test_ident_alone_is_owned() -> None:
    assert config.is_owned_name("pharma_twin")
    assert config.is_owned_name("pharma_twin_zb")
    assert not config.is_owned_name("rtm_lakets_zb")


def test_not_owned_error_is_a_value_error() -> None:
    with pytest.raises(ValueError):
        config.assert_owned_name("iot-pipelines")


# --- provisioning_service entry points ------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize("project", FOREIGN)
async def test_provision_lakebase_table_rejects_foreign_project(project, no_client) -> None:
    from iot_twin.services import provisioning_service as ps

    with pytest.raises(NotOwnedError):
        await ps.provision_lakebase_table({"mode": "autoscale", "project_id": project})


@pytest.mark.asyncio
@pytest.mark.parametrize("project", FOREIGN)
async def test_provision_project_and_branch_rejects_foreign_project(project, no_client) -> None:
    from iot_twin.services import provisioning_service as ps

    with pytest.raises(NotOwnedError):
        await ps.provision_lakebase_project_and_branch(project, "production")


@pytest.mark.asyncio
@pytest.mark.parametrize("project", FOREIGN)
async def test_provision_role_rejects_foreign_project(project) -> None:
    from iot_twin.services import provisioning_service as ps

    ws = MagicMock()
    with pytest.raises(NotOwnedError):
        await ps.provision_lakebase_role(
            sp_client_id="sp", project_id=project, branch_id="production", ws=ws
        )
    ws.postgres.create_role.assert_not_called()


@pytest.mark.asyncio
async def test_provision_role_rejects_foreign_endpoint() -> None:
    from iot_twin.services import provisioning_service as ps

    ws = MagicMock()
    with pytest.raises(NotOwnedError):
        await ps.provision_lakebase_role(
            sp_client_id="sp",
            project_id="pharma-twin-pipelines",
            branch_id="production",
            endpoint_name="projects/iot-pipelines/branches/production/endpoints/primary",
            ws=ws,
        )
    ws.postgres.create_role.assert_not_called()


# --- LakeTS connection choke point ------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize("project", FOREIGN)
async def test_open_lakebase_conn_rejects_foreign_project(project) -> None:
    from iot_twin.routes import lakets

    with patch.object(lakets, "WorkspaceClient") as ws, pytest.raises(NotOwnedError):
        await lakets._open_lakebase_conn({"mode": "autoscale", "project_id": project})
    ws.assert_not_called()


# --- stored sink_config ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_pipeline_db_refuses_stored_foreign_project() -> None:
    from iot_twin.core.state import AppState
    from iot_twin.services.pipelines import pipeline_db

    resolve = AsyncMock(return_value="host")
    with (
        patch.object(
            pipeline_db,
            "_load_pipeline_sink_config",
            AsyncMock(return_value={"lb_project_id": "iot-pipelines"}),
        ),
        patch.object(pipeline_db, "_resolve_endpoint_host_bounded", resolve),
    ):
        assert await pipeline_db.get_pipeline_lakebase(AppState(), "pipeline-x") is None
    resolve.assert_not_awaited()


# --- routes answer 403 --------------------------------------------------------------------
def _app(*routers) -> FastAPI:
    from iot_twin.core.state import AppState

    app = FastAPI()
    state = AppState()
    state.db = None
    state.db_available = False
    app.state.app_state = state
    for router in routers:
        app.include_router(router)
    app.dependency_overrides[get_current_user] = lambda: "user@example.com"
    return app


async def _post(app: FastAPI, path: str, body: dict):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(path, json=body)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "path", "body"),
    [
        (
            "provisioning",
            "/api/provision/lakebase-table",
            {"mode": "autoscale", "table_name": "t", "project_id": "iot-pipelines"},
        ),
        (
            "settings",
            "/api/settings/provision",
            {"lakebase": {"mode": "autoscale", "project_id": "iot-pipelines"}},
        ),
        (
            "settings",
            "/api/settings/provision/lakebase",
            {"lakebase": {"mode": "autoscale", "project_id": "iot-app-backend"}},
        ),
        ("lakets", "/api/lakets/sync/check", {"lakebase": {"project_id": "iot-lakets-demo"}}),
        (
            "lakets",
            "/api/lakets/retention/deploy",
            {"lakebase": {"project_id": "iot-lakets-demo"}},
        ),
    ],
)
async def test_routes_return_403_for_foreign_project(module, path, body, no_client) -> None:
    import importlib

    router = importlib.import_module(f"iot_twin.routes.{module}").router
    with (
        patch("iot_twin.services.provisioning_service.provision_lakebase_table") as table,
        patch("iot_twin.services.lakets.lakets_deploy_service.deploy_retention_job") as deploy,
    ):
        resp = await _post(_app(router), path, body)
    assert resp.status_code == 403, resp.text
    assert "not owned" in resp.json()["error"]
    table.assert_not_called()
    deploy.assert_not_called()


@pytest.mark.asyncio
async def test_app_level_handler_maps_not_owned_to_403() -> None:
    from iot_twin.app import _not_owned_handler

    resp = await _not_owned_handler(None, NotOwnedError("x is not owned"))
    assert resp.status_code == 403


# --- startup + scripts ---------------------------------------------------------------------
@pytest.mark.parametrize(
    ("var", "value"),
    [
        ("UC_SCHEMA", "rtm_lakets_zb"),
        ("LAKEBASE_PROJECT", "iot-app-backend"),
        ("PIPELINES_LAKEBASE_PROJECT", "iot-pipelines"),
        ("SECRET_SCOPE", "iot-secrets"),
        ("RESOURCE_PREFIX", "iot"),
    ],
)
def test_startup_validation_rejects_foreign_overrides(monkeypatch, var, value) -> None:
    from iot_twin.core.workspace_config import validate_isolation_config

    monkeypatch.setenv(var, value)
    with pytest.raises(ValueError):
        validate_isolation_config()


def test_startup_validation_accepts_owned_values(monkeypatch) -> None:
    from iot_twin.core.workspace_config import validate_isolation_config

    for var in ("LAKEBASE_PROJECT", "LAKEBASE_ENDPOINT", "PIPELINES_LAKEBASE_PROJECT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("UC_SCHEMA", "pharma_twin")
    monkeypatch.setenv("SECRET_SCOPE", "pharma-twin-secrets")
    monkeypatch.delenv("ZEROBUS_SECRET_SCOPE", raising=False)
    assert validate_isolation_config()["UC_SCHEMA"] == "pharma_twin"


@pytest.mark.parametrize(
    "script",
    ["bootstrap_lakets_demo.py", "smoke_pipeline_deploy.py", "run_lakets_retention.py"],
)
def test_scripts_read_lakets_project_through_owned_env(script: str) -> None:
    source = (ROOT / "scripts" / script).read_text()
    assert 'owned_env("LAKETS_PROJECT_ID")' in source
    assert 'os.getenv("LAKETS_PROJECT_ID"' not in source


# --- ledger rows (pharma_e2e teardown guard) --------------------------------------------------
SHARED_CATALOG = "shared-catalog"


@pytest.mark.parametrize(
    "row",
    [
        {
            "resource_type": "lakebase_branch",
            "identifier": "projects/pharma-twin-pipelines/branches/x",
        },
        {"resource_type": "uc_schema", "identifier": "`shared-catalog`.pharma_twin"},
        {"resource_type": "uc_table", "identifier": "shared-catalog.pharma_twin.t"},
        {"resource_type": "uc_volume", "identifier": "pharma_twin_pharmaceutical.checkpoints.v"},
        {"resource_type": "kafka_topic", "identifier": "pharma-twin.pharmaceutical.realtime"},
        {"resource_type": "secret_scope", "identifier": "pharma-twin-secrets"},
        {"resource_type": "service_principal", "identifier": "uuid", "metadata": {"shared": True}},
        {"resource_type": "job", "identifier": "123"},
    ],
)
def test_owned_ledger_rows_pass(row) -> None:
    from iot_twin.services.ownership import assert_owned_ledger_row

    assert_owned_ledger_row(row, shared_catalog=SHARED_CATALOG)


@pytest.mark.parametrize(
    "row",
    [
        {"resource_type": "lakebase_branch", "identifier": "projects/iot-pipelines/branches/x"},
        {"resource_type": "uc_schema", "identifier": "shared-catalog.rtm_lakets_zb"},
        {"resource_type": "uc_table", "identifier": "iot_pharmaceutical.metrics.t"},
        {"resource_type": "uc_volume", "identifier": "`shared-catalog`.rtm_lakets_zb.v"},
        {"resource_type": "uc_catalog", "identifier": "shared-catalog"},
        {"resource_type": "kafka_topic", "identifier": "iot.pharmaceutical.realtime"},
        {"resource_type": "secret_scope", "identifier": "iot-secrets"},
        {"resource_type": "service_principal", "identifier": "uuid", "metadata": {}},
        {
            "resource_type": "service_principal",
            "identifier": "uuid",
            "metadata": {"display_name": "iot-sss-industries"},
        },
    ],
)
def test_foreign_ledger_rows_rejected(row) -> None:
    from iot_twin.services.ownership import assert_owned_ledger_row

    with pytest.raises(NotOwnedError):
        assert_owned_ledger_row(row, shared_catalog=SHARED_CATALOG)


def test_e2e_setup_guard_uses_the_ledger_check() -> None:
    source = (ROOT / "scripts" / "e2e" / "pharma_e2e.py").read_text()
    guard = source[source.index("async def _assert_owned_setup") :]
    guard = guard[: guard.index("\ndef ")]
    assert "assert_owned_ledger(" in guard and "shared_catalog=get_uc_catalog()" in guard
