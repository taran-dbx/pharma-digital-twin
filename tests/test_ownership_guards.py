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
from tests._fakes import fake_credential

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


# =====================================================================================
# Round 3: secret scopes, UC targets, direct-mode Lakebase, admin gate, teardown
# =====================================================================================
UPSTREAM_SCOPE = "iot-secrets"
FOREIGN_SCOPES = (UPSTREAM_SCOPE, "other-scope", "pharma-twin/../iot-secrets")


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", FOREIGN_SCOPES)
async def test_provision_secrets_scope_rejects_foreign_scope(scope, no_client) -> None:
    from iot_twin.services import provisioning_service as ps

    with pytest.raises(NotOwnedError):
        await ps.provision_secrets_scope(scope)


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", FOREIGN_SCOPES)
async def test_ensure_streaming_sp_rejects_before_any_workspace_call(scope) -> None:
    from iot_twin.services import provisioning_service as ps

    w = MagicMock()
    provision_scope = AsyncMock()
    with pytest.raises(NotOwnedError):
        await ps.ensure_streaming_sp(w, scope, provision_scope=provision_scope)
    assert w.mock_calls == []
    provision_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipeline_setup_rejects_foreign_scope_before_client() -> None:
    from iot_twin.core.state import AppState
    from iot_twin.services.pipelines import pipeline_setup_service as pss

    with patch.object(pss, "_new_ws") as new_ws, pytest.raises(NotOwnedError):
        await pss.setup_pipeline_lakebase(
            AppState(), {"pipeline_id": "p", "secret_scope": UPSTREAM_SCOPE}
        )
    new_ws.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "path", "body"),
    [
        ("settings", "/api/settings/provision", {"secrets": {"scope": "iot-secrets"}}),
        ("settings", "/api/settings/provision/secrets", {"secrets": {"scope": "iot-secrets"}}),
        (
            "settings",
            "/api/settings/provision/lakebase",
            {"lakebase": {"project_id": "pharma-twin-p"}, "secrets": {"scope": "iot-secrets"}},
        ),
        (
            "setup",
            "/api/setup/pipeline-lakebase",
            {
                "pipeline_id": "p-1",
                "lb_project_id": "pharma-twin-p",
                "secret_scope": UPSTREAM_SCOPE,
            },
        ),
    ],
)
async def test_routes_return_403_for_foreign_scope(module, path, body, no_client) -> None:
    import importlib

    router = importlib.import_module(f"iot_twin.routes.{module}").router
    with (
        patch("iot_twin.services.provisioning_service.ensure_streaming_sp") as sp,
        patch("iot_twin.routes.settings.ensure_streaming_sp") as sp_route,
    ):
        resp = await _post(_app(router), path, body)
    assert resp.status_code == 403, resp.text
    assert "secret scope" in resp.json()["error"]
    sp.assert_not_called()
    sp_route.assert_not_called()


@pytest.mark.asyncio
async def test_pipeline_secrets_route_rechecks_stored_scope(no_client) -> None:
    import json

    from iot_twin.routes import provisioning

    app = _app(provisioning.router)
    state = app.state.app_state
    state.db_available = True
    state.db = MagicMock()
    state.db.fetchrow = AsyncMock(
        return_value={"resources": json.dumps({"secret_scope": UPSTREAM_SCOPE})}
    )
    resp = await _post(app, "/api/pipelines/p-1/secrets", {"secrets": {"k": "v"}})
    assert resp.status_code == 403, resp.text


# --- UC targets -------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("catalog", "schema"),
    [
        ("iot_pharmaceutical", "raw"),
        ("other_catalog", "raw"),
        ("shared-catalog", "rtm_lakets_zb"),
        ("shared-catalog", None),
    ],
)
def test_assert_owned_uc_rejects(monkeypatch, catalog, schema) -> None:
    from iot_twin.services.ownership import assert_owned_uc

    with pytest.raises(NotOwnedError):
        assert_owned_uc(catalog, schema, shared_catalog="shared-catalog")


@pytest.mark.parametrize(
    ("catalog", "schema"),
    [
        ("pharma_twin_pharmaceutical", "raw"),
        ("shared-catalog", "pharma_twin"),
        ("pharma-twin", None),
    ],
)
def test_assert_owned_uc_accepts(catalog, schema) -> None:
    from iot_twin.services.ownership import assert_owned_uc

    assert_owned_uc(catalog, schema, shared_catalog="shared-catalog")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uc",
    [
        {"catalog_pattern": "iot_{name}"},
        {"catalog_pattern": "shared-catalog", "schemas": ["raw"]},
        {"catalog_pattern": "shared-catalog", "schemas": ["pharma_twin", "rtm_lakets_zb"]},
    ],
)
async def test_provision_uc_catalog_rejects_foreign_target(monkeypatch, uc, no_client) -> None:
    from iot_twin.services import provisioning_service as ps

    monkeypatch.setenv("UC_CATALOG", "shared-catalog")
    with pytest.raises(NotOwnedError):
        await ps.provision_uc_catalog(uc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "path", "body"),
    [
        ("provisioning", "/api/provision/uc", {"catalog": "iot_pharmaceutical", "schema": "raw"}),
        ("provisioning", "/api/provision/uc", {"catalog": "shared-catalog", "schema": "raw"}),
        (
            "provisioning",
            "/api/provision/pipeline-catalog",
            {"pipeline_id": "p-1", "catalog_pattern": "iot_{name}"},
        ),
        ("settings", "/api/settings/provision/uc", {"uc": {"catalog_pattern": "iot_{name}"}}),
        ("settings", "/api/settings/provision", {"uc": {"catalog_pattern": "iot_{name}"}}),
    ],
)
async def test_routes_return_403_for_foreign_uc(monkeypatch, module, path, body, no_client) -> None:
    import importlib

    monkeypatch.setenv("UC_CATALOG", "shared-catalog")
    router = importlib.import_module(f"iot_twin.routes.{module}").router
    resp = await _post(_app(router), path, body)
    assert resp.status_code == 403, resp.text
    assert "not owned" in resp.json()["error"] or "shared catalog" in resp.json()["error"]


# --- Direct-mode Lakebase -------------------------------------------------------------------
DIRECT = {"mode": "direct", "host": "pg.example", "user": "u", "password": fake_credential("pg")}


@pytest.mark.asyncio
async def test_direct_mode_disabled_by_default(monkeypatch, no_client) -> None:
    from iot_twin.routes import lakets
    from iot_twin.services import ownership
    from iot_twin.services import provisioning_service as ps

    monkeypatch.delenv(ownership.ALLOW_DIRECT_LAKEBASE_ENV, raising=False)
    lb = {**DIRECT, "project_id": "pharma-twin-p"}
    with pytest.raises(NotOwnedError, match="disabled"):
        await ps.provision_lakebase_table(lb)
    with patch.object(lakets.asyncpg, "connect") as connect, pytest.raises(NotOwnedError):
        await lakets._open_lakebase_conn(lb)
    connect.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lb",
    [
        {**DIRECT},  # no project to resolve the host from
        {**DIRECT, "project_id": "iot-pipelines"},  # foreign project
        {**DIRECT, "project_id": "pharma-twin-p", "host": "upstream.example"},  # host mismatch
    ],
)
async def test_direct_mode_requires_host_of_owned_endpoint(monkeypatch, lb) -> None:
    from iot_twin.services import ownership

    monkeypatch.setenv(ownership.ALLOW_DIRECT_LAKEBASE_ENV, "true")
    ws = MagicMock()
    ws.postgres.list_endpoints.return_value = [MagicMock(name="ep")]
    ws.postgres.get_endpoint.return_value.status.hosts.host = "pg.example"
    with pytest.raises(NotOwnedError):
        ownership.assert_direct_lakebase(lb, ws=ws)


def test_direct_mode_accepts_host_of_owned_endpoint(monkeypatch) -> None:
    from iot_twin.services import ownership

    monkeypatch.setenv(ownership.ALLOW_DIRECT_LAKEBASE_ENV, "true")
    ws = MagicMock()
    ws.postgres.list_endpoints.return_value = [MagicMock()]
    ws.postgres.get_endpoint.return_value.status.hosts.host = "pg.example"
    ownership.assert_direct_lakebase({**DIRECT, "project_id": "pharma-twin-p"}, ws=ws)
    ws.postgres.list_endpoints.assert_called_once_with(
        parent="projects/pharma-twin-p/branches/production"
    )


def test_internal_direct_bypasses_flag_but_not_host_check(monkeypatch) -> None:
    from iot_twin.services import ownership

    monkeypatch.delenv(ownership.ALLOW_DIRECT_LAKEBASE_ENV, raising=False)
    ws = MagicMock()
    ws.postgres.list_endpoints.return_value = [MagicMock()]
    ws.postgres.get_endpoint.return_value.status.hosts.host = "pg.example"
    ownership.assert_direct_lakebase(
        {**DIRECT, "project_id": "pharma-twin-p"}, internal=True, ws=ws
    )
    with pytest.raises(NotOwnedError):
        ownership.assert_direct_lakebase(
            {**DIRECT, "project_id": "pharma-twin-p", "host": "other"}, internal=True, ws=ws
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "path", "body"),
    [
        ("provisioning", "/api/provision/lakebase-table", {**DIRECT, "table_name": "t"}),
        ("settings", "/api/settings/provision/lakebase", {"lakebase": DIRECT}),
        ("settings", "/api/settings/provision", {"lakebase": DIRECT}),
    ],
)
async def test_routes_reject_direct_mode_by_default(monkeypatch, module, path, body, no_client):
    import importlib

    from iot_twin.services import ownership

    monkeypatch.delenv(ownership.ALLOW_DIRECT_LAKEBASE_ENV, raising=False)
    router = importlib.import_module(f"iot_twin.routes.{module}").router
    resp = await _post(_app(router), path, body)
    assert resp.status_code == 403, resp.text
    assert "direct-mode" in resp.json()["error"]


# --- Admin gate --------------------------------------------------------------------------------
ADMIN_ROUTES = [
    ("settings", "/api/settings/provision"),
    ("settings", "/api/settings/provision/uc"),
    ("settings", "/api/settings/provision/secrets"),
    ("settings", "/api/settings/provision/lakebase"),
    ("provisioning", "/api/provision/uc"),
    ("provisioning", "/api/provision/pipeline-catalog"),
    ("provisioning", "/api/provision/lakebase-table"),
    ("provisioning", "/api/pipelines/p-1/secrets"),
    ("lakets", "/api/lakets/sync/check"),
    ("lakets", "/api/lakets/retention/deploy"),
    ("setup", "/api/setup/pipeline-lakebase"),
]


@pytest.mark.non_admin
@pytest.mark.asyncio
@pytest.mark.parametrize(("module", "path"), ADMIN_ROUTES)
async def test_write_routes_require_admin(module, path, no_client) -> None:
    import importlib

    router = importlib.import_module(f"iot_twin.routes.{module}").router
    resp = await _post(_app(router), path, {"secrets": {"scope": "pharma-twin-secrets"}})
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"] == "admin access required"


def test_admin_gate_uses_app_permissions() -> None:
    """The gate defers to the industry admin check (ADMIN_EMAILS / ADMIN_GROUP / owner)."""
    import inspect

    from iot_twin.routes import _admin

    assert "app_permissions.is_admin(request)" in inspect.getsource(_admin.require_admin)


# --- Misconfiguration is a 500 -------------------------------------------------------------------
def test_env_misconfiguration_maps_to_500(monkeypatch) -> None:
    from iot_twin.core.config import IsolationMisconfiguredError, owned_env
    from iot_twin.routes._ownership import not_owned_response

    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", "iot-pipelines")
    with pytest.raises(IsolationMisconfiguredError) as info:
        owned_env("PIPELINES_LAKEBASE_PROJECT")
    resp = not_owned_response(info.value)
    assert resp.status_code == 500
    assert b"isolation misconfiguration" in resp.body
    assert not_owned_response(NotOwnedError("request name")).status_code == 403


# --- Teardown -----------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_teardown_refuses_foreign_ledger_before_any_deleter(monkeypatch) -> None:
    from iot_twin.services import resource_ledger, teardown_service

    monkeypatch.setenv("UC_CATALOG", "shared-catalog")
    rows = [
        {
            "id": "r1",
            "resource_type": "job",
            "identifier": "1",
            "metadata": {},
            "teardown_order": 90,
        },
        {
            "id": "r2",
            "resource_type": "lakebase_branch",
            "identifier": "projects/iot-pipelines/branches/pharmaceutical",
            "metadata": {},
            "teardown_order": 20,
        },
    ]
    deleter = AsyncMock()
    with (
        patch.object(resource_ledger, "resources_in_teardown_order", AsyncMock(return_value=rows)),
        pytest.raises(NotOwnedError),
    ):
        await teardown_service.teardown_setup(
            MagicMock(), "setup-1", {"job": deleter, "lakebase_branch": deleter}
        )
    deleter.assert_not_awaited()


# =====================================================================================
# Round 4: workspace objects addressed by numeric / opaque id
# =====================================================================================
from types import SimpleNamespace  # noqa: E402

from tests._fakes import owned_cluster_info, owned_job_settings  # noqa: E402

UPSTREAM_JOB = SimpleNamespace(
    settings=SimpleNamespace(name="iot-pharmaceutical-realtime", tags={"app": "iot-digital-twin"})
)
FOREIGN_JOBS = [
    UPSTREAM_JOB,
    SimpleNamespace(settings=SimpleNamespace(name="pharma-twin-x", tags={})),  # no app tag
    SimpleNamespace(settings=SimpleNamespace(name="pharma-twin-x", tags={"app": "other"})),
    SimpleNamespace(settings=SimpleNamespace(name="other-job", tags={"app": "pharma-twin"})),
    SimpleNamespace(settings=None),
]
FOREIGN_CLUSTERS = [
    SimpleNamespace(cluster_name="iot-validate-pharmaceutical", custom_tags={"app": "iot"}),
    SimpleNamespace(cluster_name="pharma-twin-x", custom_tags=None),
    SimpleNamespace(cluster_name="shared-cluster", custom_tags={"app": "pharma-twin"}),
]


def _ws_with(job=None, cluster=None, *, job_error=None):
    w = MagicMock()
    if job_error is not None:
        w.jobs.get.side_effect = job_error
    else:
        w.jobs.get.return_value = job
    w.clusters.get.return_value = cluster
    return w


def _assert_no_mutation(w) -> None:
    for call in (
        w.jobs.cancel_all_runs,
        w.jobs.update,
        w.jobs.reset,
        w.jobs.delete,
        w.jobs.run_now,
        w.jobs.cancel_run,
        w.clusters.start,
        w.clusters.delete,
        w.clusters.permanent_delete,
        w.workspace.import_,
        w.workspace.mkdirs,
        w.workspace.delete,
        w.command_execution.create,
    ):
        call.assert_not_called()


def test_owned_job_and_cluster_accepted() -> None:
    from iot_twin.services.ownership import assert_owned_cluster, assert_owned_job

    w = _ws_with(owned_job_settings(), owned_cluster_info())
    assert assert_owned_job(w, "42") == 42
    assert assert_owned_cluster(w, "0101-abc") == "0101-abc"


def test_owned_lakets_job_display_name_accepted() -> None:
    from iot_twin.services.lakets import lakets_deploy_service as lds
    from iot_twin.services.ownership import assert_owned_job

    job = SimpleNamespace(
        settings=SimpleNamespace(
            name=lds._job_display_name("pharma-twin-lakets-demo", "production"),
            tags={"app": "pharma-twin", "component": "lakets-retention"},
        )
    )
    assert assert_owned_job(_ws_with(job), 7) == 7


@pytest.mark.parametrize("job", FOREIGN_JOBS)
def test_foreign_jobs_rejected(job) -> None:
    from iot_twin.services.ownership import assert_owned_job

    with pytest.raises(NotOwnedError):
        assert_owned_job(_ws_with(job), 42)


@pytest.mark.parametrize("bad", ["", "abc", None, "1; drop"])
def test_invalid_job_ids_rejected(bad) -> None:
    from iot_twin.services.ownership import assert_owned_job

    w = _ws_with(owned_job_settings())
    with pytest.raises(NotOwnedError):
        assert_owned_job(w, bad)
    w.jobs.get.assert_not_called()


def test_job_lookup_failure_fails_closed() -> None:
    from iot_twin.services.ownership import assert_owned_job, job_lookup_missing

    with pytest.raises(NotOwnedError) as info:
        assert_owned_job(_ws_with(job_error=RuntimeError("boom")), 42)
    assert not job_lookup_missing(info.value)
    with pytest.raises(NotOwnedError) as info:
        assert_owned_job(_ws_with(job_error=RuntimeError("RESOURCE_DOES_NOT_EXIST: not found")), 42)
    assert job_lookup_missing(info.value)


@pytest.mark.parametrize("cluster", FOREIGN_CLUSTERS)
def test_foreign_clusters_rejected(cluster) -> None:
    from iot_twin.services.ownership import assert_owned_cluster

    with pytest.raises(NotOwnedError):
        assert_owned_cluster(_ws_with(cluster=cluster), "0101-abc")


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["stop", "restart"])
async def test_job_routes_reject_foreign_job(action) -> None:
    from iot_twin.routes import jobs

    w = _ws_with(UPSTREAM_JOB)
    with patch("databricks.sdk.WorkspaceClient", return_value=w):
        resp = await _post(_app(jobs.router), f"/api/job/123/{action}", {})
    assert resp.status_code == 403, resp.text
    _assert_no_mutation(w)


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["body", "stored"])
async def test_deploy_job_rejects_foreign_job_before_any_write(source) -> None:
    import json

    from iot_twin.routes import deployment

    w = _ws_with(UPSTREAM_JOB)
    app = _app(deployment.router)
    body = {
        "notebook": "print(1)",
        "cluster_config": {"spark_version": "18.2.x", "worker_node_type": "i3.xlarge"},
    }
    if source == "body":
        body["job_id"] = 123
    else:
        body["pipeline_id"] = "00000000-0000-0000-0000-000000000001"
        state = app.state.app_state
        state.db_available = True
        state.db = MagicMock()
        state.db.fetchrow = AsyncMock(return_value={"resources": json.dumps({"job_id": 123})})
    with patch("databricks.sdk.WorkspaceClient", return_value=w):
        resp = await _post(app, "/api/cluster/deploy-job", body)
    assert resp.status_code == 403, resp.text
    _assert_no_mutation(w)
    w.jobs.create.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "path", "body"),
    [
        ("cluster", "/api/cluster/0101-abc/start", {}),
        ("deployment", "/api/cluster/0101-abc/validate", {"notebook": "print(1)"}),
    ],
)
async def test_cluster_routes_reject_foreign_cluster(module, path, body) -> None:
    import importlib

    router = importlib.import_module(f"iot_twin.routes.{module}").router
    w = _ws_with(cluster=FOREIGN_CLUSTERS[0])
    with (
        patch("databricks.sdk.WorkspaceClient", return_value=w),
        patch("iot_twin.routes.deployment.WorkspaceClient", return_value=w),
    ):
        resp = await _post(_app(router), path, body)
    assert resp.status_code == 403, resp.text
    _assert_no_mutation(w)


@pytest.mark.asyncio
async def test_cluster_provision_rejects_foreign_name_and_tags_owned() -> None:
    from iot_twin.routes import cluster

    w = MagicMock()
    with (
        patch("databricks.sdk.WorkspaceClient", return_value=w),
        patch.object(cluster, "detect_cloud", return_value="aws"),
    ):
        resp = await _post(
            _app(cluster.router),
            "/api/cluster/provision",
            {"cluster_name": "iot-digital-twin-1", "worker_node_type": "i3", "spark_version": "18"},
        )
        assert resp.status_code == 403, resp.text
        w.clusters.create.assert_not_called()
        resp = await _post(
            _app(cluster.router),
            "/api/cluster/provision",
            {"worker_node_type": "i3.xlarge", "spark_version": "18.2.x"},
        )
    assert w.clusters.create.call_args.kwargs["custom_tags"] == {"app": "pharma-twin"}


def test_validation_cluster_spec_carries_app_tag() -> None:
    from iot_twin.services.validation import validation_config as vc

    spec = vc.build_validation_cluster_spec(
        node_type_id="i3.xlarge", num_workers=1, single_user_name="u", spark_version="18.2"
    )
    assert spec["custom_tags"] == {"app": "pharma-twin"}


def test_validation_cluster_terminate_skips_foreign_cluster() -> None:
    from iot_twin.services.validation import validation_steps

    w = _ws_with(cluster=FOREIGN_CLUSTERS[0])
    validation_steps._terminate_cluster_sync(w, "0101-abc")
    w.clusters.delete.assert_not_called()


@pytest.mark.asyncio
async def test_pipeline_put_cannot_write_resources() -> None:
    from iot_twin.routes import pipelines

    app = _app(pipelines.router)
    state = app.state.app_state
    state.db_available = True
    state.db = MagicMock()
    state.db.fetchrow = AsyncMock(return_value={"id": "p", "name": "n", "user_email": "u"})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.put("/api/pipelines/p-1", json={"resources": {"job_id": 123}, "status": "x"})
    updates = [c.args[0] for c in state.db.fetchrow.await_args_list if "UPDATE" in c.args[0]]
    assert updates, "the PUT must reach its UPDATE for this test to mean anything"
    for query in updates:
        assert "resources" not in query, query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "resources"),
    [
        ("delete", "/api/pipelines/00000000-0000-0000-0000-000000000001", {"job_id": "123"}),
        ("delete", "/api/pipelines/00000000-0000-0000-0000-000000000001", {"cluster_id": "c-1"}),
        ("post", "/api/pipelines/00000000-0000-0000-0000-000000000001/reset", {"job_id": "123"}),
    ],
)
async def test_pipeline_delete_and_reset_reject_planted_foreign_ids(method, path, resources):
    import json

    from iot_twin.routes import pipelines

    app = _app(pipelines.router)
    state = app.state.app_state
    state.db_available = True
    state.db = MagicMock()
    state.db.fetchrow = AsyncMock(return_value={"id": "p", "resources": json.dumps(resources)})
    state.db.execute = AsyncMock()
    w = _ws_with(UPSTREAM_JOB, FOREIGN_CLUSTERS[0])
    with patch.object(pipelines, "WorkspaceClient", return_value=w):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await getattr(client, method)(path)
    assert resp.status_code == 403, resp.text
    _assert_no_mutation(w)
    state.db.execute.assert_not_awaited()


def test_stop_pipeline_refuses_foreign_job() -> None:
    from iot_twin.services.deployment import deployment_service as ds

    w = _ws_with(UPSTREAM_JOB)
    with pytest.raises(NotOwnedError):
        ds.stop_pipeline(123, w=w)
    with pytest.raises(NotOwnedError):
        ds.start_pipeline(123, w=w)
    _assert_no_mutation(w)


def test_industry_stop_job_refuses_foreign_job() -> None:
    from iot_twin.services.industry import industry_service

    w = _ws_with(UPSTREAM_JOB)
    result = industry_service._stop_job("123", SimpleNamespace(workspace_client=w))
    assert result["status"] == "failed"
    _assert_no_mutation(w)


@pytest.mark.asyncio
async def test_connectivity_status_never_deletes_foreign_notebook() -> None:
    from iot_twin.routes import deployment

    w = MagicMock()
    w.current_user.me.return_value = SimpleNamespace(user_name="app-sp")
    task = SimpleNamespace(run_id=None, notebook_task=SimpleNamespace(notebook_path="/Users/x/nb"))
    w.jobs.get_run.return_value = SimpleNamespace(
        state=SimpleNamespace(
            life_cycle_state=SimpleNamespace(value="TERMINATED"),
            result_state=SimpleNamespace(value="SUCCESS"),
            state_message="",
        ),
        tasks=[task],
    )
    with patch("databricks.sdk.WorkspaceClient", return_value=w):
        async with AsyncClient(
            transport=ASGITransport(app=_app(deployment.router)), base_url="http://test"
        ) as client:
            resp = await client.get("/api/test-connectivity/5/status")
    assert resp.status_code == 200, resp.text
    w.workspace.delete.assert_not_called()
    task.notebook_task.notebook_path = "/Workspace/Users/app-sp/.pharma-twin/connectivity_test_1"
    with patch("databricks.sdk.WorkspaceClient", return_value=w):
        async with AsyncClient(
            transport=ASGITransport(app=_app(deployment.router)), base_url="http://test"
        ) as client:
            await client.get("/api/test-connectivity/5/status")
    w.workspace.delete.assert_called_once_with(
        "/Workspace/Users/app-sp/.pharma-twin/connectivity_test_1"
    )


ID_ADMIN_ROUTES = [
    ("jobs", "/api/job/1/stop"),
    ("jobs", "/api/job/1/restart"),
    ("deployment", "/api/cluster/deploy-job"),
    ("deployment", "/api/cluster/c-1/validate"),
    ("deployment", "/api/test-connectivity/submit"),
    ("cluster", "/api/cluster/provision"),
    ("cluster", "/api/cluster/c-1/start"),
    ("warehouses", "/api/warehouses"),
]


@pytest.mark.non_admin
@pytest.mark.asyncio
@pytest.mark.parametrize(("module", "path"), ID_ADMIN_ROUTES)
async def test_id_routes_require_admin(module, path, no_client) -> None:
    import importlib

    router = importlib.import_module(f"iot_twin.routes.{module}").router
    resp = await _post(_app(router), path, {})
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"] == "admin access required"


# --- verified caller identity -------------------------------------------------------------
def _request(headers: dict) -> SimpleNamespace:
    return SimpleNamespace(headers=headers, state=SimpleNamespace())


def test_caller_email_never_trusts_an_unverified_jwt() -> None:
    import base64
    import json

    from iot_twin.services import app_permissions as ap

    claims = base64.urlsafe_b64encode(json.dumps({"email": "admin@example.com"}).encode())
    forged = "x." + claims.decode().rstrip("=") + ".sig"
    req = _request({"x-forwarded-access-token": forged, "x-forwarded-email": "admin@example.com"})
    with (
        patch("databricks.sdk.WorkspaceClient") as ws,
        patch("databricks.sdk.config.Config"),
    ):
        ws.return_value.current_user.me.side_effect = RuntimeError("invalid token")
        assert ap._caller_email(req) is None
        assert ap._user_groups(req) == set()
    assert ws.call_count == 1  # one verified lookup per request, cached


def test_caller_email_and_groups_come_from_verified_me() -> None:
    from iot_twin.services import app_permissions as ap

    me = SimpleNamespace(user_name="real@example.com", groups=[SimpleNamespace(display="g1")])
    req = _request(
        {
            "x-forwarded-access-token": fake_credential("tok"),
            "x-forwarded-email": "spoof@example.com",
        }
    )
    with (
        patch("databricks.sdk.WorkspaceClient") as ws,
        patch("databricks.sdk.config.Config"),
    ):
        ws.return_value.current_user.me.return_value = me
        assert ap._caller_email(req) == "real@example.com"
        assert ap._user_groups(req) == {"g1"}
    assert ws.call_count == 1
