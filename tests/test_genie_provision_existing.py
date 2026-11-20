"""POST /api/admin/pipelines/{setup_id}/genie/{mode} — Genie space for an
already-deployed pipeline, without touching job/stream/Kafka/Lakebase."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import pipeline_deploy
from iot_twin.services.deployment import deployment_service
from iot_twin.services.genie import genie_provisioning
from iot_twin.services.genie import genie_space_service as gss

SETUP_ID = "11111111-1111-4111-8111-111111111111"
PHARMA_ID = "22222222-2222-4222-8222-222222222222"
DEP_ID = "33333333-3333-4333-8333-333333333333"
URL = f"/api/admin/pipelines/{SETUP_ID}/genie/realtime"

SETUPS = {
    SETUP_ID: {"id": SETUP_ID, "industry": "manufacturing", "warehouse_id": "wh-1"},
    PHARMA_ID: {"id": PHARMA_ID, "industry": "retail"},
}
DEPLOYED = {
    "id": DEP_ID,
    "setup_id": SETUP_ID,
    "mode": "realtime",
    "status": "deployed",
    "job_id": "4242",
}


class _Ledger:
    """In-memory provisioned_resource rows so create→update is real."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    async def record(self, _db, **values):
        row = {"id": f"r{len(self.rows)}", "status": "active", **values}
        self.rows = [*self.rows, row]
        return row

    async def list(self, _db, _setup_id):
        return list(self.rows)


@pytest.fixture()
def env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    monkeypatch.delenv(gss.PARENT_PATH_ENV, raising=False)
    deployments = {("realtime", SETUP_ID): dict(DEPLOYED)}

    async def _get_setup(_db, sid):
        return SETUPS.get(str(sid))

    async def _get_deployment(_db, did):
        return next((d for d in deployments.values() if d["id"] == did), None)

    async def _by_mode(_db, sid, mode):
        return deployments.get((mode, str(sid)))

    ledger = _Ledger()
    monkeypatch.setattr(gss.pipeline_setup_repo, "get_setup", _get_setup)
    monkeypatch.setattr(genie_provisioning.deployment_repo, "get_deployment", _get_deployment)
    monkeypatch.setattr(genie_provisioning.deployment_repo, "get_deployment_by_mode", _by_mode)
    monkeypatch.setattr(gss.resource_ledger, "record_resource", ledger.record)
    monkeypatch.setattr(gss.resource_ledger, "list_resources", ledger.list)
    monkeypatch.setattr(gss.resource_ledger, "mark_resource_status", AsyncMock())

    ws = MagicMock()
    ws.tables.exists.return_value = SimpleNamespace(table_exists=True)
    ws.genie.create_space.return_value = SimpleNamespace(space_id="sp-new")
    monkeypatch.setattr(gss, "_new_ws", lambda: ws)

    # Every pipeline-runtime entry point must stay untouched.
    forbidden = {
        name: MagicMock(side_effect=AssertionError(f"{name} must not be called"))
        for name in (
            "deploy_pipeline",
            "start_pipeline",
            "stop_pipeline",
            "start_deployment",
            "sync_runtime_oauth_secret",
            "_create_or_reset_continuous_job",
            "_create_or_reset_serverless_job",
            "_upload_notebook",
        )
    }
    for name, mock in forbidden.items():
        monkeypatch.setattr(deployment_service, name, mock)
    return SimpleNamespace(ws=ws, ledger=ledger, deployments=deployments, forbidden=forbidden)


def _client(admin: bool = True) -> tuple[AsyncClient, patch]:
    state = AppState()
    state.app_backend_db = MagicMock()
    state.app_backend_available = True
    app = FastAPI()
    app.state.app_state = state
    app.include_router(pipeline_deploy.router)
    guard = patch.object(pipeline_deploy.app_permissions, "is_admin", MagicMock(return_value=admin))
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test"), guard


async def _post(url: str = URL, admin: bool = True):
    client, guard = _client(admin)
    with guard:
        async with client as c:
            return await c.post(url)


@pytest.mark.asyncio
class TestProvisionExisting:
    async def test_creates_space_for_existing_deployment(self, env) -> None:
        resp = await _post()
        assert resp.status_code == 200
        assert resp.json() == {"space_id": "sp-new", "created": True, "warnings": []}
        kwargs = env.ws.genie.create_space.call_args.kwargs
        assert kwargs["warehouse_id"] == "wh-1"
        assert [r["resource_type"] for r in env.ledger.rows] == ["genie_space"]
        assert env.ledger.rows[0]["deployment_id"] == DEP_ID
        assert env.ledger.rows[0]["metadata"]["mode"] == "realtime"
        for mock in env.forbidden.values():
            mock.assert_not_called()
        env.ws.jobs.assert_not_called()
        assert env.ws.jobs.method_calls == []
        assert env.ws.workspace.method_calls == []

    async def test_second_call_updates_idempotently(self, env) -> None:
        await _post()
        resp = await _post()
        assert resp.status_code == 200
        assert resp.json() == {"space_id": "sp-new", "created": False, "warnings": []}
        env.ws.genie.create_space.assert_called_once()
        env.ws.genie.update_space.assert_called_once()
        assert len(env.ledger.rows) == 1

    async def test_accepts_deployment_id(self, env) -> None:
        resp = await _post(f"/api/admin/pipelines/{DEP_ID}/genie/realtime")
        assert resp.status_code == 200
        assert resp.json()["space_id"] == "sp-new"

    async def test_non_admin_forbidden(self, env) -> None:
        resp = await _post(admin=False)
        assert resp.status_code == 403
        env.ws.genie.create_space.assert_not_called()

    @pytest.mark.parametrize("bad", ["setup-1", "abc", "1234"])
    async def test_non_uuid_404(self, env, bad: str) -> None:
        assert (await _post(f"/api/admin/pipelines/{bad}/genie/realtime")).status_code == 404

    async def test_unknown_id_404(self, env) -> None:
        url = "/api/admin/pipelines/44444444-4444-4444-8444-444444444444/genie/realtime"
        assert (await _post(url)).status_code == 404

    async def test_no_deployment_for_mode_404(self, env) -> None:
        resp = await _post(f"/api/admin/pipelines/{SETUP_ID}/genie/zerobus")
        assert resp.status_code == 404
        env.ws.genie.create_space.assert_not_called()

    async def test_deployment_id_of_other_mode_404(self, env) -> None:
        assert (await _post(f"/api/admin/pipelines/{DEP_ID}/genie/zerobus")).status_code == 404

    @pytest.mark.parametrize(
        "patch_fields",
        [
            {"status": "deploying"},
            {"status": "failed"},
            {"job_id": None},
        ],
    )
    async def test_not_deployed_409(self, env, patch_fields: dict) -> None:
        env.deployments[("realtime", SETUP_ID)] = {**DEPLOYED, **patch_fields}
        assert (await _post()).status_code == 409
        env.ws.genie.create_space.assert_not_called()

    async def test_unknown_industry_422(self, env) -> None:
        resp = await _post(f"/api/admin/pipelines/{PHARMA_ID}/genie/realtime")
        assert resp.status_code == 422

    async def test_unknown_mode_422(self, env) -> None:
        assert (await _post(f"/api/admin/pipelines/{SETUP_ID}/genie/batch")).status_code == 422

    async def test_missing_intelligence_tables_409_with_hint(self, env) -> None:
        env.ws.tables.exists.return_value = SimpleNamespace(table_exists=False)
        resp = await _post()
        assert resp.status_code == 409
        assert "/api/admin/intelligence/sync" in resp.json()["detail"]
        env.ws.genie.create_space.assert_not_called()

    async def test_timeout_504(self, env, monkeypatch) -> None:
        import asyncio

        async def _slow(*_a, **_k):
            await asyncio.sleep(5)

        monkeypatch.setattr(gss, "ensure_pipeline_space", _slow)
        monkeypatch.setattr(genie_provisioning, "GENIE_SPACE_TIMEOUT_S", 0.05)
        assert (await _post()).status_code == 504

    async def test_recreated_space_reports_warning(self, env) -> None:
        from databricks.sdk.errors import NotFound

        await _post()
        env.ws.genie.update_space.side_effect = NotFound("gone")
        env.ws.genie.create_space.return_value = SimpleNamespace(space_id="sp-2")
        body = (await _post()).json()
        assert body["space_id"] == "sp-2" and body["created"] is True
        assert body["warnings"] and "recreated" in body["warnings"][0]

    async def test_sdk_failure_502_sanitized(self, env) -> None:
        env.ws.genie.create_space.side_effect = RuntimeError("token=fake-token-for-tests")
        resp = await _post()
        assert resp.status_code == 502
        assert "fake-token-for-tests" not in resp.text
