"""Route tests for the admin validation-flow API (plan Phase 4).

Covers admin gating (403), the app-backend DB gate (503), and endpoint wiring /
status codes.  The service layer is patched (it has its own unit tests) so these
tests isolate the HTTP surface; the background driver is stubbed so no cluster or
Kafka is touched.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.services import app_permissions
from iot_twin.services.validation import validation_service
from iot_twin.services.validation.validation_service import ValidationError

pytestmark = pytest.mark.asyncio

_STATE = {
    "sequence": ["provision_cluster", "done"],
    "steps": {"provision_cluster": {"status": "pending"}, "done": {"status": "pending"}},
}


def _build_app(state: AppState):
    from fastapi import FastAPI

    from iot_twin.routes.validation_admin import router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(router)
    return app


@pytest_asyncio.fixture()
async def client(monkeypatch):
    # Admin by default; DB present; driver + service stubbed.
    monkeypatch.setattr(app_permissions, "is_admin", lambda request: True)
    monkeypatch.setattr(validation_service, "run_machine", AsyncMock(return_value=_STATE))

    state = AppState()
    state.app_backend_db = object()  # truthy pool stand-in
    app = _build_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        c._app_state = state  # type: ignore[attr-defined]
        yield c, monkeypatch


# ---------------------------------------------------------------------------
# Admin gate
# ---------------------------------------------------------------------------
async def test_start_requires_admin(client):
    c, monkeypatch = client
    monkeypatch.setattr(app_permissions, "is_admin", lambda request: False)
    resp = await c.post("/api/admin/industries/pharmaceutical/validate/start")
    assert resp.status_code == 403


async def test_state_requires_admin(client):
    c, monkeypatch = client
    monkeypatch.setattr(app_permissions, "is_admin", lambda request: False)
    resp = await c.get("/api/admin/industries/pharmaceutical/validate/state")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# DB gate
# ---------------------------------------------------------------------------
async def test_503_when_db_unavailable(client):
    c, monkeypatch = client
    c._app_state.app_backend_db = None  # type: ignore[attr-defined]
    resp = await c.get("/api/admin/industries/pharmaceutical/validate/can-deploy")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------
async def test_start_returns_state_and_schedules_run(client):
    c, monkeypatch = client
    monkeypatch.setattr(
        validation_service,
        "prepare_start",
        AsyncMock(
            return_value={"id": "s1", "industry": "pharmaceutical", "validation_state": _STATE}
        ),
    )
    resp = await c.post("/api/admin/industries/pharmaceutical/validate/start")
    assert resp.status_code == 200
    body = resp.json()
    assert body["industry"] == "pharmaceutical"
    assert body["validation_state"] == _STATE


async def test_start_404_when_no_setup(client):
    c, monkeypatch = client
    monkeypatch.setattr(
        validation_service,
        "prepare_start",
        AsyncMock(side_effect=ValidationError("no pipeline setup for 'x'")),
    )
    resp = await c.post("/api/admin/industries/x/validate/start")
    assert resp.status_code == 404


async def test_start_400_when_setup_incomplete(client):
    # An incomplete draft (e.g. no bootstrap_servers / warehouse_id) must return a
    # clean 4xx with a helpful message — never a 500 — so the health-page e2e
    # button can be pressed safely on a draft pipeline.
    c, monkeypatch = client
    from iot_twin.services.validation.validation_service import IncompleteSetupError

    monkeypatch.setattr(
        validation_service,
        "prepare_start",
        AsyncMock(
            side_effect=IncompleteSetupError(
                "complete the source config first — missing: warehouse_id"
            )
        ),
    )
    resp = await c.post("/api/admin/industries/automotive/validate/start")
    assert resp.status_code == 400
    assert "complete the source config first" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------
async def test_state_returns_persisted(client):
    c, monkeypatch = client
    monkeypatch.setattr(validation_service, "get_state", AsyncMock(return_value=_STATE))
    resp = await c.get("/api/admin/industries/pharmaceutical/validate/state")
    assert resp.status_code == 200
    assert resp.json()["validation_state"] == _STATE


async def test_state_404_when_no_setup(client):
    c, monkeypatch = client
    monkeypatch.setattr(validation_service, "get_state", AsyncMock(return_value=None))
    from iot_twin.services.pipelines import pipeline_setup_repo

    monkeypatch.setattr(pipeline_setup_repo, "get_setup_by_industry", AsyncMock(return_value=None))
    resp = await c.get("/api/admin/industries/ghost/validate/state")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# retry (namespaced step key with a colon)
# ---------------------------------------------------------------------------
async def test_retry_accepts_namespaced_step(client):
    c, monkeypatch = client
    monkeypatch.setattr(
        validation_service,
        "prepare_retry",
        AsyncMock(
            return_value={"id": "s1", "industry": "pharmaceutical", "validation_state": _STATE}
        ),
    )
    resp = await c.post("/api/admin/industries/pharmaceutical/validate/retry/realtime:cdf_verify")
    assert resp.status_code == 200
    assert resp.json()["retried"] == "realtime:cdf_verify"


async def test_retry_400_on_unknown_step(client):
    c, monkeypatch = client
    monkeypatch.setattr(
        validation_service,
        "prepare_retry",
        AsyncMock(side_effect=ValidationError("unknown validation step: 'nope'")),
    )
    resp = await c.post("/api/admin/industries/pharmaceutical/validate/retry/nope")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# cdf-enabled
# ---------------------------------------------------------------------------
async def test_cdf_enabled_defaults_realtime(client):
    c, monkeypatch = client
    captured = {}

    async def _prep(db, industry, mode="realtime"):
        captured["mode"] = mode
        return {"id": "s1", "industry": industry, "validation_state": _STATE}

    monkeypatch.setattr(validation_service, "prepare_cdf_enabled", _prep)
    resp = await c.post("/api/admin/industries/pharmaceutical/validate/cdf-enabled")
    assert resp.status_code == 200
    assert captured["mode"] == "realtime"
    assert resp.json()["mode"] == "realtime"


# ---------------------------------------------------------------------------
# can-deploy
# ---------------------------------------------------------------------------
async def test_can_deploy_returns_gate(client):
    c, monkeypatch = client
    monkeypatch.setattr(
        validation_service,
        "can_deploy",
        AsyncMock(
            return_value={
                "can_deploy": True,
                "data_purged": True,
                "outstanding": [],
                "modes": {
                    "microbatch": {"can_deploy": True, "data_purged": True, "outstanding": []},
                },
            }
        ),
    )
    resp = await c.get("/api/admin/industries/pharmaceutical/validate/can-deploy")
    assert resp.status_code == 200
    body = resp.json()
    assert body["can_deploy"] is True
    assert body["outstanding"] == []
    # Per-mode verdicts are surfaced so the frontend can gate Deploy per lane.
    assert body["modes"]["microbatch"]["can_deploy"] is True
