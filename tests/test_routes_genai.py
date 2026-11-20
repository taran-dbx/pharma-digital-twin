"""AI action-plan routes: authz, validation, 404s, mocked endpoint, apply link [qa]."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
import pytest_asyncio
from databricks.sdk import errors as sdk_errors
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes.alerts import router as alerts_router
from iot_twin.routes.genai import router as genai_router
from iot_twin.services.alerts.alert_engine import AlertEngine
from iot_twin.services.genai import fm_client
from iot_twin.services.genai.grounding import GroundingItem, GroundingSet
from iot_twin.services.genai.prompt import UNTRUSTED_TAG
from tests._genai_fixtures import fake_client, make_alert, plan_text, reply

ADMIN = "iot_twin.routes.genai.app_permissions.is_admin"
ALERTS_ADMIN = "iot_twin.routes.alerts.app_permissions.is_admin"


def grounded_answer(**kwargs):
    """Answer from the rows actually sent, as a well-behaved model would."""
    user = kwargs["messages"][1].content
    body = user.split(f"[{UNTRUSTED_TAG}]\n", 1)[1].split(f"\n[/{UNTRUSTED_TAG}]", 1)[0]
    rows = json.loads(body.split("\n", 1)[1])
    items = tuple(GroundingItem(r["id"], r["source"], r["key"], r["data"]) for r in rows)
    return reply(plan_text(GroundingSet(alert_id="x", items=items)))


class ModelServing:
    def __init__(self, outcome=None) -> None:
        self.outcome = outcome

    def query(self, **kwargs):
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        if isinstance(self.outcome, str):
            return reply(self.outcome)
        return grounded_answer(**kwargs)


@pytest_asyncio.fixture()
async def client() -> AsyncGenerator[tuple[AsyncClient, AppState, str], None]:
    state = AppState()
    state.alert_engine = AlertEngine()
    alert = make_alert(opened=datetime.now(UTC) - timedelta(seconds=50))
    state.alert_engine._store(alert)
    app = FastAPI()
    app.state.app_state = state
    app.include_router(alerts_router)
    app.include_router(genai_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        yield http, state, alert.id


def _model(outcome=None):
    serving = ModelServing(outcome)
    return patch.object(fm_client, "WorkspaceClient", return_value=fake_client_with(serving))


def fake_client_with(serving):
    holder = fake_client(None)
    holder.serving_endpoints = serving
    return holder


@pytest.mark.asyncio
class TestCreate:
    async def test_non_admin_is_forbidden(self, client) -> None:
        http, _state, alert_id = client
        with patch(ADMIN, return_value=False):
            resp = await http.post(f"/api/alerts/{alert_id}/action-plan")
        assert resp.status_code == 403

    @pytest.mark.parametrize(
        ("path", "body", "detail"),
        [
            ("not-a-uuid", None, "alert_id must be a UUID"),
            (None, {"tables": ["x"]}, "unknown field(s): tables"),
            (None, {"mode": "batch"}, "mode must be one of"),
            (None, ["realtime"], "body must be a JSON object"),
        ],
    )
    async def test_input_validation(self, client, path, body, detail) -> None:
        http, _state, alert_id = client
        with patch(ADMIN, return_value=True):
            resp = await http.post(
                f"/api/alerts/{path or alert_id}/action-plan",
                content=json.dumps(body) if body is not None else None,
            )
        assert resp.status_code == 422
        assert detail in resp.json()["detail"]

    async def test_unknown_alert_is_404(self, client) -> None:
        http, _state, _alert_id = client
        with patch(ADMIN, return_value=True):
            resp = await http.post(f"/api/alerts/{uuid.uuid4()}/action-plan")
        assert resp.status_code == 404

    async def test_generate_list_and_trace(self, client) -> None:
        http, state, alert_id = client
        with patch(ADMIN, return_value=True), _model():
            resp = await http.post(
                f"/api/alerts/{alert_id}/action-plan", json={"mode": "microbatch"}
            )
        assert resp.status_code == 201, resp.text
        plan = resp.json()
        assert plan["grounding_check"] == "pass", plan["grounding_reasons"]
        assert plan["status"] == "generated" and plan["advisory"] is True
        assert plan["endpoint"] == "databricks-claude-sonnet-5-5"
        assert f"alert:{alert_id}" in plan["citation_ids"]
        assert "grounding_rows" not in plan

        listed = (await http.get(f"/api/alerts/{alert_id}/action-plan")).json()
        assert [p["plan_id"] for p in listed["plans"]] == [plan["plan_id"]]
        trace = (await http.get(f"/api/genai/audit/{plan['plan_id']}")).json()
        assert [row["id"] for row in trace["grounding_rows"]] == plan["citation_ids"]
        assert trace["response"]["plan"] == plan["plan"]
        # Advisory: the alert and its decisions are untouched.
        assert state.alert_engine.get_alert_sync(alert_id).status == "open"
        assert (await http.get(f"/api/alerts/{alert_id}/responses")).json()["responses"] == []

    async def test_endpoint_timeout_is_504_and_audited(self, client, monkeypatch) -> None:
        http, _state, alert_id = client
        monkeypatch.setenv(fm_client.TIMEOUT_ENV, "0.01")
        monkeypatch.setenv(fm_client.RETRIES_ENV, "0")
        with patch(ADMIN, return_value=True), _model(TimeoutError()):
            resp = await http.post(f"/api/alerts/{alert_id}/action-plan")
        assert resp.status_code == 504
        body = resp.json()
        assert body["plan"]["status"] == "error" and body["plan"]["grounding_check"] == "fail"
        trace = await http.get(f"/api/genai/audit/{body['plan']['plan_id']}")
        assert trace.status_code == 200

    async def test_endpoint_permission_error_is_502(self, client, monkeypatch) -> None:
        http, _state, alert_id = client
        with patch(ADMIN, return_value=True), _model(sdk_errors.PermissionDenied("no")):
            resp = await http.post(f"/api/alerts/{alert_id}/action-plan")
        assert resp.status_code == 502

    async def test_malformed_answer_is_logged_as_rejected(self, client) -> None:
        http, _state, alert_id = client
        with patch(ADMIN, return_value=True), _model("I cannot help with that."):
            resp = await http.post(f"/api/alerts/{alert_id}/action-plan")
        assert resp.status_code == 201
        assert resp.json()["status"] == "rejected"
        assert resp.json()["plan"] is None


@pytest.mark.asyncio
class TestReadAndApply:
    async def test_list_unknown_alert_and_trace_validation(self, client) -> None:
        http, _state, _alert_id = client
        assert (await http.get(f"/api/alerts/{uuid.uuid4()}/action-plan")).status_code == 404
        assert (await http.get("/api/genai/audit/nope")).status_code == 422
        assert (await http.get(f"/api/genai/audit/{uuid.uuid4()}")).status_code == 404

    async def test_signing_with_the_plan_marks_it_applied(self, client) -> None:
        http, _state, alert_id = client
        with patch(ADMIN, return_value=True), _model():
            plan = (await http.post(f"/api/alerts/{alert_id}/action-plan")).json()
        sign = {
            "action": "quarantine_lot",
            "reason": f"Per AI action plan {plan['plan_id'][:8]}",
            "signature_meaning": "approved",
            "action_plan_id": plan["plan_id"],
        }
        with patch(ALERTS_ADMIN, return_value=True):
            resp = await http.post(f"/api/alerts/{alert_id}/responses", json=sign)
        assert resp.status_code == 200, resp.text
        applied = resp.json()["action_plan"]
        assert (
            applied["applied_by"]
            and applied["applied_response_id"] == resp.json()["response"]["id"]
        )
        trace = (await http.get(f"/api/genai/audit/{plan['plan_id']}")).json()
        assert trace["applied_at"] is not None

    @pytest.mark.parametrize("plan_id", ["nope", str(uuid.uuid4())])
    async def test_foreign_or_bad_plan_ids_are_rejected(self, client, plan_id) -> None:
        http, _state, alert_id = client
        sign = {
            "action": "open_ncr",
            "reason": "excursion",
            "signature_meaning": "approved",
            "action_plan_id": plan_id,
        }
        with patch(ALERTS_ADMIN, return_value=True):
            resp = await http.post(f"/api/alerts/{alert_id}/responses", json=sign)
        assert resp.status_code == 422
