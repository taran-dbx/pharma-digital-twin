"""Per-pipeline manufacturing Genie routes — auth, ownership, 404s, result caps."""

from __future__ import annotations

import base64
import json
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from databricks.sdk.errors import NotFound
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import genie_pipelines
from iot_twin.routes.genie import router as genie_router
from iot_twin.services import genie_service
from iot_twin.services.genie import genie_space_service as gss

BASE = "/api/genie/pipelines/11111111-1111-4111-8111-111111111111/realtime"
SETUPS = {
    "11111111-1111-4111-8111-111111111111": {
        "id": "11111111-1111-4111-8111-111111111111",
        "industry": "manufacturing",
    },
    "22222222-2222-4222-8222-222222222222": {
        "id": "22222222-2222-4222-8222-222222222222",
        "industry": "retail",  # not an offered industry
    },
}
DEPLOYMENTS = {
    "33333333-3333-4333-8333-333333333333": {
        "id": "33333333-3333-4333-8333-333333333333",
        "setup_id": "11111111-1111-4111-8111-111111111111",
        "mode": "realtime",
    },
}
DEP_RT = "33333333-3333-4333-8333-333333333333"
SPACES = {
    ("11111111-1111-4111-8111-111111111111", "realtime"): "sp-rt",
    ("11111111-1111-4111-8111-111111111111", "zerobus"): "sp-zb",
}


def _token(user: str) -> dict[str, str]:
    payload = base64.urlsafe_b64encode(json.dumps({"sub": user}).encode()).decode().rstrip("=")
    return {"x-forwarded-access-token": f"h.{payload}.s"}


ALICE = _token("alice@example.com")
BOB = _token("bob@example.com")


def _genie_client(mode: str = "realtime") -> MagicMock:
    client = MagicMock()
    client.genie.get_space.return_value = SimpleNamespace(
        serialized_space=gss.build_serialized_space("manufacturing", mode)
    )
    client.genie.start_conversation.return_value = SimpleNamespace(
        response=SimpleNamespace(conversation_id="conv-1", message_id="msg-1")
    )
    client.genie.create_message.return_value = SimpleNamespace(
        response=SimpleNamespace(conversation_id="conv-1", message_id="msg-2")
    )
    client.genie.get_message.return_value = SimpleNamespace(
        id="msg-1",
        conversation_id="conv-1",
        status=SimpleNamespace(value="COMPLETED"),
        content="q",
        error=None,
        attachments=[
            SimpleNamespace(
                attachment_id="att-q",
                text=None,
                query=SimpleNamespace(query="SELECT 1", description="Downtime"),
                suggested_questions=None,
            ),
            SimpleNamespace(
                attachment_id="att-t",
                text=SimpleNamespace(content="12 minutes"),
                query=None,
                suggested_questions=SimpleNamespace(questions=["And OEE?"]),
            ),
        ],
    )
    return client


@pytest_asyncio.fixture()
async def api(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    for name in (
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "GENIE_ALLOWLIST_CONFIRMED",
        "DATABRICKS_APP_NAME",
        "DATABRICKS_APP_PORT",
        "DATABRICKS_APP_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    async def _get_setup(_db, setup_id):
        return SETUPS.get(setup_id)

    async def _find_space_id(_db, setup_id, mode):
        return SPACES.get((setup_id, mode))

    async def _get_deployment(_db, deployment_id):
        return DEPLOYMENTS.get(deployment_id)

    monkeypatch.setattr(gss.pipeline_setup_repo, "get_setup", _get_setup)
    monkeypatch.setattr(gss.deployment_repo, "get_deployment", _get_deployment)
    monkeypatch.setattr(gss, "find_space_id", _find_space_id)
    monkeypatch.setattr(genie_service, "fetch_telemetry_summary", AsyncMock(return_value=[]))
    client = _genie_client()
    monkeypatch.setattr(genie_service, "_workspace_client", lambda: client)

    state = AppState()
    state.app_backend_db = MagicMock()
    state.app_backend_available = True
    app = FastAPI()
    app.state.app_state = state
    app.include_router(genie_router)
    app.include_router(genie_pipelines.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        http.app_state = state  # type: ignore[attr-defined]
        http.ws = client  # type: ignore[attr-defined]
        yield http


async def _start(api: AsyncClient, headers=ALICE, base: str = BASE):
    return await api.post(
        f"{base}/conversations", json={"question": "What is OEE?"}, headers=headers
    )


# ---------------------------------------------------------------------------
# status / starters
# ---------------------------------------------------------------------------
class TestStatus:
    @pytest.mark.asyncio
    async def test_available_when_space_matches(self, api: AsyncClient) -> None:
        resp = await api.get(f"{BASE}/status", headers=ALICE)
        assert resp.status_code == 200
        assert resp.json() == {
            "available": True,
            "space_id": "sp-rt",
            "industry": "manufacturing",
            "supported": True,
        }
        api.ws.genie.get_space.assert_called_with("sp-rt", include_serialized_space=True)

    @pytest.mark.asyncio
    async def test_unavailable_when_not_provisioned(self, api: AsyncClient) -> None:
        body = (
            await api.get(
                "/api/genie/pipelines/11111111-1111-4111-8111-111111111111/microbatch/status"
            )
        ).json()
        assert body["available"] is False and body["space_id"] is None
        assert "Redeploy" in body["reason"]

    @pytest.mark.asyncio
    async def test_unavailable_for_unknown_industry(self, api: AsyncClient) -> None:
        body = (
            await api.get(
                "/api/genie/pipelines/22222222-2222-4222-8222-222222222222/realtime/status"
            )
        ).json()
        assert body["available"] is False
        assert body["supported"] is False and body["industry"] == "retail"
        assert "retail" in body["reason"]

    @pytest.mark.asyncio
    async def test_unavailable_on_allowlist_mismatch(self, api: AsyncClient) -> None:
        api.ws.genie.get_space.return_value = SimpleNamespace(
            serialized_space=gss.build_serialized_space("manufacturing", "microbatch")
        )
        body = (await api.get(f"{BASE}/status")).json()
        assert body["available"] is False
        assert body["space_id"] == "sp-rt"
        assert body["reason"]

    @pytest.mark.asyncio
    async def test_unknown_setup_404(self, api: AsyncClient) -> None:
        assert (await api.get("/api/genie/pipelines/nope/realtime/status")).status_code == 404

    @pytest.mark.asyncio
    async def test_unknown_mode_422(self, api: AsyncClient) -> None:
        assert (
            await api.get("/api/genie/pipelines/11111111-1111-4111-8111-111111111111/batch/status")
        ).status_code == 422

    @pytest.mark.asyncio
    async def test_db_unavailable_503(self, api: AsyncClient) -> None:
        api.app_state.app_backend_available = False
        assert (await api.get(f"{BASE}/status")).status_code == 503

    @pytest.mark.asyncio
    async def test_requires_identity_in_apps_runtime(
        self, api: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATABRICKS_APP_NAME", "iot")
        monkeypatch.delenv("IOT_ALLOW_ANONYMOUS_IDENTITY", raising=False)
        assert (await api.get(f"{BASE}/status")).status_code == 401
        assert (await api.get(f"{BASE}/starters")).status_code == 401
        assert (await _start(api, headers={})).status_code == 401
        assert (await api.get(f"{BASE}/status", headers=ALICE)).status_code == 200


class TestStarters:
    @pytest.mark.asyncio
    async def test_mode_aware_questions(self, api: AsyncClient) -> None:
        rt = (await api.get(f"{BASE}/starters")).json()
        zb = (
            await api.get(
                "/api/genie/pipelines/11111111-1111-4111-8111-111111111111/zerobus/starters"
            )
        ).json()
        assert rt == {"questions": list(gss.starter_questions("manufacturing", "realtime"))}
        assert zb["questions"] != rt["questions"]

    @pytest.mark.asyncio
    async def test_unknown_industry_404(self, api: AsyncClient) -> None:
        resp = await api.get(
            "/api/genie/pipelines/22222222-2222-4222-8222-222222222222/realtime/starters"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# conversations
# ---------------------------------------------------------------------------
class TestConversations:
    @pytest.mark.asyncio
    async def test_start_returns_ids_and_records_owner(self, api: AsyncClient) -> None:
        resp = await _start(api)
        assert resp.status_code == 200
        assert resp.json() == {"conversation_id": "conv-1", "message_id": "msg-1"}
        kwargs = api.ws.genie.start_conversation.call_args.kwargs
        assert kwargs["space_id"] == "sp-rt"
        assert "[TRUSTED_CONTEXT]" in kwargs["content"]
        assert "What is OEE?" in kwargs["content"]
        record = api.app_state.genie_conversations["conv-1"]
        assert record["owner"] == "alice@example.com"
        assert (
            record["setup_id"] == "11111111-1111-4111-8111-111111111111"
            and record["mode"] == "realtime"
        )

    @pytest.mark.asyncio
    async def test_start_rejects_injection_fields(self, api: AsyncClient) -> None:
        resp = await api.post(
            f"{BASE}/conversations", json={"question": "q", "tables": ["cat.sch.payroll"]}
        )
        assert resp.status_code == 422
        api.ws.genie.start_conversation.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [{}, {"question": "  "}, {"question": "x" * 4001}, ["q"]])
    async def test_start_validates_question(self, api: AsyncClient, body) -> None:
        resp = await api.post(f"{BASE}/conversations", json=body)
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_start_503_when_not_provisioned(self, api: AsyncClient) -> None:
        resp = await _start(
            api, base="/api/genie/pipelines/11111111-1111-4111-8111-111111111111/microbatch"
        )
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_start_404_for_unknown_industry(self, api: AsyncClient) -> None:
        resp = await _start(
            api, base="/api/genie/pipelines/22222222-2222-4222-8222-222222222222/realtime"
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_start_blocked_on_allowlist_mismatch(self, api: AsyncClient) -> None:
        api.ws.genie.get_space.return_value = SimpleNamespace(
            serialized_space=json.dumps(
                {"data_sources": {"tables": [{"identifier": "cat.sch.payroll"}]}}
            )
        )
        assert (await _start(api)).status_code == 503
        api.ws.genie.start_conversation.assert_not_called()

    @pytest.mark.asyncio
    async def test_follow_up(self, api: AsyncClient) -> None:
        await _start(api)
        resp = await api.post(
            f"{BASE}/conversations/conv-1/messages",
            json={"question": "And yesterday?"},
            headers=ALICE,
        )
        assert resp.status_code == 200
        assert resp.json() == {"conversation_id": "conv-1", "message_id": "msg-2"}
        kwargs = api.ws.genie.create_message.call_args.kwargs
        assert kwargs["space_id"] == "sp-rt" and kwargs["conversation_id"] == "conv-1"

    @pytest.mark.asyncio
    async def test_follow_up_other_user_404(self, api: AsyncClient) -> None:
        await _start(api)
        resp = await api.post(
            f"{BASE}/conversations/conv-1/messages", json={"question": "x"}, headers=BOB
        )
        assert resp.status_code == 404
        api.ws.genie.create_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_conversation_404(self, api: AsyncClient) -> None:
        resp = await api.post(
            f"{BASE}/conversations/never/messages", json={"question": "x"}, headers=ALICE
        )
        assert resp.status_code == 404
        assert (
            await api.get(f"{BASE}/conversations/never/messages/m", headers=ALICE)
        ).status_code == 404

    @pytest.mark.asyncio
    async def test_conversation_bound_to_pipeline(self, api: AsyncClient) -> None:
        await _start(api)
        api.ws.genie.get_space.return_value = SimpleNamespace(
            serialized_space=gss.build_serialized_space("manufacturing", "zerobus")
        )
        other = "/api/genie/pipelines/11111111-1111-4111-8111-111111111111/zerobus/conversations/conv-1/messages/msg-1"
        assert (await api.get(other, headers=ALICE)).status_code == 404

    @pytest.mark.asyncio
    async def test_static_routes_do_not_accept_pipeline_conversations(
        self, api: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        await _start(api)
        monkeypatch.setenv("GENIE_SPACE_ID", "static-space")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        api.ws.genie.get_space.side_effect = RuntimeError("uninspectable")
        resp = await api.get("/api/genie/conversations/conv-1/messages/msg-1", headers=ALICE)
        assert resp.status_code == 404


class TestPoll:
    @pytest.mark.asyncio
    async def test_poll_exposes_attachment_fields(self, api: AsyncClient) -> None:
        await _start(api)
        resp = await api.get(f"{BASE}/conversations/conv-1/messages/msg-1", headers=ALICE)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "COMPLETED"
        assert body["conversation_id"] == "conv-1" and body["message_id"] == "msg-1"
        assert body["attachments"] == [
            {
                "attachment_id": "att-q",
                "text": None,
                "query": {"sql": "SELECT 1", "description": "Downtime"},
                "suggested_questions": [],
            },
            {
                "attachment_id": "att-t",
                "text": "12 minutes",
                "query": None,
                "suggested_questions": ["And OEE?"],
            },
        ]
        assert api.ws.genie.get_message.call_args.kwargs["space_id"] == "sp-rt"

    @pytest.mark.asyncio
    async def test_poll_other_user_404(self, api: AsyncClient) -> None:
        await _start(api)
        resp = await api.get(f"{BASE}/conversations/conv-1/messages/msg-1", headers=BOB)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_sdk_not_found_maps_to_404(self, api: AsyncClient) -> None:
        await _start(api)
        api.ws.genie.get_message.side_effect = NotFound("no message")
        resp = await api.get(f"{BASE}/conversations/conv-1/messages/zzz", headers=ALICE)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_sdk_error_is_sanitized_500(self, api: AsyncClient) -> None:
        await _start(api)
        api.ws.genie.get_message.side_effect = RuntimeError("token=fake-token-for-tests")
        resp = await api.get(f"{BASE}/conversations/conv-1/messages/msg-1", headers=ALICE)
        assert resp.status_code == 500
        assert "fake-token-for-tests" not in resp.text


class TestAttachmentResult:
    URL = f"{BASE}/conversations/conv-1/messages/msg-1/attachments/att-q/result"

    def _result(self, n_rows: int, total: int | None = None) -> SimpleNamespace:
        manifest = SimpleNamespace(
            schema=SimpleNamespace(
                columns=[
                    SimpleNamespace(name="sensor_name", type_text="STRING"),
                    SimpleNamespace(name="downtime_minutes", type_text="BIGINT"),
                ]
            ),
            total_row_count=total if total is not None else n_rows,
            truncated=False,
        )
        return SimpleNamespace(
            statement_response=SimpleNamespace(
                manifest=manifest,
                result=SimpleNamespace(data_array=[[f"s{i}", str(i)] for i in range(n_rows)]),
            )
        )

    @pytest.mark.asyncio
    async def test_result_shape(self, api: AsyncClient) -> None:
        await _start(api)
        api.ws.genie.get_message_attachment_query_result.return_value = self._result(2)
        resp = await api.get(self.URL, headers=ALICE)
        assert resp.status_code == 200
        assert resp.json() == {
            "columns": [
                {"name": "sensor_name", "type": "STRING"},
                {"name": "downtime_minutes", "type": "BIGINT"},
            ],
            "rows": [["s0", "0"], ["s1", "1"]],
            "truncated": False,
        }
        api.ws.genie.get_message_attachment_query_result.assert_called_once_with(
            space_id="sp-rt",
            conversation_id="conv-1",
            message_id="msg-1",
            attachment_id="att-q",
        )

    @pytest.mark.asyncio
    async def test_result_capped_at_500(self, api: AsyncClient) -> None:
        await _start(api)
        api.ws.genie.get_message_attachment_query_result.return_value = self._result(1200)
        body = (await api.get(self.URL, headers=ALICE)).json()
        assert len(body["rows"]) == 500
        assert body["truncated"] is True

    @pytest.mark.asyncio
    async def test_result_other_user_404(self, api: AsyncClient) -> None:
        await _start(api)
        resp = await api.get(self.URL, headers=BOB)
        assert resp.status_code == 404
        api.ws.genie.get_message_attachment_query_result.assert_not_called()

    @pytest.mark.asyncio
    async def test_result_unknown_conversation_404(self, api: AsyncClient) -> None:
        url = f"{BASE}/conversations/nope/messages/msg-1/attachments/att-q/result"
        assert (await api.get(url, headers=ALICE)).status_code == 404


# ---------------------------------------------------------------------------
# Review fixes: id resolution, timeouts, cross-scope follow-ups
# ---------------------------------------------------------------------------
class TestIdResolution:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad", ["setup-1", "not-a-uuid", "1234", "%27%20OR%201%3D1"])
    async def test_non_uuid_ids_404(self, api: AsyncClient, bad: str) -> None:
        base = f"/api/genie/pipelines/{bad}/realtime"
        assert (await api.get(f"{base}/status")).status_code == 404
        assert (await api.get(f"{base}/starters")).status_code == 404
        assert (await _start(api, base=base)).status_code == 404

    @pytest.mark.asyncio
    async def test_deployment_id_resolves_to_setup(self, api: AsyncClient) -> None:
        base = f"/api/genie/pipelines/{DEP_RT}/realtime"
        status = (await api.get(f"{base}/status")).json()
        assert status["available"] is True and status["space_id"] == "sp-rt"
        resp = await _start(api, base=base)
        assert resp.status_code == 200
        assert (
            api.app_state.genie_conversations["conv-1"]["setup_id"]
            == "11111111-1111-4111-8111-111111111111"
        )
        poll = await api.get(f"{BASE}/conversations/conv-1/messages/msg-1", headers=ALICE)
        assert poll.status_code == 200

    @pytest.mark.asyncio
    async def test_deployment_id_with_other_mode_404(self, api: AsyncClient) -> None:
        resp = await api.get(f"/api/genie/pipelines/{DEP_RT}/zerobus/status")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_unknown_uuid_404(self, api: AsyncClient) -> None:
        resp = await api.get(
            "/api/genie/pipelines/44444444-4444-4444-8444-444444444444/realtime/status"
        )
        assert resp.status_code == 404


class TestReviewFixes:
    @pytest.mark.asyncio
    async def test_follow_up_to_other_mode_rejected(self, api: AsyncClient) -> None:
        await _start(api)
        api.ws.genie.get_space.return_value = SimpleNamespace(
            serialized_space=gss.build_serialized_space("manufacturing", "zerobus")
        )
        resp = await api.post(
            "/api/genie/pipelines/11111111-1111-4111-8111-111111111111/zerobus/conversations/conv-1/messages",
            json={"question": "x"},
            headers=ALICE,
        )
        assert resp.status_code == 404
        api.ws.genie.create_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_follow_up_to_other_setup_rejected(self, api: AsyncClient) -> None:
        await _start(api)
        resp = await api.post(
            "/api/genie/pipelines/22222222-2222-4222-8222-222222222222/realtime/conversations/conv-1/messages",
            json={"question": "x"},
            headers=ALICE,
        )
        assert resp.status_code == 404
        api.ws.genie.create_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_sdk_timeout_504(self, api: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
        import threading

        await _start(api)
        release = threading.Event()
        monkeypatch.setattr(genie_service, "SDK_TIMEOUT_S", 0.05)
        api.ws.genie.get_message.side_effect = lambda **_k: release.wait(2)
        try:
            resp = await api.get(f"{BASE}/conversations/conv-1/messages/msg-1", headers=ALICE)
        finally:
            release.set()
        assert resp.status_code == 504

    @pytest.mark.asyncio
    async def test_manual_override_not_honored(
        self, api: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        api.ws.genie.get_space.side_effect = RuntimeError("uninspectable")
        body = (await api.get(f"{BASE}/status")).json()
        assert body["available"] is False
        assert (await _start(api)).status_code == 503
        api.ws.genie.start_conversation.assert_not_called()

    @pytest.mark.asyncio
    async def test_slow_telemetry_does_not_block_start(
        self, api: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import asyncio as _asyncio

        async def _slow(*_a, **_k):
            await _asyncio.sleep(5)
            return [{"sensor_name": "zz-slow-marker"}]

        monkeypatch.setattr(genie_service, "fetch_telemetry_summary", _slow)
        monkeypatch.setattr(genie_service, "TELEMETRY_TIMEOUT_S", 0.05)
        resp = await _start(api)
        assert resp.status_code == 200
        assert "zz-slow-marker" not in api.ws.genie.start_conversation.call_args.kwargs["content"]

    @pytest.mark.asyncio
    async def test_allowlist_cached_across_polls(self, api: AsyncClient) -> None:
        await _start(api)
        for _ in range(3):
            await api.get(f"{BASE}/conversations/conv-1/messages/msg-1", headers=ALICE)
        assert api.ws.genie.get_space.call_count == 1
