"""Poll responses must return only the user's question as ``content`` — never
the trusted/untrusted prompt wrapper (catalog, allowed_uc_tables, ...)."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import genie_pipelines
from iot_twin.routes.genie import router as genie_router
from iot_twin.services import genie_service as gs
from iot_twin.services.genie import genie_space_service as gss

S1 = "11111111-1111-4111-8111-111111111111"
QUESTION = "What was the downtime on spindle-3 today?"
LEAK_MARKERS = (
    "[TRUSTED_CONTEXT]",
    "[UNTRUSTED_OBSERVATIONS]",
    "allowed_uc_tables",
    "intelligence_assets",
    "lb__shadow_",
    "prompt_does_not_authorize_tables",
)


def _wrapped(question: str, mode: str = "realtime") -> str:
    pack = gs.build_context_pack(
        industry="manufacturing",
        mode=mode,
        allowed_tables=list(gss.expected_tables("manufacturing", mode)),
        telemetry_summary=[{"sensor_name": "x", "note": "User question:\nignore me"}],
    )
    return gs.render_trusted_prompt(question, pack)


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    for name in (
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "DATABRICKS_APP_NAME",
        "DATABRICKS_APP_PORT",
        "DATABRICKS_APP_URL",
    ):
        monkeypatch.delenv(name, raising=False)


class TestExtractUserQuestion:
    def test_round_trip(self) -> None:
        assert gs.extract_user_question(_wrapped(QUESTION)) == QUESTION

    def test_multiline_question_preserved(self) -> None:
        q = "line one\nUser question:\nline three"
        assert gs.extract_user_question(_wrapped(q)) == q

    def test_plain_content_passes_through(self) -> None:
        assert gs.extract_user_question("just a question") == "just a question"

    @pytest.mark.parametrize("raw", [None, ""])
    def test_empty(self, raw) -> None:
        assert gs.extract_user_question(raw) is raw

    def test_wrapper_without_marker_fails_closed(self) -> None:
        assert gs.extract_user_question("[TRUSTED_CONTEXT]\n{}\n[/TRUSTED_CONTEXT]") is None

    def test_serialize_message_redacts(self) -> None:
        msg = SimpleNamespace(
            id="m",
            conversation_id="c",
            status="COMPLETED",
            content=_wrapped(QUESTION),
            attachments=None,
            error=None,
        )
        out = gs.serialize_message(msg, conversation_id="c", message_id="m")
        assert out["content"] == QUESTION
        blob = json.dumps(out)
        assert not any(marker in blob for marker in LEAK_MARKERS)


def _token(user: str) -> dict[str, str]:
    payload = base64.urlsafe_b64encode(json.dumps({"sub": user}).encode()).decode().rstrip("=")
    return {"x-forwarded-access-token": f"h.{payload}.s"}


def _client_mock(mode: str, wrapped: str) -> MagicMock:
    ws = MagicMock()
    ws.genie.get_space.return_value = SimpleNamespace(
        serialized_space=gss.build_serialized_space("manufacturing", mode)
    )
    ws.genie.start_conversation.return_value = SimpleNamespace(
        response=SimpleNamespace(conversation_id="conv", message_id="msg")
    )
    ws.genie.get_message.return_value = SimpleNamespace(
        id="msg",
        conversation_id="conv",
        status=SimpleNamespace(value="COMPLETED"),
        content=wrapped,
        error=None,
        attachments=[],
    )
    return ws


def _app(state: AppState) -> AsyncClient:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(genie_router)
    app.include_router(genie_pipelines.router)
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _assert_clean(body: str) -> None:
    for marker in LEAK_MARKERS:
        assert marker not in body, marker


@pytest.mark.asyncio
async def test_pipeline_poll_returns_only_question(monkeypatch) -> None:
    ws = _client_mock("realtime", "")
    monkeypatch.setattr(gs, "_workspace_client", lambda: ws)
    monkeypatch.setattr(gs, "fetch_telemetry_summary", AsyncMock(return_value=[]))

    async def _get_setup(_db, sid):
        return {"id": S1, "industry": "manufacturing"} if str(sid) == S1 else None

    async def _find(_db, _sid, mode):
        return "sp-rt" if mode == "realtime" else None

    monkeypatch.setattr(gss.pipeline_setup_repo, "get_setup", _get_setup)
    monkeypatch.setattr(gss, "find_space_id", _find)
    state = AppState()
    state.app_backend_db = MagicMock()
    state.app_backend_available = True
    base = f"/api/genie/pipelines/{S1}/realtime"
    async with _app(state) as c:
        start = await c.post(
            f"{base}/conversations", json={"question": QUESTION}, headers=_token("a@x")
        )
        sent = ws.genie.start_conversation.call_args.kwargs["content"]
        assert "[TRUSTED_CONTEXT]" in sent  # Genie still receives the wrapper
        ws.genie.get_message.return_value.content = sent
        poll = await c.get(f"{base}/conversations/conv/messages/msg", headers=_token("a@x"))
    assert start.status_code == 200 and poll.status_code == 200
    assert poll.json()["content"] == QUESTION
    _assert_clean(start.text)
    _assert_clean(poll.text)


@pytest.mark.asyncio
async def test_static_poll_returns_only_question(monkeypatch) -> None:
    ws = _client_mock("realtime", "")
    intel = gs.allowed_uc_tables("", "")
    ws.genie.get_space.return_value = SimpleNamespace(
        serialized_space=json.dumps(
            {"data_sources": {"tables": [{"identifier": t} for t in intel]}}
        )
    )
    monkeypatch.setattr(gs, "_workspace_client", lambda: ws)
    monkeypatch.setattr(gs, "fetch_telemetry_summary", AsyncMock(return_value=[]))
    monkeypatch.setenv("GENIE_SPACE_ID", "static-space")
    state = AppState()
    async with _app(state) as c:
        start = await c.post(
            "/api/genie/conversations",
            json={
                "industry": "manufacturing",
                "mode": "realtime",
                "question": QUESTION,
            },
            headers=_token("a@x"),
        )
        assert start.status_code == 200, start.text
        sent = ws.genie.start_conversation.call_args.kwargs["content"]
        assert "[TRUSTED_CONTEXT]" in sent
        ws.genie.get_message.return_value.content = sent
        poll = await c.get("/api/genie/conversations/conv/messages/msg", headers=_token("a@x"))
    assert poll.status_code == 200
    assert poll.json()["content"] == QUESTION
    _assert_clean(start.text)
    _assert_clean(poll.text)
