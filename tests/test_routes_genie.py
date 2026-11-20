"""REST routes for the Genie copilot [qa]."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes.genie import router as genie_router
from iot_twin.services.genie_service import GenieDisabledError, GenieValidationError

# Hang guard for the event-loop regression test; never a timing budget.
_HANG_GUARD_S = 30.0


def _build_app(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(genie_router)
    return app


@pytest_asyncio.fixture()
async def genie_client(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
    monkeypatch.delenv("UC_CATALOG", raising=False)
    monkeypatch.delenv("UC_SCHEMA", raising=False)
    state = AppState()
    app = _build_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        client.app_state = state  # type: ignore[attr-defined]
        yield client


class TestGenieRoutes:
    @pytest.mark.asyncio
    async def test_status_disabled_when_space_missing_c7502_smoke(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][smoke] Status reports disabled when GENIE_SPACE_ID is unset."""
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        resp = await genie_client.get("/api/genie/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["enabled"] is False
        assert body["permission"] == "CAN_VIEW"
        assert body["auth"] == "app_service_principal"
        assert "GENIE_SPACE_ID" in body["reason"]

    @pytest.mark.asyncio
    async def test_status_enabled_when_space_set_c7502_regression(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][regression] Uninspectable spaces stay disabled until allowlist is confirmed."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-from-resource")
        monkeypatch.delenv("GENIE_ALLOWLIST_CONFIRMED", raising=False)
        resp = await genie_client.get("/api/genie/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["enabled"] is False
        assert body["authorization_enforced"] is False
        assert body["table_allowlist"]["status"] == "manual_required"
        assert "GENIE_ALLOWLIST_CONFIRMED" in body["reason"]
        assert "space-from-resource" not in str(body)

    @pytest.mark.asyncio
    async def test_start_returns_ids_c7502_regression(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][regression] Start conversation returns ids immediately."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        with patch(
            "iot_twin.routes.genie.genie_service.start_conversation",
            return_value={
                "conversation_id": "c1",
                "message_id": "m1",
            },
        ) as start:
            resp = await genie_client.post(
                "/api/genie/conversations",
                json={
                    "industry": "manufacturing",
                    "mode": "realtime",
                    "asset_id": "mfg-asset-cnc-1",
                    "question": "Explain the spindle trend",
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"conversation_id": "c1", "message_id": "m1"}
        start.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_disabled_is_503_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Unconfigured Genie cannot start a conversation."""
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        resp = await genie_client.post(
            "/api/genie/conversations",
            json={"industry": "manufacturing", "mode": "realtime", "question": "hi"},
        )
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_invalid_industry_is_422_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Unknown industry/mode/asset is rejected."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        resp = await genie_client.post(
            "/api/genie/conversations",
            json={"industry": "nope", "mode": "realtime", "question": "hi"},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_rejects_injected_tables_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Client-supplied tables/context are not accepted."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        resp = await genie_client.post(
            "/api/genie/conversations",
            json={
                "industry": "manufacturing",
                "mode": "realtime",
                "question": "hi",
                "tables": ["evil.table"],
            },
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_followup_and_status_c7502_regression(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][regression] Follow-up and get-message endpoints round-trip ids."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        genie_client.app_state.genie_conversations["c1"] = {  # type: ignore[attr-defined]
            "owner": "anonymous",
            "industry": "manufacturing",
        }
        with patch(
            "iot_twin.routes.genie.genie_service.create_message",
            return_value={
                "conversation_id": "c1",
                "message_id": "m2",
            },
        ):
            follow = await genie_client.post(
                "/api/genie/conversations/c1/messages",
                json={"industry": "manufacturing", "mode": "zerobus", "question": "next?"},
            )
        assert follow.status_code == 200
        assert follow.json()["message_id"] == "m2"

        with patch(
            "iot_twin.routes.genie.genie_service.get_message",
            return_value={
                "conversation_id": "c1",
                "message_id": "m2",
                "status": "COMPLETED",
                "content": "Inspect coolant flow",
                "attachments": [],
                "error": None,
            },
        ):
            status = await genie_client.get("/api/genie/conversations/c1/messages/m2")
        assert status.status_code == 200
        assert status.json()["status"] == "COMPLETED"
        assert status.json()["content"] == "Inspect coolant flow"

    @pytest.mark.asyncio
    async def test_unknown_alert_is_422_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Unknown alert ids cannot be attached to a prompt."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        with patch(
            "iot_twin.routes.genie.genie_service.validate_request",
            side_effect=GenieValidationError("unknown alert"),
        ):
            resp = await genie_client.post(
                "/api/genie/conversations",
                json={
                    "industry": "manufacturing",
                    "mode": "realtime",
                    "alert_id": "missing",
                    "question": "Explain this alert",
                },
            )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_disabled_error_maps_503_c7502_usability(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][usability] Service disabled errors surface as 503."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        with patch(
            "iot_twin.routes.genie.genie_service.start_conversation",
            side_effect=GenieDisabledError("GENIE_SPACE_ID is not configured"),
        ):
            resp = await genie_client.post(
                "/api/genie/conversations",
                json={"industry": "manufacturing", "mode": "realtime", "question": "hi"},
            )
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_sdk_errors_are_generic_500s_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Unexpected Genie SDK failures log a generic 500."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        with patch(
            "iot_twin.routes.genie.genie_service.start_conversation",
            side_effect=RuntimeError("secret warehouse token leaked"),
        ):
            resp = await genie_client.post(
                "/api/genie/conversations",
                json={"industry": "manufacturing", "mode": "realtime", "question": "hi"},
            )
        assert resp.status_code == 500
        detail = str(resp.json().get("detail", ""))
        assert "secret warehouse token leaked" not in detail
        assert "token" not in detail.lower()

    @pytest.mark.asyncio
    async def test_sdk_calls_do_not_block_event_loop_c7502_regression(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][regression] Synchronous SDK network stays off the event loop."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        loop_thread = threading.get_ident()
        entered = threading.Event()
        release = threading.Event()
        sdk_threads: list[int] = []

        def blocking_start(*_args, **_kwargs):
            sdk_threads.append(threading.get_ident())
            entered.set()
            # Parks the SDK call until the test releases it. The timeout is a
            # hang guard only: it is reached solely when the call wrongly runs
            # on the event-loop thread (nothing can then set ``release``).
            if not release.wait(timeout=_HANG_GUARD_S):
                raise AssertionError("SDK call blocked the event loop")
            return {"conversation_id": "c-block", "message_id": "m-block"}

        with (
            patch(
                "iot_twin.routes.genie.genie_service.require_table_allowlist",
                return_value={"authorization_enforced": True},
            ),
            patch(
                "iot_twin.routes.genie.genie_service.start_conversation",
                side_effect=blocking_start,
            ),
        ):
            started = asyncio.create_task(
                genie_client.post(
                    "/api/genie/conversations",
                    json={
                        "industry": "manufacturing",
                        "mode": "realtime",
                        "question": "Explain spindle heat",
                    },
                )
            )
            # The loop keeps turning until the SDK call is parked in a worker.
            while not entered.is_set():
                if started.done():
                    pytest.fail(f"request ended before the SDK call: {started.result()}")
                await asyncio.sleep(0)

            # While the SDK call is still blocked, the loop must stay responsive.
            for _ in range(5):
                await asyncio.sleep(0)
            assert not release.is_set()
            assert not started.done()

            release.set()
            conv = await asyncio.wait_for(started, timeout=_HANG_GUARD_S)

        assert sdk_threads and sdk_threads[0] != loop_thread
        assert conv.status_code == 200
        assert conv.json()["conversation_id"] == "c-block"

    @pytest.mark.asyncio
    async def test_poll_fails_closed_without_lease_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Follow-up/poll fail closed after process restart."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        resp = await genie_client.get("/api/genie/conversations/missing/messages/m1")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_start_blocked_until_allowlist_verified_c7502_defect(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][defect] Conversations cannot start on an unverified table allowlist."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        monkeypatch.delenv("GENIE_ALLOWLIST_CONFIRMED", raising=False)
        with patch("iot_twin.routes.genie.genie_service.start_conversation") as start:
            resp = await genie_client.post(
                "/api/genie/conversations",
                json={"industry": "manufacturing", "mode": "realtime", "question": "hi"},
            )
        assert resp.status_code == 503
        assert "GENIE_ALLOWLIST_CONFIRMED" in str(resp.json().get("detail", ""))
        start.assert_not_called()

    @pytest.mark.asyncio
    async def test_status_inspection_does_not_block_event_loop_c7502_regression(
        self, genie_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7502][regression] Genie status SDK inspection stays off the event loop."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-1")
        release = threading.Event()

        def blocking_status(*_args, **_kwargs):
            release.wait(timeout=5)
            return {
                "enabled": False,
                "permission": "CAN_VIEW",
                "auth": "app_service_principal",
                "reason": "inspection blocked",
                "capabilities": {"can_mutate_alerts": False},
                "table_allowlist": {"status": "manual_required"},
                "authorization_enforced": False,
            }

        with patch(
            "iot_twin.routes.genie.genie_service.status_payload",
            side_effect=blocking_status,
        ):
            started = asyncio.create_task(genie_client.get("/api/genie/status"))
            heartbeats: list[int] = []

            async def beat() -> None:
                for i in range(5):
                    heartbeats.append(i)
                    await asyncio.sleep(0)

            await asyncio.wait_for(beat(), timeout=1)
            assert heartbeats == [0, 1, 2, 3, 4]
            release.set()
            resp = await asyncio.wait_for(started, timeout=2)
        assert resp.status_code == 200
        assert resp.json()["reason"] == "inspection blocked"
