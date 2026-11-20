"""Tests for the POST /api/sss/alerts endpoint."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_alerts_app(state: AppState):
    """Create a minimal FastAPI app that includes the websocket router."""
    from fastapi import FastAPI

    from iot_twin.routes.websocket import router as ws_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(ws_router)
    return app


@pytest.fixture()
def app_state_with_queue() -> tuple[AppState, asyncio.Queue]:
    """Return an AppState and a pre-registered SSE client queue."""
    state = AppState()
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    state.sse_clients.append(queue)
    return state, queue


@pytest_asyncio.fixture()
async def alerts_client(app_state_with_queue: tuple) -> AsyncGenerator[tuple, None]:
    """Yield (AsyncClient, queue) bound to a test app with one SSE client."""
    state, queue = app_state_with_queue
    app = _build_alerts_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, queue


@pytest_asyncio.fixture()
async def alerts_client_no_sse() -> AsyncGenerator[AsyncClient, None]:
    """Yield an AsyncClient where no SSE client is registered."""
    state = AppState()
    app = _build_alerts_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSssAlertsIngest:
    """POST /api/sss/alerts endpoint behaviour."""

    @pytest.mark.asyncio
    async def test_returns_200_with_empty_alerts(self, alerts_client: tuple) -> None:
        client, _ = alerts_client
        resp = await client.post("/api/sss/alerts", json={"alerts": []})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_empty_alerts_count_is_zero(self, alerts_client: tuple) -> None:
        client, _ = alerts_client
        resp = await client.post("/api/sss/alerts", json={"alerts": []})
        assert resp.json()["count"] == 0

    @pytest.mark.asyncio
    async def test_empty_alerts_status_is_ok(self, alerts_client: tuple) -> None:
        client, _ = alerts_client
        resp = await client.post("/api/sss/alerts", json={"alerts": []})
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_missing_alerts_key_treated_as_empty(self, alerts_client: tuple) -> None:
        client, _ = alerts_client
        resp = await client.post("/api/sss/alerts", json={})
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    @pytest.mark.asyncio
    async def test_single_alert_returns_count_one(self, alerts_client: tuple) -> None:
        client, _ = alerts_client
        payload = {
            "alerts": [
                {
                    "pipeline_id": "pipe-1",
                    "sensor_id": "temp-01",
                    "sensor_type": "temperature",
                    "industry": "manufacturing",
                    "value": 120.5,
                    "threshold_min": 0,
                    "threshold_max": 100,
                    "timestamp": "2026-04-05T10:00:00Z",
                }
            ]
        }
        resp = await client.post("/api/sss/alerts", json=payload)
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

    @pytest.mark.asyncio
    async def test_alert_dispatched_to_sse_queue(self, alerts_client: tuple) -> None:
        client, queue = alerts_client
        payload = {
            "alerts": [
                {
                    "pipeline_id": "pipe-1",
                    "sensor_id": "temp-01",
                    "sensor_type": "temperature",
                    "industry": "manufacturing",
                    "value": 120.5,
                    "threshold_min": 0,
                    "threshold_max": 100,
                    "timestamp": "2026-04-05T10:00:00Z",
                }
            ]
        }
        await client.post("/api/sss/alerts", json=payload)
        assert not queue.empty()

    @pytest.mark.asyncio
    async def test_event_type_is_alert_opened(self, alerts_client: tuple) -> None:
        client, queue = alerts_client
        payload = {
            "alerts": [
                {
                    "pipeline_id": "pipe-1",
                    "sensor_id": "temp-01",
                    "sensor_type": "temperature",
                    "industry": "manufacturing",
                    "value": 120.5,
                    "threshold_min": 0,
                    "threshold_max": 100,
                    "timestamp": "2026-04-05T10:00:00Z",
                }
            ]
        }
        await client.post("/api/sss/alerts", json=payload)
        event = queue.get_nowait()
        assert event["type"] == "alert_opened"

    @pytest.mark.asyncio
    async def test_event_fields_match_alert_payload(self, alerts_client: tuple) -> None:
        client, queue = alerts_client
        alert = {
            "pipeline_id": "pipe-1",
            "sensor_id": "temp-01",
            "sensor_type": "temperature",
            "industry": "manufacturing",
            "value": 120.5,
            "threshold_min": 0.0,
            "threshold_max": 100.0,
            "timestamp": "2026-04-05T10:00:00Z",
        }
        await client.post("/api/sss/alerts", json={"alerts": [alert]})
        event = queue.get_nowait()
        assert event["pipeline_id"] == alert["pipeline_id"]
        assert event["sensor_id"] == alert["sensor_id"]
        assert event["sensor_type"] == alert["sensor_type"]
        assert event["value"] == alert["value"]
        assert event["threshold_min"] == alert["threshold_min"]
        assert event["threshold_max"] == alert["threshold_max"]
        assert event["timestamp"] == alert["timestamp"]

    @pytest.mark.asyncio
    async def test_multiple_alerts_all_dispatched(self, alerts_client: tuple) -> None:
        client, queue = alerts_client
        alerts = [
            {
                "pipeline_id": f"pipe-{i}",
                "sensor_id": f"s-{i}",
                "sensor_type": "temperature",
                "industry": "manufacturing",
                "value": float(i),
                "threshold_min": None,
                "threshold_max": None,
                "timestamp": "2026-04-05T10:00:00Z",
            }
            for i in range(3)
        ]
        await client.post("/api/sss/alerts", json={"alerts": alerts})
        assert queue.qsize() == 3

    @pytest.mark.asyncio
    async def test_no_sse_clients_still_returns_ok(self, alerts_client_no_sse: AsyncClient) -> None:
        payload = {
            "alerts": [
                {
                    "pipeline_id": "pipe-1",
                    "sensor_id": "temp-01",
                    "sensor_type": "temperature",
                    "industry": "manufacturing",
                    "value": 42.0,
                    "threshold_min": None,
                    "threshold_max": None,
                    "timestamp": "2026-04-05T10:00:00Z",
                }
            ]
        }
        resp = await alerts_client_no_sse.post("/api/sss/alerts", json=payload)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_partial_alert_fields_handled_gracefully(self, alerts_client: tuple) -> None:
        """Alerts with missing fields should not raise — None values are acceptable."""
        client, queue = alerts_client
        resp = await client.post(
            "/api/sss/alerts",
            json={"alerts": [{"industry": "manufacturing"}]},
        )
        assert resp.status_code == 200
        event = queue.get_nowait()
        assert event["type"] == "alert_opened"
        assert event["sensor_id"] in (None, "")

    @pytest.mark.asyncio
    async def test_missing_industry_is_not_broadcast_c7410_defect(
        self, alerts_client: tuple
    ) -> None:
        """[C7410][defect] Legacy SSS alerts without industry are rejected, not global."""
        client, queue = alerts_client
        resp = await client.post(
            "/api/sss/alerts",
            json={
                "alerts": [
                    {
                        "pipeline_id": "pipe-1",
                        "sensor_id": "temp-01",
                        "value": 120.5,
                        "threshold_max": 100,
                    }
                ]
            },
        )
        assert resp.status_code in {200, 400, 422}
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_transitions_mark_snapshot_dirty_c7410_regression(self) -> None:
        """[C7410][regression] SSS lifecycle transitions dirty the alert snapshot."""
        from httpx import ASGITransport, AsyncClient

        from iot_twin.services.alerts.alert_snapshot_sync import AlertSnapshotSynchronizer

        state = AppState()
        syncer = AlertSnapshotSynchronizer(state)
        syncer.enabled = True
        state.alert_snapshot_synchronizer = syncer
        app = _build_alerts_app(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post(
                "/api/sss/alerts",
                json={
                    "alerts": [
                        {
                            "pipeline_id": "pipe-1",
                            "sensor_id": "temp-01",
                            "industry": "manufacturing",
                            "value": 120.5,
                            "threshold_max": 100,
                        }
                    ]
                },
            )
        assert syncer.dirty is True
