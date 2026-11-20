"""Realtime ingest evaluates catalog alerts without breaking SSE [qa]."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes.sensor_data import router as sensor_router
from iot_twin.services.alerts.alert_engine import AlertEngine


def _build_app(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(sensor_router)
    return app


@pytest_asyncio.fixture()
async def ingest_client(
    instant_alert_rules: None,
) -> AsyncGenerator[tuple[AsyncClient, AppState, asyncio.Queue], None]:
    state = AppState()
    state.alert_engine = AlertEngine()
    queue: asyncio.Queue = asyncio.Queue()
    state.sse_clients.append(queue)
    app = _build_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, state, queue


class TestSssIngestAlertEvaluation:
    @pytest.mark.asyncio
    async def test_ingest_still_returns_record_count_c7409_smoke(
        self, ingest_client: tuple
    ) -> None:
        """[C7409][smoke] Ingest response shape is unchanged."""
        client, _, _ = ingest_client
        resp = await client.post(
            "/api/sss/ingest",
            json=[{"sensor_name": "a", "value": 1.0, "industry": "manufacturing"}],
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["records"] == 1
        assert "lakebase_inserted" in body

    @pytest.mark.asyncio
    async def test_sensor_update_is_emitted_first_c7409_regression(
        self, ingest_client: tuple
    ) -> None:
        """[C7409][regression] Alert evaluation does not replace sensor_update."""
        client, _, queue = ingest_client
        resp = await client.post(
            "/api/sss/ingest",
            json=[
                {
                    "sensor_name": "cnc_1_spindle_temp",
                    "industry": "manufacturing",
                    "value": 91.0,
                    "unit": "C",
                    "sensor_type": "temperature",
                }
            ],
            headers={"X-Pipeline-Id": "industry:manufacturing"},
        )
        assert resp.status_code == 200
        first = queue.get_nowait()
        assert first["type"] == "sensor_update"
        second = queue.get_nowait()
        assert second["type"] == "alert_opened"
        assert second["sensor_id"] == "mfg-sns-cnc_1_spindle_temp"
        assert second["status"] == "open"
