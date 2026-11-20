"""REST routes for alert list/detail/ack/evaluate [qa]."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import list_alert_rules
from iot_twin.routes.alerts import router as alerts_router
from iot_twin.services.alerts.alert_engine import AlertEngine, engine_for


def _build_app(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(alerts_router)
    return app


@pytest_asyncio.fixture()
async def alerts_client(
    instant_alert_rules: None,
) -> AsyncGenerator[tuple[AsyncClient, AppState], None]:
    state = AppState()
    state.alert_engine = AlertEngine()
    app = _build_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, state


class TestAlertRoutes:
    @pytest.mark.asyncio
    async def test_list_empty_c7408_smoke(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][smoke] List returns an empty collection when nothing is open."""
        client, _ = alerts_client
        resp = await client.get("/api/alerts")
        assert resp.status_code == 200
        body = resp.json()
        assert body["alerts"] == []

    @pytest.mark.asyncio
    async def test_evaluate_opens_and_list_detail_ack_c7408_regression(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][regression] Evaluate → list → detail → acknowledge lifecycle."""
        client, state = alerts_client
        rule = next(
            item
            for item in list_alert_rules("manufacturing")
            if item.rule_id == "mfg-alert-spindle-temp"
        )
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True):
            resp = await client.post(
                "/api/alerts/evaluate",
                json={
                    "industry": "manufacturing",
                    "pipeline_id": "industry:manufacturing",
                    "readings": [
                        {
                            "sensor_id": rule.sensor_id,
                            "sensor_name": "cnc_1_spindle_temp",
                            "value": 91.0,
                        }
                    ],
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["transitions"][0]["type"] == "alert_opened"
        alert_id = body["transitions"][0]["alert_id"]

        listed = await client.get("/api/alerts")
        assert listed.status_code == 200
        assert listed.json()["alerts"][0]["id"] == alert_id

        detail = await client.get(f"/api/alerts/{alert_id}")
        assert detail.status_code == 200
        assert detail.json()["status"] == "open"

        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True):
            ack = await client.post(f"/api/alerts/{alert_id}/acknowledge")
        assert ack.status_code == 200
        assert ack.json()["type"] == "alert_acknowledged"
        assert ack.json()["status"] == "acknowledged"
        assert engine_for(state).get_alert_sync(alert_id).status == "acknowledged"

    @pytest.mark.asyncio
    async def test_detail_unknown_is_404_c7408_defect(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][defect] Missing alert ids return 404."""
        client, _ = alerts_client
        resp = await client.get("/api/alerts/does-not-exist")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_evaluate_empty_readings_ok_c7408_defect(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][defect] Empty evaluate payload is a no-op success."""
        client, _ = alerts_client
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True):
            resp = await client.post("/api/alerts/evaluate", json={"readings": []})
        assert resp.status_code == 200
        assert resp.json()["transitions"] == []

    @pytest.mark.asyncio
    async def test_list_uses_db_when_available_c7407_regression(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7407][regression] List prefers Lakebase when the app backend is up."""
        client, state = alerts_client
        db = MagicMock()
        db.fetch = AsyncMock(return_value=[])
        db.fetchrow = AsyncMock(return_value=None)
        db.execute = AsyncMock()
        state.app_backend_db = db
        state.app_backend_available = True
        resp = await client.get("/api/alerts?status=open")
        assert resp.status_code == 200
        assert db.fetch.await_count >= 1

    @pytest.mark.asyncio
    async def test_list_without_status_uses_active_filter_c7408_regression(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][regression] Unfiltered list asks Lakebase for active states only."""
        client, state = alerts_client
        db = MagicMock()
        db.fetch = AsyncMock(return_value=[])
        db.fetchrow = AsyncMock(return_value=None)
        db.execute = AsyncMock()
        state.app_backend_db = db
        state.app_backend_available = True
        resp = await client.get("/api/alerts")
        assert resp.status_code == 200
        sql = str(db.fetch.await_args.args[0]).lower()
        assert "open" in sql
        assert "acknowledged" in sql
        assert "limit" in sql

    @pytest.mark.asyncio
    async def test_acknowledge_requires_admin_c7408_defect(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][defect] Non-admin callers cannot acknowledge alerts."""
        client, _ = alerts_client
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True):
            opened = await client.post(
                "/api/alerts/evaluate",
                json={
                    "industry": "manufacturing",
                    "readings": [
                        {
                            "sensor_id": "mfg-sns-cnc_1_spindle_temp",
                            "sensor_name": "cnc_1_spindle_temp",
                            "value": 91.0,
                        }
                    ],
                },
            )
        alert_id = opened.json()["transitions"][0]["alert_id"]
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=False):
            resp = await client.post(f"/api/alerts/{alert_id}/acknowledge")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_evaluate_requires_admin_c7408_defect(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][defect] Non-admin callers cannot mutate alerts via evaluate."""
        client, _ = alerts_client
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=False):
            resp = await client.post(
                "/api/alerts/evaluate",
                json={
                    "industry": "manufacturing",
                    "readings": [
                        {
                            "sensor_id": "mfg-sns-cnc_1_spindle_temp",
                            "sensor_name": "cnc_1_spindle_temp",
                            "value": 91.0,
                        }
                    ],
                },
            )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_evaluate_succeeds_for_admin_c7408_regression(
        self, alerts_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7408][regression] Admins can evaluate readings and open alerts."""
        client, _ = alerts_client
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True):
            resp = await client.post(
                "/api/alerts/evaluate",
                json={
                    "industry": "manufacturing",
                    "readings": [
                        {
                            "sensor_id": "mfg-sns-cnc_1_spindle_temp",
                            "sensor_name": "cnc_1_spindle_temp",
                            "value": 91.0,
                        }
                    ],
                },
            )
        assert resp.status_code == 200
        assert resp.json()["transitions"][0]["type"] == "alert_opened"
