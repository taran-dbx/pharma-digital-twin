"""Read-only industry asset/reference APIs and simulator industry mapping."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState


def _build_app_with_state(state: AppState) -> FastAPI:
    from iot_twin.routes.industries import router as industries_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(industries_router)
    return app


def _state() -> AppState:
    state = AppState()
    state.db = MagicMock()
    state.db_available = True
    state.db_error = None
    return state


def _generator(records: list[SimpleNamespace], sensor_count: int = 1) -> MagicMock:
    gen = MagicMock()
    gen.tick.return_value = records
    gen.sensor_count = sensor_count
    gen._sensors = []
    return gen


class TestIndustryAssetsApi:
    @pytest.mark.asyncio
    async def test_assets_endpoint_returns_hierarchy_c7307_smoke(self) -> None:
        """[C7307][smoke] Assets API is read-only catalog data."""
        app = _build_app_with_state(_state())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/industries/manufacturing/assets")

        assert resp.status_code == 200
        body = resp.json()
        assert body["industry"] == "manufacturing"
        assert body["simulator_industry"] == "manufacturing"
        assert body["sites"]
        assert body["sites"][0]["site_id"] == "mfg-site-detroit"
        assert body["sites"][0]["assets"][0]["asset_id"] == "mfg-asset-cnc-1"
        assert "POST" not in (resp.headers.get("allow") or "")

    @pytest.mark.asyncio
    async def test_energy_assets_keep_record_industry_c7302_regression(self) -> None:
        """[C7302][regression] Energy assets expose electric_power alias only for the simulator."""
        app = _build_app_with_state(_state())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/industries/energy/assets")

        assert resp.status_code == 200
        body = resp.json()
        assert body["industry"] == "energy"
        assert body["simulator_industry"] == "electric_power"
        assert body["record_industry"] == "energy"

    @pytest.mark.asyncio
    async def test_reference_endpoint_returns_kpis_alerts_maintenance_c7308_smoke(
        self,
    ) -> None:
        """[C7308][smoke] Reference API returns KPI, alert, and maintenance fixtures."""
        app = _build_app_with_state(_state())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/industries/pharmaceutical/reference")

        assert resp.status_code == 200
        body = resp.json()
        assert body["industry"] == "pharmaceutical"
        assert body["kpis"]
        assert body["alert_rules"]
        assert body["maintenance_history"]
        assert "fault_code" in body["alert_rules"][0]

    @pytest.mark.asyncio
    async def test_unknown_industry_assets_are_rejected_c7307_defect(self) -> None:
        """[C7307][defect] Unknown industries do not invent asset trees."""
        app = _build_app_with_state(_state())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/industries/banking/assets")

        assert resp.status_code == 400
        assert "banking" in resp.json()["error"]


class TestSimulatorFacingEnergyAlias:
    @pytest.mark.asyncio
    async def test_energy_sensors_use_electric_power_generator_c7302_regression(
        self,
    ) -> None:
        """[C7302][regression] /sensors instantiates the electric_power generator for energy."""
        record = SimpleNamespace(
            sensor_name="gt_1_power_output",
            industry="electric_power",
            sensor_type="power",
            unit="MW",
            min_value=0,
            max_value=100,
            nominal_value=50,
        )
        gen = _generator([record])
        app = _build_app_with_state(_state())
        transport = ASGITransport(app=app)
        with patch("iot_twin.routes.industries.DataGenerator", return_value=gen) as ctor:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/industries/energy/sensors")

        assert resp.status_code == 200
        ctor.assert_called_once_with(industries=["electric_power"])
        assert resp.json()[0]["name"] == "gt_1_power_output"

    @pytest.mark.asyncio
    async def test_generate_sample_rewrites_energy_records_c7302_regression(self) -> None:
        """[C7302][regression] Sample ticks keep record industry energy."""
        record = SimpleNamespace(
            sensor_name="gt_1_power_output",
            industry="electric_power",
            value=42.125,
            unit="MW",
            sensor_type="power",
            timestamp=1.0,
            fault_active=False,
        )
        gen = _generator([record], sensor_count=1)
        app = _build_app_with_state(_state())
        transport = ASGITransport(app=app)
        with patch("iot_twin.routes.industries.DataGenerator", return_value=gen) as ctor:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/generate-sample",
                    json={"industry": "energy", "ticks": 1},
                )

        assert resp.status_code == 200
        ctor.assert_called_once_with(industries=["electric_power"])
        body = resp.json()
        assert body["industry"] == "energy"
        assert body["records"][0]["industry"] == "energy"
        assert body["records"][0]["sensor_name"] == "gt_1_power_output"
