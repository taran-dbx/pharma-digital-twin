"""Tests for routes/industries.py — catalog + mode start/stop endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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


# ---------------------------------------------------------------------------
# GET /api/industries/catalog
# ---------------------------------------------------------------------------
class TestIndustriesCatalog:
    @pytest.mark.asyncio
    async def test_catalog_endpoint_returns_industries_and_is_admin(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        fake_catalog = [{"id": "pharmaceutical", "modes": []}]
        with (
            patch(
                "iot_twin.routes.industries.industry_service.get_catalog",
                AsyncMock(return_value=fake_catalog),
            ),
            patch(
                "iot_twin.routes.industries.app_permissions.is_admin", MagicMock(return_value=False)
            ),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/industries/catalog")

        assert resp.status_code == 200
        body = resp.json()
        assert body["is_admin"] is False
        assert body["industries"][0]["id"] == "pharmaceutical"

    @pytest.mark.asyncio
    async def test_catalog_endpoint_reports_admin_true(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch(
                "iot_twin.routes.industries.industry_service.get_catalog",
                AsyncMock(return_value=[]),
            ),
            patch(
                "iot_twin.routes.industries.app_permissions.is_admin", MagicMock(return_value=True)
            ),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/industries/catalog")

        assert resp.status_code == 200
        assert resp.json() == {"industries": [], "is_admin": True}


# ---------------------------------------------------------------------------
# POST /api/industries/{industry}/modes/{mode}/start
# ---------------------------------------------------------------------------
class TestStartIndustryMode:
    @pytest.mark.asyncio
    async def test_start_rejects_unknown_industry(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.start_mode", AsyncMock()
        ) as start_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/banking/modes/realtime/start")

        assert resp.status_code == 400
        assert "banking" in resp.json()["error"]
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_rejects_unknown_mode(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.start_mode", AsyncMock()
        ) as start_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/pharmaceutical/modes/nightly/start")

        assert resp.status_code == 400
        assert "nightly" in resp.json()["error"]
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_curated_industry_and_mode_calls_service(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        fake_result = {"pipeline": "iot-pharmaceutical-realtime", "status": "starting"}
        start_mock = AsyncMock(return_value=fake_result)
        with patch("iot_twin.routes.industries.industry_service.start_mode", start_mock):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/pharmaceutical/modes/realtime/start")

        assert resp.status_code == 200
        assert resp.json() == fake_result
        start_mock.assert_awaited_once_with(state, "pharmaceutical", "realtime")


# ---------------------------------------------------------------------------
# POST /api/industries/{industry}/modes/{mode}/stop
# ---------------------------------------------------------------------------
class TestStopIndustryMode:
    @pytest.mark.asyncio
    async def test_stop_rejects_unknown_industry(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.stop_mode", AsyncMock()
        ) as stop_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/banking/modes/realtime/stop")

        assert resp.status_code == 400
        stop_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_rejects_unknown_mode(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.stop_mode", AsyncMock()
        ) as stop_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/pharmaceutical/modes/nightly/stop")

        assert resp.status_code == 400
        stop_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_curated_industry_and_mode_calls_service(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        fake_result = {"pipeline": "iot-manufacturing-microbatch", "status": "idle"}
        stop_mock = AsyncMock(return_value=fake_result)
        with patch("iot_twin.routes.industries.industry_service.stop_mode", stop_mock):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/manufacturing/modes/microbatch/stop")

        assert resp.status_code == 200
        assert resp.json() == fake_result
        stop_mock.assert_awaited_once_with(state, "manufacturing", "microbatch")


# ---------------------------------------------------------------------------
# POST /api/industries/{industry}/modes/{mode}/config
# ---------------------------------------------------------------------------
class TestUpdateIndustryModeConfig:
    @pytest.mark.asyncio
    async def test_rejects_unknown_industry(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode", AsyncMock()
        ) as resolve_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/banking/modes/realtime/config",
                    json={"rate_hz": 5},
                )

        assert resp.status_code == 400
        resolve_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejects_unknown_mode(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode", AsyncMock()
        ) as resolve_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/nightly/config",
                    json={"rate_hz": 5},
                )

        assert resp.status_code == 400
        resolve_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_pipeline_returns_404(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode",
            AsyncMock(return_value=None),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/realtime/config",
                    json={"rate_hz": 5},
                )

        assert resp.status_code == 404
        assert "create one first" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_running_pipeline_returns_409(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        running_row = {"id": "pid-1", "status": "running"}
        with (
            patch(
                "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode",
                AsyncMock(return_value=running_row),
            ),
            patch(
                "iot_twin.routes.industries.industry_service.update_pipeline_config", AsyncMock()
            ) as update_mock,
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/realtime/config",
                    json={"rate_hz": 5},
                )

        assert resp.status_code == 409
        assert "stop the pipeline" in resp.json()["detail"]
        update_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_starting_pipeline_returns_409(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        starting_row = {"id": "pid-1", "status": "starting"}
        with (
            patch(
                "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode",
                AsyncMock(return_value=starting_row),
            ),
            patch(
                "iot_twin.routes.industries.industry_service.update_pipeline_config", AsyncMock()
            ) as update_mock,
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/realtime/config",
                    json={"rate_hz": 5},
                )

        assert resp.status_code == 409
        update_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stopped_pipeline_persists_config(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        stopped_row = {"id": "pid-1", "status": "stopped"}
        merged = {"rate_hz": 10, "capture_metrics": False}
        with (
            patch(
                "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode",
                AsyncMock(return_value=stopped_row),
            ),
            patch(
                "iot_twin.routes.industries.industry_service.update_pipeline_config",
                AsyncMock(return_value=merged),
            ) as update_mock,
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/realtime/config",
                    json={"rate_hz": 10, "capture_metrics": False},
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["config"] == merged
        assert body["applied"] == "on next (re)deploy"
        update_mock.assert_awaited_once_with(
            state, "pid-1", {"rate_hz": 10, "capture_metrics": False}
        )

    @pytest.mark.asyncio
    async def test_idle_pipeline_persists_config(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        idle_row = {"id": "pid-1", "status": "idle"}
        with (
            patch(
                "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode",
                AsyncMock(return_value=idle_row),
            ),
            patch(
                "iot_twin.routes.industries.industry_service.update_pipeline_config",
                AsyncMock(return_value={"rate_hz": 3}),
            ),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/realtime/config",
                    json={"rate_hz": 3},
                )

        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_zerobus_ignores_capture_metrics_but_notes_it(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        stopped_row = {"id": "pid-2", "status": "stopped"}
        with (
            patch(
                "iot_twin.routes.industries.industry_service.resolve_pipeline_for_mode",
                AsyncMock(return_value=stopped_row),
            ),
            patch(
                "iot_twin.routes.industries.industry_service.update_pipeline_config",
                AsyncMock(return_value={"rate_hz": 2}),
            ) as update_mock,
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/zerobus/config",
                    json={"rate_hz": 2, "capture_metrics": True},
                )

        assert resp.status_code == 200
        body = resp.json()
        assert "note" in body
        assert "zerobus" in body["note"]
        # capture_metrics must not be forwarded into the persisted patch.
        update_mock.assert_awaited_once_with(state, "pid-2", {"rate_hz": 2})
