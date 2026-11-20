"""Protected intelligence catalog sync endpoint [qa]."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes.intelligence import router as intelligence_router


def _build_app(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(intelligence_router)
    return app


@pytest_asyncio.fixture()
async def intel_client() -> AsyncGenerator[tuple[AsyncClient, AppState], None]:
    state = AppState()
    app = _build_app(state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, state


class TestIntelligenceRoutes:
    @pytest.mark.asyncio
    async def test_sync_requires_admin_c7504_smoke(
        self, intel_client: tuple[AsyncClient, AppState]
    ) -> None:
        """[C7504][smoke] Sync is admin-gated."""
        client, _ = intel_client
        with patch("iot_twin.routes.intelligence.app_permissions.is_admin", return_value=False):
            resp = await client.post("/api/admin/intelligence/sync")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_sync_calls_service_c7504_regression(
        self, intel_client: tuple[AsyncClient, AppState], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7504][regression] Admin sync provisions and loads reference tables."""
        monkeypatch.setenv("UC_CATALOG", "test_catalog")
        monkeypatch.setenv("UC_SCHEMA", "test_schema")
        client, _ = intel_client
        with (
            patch("iot_twin.routes.intelligence.app_permissions.is_admin", return_value=True),
            patch(
                "iot_twin.routes.intelligence.intelligence_catalog_service.provision_reference_tables",
                new=AsyncMock(return_value={"tables": {"assets": "c.s.intelligence_assets"}}),
            ) as provision,
            patch(
                "iot_twin.routes.intelligence.intelligence_catalog_service.sync_reference_data",
                new=AsyncMock(return_value={"rows": {"assets": 4}}),
            ) as sync,
        ):
            resp = await client.post("/api/admin/intelligence/sync")
        assert resp.status_code == 200
        body = resp.json()
        assert body["provision"]["tables"]["assets"].endswith("intelligence_assets")
        assert body["sync"]["rows"]["assets"] == 4
        provision.assert_awaited()
        sync.assert_awaited()
        assert "contract" in body
        assert "freshness" in body["contract"]

    @pytest.mark.asyncio
    async def test_status_exposes_snapshot_freshness_c7505_usability(
        self, intel_client: tuple[AsyncClient, AppState], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7505][usability] Status reports snapshot freshness without requiring a full sync."""
        monkeypatch.setenv("UC_CATALOG", "test_catalog")
        monkeypatch.setenv("UC_SCHEMA", "test_schema")
        client, _ = intel_client
        with patch("iot_twin.routes.intelligence.app_permissions.is_admin", return_value=True):
            resp = await client.get("/api/admin/intelligence/status")
        assert resp.status_code == 200
        body = resp.json()
        assert "freshness" in body["contract"]
        assert "last_sync_at" in body["contract"]
        assert "last_error" in body["contract"]

    @pytest.mark.asyncio
    async def test_status_disabled_without_uc_config_c7505_defect(
        self, intel_client: tuple[AsyncClient, AppState], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7505][defect] Intelligence status returns a disabled contract when UC is absent."""
        monkeypatch.delenv("UC_CATALOG", raising=False)
        monkeypatch.delenv("UC_SCHEMA", raising=False)
        client, _ = intel_client
        with patch("iot_twin.routes.intelligence.app_permissions.is_admin", return_value=True):
            resp = await client.get("/api/admin/intelligence/status")
        assert resp.status_code == 200
        contract = resp.json()["contract"]
        assert contract["freshness"] == "disabled"
        assert contract["access"] == "read_only"
