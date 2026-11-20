"""Integration tests for health, metrics, and app-defaults endpoints."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import AsyncClient

from tests._fakes import fake_credential


class TestHealthEndpoint:
    """GET /api/health should return status and configuration info."""

    @pytest.mark.asyncio
    async def test_returns_200(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_contains_status_ok(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        body = resp.json()
        assert body["status"] == "ok"

    @pytest.mark.asyncio
    async def test_contains_db_available(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        body = resp.json()
        assert "db_available" in body

    @pytest.mark.asyncio
    async def test_contains_db_error(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        body = resp.json()
        assert "db_error" in body

    @pytest.mark.asyncio
    async def test_contains_industries(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        body = resp.json()
        assert "industries" in body
        assert body["industries"] == "pharmaceutical"

    @pytest.mark.asyncio
    async def test_contains_databricks_fields(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        body = resp.json()
        assert "databricks_client_id" in body
        assert "databricks_host" in body

    @pytest.mark.asyncio
    async def test_contains_zerobus_fields(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/health")
        body = resp.json()
        assert "zerobus_client_id" in body
        assert "zerobus_endpoint" in body


class TestMetricsEndpoint:
    """GET /metrics should return a simple health status."""

    @pytest.mark.asyncio
    async def test_returns_200(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/metrics")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_returns_healthy_status(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/metrics")
        body = resp.json()
        assert body["status"] == "healthy"


class TestAppDefaultsEndpoint:
    """GET /api/app-defaults returns workspace-level defaults."""

    @pytest.mark.asyncio
    async def test_returns_200(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/app-defaults")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_contains_expected_keys(self, test_client: AsyncClient) -> None:
        resp = await test_client.get("/api/app-defaults")
        body = resp.json()
        expected_keys = {
            "zerobus_endpoint",
            "zerobus_client_id",
            "zerobus_client_secret_set",
            "workspace_url",
            "warehouse_id",
        }
        assert expected_keys.issubset(body.keys())

    @pytest.mark.asyncio
    @patch.dict(
        "os.environ",
        {
            "ZEROBUS_SERVER_ENDPOINT": "https://zb.example.com",
            "ZEROBUS_CLIENT_ID": "legacy-zb-client",
            "ZEROBUS_CLIENT_SECRET": fake_credential("legacy-zb"),
            "DATABRICKS_CLIENT_ID": "app-client-123",
            "DATABRICKS_CLIENT_SECRET": fake_credential("app"),
            "DATABRICKS_HOST": "https://adb-999.azuredatabricks.net",
            "DATABRICKS_SQL_WAREHOUSE_ID": "warehouse-123",
        },
    )
    async def test_returns_env_values(self, test_client: AsyncClient) -> None:
        """[C7256][defect][regression] Zerobus uses one matching app-SP OAuth pair."""
        resp = await test_client.get("/api/app-defaults")
        body = resp.json()
        assert body["zerobus_endpoint"] == "https://zb.example.com"
        assert body["zerobus_client_id"] == "app-client-123"
        assert body["zerobus_client_secret_set"] is True
        assert body["workspace_url"] == "https://adb-999.azuredatabricks.net"
        assert body["warehouse_id"] == "warehouse-123"
