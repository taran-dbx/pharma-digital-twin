"""Tests for routes/lakets.py — /api/lakets/sync/check."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes.lakets import router as lakets_router
from iot_twin.services.lakets import lakets_service
from tests._fakes import fake_credential

DEPLOYMENT_ID = "00000000-0000-0000-0000-000000000001"


@pytest_asyncio.fixture()
async def client():
    app = FastAPI()
    app.include_router(lakets_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _mock_lb(
    fetch_return=None, fetchval_return=None, fetch_side=None, fetchval_side=None
) -> MagicMock:
    lb = MagicMock()
    lb.fetch = AsyncMock(return_value=fetch_return or [], side_effect=fetch_side)
    lb.fetchval = AsyncMock(return_value=fetchval_return, side_effect=fetchval_side)
    return lb


def _build_app_with_state(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(lakets_router)
    return app


def _patch_pipeline_lakebase(lb):
    return patch("iot_twin.routes.lakets.get_pipeline_lakebase", AsyncMock(return_value=lb))


def _ready_conn() -> MagicMock:
    conn = MagicMock()
    conn.fetchval = AsyncMock(
        side_effect=[
            "databricks_postgres",  # current_database
            True,  # wal2delta available
            "0.1.0",  # wal2delta installed
            True,  # lakets.enable_sync
        ]
    )
    conn.close = AsyncMock(return_value=None)
    return conn


class TestSyncCheckRoute:
    @pytest.mark.asyncio
    async def test_direct_mode_returns_ready(self, client: AsyncClient) -> None:
        conn = _ready_conn()
        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            response = await client.post(
                "/api/lakets/sync/check",
                json={
                    "lakebase": {
                        "mode": "direct",
                        "host": "h",
                        "user": "u",
                        "password": fake_credential("lakebase"),
                    },
                },
            )
        assert response.status_code == 200
        body = response.json()
        assert body["ready_to_enable"] is True
        assert body["current_database"] == "databricks_postgres"

    @pytest.mark.asyncio
    async def test_returns_warning_when_not_ready(self, client: AsyncClient) -> None:
        conn = MagicMock()
        conn.fetchval = AsyncMock(
            side_effect=[
                "custom_db",
                True,
                "0.1.0",
                True,
            ]
        )
        conn.close = AsyncMock(return_value=None)
        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            response = await client.post(
                "/api/lakets/sync/check",
                json={
                    "lakebase": {
                        "mode": "direct",
                        "host": "h",
                        "user": "u",
                        "password": fake_credential("lakebase"),
                    },
                },
            )
        body = response.json()
        assert body["ready_to_enable"] is False
        assert "databricks_postgres" in body["reason"]

    @pytest.mark.asyncio
    async def test_connection_failure_surfaces_as_not_ready(self, client: AsyncClient) -> None:
        with patch(
            "asyncpg.connect",
            new=AsyncMock(side_effect=RuntimeError("host unreachable")),
        ):
            response = await client.post(
                "/api/lakets/sync/check",
                json={
                    "lakebase": {
                        "mode": "direct",
                        "host": "h",
                        "user": "u",
                        "password": fake_credential("lakebase"),
                    },
                },
            )
        body = response.json()
        assert body["ready_to_enable"] is False
        assert "host unreachable" in body["error"]

    @pytest.mark.asyncio
    async def test_autoscale_missing_project_id_surfaces_error(self, client: AsyncClient) -> None:
        response = await client.post(
            "/api/lakets/sync/check",
            json={
                "lakebase": {"mode": "autoscale"},
            },
        )
        body = response.json()
        assert body["ready_to_enable"] is False
        assert "project_id" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self, client: AsyncClient) -> None:
        response = await client.post(
            "/api/lakets/sync/check",
            content="not json",
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 400


class TestCdfStatus:
    @pytest.mark.asyncio
    async def test_lb_none_returns_none_status(self) -> None:
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(None):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "NONE"
        assert body["enabled"] is False
        assert body["streaming"] is False

    @pytest.mark.asyncio
    async def test_streaming_reports_latest_hot_timestamp_c7265(self) -> None:
        """[C7265][defect][regression] CDF status uses a cheap latest-row probe."""
        latest = datetime(2026, 9, 22, 10, 30, tzinfo=UTC)
        lb = _mock_lb(
            fetch_return=[{"status": "STREAMING"}],
            fetchval_return=latest,
        )
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "STREAMING"
        assert body["enabled"] is True
        assert body["streaming"] is True
        assert body["hot_latest_at"] == "2026-09-22T10:30:00+00:00"
        assert "hot_rows" not in body
        latest_sql = " ".join(lb.fetchval.await_args.args[0].split())
        assert latest_sql == "SELECT MAX(event_time) FROM sensor_readings"
        assert "COUNT(" not in latest_sql

    @pytest.mark.asyncio
    async def test_hot_timestamp_timeout_preserves_streaming_status_c7265(self) -> None:
        """[C7265][defect][regression] Freshness timeout cannot disable CDF."""

        async def slow_latest_timestamp(*_args, **_kwargs):
            await asyncio.sleep(1)

        lb = _mock_lb(
            fetch_return=[{"status": "STREAMING"}],
            fetchval_side=slow_latest_timestamp,
        )
        state = AppState()
        app = _build_app_with_state(state)
        with (
            _patch_pipeline_lakebase(lb),
            patch("iot_twin.routes.lakets._LAKEBASE_PROBE_TIMEOUT_SECONDS", 0.01),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/api/pipelines/pipe-1/cdf-status")

        assert response.status_code == 200
        assert response.json() == {
            "enabled": True,
            "streaming": True,
            "status": "STREAMING",
            "reason": "wal2delta tables report status=STREAMING",
            "hot_latest_at": None,
        }

    @pytest.mark.asyncio
    async def test_legacy_status_uses_registered_shadow_c7256(self) -> None:
        """Legacy status never scans unrelated CDF rows [C7256][defect][regression]."""
        lb = _mock_lb(
            fetch_return=[
                {
                    "status": "STREAMING",
                    "shadow_table": "_shadow_sensor_readings",
                }
            ],
            fetchval_return=12,
        )
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/api/pipelines/legacy-pipeline/cdf-status")

        query = lb.fetch.await_args.args[0]
        assert response.json()["status"] == "STREAMING"
        assert "lakets._chronotable_registry" in query
        assert "LIMIT" not in query
        assert lb.fetch.await_args.args[1:] == ("lakets_cdf",)

    @pytest.mark.asyncio
    async def test_snapshotting_status_enabled_not_streaming(self) -> None:
        lb = _mock_lb(
            fetch_return=[{"status": "SNAPSHOTTING"}],
            fetchval_return=10,
        )
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        body = resp.json()
        assert body["status"] == "SNAPSHOTTING"
        assert body["enabled"] is True
        assert body["streaming"] is False

    @pytest.mark.asyncio
    async def test_query_error_fails_closed_to_none(self) -> None:
        lb = _mock_lb(fetch_side=Exception('relation "wal2delta.tables" does not exist'))
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "NONE"
        assert body["enabled"] is False
        assert body["streaming"] is False
        assert "reason" in body

    @pytest.mark.asyncio
    async def test_industry_status_filters_exact_shadow_c7252(self) -> None:
        """A sibling STREAMING shadow cannot satisfy status [C7252][defect][regression]."""
        lb = _mock_lb(fetchval_return=10)

        async def fetch(_query, *args):
            if args == (
                "lakets_cdf",
                "_shadow_manufacturing_sensor_readings",
            ):
                return []
            return [{"status": "STREAMING"}]

        lb.fetch = AsyncMock(side_effect=fetch)
        state = AppState()
        app = _build_app_with_state(state)
        with (
            patch(
                "iot_twin.routes.lakets.get_industry_lakebase",
                new=AsyncMock(return_value=lb),
            ),
            patch(
                "iot_twin.routes.lakets._is_realtime_industry_deployment",
                new=AsyncMock(return_value=True),
            ),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/pipelines/00000000-0000-4000-8000-000000000001/cdf-status",
                    params={"industry": "manufacturing"},
                )

        assert response.status_code == 200
        assert response.json()["status"] == "NONE"
        assert lb.fetch.await_args.args[1:] == (
            "lakets_cdf",
            "_shadow_manufacturing_sensor_readings",
        )

    @pytest.mark.asyncio
    async def test_cdf_status_timeout_fails_closed_c7263(self) -> None:
        """[C7263][defect][regression] CDF status never hangs the app proxy."""
        import asyncio

        async def hang(*_args, **_kwargs):
            await asyncio.sleep(60)
            return None

        state = AppState()
        app = _build_app_with_state(state)
        with (
            patch(
                "iot_twin.routes.lakets.get_industry_lakebase",
                new=AsyncMock(side_effect=hang),
            ),
            patch(
                "iot_twin.routes.lakets._is_realtime_industry_deployment",
                new=AsyncMock(return_value=True),
            ),
            patch("iot_twin.routes.lakets._LAKEBASE_RESOLVE_TIMEOUT_SECONDS", 0.05),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await asyncio.wait_for(
                    client.get(
                        "/api/pipelines/00000000-0000-4000-8000-000000000001/cdf-status",
                        params={"industry": "manufacturing"},
                    ),
                    timeout=2,
                )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NONE"
        assert body["enabled"] is False
        assert "timeout" in body["reason"].lower()


class TestLaketsDashboard:
    """Consolidated LakeTS observability endpoint contract."""

    @pytest.mark.asyncio
    async def test_resolves_industry_lakebase_and_returns_truthful_sections_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Resolve industry Lakebase and expose all sections."""
        state = AppState()
        state.app_backend_db = MagicMock()
        state.app_backend_db.fetchrow = AsyncMock(return_value={"exists": 1})
        app = _build_app_with_state(state)
        lb = MagicMock(name="industry_lakebase")
        dashboard = {
            "partitions": {
                "value": [{"name": "_chunk_42", "status": "active"}],
                "reason": None,
            },
            "current_partition": {
                "value": {"name": "_chunk_42", "status": "active"},
                "reason": None,
            },
            "tiering": {
                "value": None,
                "reason": "No enabled tiering policy",
            },
            "rollup": {
                "value": {"name": "sensor_1m", "bucket": "1 minute"},
                "reason": None,
            },
            "query_comparison": {
                "value": None,
                "reason": "Insufficient query samples",
            },
            "data_quality": {
                "value": None,
                "reason": "No readings in the lookback window",
            },
            "lvc": {
                "value": {"enabled": True, "cached_series": 12},
                "reason": None,
            },
            "cdf": {
                "value": {"hot_latest": "2026-09-01T08:00:00+00:00"},
                "reason": "CDF streaming WAL position is unavailable",
            },
        }
        industry_resolver = AsyncMock(return_value=lb)
        legacy_resolver = AsyncMock()
        dashboard_service = AsyncMock(return_value=dashboard)

        with (
            patch(
                "iot_twin.routes.lakets.get_industry_lakebase",
                industry_resolver,
            ),
            patch(
                "iot_twin.routes.lakets.get_pipeline_lakebase",
                legacy_resolver,
            ),
            patch(
                "iot_twin.routes.lakets.lakets_service.get_dashboard",
                dashboard_service,
                create=True,
            ),
        ):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get(
                    f"/api/pipelines/{DEPLOYMENT_ID}/lakets/dashboard?industry=pharmaceutical"
                )

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body == dashboard
        state.app_backend_db.fetchrow.assert_awaited_once()
        assert state.app_backend_db.fetchrow.await_args.args[1:] == (
            DEPLOYMENT_ID,
            "pharmaceutical",
        )
        industry_resolver.assert_awaited_once_with(state, "pharmaceutical")
        legacy_resolver.assert_not_awaited()
        dashboard_service.assert_awaited_once_with(lb)

        for name in ("tiering", "query_comparison", "data_quality"):
            assert body[name]["value"] is None
            assert body[name]["reason"]

    @pytest.mark.asyncio
    async def test_invalid_industry_is_rejected_before_resolution_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Reject unknown or malformed industry identifiers."""
        state = AppState()
        state.app_backend_db = MagicMock()
        resolver = AsyncMock()
        app = _build_app_with_state(state)

        with patch("iot_twin.routes.lakets.get_industry_lakebase", resolver):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                unknown = await c.get("/api/pipelines/pipe-1/lakets/dashboard?industry=pharma")
                malformed = await c.get(
                    "/api/pipelines/pipe-1/lakets/dashboard?industry=pharmaceutical%2F.."
                )

        assert unknown.status_code == 400
        assert malformed.status_code == 400
        resolver.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unbound_realtime_deployment_returns_404_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Require matching realtime deployment metadata."""
        state = AppState()
        state.app_backend_db = MagicMock()
        state.app_backend_db.fetchrow = AsyncMock(return_value=None)
        resolver = AsyncMock()
        app = _build_app_with_state(state)

        with patch("iot_twin.routes.lakets.get_industry_lakebase", resolver):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get(
                    f"/api/pipelines/{DEPLOYMENT_ID}/lakets/dashboard?industry=pharmaceutical"
                )

        assert resp.status_code == 404
        assert resp.json()["error"] == "Realtime industry deployment not found"
        resolver.assert_not_awaited()
        binding_sql = state.app_backend_db.fetchrow.await_args.args[0]
        assert "pd.mode = 'realtime'" in binding_sql
        assert "ps.industry = $2" in binding_sql

    @pytest.mark.asyncio
    async def test_invalid_deployment_id_returns_400_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Reject malformed deployment ids before DB access."""
        state = AppState()
        state.app_backend_db = MagicMock()
        state.app_backend_db.fetchrow = AsyncMock()
        resolver = AsyncMock()
        app = _build_app_with_state(state)

        with patch("iot_twin.routes.lakets.get_industry_lakebase", resolver):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get(
                    "/api/pipelines/not-a-uuid/lakets/dashboard?industry=pharmaceutical"
                )

        assert resp.status_code == 400
        state.app_backend_db.fetchrow.assert_not_awaited()
        resolver.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_metadata_failure_returns_503_c7225_regression(self) -> None:
        """[C7225][regression] Distinguish metadata outages from missing rows."""
        state = AppState()
        state.app_backend_db = MagicMock()
        state.app_backend_db.fetchrow = AsyncMock(
            side_effect=RuntimeError("connection details must stay private")
        )
        resolver = AsyncMock()
        app = _build_app_with_state(state)

        with patch("iot_twin.routes.lakets.get_industry_lakebase", resolver):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get(
                    f"/api/pipelines/{DEPLOYMENT_ID}/lakets/dashboard?industry=pharmaceutical"
                )

        assert resp.status_code == 503
        assert resp.json() == {"error": "Pipeline metadata unavailable"}
        assert "connection details" not in resp.text
        resolver.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolvable_lakebase_returns_every_section_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Keep dashboard shape when Lakebase is unavailable."""
        state = AppState()
        app = _build_app_with_state(state)

        with patch(
            "iot_twin.routes.lakets.get_pipeline_lakebase",
            AsyncMock(return_value=None),
        ):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                resp = await c.get("/api/pipelines/pipe-1/lakets/dashboard")

        assert resp.status_code == 200
        body = resp.json()
        assert set(body) == set(lakets_service.DASHBOARD_SECTIONS)
        assert all(section["value"] is None for section in body.values())
        assert all(section["reason"] for section in body.values())
