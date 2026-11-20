"""Tests for routes/sensor_data.py — focused on the LakeTS analytics endpoints."""

from __future__ import annotations

import asyncio
import datetime as _dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState


def _build_app_with_state(state: AppState) -> FastAPI:
    from iot_twin.routes.sensor_data import router as sensor_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(sensor_router)
    return app


def _mock_db(
    fetchval_return: Any = None,
    fetch_return: list | None = None,
) -> MagicMock:
    db = MagicMock()
    db.fetchval = AsyncMock(return_value=fetchval_return)
    db.fetch = AsyncMock(return_value=fetch_return or [])
    db.execute = AsyncMock(return_value=None)
    db.fetchrow = AsyncMock(return_value=None)
    return db


def _state_with_db(db: MagicMock) -> AppState:
    state = AppState()
    state.db = db
    state.db_available = True
    state.db_error = None
    metadata_db = MagicMock()
    metadata_db.fetchrow = AsyncMock(return_value={"exists": 1})
    state.app_backend_db = metadata_db
    state.app_backend_available = True
    return state


def _mock_pipeline_lakebase(db: MagicMock) -> MagicMock:
    """Build a MagicMock that looks like a ``PipelineLakebaseHandle``."""
    lb = MagicMock()
    lb.fetch = db.fetch
    lb.fetchrow = db.fetchrow
    lb.fetchval = db.fetchval
    lb.execute = db.execute
    return lb


def _patch_pipeline_lakebase(db: MagicMock | None):
    """Patch ``get_pipeline_lakebase`` to return a handle wrapping ``db`` (or None)."""
    handle = _mock_pipeline_lakebase(db) if db is not None else None
    return patch(
        "iot_twin.routes.sensor_data.get_pipeline_lakebase", AsyncMock(return_value=handle)
    )


# ---------------------------------------------------------------------------
# Gapfill endpoint
# ---------------------------------------------------------------------------
class TestLaketsGapfill:
    @pytest.mark.asyncio
    async def test_lakets_not_installed_returns_reason(self) -> None:
        # fetchval raises — treated as "not installed".
        db = _mock_db()
        db.fetchval = AsyncMock(side_effect=Exception('relation "lakets._version" does not exist'))
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/gapfill?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert body["reason"].startswith("LakeTS probe failed")
        assert "lakets._version" in body["reason"]

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        bucket_b = _dt.datetime(2026, 4, 21, 10, 1, tzinfo=_dt.UTC)
        fetch_rows = [
            {
                "bucket": bucket_a,
                "sensor_name": "temp-01",
                "value": 22.5,
                "gapfilled": False,
            },
            {
                "bucket": bucket_b,
                "sensor_name": "temp-01",
                "value": 22.5,
                "gapfilled": True,
            },
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/gapfill?minutes=15")

        assert resp.status_code == 200
        body = resp.json()
        assert body["resolution"] == "1 min"
        assert body["minutes"] == 15
        assert isinstance(body["rows"], list)
        assert len(body["rows"]) == 2
        first = body["rows"][0]
        assert {"bucket", "sensor_name", "value"}.issubset(first.keys())
        assert first["sensor_name"] == "temp-01"
        assert first["value"] == 22.5
        assert first["gapfilled"] is False
        assert body["rows"][1]["gapfilled"] is True
        assert body["gapfilled_count"] == 1

    @pytest.mark.asyncio
    async def test_missing_minutes_use_bounded_bucket_spine_c7227_defect_regression(
        self,
    ) -> None:
        """[C7227][defect][regression] Generate and carry missing minute buckets."""
        db = _mock_db(fetchval_return="0.1.0", fetch_return=[])
        state = _state_with_db(db)
        app = _build_app_with_state(state)

        with _patch_pipeline_lakebase(db):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/api/pipelines/pipe-1/lakets/gapfill?minutes=30")

        assert response.status_code == 200
        query = " ".join(db.fetch.await_args.args[0].lower().split())
        assert "generate_series(" in query
        assert "cross join active_sensors" in query
        assert "left join base current_bucket" in query
        assert "left join lateral" in query
        assert "previous.bucket < spine.bucket" in query
        assert "make_interval(mins => $1 - 1)" in query
        assert "event_time >= bounds.window_start" in query
        assert "event_time < bounds.window_end + interval '1 minute'" in query
        assert "limit 5000" in query
        assert "lag(avg_value)" not in query
        assert "lakets.locf" not in query

    @pytest.mark.asyncio
    async def test_gapfill_timeout_returns_reason_c7271_defect(self) -> None:
        """[C7271][defect] Gapfill must fail closed instead of hanging the chart."""

        async def _slow(*_args, **_kwargs):
            await asyncio.sleep(1)
            return []

        db = _mock_db(fetchval_return="0.1.0")
        db.fetch = AsyncMock(side_effect=_slow)
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        with (
            patch("iot_twin.routes.sensor_data._LAKEBASE_PROBE_TIMEOUT_SECONDS", 0.01),
            _patch_pipeline_lakebase(db),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/gapfill?minutes=15")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert "timed out" in body["reason"].lower()


# ---------------------------------------------------------------------------
# Rate endpoint
# ---------------------------------------------------------------------------
class TestLaketsRate:
    @pytest.mark.asyncio
    async def test_rate_query_disables_counter_reset_handling_c7222_defect_regression(
        self,
    ) -> None:
        """[C7222][defect][regression] Preserve negative gauge rates."""
        db = _mock_db(fetchval_return="0.1.0", fetch_return=[])
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        rate_sql = db.fetch.await_args.args[0]
        normalized_sql = " ".join(rate_sql.upper().split())
        assert "LAKETS.RATE(" in normalized_sql
        assert "FALSE ) AS RATE" in normalized_sql

    @pytest.mark.asyncio
    async def test_rate_response_uses_frontend_key_and_preserves_null_c7222_defect_regression(
        self,
    ) -> None:
        """[C7222][defect][regression] Return `rate`, including null first values."""
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        bucket_b = _dt.datetime(2026, 4, 21, 10, 1, tzinfo=_dt.UTC)
        fetch_rows = [
            {
                "bucket": bucket_a,
                "sensor_name": "temperature-01",
                "rate": None,
                "value": None,
            },
            {
                "bucket": bucket_b,
                "sensor_name": "temperature-01",
                "rate": -0.25,
                "value": -0.25,
            },
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        assert resp.json()["rows"] == [
            {
                "bucket": str(bucket_a),
                "sensor_name": "temperature-01",
                "rate": None,
            },
            {
                "bucket": str(bucket_b),
                "sensor_name": "temperature-01",
                "rate": -0.25,
            },
        ]

    @pytest.mark.asyncio
    async def test_lakets_not_installed_returns_reason(self) -> None:
        db = _mock_db(fetchval_return=None)  # probe returns None
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert "not installed" in body["reason"].lower()

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        bucket = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        fetch_rows = [
            {"bucket": bucket, "sensor_name": "pressure-02", "rate": 0.15},
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["minutes"] == 30
        assert body["resolution"] == "1 min"
        assert len(body["rows"]) == 1
        first = body["rows"][0]
        assert first["sensor_name"] == "pressure-02"
        assert first["rate"] == 0.15


# ---------------------------------------------------------------------------
# Histogram endpoint
# ---------------------------------------------------------------------------
class TestLaketsHistogram:
    @pytest.mark.asyncio
    async def test_lakets_not_installed_returns_reason(self) -> None:
        db = _mock_db(fetchval_return=None)  # probe returns None
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/lakets/histogram?sensor=temp-01&minutes=30"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert "not installed" in body["reason"].lower()

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        fetch_rows = [
            {"bucket_idx": 0, "min_v": 0.0, "max_v": 10.0, "cnt": 5},
            {"bucket_idx": 1, "min_v": 0.0, "max_v": 10.0, "cnt": 12},
            {"bucket_idx": 2, "min_v": 0.0, "max_v": 10.0, "cnt": 3},
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/lakets/histogram?sensor=temp-01&minutes=30&buckets=20"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["resolution"] == "1 min"
        assert body["minutes"] == 30
        assert len(body["rows"]) == 3
        first = body["rows"][0]
        assert {"bucket_idx", "lower", "upper", "count"}.issubset(first.keys())
        assert first["bucket_idx"] == 0
        assert first["count"] == 5
        # First bucket width is (10-0)/20 = 0.5 — lower=0.0, upper=0.5
        assert first["lower"] == 0.0
        assert first["upper"] == 0.5

    @pytest.mark.asyncio
    async def test_missing_sensor_query_param_returns_422(self) -> None:
        db = _mock_db(fetchval_return="0.1.0")
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakets/histogram?minutes=30")

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Lakebase insertion-time performance endpoint
# ---------------------------------------------------------------------------
class TestLakebasePerf:
    @pytest.mark.asyncio
    async def test_lb_none_returns_graceful_zeros(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(None):  # lb resolves to None
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_per_sec"] == 0
        assert body["rows_per_min"] == 0
        # No lb resolvable => no lag samples, so p50/p95 are null (not fake 0).
        assert body["p50_insert_lag_ms"] is None
        assert body["p95_insert_lag_ms"] is None
        assert body["lag_sample_count"] == 0
        assert body["series"] == []
        # Self-describing lag metadata is always present.
        assert body["lag_basis"] == "producer_event_to_lakebase_insert"
        assert "Ingest lag" in body["lag_definition"]

    @pytest.mark.asyncio
    async def test_no_lag_samples_returns_null_p50_p95(self) -> None:
        # lb resolves, rows come back, but the sss source has zero e2e samples.
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        fetch_rows = [{"bucket": bucket_a, "cnt": 60}]
        db = _mock_db(fetch_return=fetch_rows)
        state = _state_with_db(db)
        # Do NOT seed any e2e latency samples on the sss source.

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        # Rates still computed from the SQL buckets.
        assert body["rows_per_min"] == 12
        # No samples => null p50/p95 and a zero sample count (UI shows "—").
        assert body["p50_insert_lag_ms"] is None
        assert body["p95_insert_lag_ms"] is None
        assert body["lag_sample_count"] == 0
        assert body["lag_basis"] == "producer_event_to_lakebase_insert"
        assert "Ingest lag" in body["lag_definition"]

    @pytest.mark.asyncio
    async def test_happy_path_computes_rates_and_keys(self) -> None:
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        bucket_b = _dt.datetime(2026, 4, 21, 10, 1, tzinfo=_dt.UTC)
        # Two 1-minute buckets: 120 rows + 60 rows over a 5-minute window.
        fetch_rows = [
            {"bucket": bucket_a, "cnt": 120},
            {"bucket": bucket_b, "cnt": 60},
        ]
        db = _mock_db(fetch_return=fetch_rows)
        state = _state_with_db(db)
        # Seed rolling e2e latency metrics on the sss source.
        src = state.sources["sss"]
        for v in (100.0, 200.0, 300.0, 400.0):
            src.record_write(1, 1.0, v)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        assert {
            "rows_per_sec",
            "rows_per_min",
            "p50_insert_lag_ms",
            "p95_insert_lag_ms",
            "lag_basis",
            "lag_definition",
            "lag_sample_count",
            "series",
        }.issubset(body.keys())
        # 180 rows over 5 minutes (300 s) => 0.6 rows/sec, 36 rows/min.
        assert body["rows_per_min"] == 36
        assert body["rows_per_sec"] == pytest.approx(0.6, abs=0.01)
        # Lag pulled from rolling e2e metrics (avg / p95).
        assert body["p50_insert_lag_ms"] > 0
        assert body["p95_insert_lag_ms"] > 0
        # Self-describing lag metadata + a non-zero sample count (4 seeded).
        assert body["lag_basis"] == "producer_event_to_lakebase_insert"
        assert "Ingest lag" in body["lag_definition"]
        assert body["lag_sample_count"] == 4
        # Series carries one entry per bucket with a per-sec rate.
        assert len(body["series"]) == 2
        first = body["series"][0]
        assert {"t", "rows_per_sec"}.issubset(first.keys())
        # 120 rows in a 60-second bucket => 2.0 rows/sec.
        assert first["rows_per_sec"] == pytest.approx(2.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_query_error_returns_500(self) -> None:
        db = _mock_db()
        db.fetch = AsyncMock(side_effect=Exception("boom"))
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 500
        assert "error" in resp.json()

    @pytest.mark.asyncio
    async def test_industry_param_resolves_via_get_industry_lakebase(self) -> None:
        """``industry`` must drive ``get_industry_lakebase``, not the legacy
        per-pipeline ``get_pipeline_lakebase`` lookup (industry-first pipelines
        have no row in the legacy ``pipelines`` table)."""
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        db = _mock_db(fetch_return=[{"bucket": bucket_a, "cnt": 60}])
        state = _state_with_db(db)
        lb = _mock_pipeline_lakebase(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.sensor_data.get_industry_lakebase", AsyncMock(return_value=lb)
            ) as mock_industry,
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_lakebase", AsyncMock(return_value=None)
            ) as mock_pipeline,
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/lakebase-perf?minutes=5&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_per_min"] == 12
        mock_industry.assert_awaited_once_with(state, "pharmaceutical")
        mock_pipeline.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_legacy_pipeline_id_with_industry_uses_legacy_model(self) -> None:
        """Industry query + legacy pipelines id authorizes via the legacy model."""
        from iot_twin.routes.sensor_data import _authorize_mode_pipeline

        state = _state_with_db(_mock_db())
        state.db.fetchrow = AsyncMock(
            return_value={
                "source_type": "sss",
                "source_config": {"rtm_sink": "lakebase"},
                "user_email": "anonymous",
            }
        )
        state.app_backend_db.fetchrow = AsyncMock(return_value=None)
        kind = await _authorize_mode_pipeline(
            state,
            "00000000-0000-4000-8000-000000000001",
            "pharmaceutical",
            "realtime",
            "anonymous",
        )
        assert kind == "legacy"

    @pytest.mark.asyncio
    async def test_deployment_id_with_industry_uses_deployment_model(self) -> None:
        """Industry-first deployment ids authorize via pipeline_deployment."""
        from iot_twin.routes.sensor_data import _authorize_mode_pipeline

        state = _state_with_db(_mock_db())
        state.app_backend_db.fetchrow = AsyncMock(return_value={"exists": 1})
        kind = await _authorize_mode_pipeline(
            state,
            "00000000-0000-4000-8000-000000000099",
            "pharmaceutical",
            "realtime",
            "anonymous",
        )
        assert kind == "deployment"

    @pytest.mark.asyncio
    async def test_industry_no_data_yet_returns_graceful_empty(self) -> None:
        """A fresh industry branch with no ``sensor_readings`` table yet (realtime
        not deployed) must return the graceful-empty payload, never a 500."""

        class _UndefinedRelation(Exception):
            sqlstate = "42P01"

        db = _mock_db()
        db.fetch = AsyncMock(side_effect=_UndefinedRelation("relation does not exist"))
        state = _state_with_db(db)
        lb = _mock_pipeline_lakebase(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with patch("iot_twin.routes.sensor_data.get_industry_lakebase", AsyncMock(return_value=lb)):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/lakebase-perf?minutes=5&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_per_sec"] == 0
        assert body["rows_per_min"] == 0
        assert body["series"] == []

    @pytest.mark.asyncio
    async def test_query_timeout_returns_graceful_zeros_c7270_defect(self) -> None:
        """[C7270][defect] Hung COUNT(*) probes must not look like zero writes."""

        async def _slow(*_args, **_kwargs):
            await asyncio.sleep(1)
            return []

        db = _mock_db()
        db.fetch = AsyncMock(side_effect=_slow)
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        with (
            patch("iot_twin.routes.sensor_data._LAKEBASE_PROBE_TIMEOUT_SECONDS", 0.01),
            _patch_pipeline_lakebase(db),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=15")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_per_sec"] == 0
        assert body["series"] == []

    @pytest.mark.asyncio
    async def test_counts_recent_chunks_instead_of_parent_c7270_defect(self) -> None:
        """[C7270][defect] Throughput must scan ChronoTable children, not the parent."""
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.UTC)
        db = _mock_db()
        db.fetch = AsyncMock(
            side_effect=[
                [{"chunk_name": "public.sensor_readings_20260421_100000"}],
                [{"bucket": bucket_a, "cnt": 180}],
            ]
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_per_min"] == 36
        metadata_sql = db.fetch.call_args_list[0].args[0].lower()
        count_sql = db.fetch.call_args_list[1].args[0].lower()
        assert "lakets._chunk_metadata" in metadata_sql
        assert "sensor_readings_20260421_100000" in count_sql
        assert "from sensor_readings\n" not in count_sql
        assert "from sensor_readings readings" not in count_sql


# ---------------------------------------------------------------------------
# Zerobus ingest-metrics endpoint
# ---------------------------------------------------------------------------
def _patch_zerobus_deps(
    catalog: str | None = "cat1",
    warehouse_id: str | None = "wh-123",
    source_config: dict | None = None,
):
    """Patch get_pipeline_catalog / resolve_warehouse_id / source_config lookup.

    Returns a list of context managers to enter via ``ExitStack`` / nested ``with``.
    """
    patches = [
        patch(
            "iot_twin.routes.sensor_data.get_pipeline_catalog",
            AsyncMock(return_value=catalog),
        ),
        patch(
            "iot_twin.routes.sensor_data.resolve_warehouse_id",
            AsyncMock(return_value=warehouse_id),
        ),
        patch(
            "iot_twin.routes.sensor_data._resolve_zerobus_target",
            AsyncMock(
                return_value=(source_config or {}).get("zerobus", {}).get("table_name")
                or f"{catalog}.raw.iot_sensor_data"
            ),
        ),
    ]
    return patches


class TestZerobusMetrics:
    @pytest.mark.asyncio
    async def test_industry_first_deployment_id_is_required(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        state.app_backend_db = _mock_db()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/not-a-deployment/zerobus-metrics?minutes=60&industry=pharmaceutical"
            )

        assert resp.status_code == 404
        assert resp.json()["error"] == "Industry deployment not found"

    @pytest.mark.asyncio
    async def test_industry_first_uses_prefixed_zerobus_table(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        state.app_backend_db = _mock_db()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        target = "catalog.schema.pharmaceutical_zerobus"

        with (
            patch(
                "iot_twin.routes.sensor_data._is_industry_deployment", AsyncMock(return_value=True)
            ),
            patch(
                "iot_twin.routes.sensor_data.industry_catalog_service.industry_table",
                MagicMock(return_value=target),
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data.execute_dbsql",
                AsyncMock(return_value={"columns": [], "data": [], "row_count": 0}),
            ),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/deployment-zb/zerobus-metrics"
                    "?minutes=60&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        assert resp.json()["table_name"] == target

    @pytest.mark.asyncio
    async def test_pipeline_unknown_returns_404(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value=None)
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_happy_path_computes_totals_series_status_errors(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)

        last_commit = "2026-04-21T10:05:00.000+00:00"
        # First execute_dbsql call -> ingest aggregate summary
        ingest_summary = {
            "columns": ["total_records", "total_bytes", "last_commit_time", "error_count"],
            "data": [
                {
                    "total_records": 600,
                    "total_bytes": 120000,
                    "last_commit_time": last_commit,
                    "error_count": 2,
                    "current_records_per_min": 240,
                }
            ],
            "row_count": 1,
        }
        # Second call -> per-minute series
        ingest_series = {
            "columns": ["bucket", "records"],
            "data": [
                {"bucket": "2026-04-21T10:00:00.000+00:00", "records": 120},
                {"bucket": "2026-04-21T10:01:00.000+00:00", "records": 240},
            ],
            "row_count": 2,
        }
        # Third call -> latest stream row (open, with errors)
        stream_result = {
            "columns": ["opened_time", "closed_time", "errors"],
            "data": [
                {
                    "opened_time": "2026-04-21T09:55:00.000+00:00",
                    "closed_time": None,
                    "errors": ["transient ack timeout", "retry succeeded"],
                }
            ],
            "row_count": 1,
        }

        exec_mock = AsyncMock(side_effect=[ingest_summary, ingest_series, stream_result])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.raw.iot_sensor_data"),
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == "cat1.raw.iot_sensor_data"
        assert body["total_records"] == 600
        assert body["total_bytes"] == 120000
        # Current throughput comes from the latest complete minute, matching
        # the rightmost complete bucket shown in the chart.
        assert body["records_per_min"] == pytest.approx(240.0, abs=0.01)
        assert body["records_per_sec"] == pytest.approx(4.0, abs=0.001)
        assert body["last_commit_time"] == last_commit
        assert body["error_count"] == 2
        # Stream is open (closed_time is null).
        assert body["stream_status"] == "open"
        # Series carries one entry per minute bucket with records_per_min.
        assert len(body["series"]) == 2
        assert {"t", "records_per_min"}.issubset(body["series"][0].keys())
        assert body["series"][0]["records_per_min"] == 120.0
        assert body["series"][1]["records_per_min"] == 240.0
        assert (
            "date_trunc('minute', now()) - INTERVAL 1 MINUTE"
            in (exec_mock.await_args_list[0].args[0])
        )
        # Errors flattened from the stream errors array (capped at ~5).
        assert "transient ack timeout" in body["recent_errors"]
        assert len(body["recent_errors"]) <= 5

    @pytest.mark.asyncio
    async def test_industry_first_returns_latest_sensor_readings_for_twin(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        state.app_backend_db = _mock_db()
        target = "catalog.schema.pharmaceutical_zerobus"
        summary = {
            "data": [
                {
                    "total_records": 600,
                    "total_bytes": 120000,
                    "last_commit_time": "2026-04-21T10:05:00.000+00:00",
                    "error_count": 0,
                    "current_records_per_min": 240,
                }
            ]
        }
        series = {"data": [{"bucket": "2026-04-21T10:04:00.000+00:00", "records": 240}]}
        stream = {
            "data": [
                {
                    "opened_time": "2026-04-21T09:55:00.000+00:00",
                    "closed_time": None,
                    "errors": [],
                }
            ]
        }
        latest = {
            "data": [
                {
                    "sensor_name": "bioreactor_1_temp",
                    "sensor_type": "temperature",
                    "value": 37.4,
                    "unit": "°C",
                    "fault_active": False,
                    "event_time": "2026-04-21T10:05:01.000+00:00",
                },
                {
                    "sensor_name": "wfi_flow",
                    "sensor_type": "flow",
                    "value": 58.0,
                    "unit": "L/min",
                    "fault_active": False,
                    "event_time": "2026-04-21T10:05:01.000+00:00",
                },
            ]
        }
        exec_mock = AsyncMock(side_effect=[summary, series, stream, latest])
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch(
                "iot_twin.routes.sensor_data._is_industry_deployment", AsyncMock(return_value=True)
            ),
            patch(
                "iot_twin.routes.sensor_data.industry_catalog_service.industry_table",
                MagicMock(return_value=target),
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/deployment-zb/zerobus-metrics"
                    "?minutes=60&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        assert resp.json()["latest_readings"] == latest["data"]
        assert target in exec_mock.await_args_list[3].args[0]
        assert "ROW_NUMBER()" in exec_mock.await_args_list[3].args[0]

    @pytest.mark.asyncio
    async def test_stalled_stream_reports_zero_current_rate(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        summary = {
            "data": [
                {
                    "total_records": 600,
                    "total_bytes": 120000,
                    "last_commit_time": "2026-04-21T09:55:00.000+00:00",
                    "error_count": 0,
                    "current_records_per_min": 0,
                }
            ]
        }
        old_series = {"data": [{"bucket": "2026-04-21T09:55:00.000+00:00", "records": 240}]}
        stream = {"data": [{"opened_time": "t0", "closed_time": None, "errors": []}]}
        exec_mock = AsyncMock(side_effect=[summary, old_series, stream])
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.raw.iot_sensor_data"),
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 200
        assert resp.json()["records_per_min"] == 0
        assert resp.json()["records_per_sec"] == 0

    @pytest.mark.asyncio
    async def test_latest_readings_failure_preserves_ingest_metrics(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        state.app_backend_db = _mock_db()
        summary = {
            "data": [
                {
                    "total_records": 600,
                    "total_bytes": 120000,
                    "last_commit_time": "2026-04-21T10:05:00.000+00:00",
                    "error_count": 0,
                    "current_records_per_min": 240,
                }
            ]
        }
        series = {"data": [{"bucket": "2026-04-21T10:04:00.000+00:00", "records": 240}]}
        stream = {"data": [{"opened_time": "t0", "closed_time": None, "errors": []}]}
        exec_mock = AsyncMock(
            side_effect=[summary, series, stream, Exception("Delta table unavailable")]
        )
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch(
                "iot_twin.routes.sensor_data._is_industry_deployment", AsyncMock(return_value=True)
            ),
            patch(
                "iot_twin.routes.sensor_data.industry_catalog_service.industry_table",
                MagicMock(return_value="catalog.schema.pharmaceutical_zerobus"),
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/deployment-zb/zerobus-metrics"
                    "?minutes=60&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        assert resp.json()["total_records"] == 600
        assert resp.json()["records_per_min"] == 240
        assert resp.json()["latest_readings"] == []

    @pytest.mark.asyncio
    async def test_closed_stream_status(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        ingest_summary = {
            "columns": ["total_records", "total_bytes", "last_commit_time", "error_count"],
            "data": [
                {
                    "total_records": 10,
                    "total_bytes": 100,
                    "last_commit_time": None,
                    "error_count": 0,
                }
            ],
            "row_count": 1,
        }
        ingest_series = {"columns": [], "data": [], "row_count": 0}
        stream_result = {
            "columns": ["opened_time", "closed_time", "errors"],
            "data": [
                {
                    "opened_time": "2026-04-21T09:55:00.000+00:00",
                    "closed_time": "2026-04-21T10:10:00.000+00:00",
                    "errors": None,
                }
            ],
            "row_count": 1,
        }
        exec_mock = AsyncMock(side_effect=[ingest_summary, ingest_series, stream_result])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.raw.iot_sensor_data"),
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 200
        body = resp.json()
        assert body["stream_status"] == "closed"
        assert body["recent_errors"] == []

    @pytest.mark.asyncio
    async def test_custom_table_name_from_source_config(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        ingest_summary = {
            "columns": ["total_records", "total_bytes", "last_commit_time", "error_count"],
            "data": [
                {"total_records": 5, "total_bytes": 50, "last_commit_time": None, "error_count": 0}
            ],
            "row_count": 1,
        }
        ingest_series = {"columns": [], "data": [], "row_count": 0}
        stream_result = {"columns": [], "data": [], "row_count": 0}
        exec_mock = AsyncMock(side_effect=[ingest_summary, ingest_series, stream_result])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.custom_schema.my_zerobus_table"),
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == "cat1.custom_schema.my_zerobus_table"
        # The fully-qualified target must be passed into the SQL filter.
        ingest_sql = exec_mock.await_args_list[0].args[0]
        assert "cat1.custom_schema.my_zerobus_table" in ingest_sql

    @pytest.mark.asyncio
    async def test_system_tables_inaccessible_returns_graceful_zeros(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        exec_mock = AsyncMock(
            side_effect=Exception(
                "INSUFFICIENT_PERMISSIONS: cannot access system.lakeflow.zerobus_ingest"
            )
        )

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.raw.iot_sensor_data"),
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == "cat1.raw.iot_sensor_data"
        assert body["total_records"] == 0
        assert body["total_bytes"] == 0
        assert body["records_per_sec"] == 0
        assert body["records_per_min"] == 0
        assert body["last_commit_time"] is None
        assert body["stream_status"] == "none"
        assert body["error_count"] == 0
        assert body["series"] == []
        assert body["recent_errors"] == []
        assert "reason" in body

    @pytest.mark.asyncio
    async def test_no_rows_returns_graceful_zeros_with_reason(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        # Ingest summary returns no data rows (no ingests in window).
        empty_summary = {"columns": [], "data": [], "row_count": 0}
        empty_series = {"columns": [], "data": [], "row_count": 0}
        empty_stream = {"columns": [], "data": [], "row_count": 0}
        exec_mock = AsyncMock(side_effect=[empty_summary, empty_series, empty_stream])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
            ),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.raw.iot_sensor_data"),
            ),
            patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_records"] == 0
        assert body["stream_status"] == "none"
        assert body["series"] == []
        assert body["recent_errors"] == []
        assert "reason" in body

    @pytest.mark.asyncio
    async def test_no_warehouse_returns_400(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
            ),
            patch("iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="")),
            patch(
                "iot_twin.routes.sensor_data._resolve_zerobus_target",
                AsyncMock(return_value="cat1.raw.iot_sensor_data"),
            ),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-1/zerobus-metrics?minutes=60")

        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# /api/sss/aggregates — graceful when the sensor_aggregates table is missing
# ---------------------------------------------------------------------------
class _UndefinedTable(Exception):
    """Stand-in for asyncpg UndefinedTableError (SQLSTATE 42P01)."""

    sqlstate = "42P01"


@pytest.mark.asyncio
class TestSssAggregates:
    async def test_missing_table_returns_empty_not_500(self) -> None:
        db = _mock_db()
        db.fetch = AsyncMock(
            side_effect=_UndefinedTable('relation "sensor_aggregates" does not exist')
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_other_db_error_still_500(self) -> None:
        db = _mock_db()
        db.fetch = AsyncMock(side_effect=Exception("connection reset by peer"))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 500

    async def test_happy_path_returns_rows(self) -> None:
        db = _mock_db(fetch_return=[{"sensor_name": "s1", "avg_value": 1.5}])
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 200
        assert resp.json() == [{"sensor_name": "s1", "avg_value": 1.5}]

    async def test_503_when_db_unavailable(self) -> None:
        state = AppState()
        state.db = None
        state.db_available = False
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Streaming metrics — SQL-safe fully-qualified metrics table
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestStreamingMetrics:
    async def test_hyphenated_catalog_is_quoted_in_both_queries_c7230(
        self,
    ) -> None:
        """[C7230][defect][regression] Quote each metrics-table FQN component."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        logical_table = "example-catalog.rtm_lakets_zb.pharmaceutical_metrics"
        empty_result = {"columns": [], "data": [], "row_count": 0}
        exec_mock = AsyncMock(side_effect=[empty_result, empty_result])

        with (
            patch(
                "iot_twin.services.industry.industry_catalog_service.industry_table",
                MagicMock(return_value=logical_table),
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch(
                "iot_twin.routes.sensor_data.execute_dbsql",
                exec_mock,
            ),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/streaming-metrics?industry=pharmaceutical&mode=realtime"
                )

        assert resp.status_code == 200
        assert resp.json()["table"] == logical_table
        assert exec_mock.await_count == 2
        expected_from = "FROM `example-catalog`.rtm_lakets_zb.pharmaceutical_metrics"
        assert expected_from in exec_mock.await_args_list[0].args[0]
        assert expected_from in exec_mock.await_args_list[1].args[0]
        assert f"FROM {logical_table}" not in exec_mock.await_args_list[0].args[0]
        assert f"FROM {logical_table}" not in exec_mock.await_args_list[1].args[0]


# ---------------------------------------------------------------------------
# DBSQL time-series — mode-aware per-mode table selection (the raw.sensor_data
# bug fix: realtime/microbatch/zerobus each land in their own schema/table).
# ---------------------------------------------------------------------------
_UC_CATALOG = "example_eu_catalog"
_UC_SCHEMA = "rtm_lakets_zb"


def _patch_dbsql_timeseries_deps(
    catalog: str | None = "iot_pharmaceutical",
    warehouse_id: str | None = "wh-1",
    exec_result: dict | None = None,
    exec_side_effect: Exception | None = None,
    is_admin: bool = False,
):
    """Build the standard patch set for /api/pipeline/{id}/dbsql-timeseries tests."""
    import os

    exec_mock = (
        AsyncMock(side_effect=exec_side_effect)
        if exec_side_effect is not None
        else AsyncMock(return_value=exec_result or {"columns": [], "data": [], "row_count": 0})
    )
    return [
        patch("iot_twin.routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value=catalog)),
        patch(
            "iot_twin.routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value=warehouse_id)
        ),
        patch("iot_twin.routes.sensor_data.execute_dbsql", exec_mock),
        patch("iot_twin.services.app_permissions.is_admin", return_value=is_admin),
        patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
    ], exec_mock


@pytest.mark.asyncio
class TestDbsqlTimeseries:
    async def test_realtime_mode_queries_cdf_synced_table_c7244(self) -> None:
        """[C7244][defect][regression] Industry-first realtime cold-tier DBSQL
        must target the CDF-managed destination
        ``{UC_CATALOG}.{UC_SCHEMA}.lb__shadow_<industry>_sensor_readings_history``, not
        the empty industry-prefixed ``{industry}_realtime`` table."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=realtime&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "realtime"
        expected_table = (
            f"{_UC_CATALOG}.{_UC_SCHEMA}.lb__shadow_pharmaceutical_sensor_readings_history"
        )
        assert body["table"] == expected_table
        assert "pharmaceutical_realtime" not in body["table"]
        sql = exec_mock.await_args_list[0].args[0]
        assert expected_table in sql
        assert "event_time" in sql
        assert "industry = 'pharmaceutical'" in sql
        assert "_pg_change_type IN ('insert', 'update_postimage')" in sql
        assert "update_preimage" not in sql
        assert "'delete'" not in sql

    async def test_realtime_latest_timestamp_uses_industry_scoped_dbsql_c7265(
        self,
    ) -> None:
        """[C7265][defect][regression] Read cold freshness without counting rows."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        chart_result = {"columns": [], "data": [], "row_count": 0}
        latest_result = {
            "columns": ["latest_event_at"],
            "data": [{"latest_event_at": "2026-09-22T10:29:55Z"}],
            "row_count": 1,
        }

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        exec_mock.side_effect = [chart_result, latest_result]
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries"
                    "?minutes=60&mode=realtime&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        assert resp.json()["latest_event_at"] == "2026-09-22T10:29:55Z"
        latest_sql = " ".join(exec_mock.await_args_list[1].args[0].split())
        assert latest_sql.startswith("SELECT MAX(event_time) AS latest_event_at FROM")
        assert "WHERE industry = 'pharmaceutical'" in latest_sql
        assert "_pg_change_type IN ('insert', 'update_postimage')" in latest_sql
        assert "COUNT(" not in latest_sql
        assert "TIMESTAMPADD" not in latest_sql

    async def test_realtime_rejects_non_curated_industry_before_sql_c7229(
        self,
    ) -> None:
        """[C7229][defect][security][regression] Reject unsafe industry input."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries",
                    params={
                        "minutes": 60,
                        "mode": "realtime",
                        "industry": "pharma'ceutical",
                    },
                )

        assert resp.status_code == 400
        assert resp.json()["error"] == "Unknown industry: pharma'ceutical"
        exec_mock.assert_not_awaited()

    async def test_realtime_normalizes_curated_industry_before_sql_c7229(
        self,
    ) -> None:
        """[C7229][defect][security][regression] Normalize curated industry input."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        chart_result = {"columns": [], "data": [], "row_count": 0}
        latest_result = {
            "columns": ["latest_event_at"],
            "data": [{"latest_event_at": "2026-09-22T10:29:55Z"}],
            "row_count": 1,
        }

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        exec_mock.side_effect = [chart_result, latest_result]
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries",
                    params={
                        "minutes": 60,
                        "mode": "realtime",
                        "industry": "  Pharmaceutical  ",
                    },
                )

        assert resp.status_code == 200
        assert resp.json()["latest_event_at"] == "2026-09-22T10:29:55Z"
        chart_sql = exec_mock.await_args_list[0].args[0]
        latest_sql = exec_mock.await_args_list[1].args[0]
        assert "industry = 'pharmaceutical'" in chart_sql
        assert "industry = 'pharmaceutical'" in latest_sql
        assert "Pharmaceutical" not in chart_sql
        assert "Pharmaceutical" not in latest_sql

    async def test_direct_write_latest_timestamp_is_industry_scoped_c7265(
        self,
    ) -> None:
        """[C7265][defect][regression] Direct-write freshness stays scoped."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        chart_result = {"columns": [], "data": [], "row_count": 0}
        latest_result = {
            "columns": ["latest_event_at"],
            "data": [{"latest_event_at": "2026-09-22T10:29:55Z"}],
            "row_count": 1,
        }

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        exec_mock.side_effect = [chart_result, latest_result]
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries"
                    "?minutes=15&mode=microbatch&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        assert resp.json()["latest_event_at"] == "2026-09-22T10:29:55Z"
        latest_sql = " ".join(exec_mock.await_args_list[1].args[0].split())
        assert latest_sql.startswith("SELECT MAX(event_time) AS latest_event_at FROM")
        assert "WHERE industry = 'pharmaceutical'" in latest_sql
        assert "_pg_change_type" not in latest_sql
        assert "COUNT(" not in latest_sql
        assert "TIMESTAMPADD" not in latest_sql

    async def test_latest_timestamp_failure_does_not_fail_chart_c7265(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """[C7265][defect][regression] Log freshness failure and preserve chart."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        chart_result = {
            "columns": ["sensor_name"],
            "data": [{"sensor_name": "temperature"}],
            "row_count": 1,
        }

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        exec_mock.side_effect = [chart_result, Exception("latest warehouse timeout")]
        with caplog.at_level("WARNING", logger="iot_digital_twin.routes.sensor_data"):
            with patches[0], patches[1], patches[2], patches[3], patches[4]:
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    resp = await client.get(
                        "/api/pipeline/pipe-1/dbsql-timeseries"
                        "?minutes=60&mode=realtime&industry=pharmaceutical"
                    )

        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        assert resp.json()["latest_event_at"] is None
        assert resp.json()["rows"] == [{"sensor_name": "temperature"}]
        assert "DBSQL latest-event query failed" in caplog.text
        assert "latest warehouse timeout" in caplog.text

    async def test_microbatch_mode_queries_one_schema_prefixed_table(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=microbatch&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "microbatch"
        expected_table = f"{_UC_CATALOG}.{_UC_SCHEMA}.pharmaceutical_microbatch"
        assert body["table"] == expected_table
        sql = exec_mock.await_args_list[0].args[0]
        assert expected_table in sql

    async def test_zerobus_mode_queries_one_schema_prefixed_table(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=zerobus&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "zerobus"
        expected_table = f"{_UC_CATALOG}.{_UC_SCHEMA}.pharmaceutical_zerobus"
        assert body["table"] == expected_table
        sql = exec_mock.await_args_list[0].args[0]
        assert expected_table in sql

    async def test_mode_inferred_from_pipeline_when_not_supplied(self) -> None:
        # No `mode` query param — falls back to `_infer_mode` on the pipeline row.
        # Without an `industry` param, uses the legacy catalog path.
        db = _mock_db()
        db.fetchrow = AsyncMock(return_value={"source_type": "zerobus", "source_config": {}})
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipeline/pipe-1/dbsql-timeseries?minutes=60")

        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "zerobus"
        # Legacy path (no industry param): iot_pharmaceutical.zerobus.iot_sensor_data
        assert body["table"] == "iot_pharmaceutical.zerobus.iot_sensor_data"

    async def test_mode_defaults_to_microbatch_when_inference_fails(self) -> None:
        # No `mode` param, and the pipeline row lookup yields nothing usable.
        db = _mock_db()
        db.fetchrow = AsyncMock(return_value=None)
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipeline/pipe-1/dbsql-timeseries?minutes=60")

        assert resp.status_code == 200
        assert resp.json()["mode"] == "microbatch"

    async def test_missing_table_returns_empty_not_500(self) -> None:
        # The mode's Delta table hasn't been created/populated yet.
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps(
            exec_side_effect=Exception(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view "
                f"`{_UC_CATALOG}`.`{_UC_SCHEMA}`.`pharmaceutical_zerobus` cannot be found"
            )
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=zerobus&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 0
        assert body["rows"] == []
        assert body["mode"] == "zerobus"

    async def test_other_query_error_still_500(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps(
            exec_side_effect=Exception("connection reset by peer")
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=microbatch&industry=pharmaceutical"
                )

        assert resp.status_code == 500

    async def test_pipeline_unknown_returns_404(self) -> None:
        # Without industry, the legacy catalog lookup returning None → 404.
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps(catalog=None)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=realtime"
                )

        assert resp.status_code == 404

    async def test_no_warehouse_returns_400(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps(warehouse_id="")
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/pipe-1/dbsql-timeseries?minutes=60&mode=realtime&industry=pharmaceutical"
                )

        assert resp.status_code == 400

    async def test_industry_param_resolves_one_schema_table_directly(self) -> None:
        """Industry-first pipelines: ``industry`` param drives the one-schema FQN,
        no legacy ``pipelines`` row needed (legacy catalog lookup skipped)."""
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        patches, exec_mock = _patch_dbsql_timeseries_deps(catalog=None)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipeline/none/dbsql-timeseries?minutes=60&mode=microbatch&industry=energy"
                )

        assert resp.status_code == 200
        body = resp.json()
        expected_table = f"{_UC_CATALOG}.{_UC_SCHEMA}.energy_microbatch"
        assert body["table"] == expected_table
        sql = exec_mock.await_args_list[0].args[0]
        assert expected_table in sql
