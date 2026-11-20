"""Tests for routes/streaming_latency.py — the latency/throughput KPI endpoints.

Covers both endpoints' happy path, the defensive empty-table / no-rows /
inaccessible-system-table degradations (200 + nulled payload, never 500), and
the input-validation guards (bad mode / industry / no warehouse).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState

_UC_CATALOG = "example_eu_catalog"
_UC_SCHEMA = "rtm_lakets_zb"


def _build_app(state: AppState) -> FastAPI:
    from iot_twin.routes.streaming_latency import router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(router)
    return app


def _state() -> AppState:
    state = AppState()
    state.db = MagicMock()
    state.db.fetchrow = AsyncMock(return_value={"exists": 1})
    state.db_available = True
    state.db_error = None
    return state


def _dbsql_result(data: list[dict[str, Any]]) -> dict[str, Any]:
    return {"columns": list(data[0].keys()) if data else [], "data": data, "row_count": len(data)}


# ---------------------------------------------------------------------------
# GET /api/latency/streaming
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestStreamingLatency:
    async def test_happy_path_returns_latest_kpis_and_series(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)

        # Newest-first rows (as returned by ORDER BY timestamp DESC).
        rows = [
            {
                "batch_id": 42,
                "timestamp": "2026-07-06T10:05:00.000+00:00",
                "num_input_rows": 1000,
                "input_rps": 500.0,
                "processed_rps": 480.0,
                "trigger_ms": 2000.0,
                "e2e_p50": 120.0,
                "e2e_p99": 350.0,
                "processing_p50": 40.0,
                "processing_p99": 90.0,
                "queuing_p50": 15.0,
                "queuing_p99": 60.0,
            },
            {
                "batch_id": 41,
                "timestamp": "2026-07-06T10:04:00.000+00:00",
                "num_input_rows": 900,
                "input_rps": 450.0,
                "processed_rps": 470.0,
                "trigger_ms": 1900.0,
                "e2e_p50": 110.0,
                "e2e_p99": 300.0,
                "processing_p50": 38.0,
                "processing_p99": 88.0,
                "queuing_p50": 12.0,
                "queuing_p99": 55.0,
            },
        ]
        exec_mock = AsyncMock(return_value=_dbsql_result(rows))
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=realtime"
                )

        expected_table = f"{_UC_CATALOG}.{_UC_SCHEMA}.pharmaceutical_metrics"
        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is True
        assert body["table"] == expected_table
        assert body["mode"] == "realtime"
        assert body["last_batch_id"] == 42
        assert body["e2e_p50"] == 120.0
        assert body["e2e_p99"] == 350.0
        assert body["processing_p50"] == 40.0
        assert body["queuing_p99"] == 60.0
        assert body["input_rps"] == 500.0
        assert body["processed_rps"] == 480.0
        assert body["trigger_ms"] == 2000.0
        assert body["num_input_rows"] == 1000
        # Summary tiles use explicit, semantically named aliases from this
        # industry+mode-scoped metrics row. They must never fall back to the
        # process-global /api/sources rolling counters.
        assert body["rows_per_sec"] == 480.0
        assert body["processing_latency_p50_ms"] == 40.0
        assert body["processing_latency_p99_ms"] == 90.0
        assert body["spark_e2e_latency_p99_ms"] == 350.0
        assert body["throughput_basis"] == "spark_processed_rows_per_second"
        assert body["e2e_basis"] == "spark_source_available_to_trigger_complete"
        assert body["scope"] == {
            "industry": "pharmaceutical",
            "mode": "realtime",
        }
        # Series is ascending (oldest → newest) with one entry per row.
        assert len(body["series"]) == 2
        assert body["series"][0]["batch_id"] == 41
        assert body["series"][1]["batch_id"] == 42
        # SQL targets the shared one-schema metrics table with a mode filter.
        sql = exec_mock.await_args_list[0].args[0]
        assert expected_table in sql
        assert "mode" in sql
        assert "realtime" in sql

    async def test_latest_null_falls_through_to_prior_non_null(self) -> None:
        # The newest batch has a null e2e_p50 (idle trigger) — the KPI should
        # surface the most recent NON-null value from an earlier row.
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        rows = [
            {
                "batch_id": 2,
                "timestamp": "t2",
                "num_input_rows": None,
                "input_rps": None,
                "processed_rps": None,
                "trigger_ms": None,
                "e2e_p50": None,
                "e2e_p99": None,
                "processing_p50": None,
                "processing_p99": None,
                "queuing_p50": None,
                "queuing_p99": None,
            },
            {
                "batch_id": 1,
                "timestamp": "t1",
                "num_input_rows": 500,
                "input_rps": 250.0,
                "processed_rps": 240.0,
                "trigger_ms": 1000.0,
                "e2e_p50": 99.0,
                "e2e_p99": 199.0,
                "processing_p50": 30.0,
                "processing_p99": 70.0,
                "queuing_p50": 10.0,
                "queuing_p99": 40.0,
            },
        ]
        exec_mock = AsyncMock(return_value=_dbsql_result(rows))
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=energy&mode=microbatch"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["last_batch_id"] == 2  # latest row id preserved
        assert body["e2e_p50"] == 99.0  # pulled from the older non-null row
        assert body["input_rps"] == 250.0

    async def test_no_rows_returns_nulled_payload(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        exec_mock = AsyncMock(return_value=_dbsql_result([]))
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=realtime"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is False
        assert body["e2e_p50"] is None
        assert body["rows_per_sec"] is None
        assert body["processing_latency_p50_ms"] is None
        assert body["processing_latency_p99_ms"] is None
        assert body["spark_e2e_latency_p99_ms"] is None
        assert body["series"] == []
        assert "reason" in body

    async def test_missing_table_returns_nulled_payload_not_500(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        exec_mock = AsyncMock(
            side_effect=Exception(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view "
                f"`{_UC_CATALOG}`.`{_UC_SCHEMA}`.`pharmaceutical_metrics` cannot be found"
            )
        )
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=realtime"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is False
        assert body["e2e_p50"] is None
        assert "not provisioned" in body["reason"].lower()

    async def test_other_query_error_degrades_gracefully_not_500(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        exec_mock = AsyncMock(side_effect=Exception("connection reset by peer"))
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=microbatch"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is False
        assert "failed" in body["reason"].lower()

    async def test_invalid_mode_returns_400(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        with patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=zerobus"
                )
        assert resp.status_code == 400

    async def test_invalid_industry_returns_400(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        with patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=%21%21%21&mode=realtime"
                )
        assert resp.status_code == 400

    async def test_no_warehouse_returns_400(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id", AsyncMock(return_value="")
            ),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=realtime"
                )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /api/latency/zerobus
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestZerobusLatency:
    async def test_happy_path_derives_freshness_throughput_errors(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)

        summary = _dbsql_result(
            [
                {
                    "last_commit_time": "2026-07-06T10:05:00.000+00:00",
                    "committed_records_total": 600,
                    "committed_bytes_total": 120000,
                    "recent_error_count": 2,
                    "seconds_since_commit": 12.0,
                }
            ]
        )
        errors = _dbsql_result(
            [
                {"error_message": "transient ack timeout"},
                {"error_message": "retry succeeded"},
            ]
        )
        exec_mock = AsyncMock(side_effect=[summary, errors])
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/zerobus?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&minutes=60"
                )

        expected_table = f"{_UC_CATALOG}.{_UC_SCHEMA}.pharmaceutical_zerobus"
        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == expected_table
        assert body["has_data"] is True
        assert body["last_commit_time"] == "2026-07-06T10:05:00.000+00:00"
        assert body["seconds_since_commit"] == 12.0
        assert body["committed_records_total"] == 600
        assert body["committed_bytes_total"] == 120000
        # 600 records over 60 min => 10 rec/min.
        assert body["records_per_min"] == pytest.approx(10.0, abs=0.01)
        assert body["recent_error_count"] == 2
        assert body["recent_errors"] == ["transient ack timeout", "retry succeeded"]
        # The fully-qualified target is in the SQL filter.
        assert expected_table in exec_mock.await_args_list[0].args[0]

    async def test_no_rows_returns_zeroed_payload(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        exec_mock = AsyncMock(side_effect=[_dbsql_result([]), _dbsql_result([])])
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/zerobus?pipeline_id=00000000-0000-4000-8000-000000000001&industry=energy"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is False
        assert body["committed_records_total"] == 0
        assert body["records_per_min"] == 0.0
        assert body["recent_errors"] == []
        assert "reason" in body

    async def test_system_tables_inaccessible_degrades_not_500(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        exec_mock = AsyncMock(
            side_effect=Exception(
                "INSUFFICIENT_PERMISSIONS: cannot access system.lakeflow.zerobus_ingest"
            )
        )
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/zerobus?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is False
        assert body["committed_records_total"] == 0
        assert body["last_commit_time"] is None
        assert "not accessible" in body["reason"].lower()

    async def test_null_commit_but_records_still_has_data(self) -> None:
        # A window with committed records but a null MAX(commit_time) edge case
        # still counts as data present.
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        summary = _dbsql_result(
            [
                {
                    "last_commit_time": None,
                    "committed_records_total": 30,
                    "committed_bytes_total": 900,
                    "recent_error_count": 0,
                    "seconds_since_commit": None,
                }
            ]
        )
        exec_mock = AsyncMock(side_effect=[summary, _dbsql_result([])])
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id",
                AsyncMock(return_value="wh-1"),
            ),
            patch("iot_twin.routes.streaming_latency.execute_dbsql", exec_mock),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/zerobus?pipeline_id=00000000-0000-4000-8000-000000000001&industry=energy&minutes=30"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is True
        assert body["seconds_since_commit"] is None
        # 30 records over 30 min => 1 rec/min.
        assert body["records_per_min"] == pytest.approx(1.0, abs=0.01)

    async def test_invalid_industry_returns_400(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        with patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/zerobus?pipeline_id=00000000-0000-4000-8000-000000000001&industry=%21%21%21"
                )
        assert resp.status_code == 400

    async def test_no_warehouse_returns_400(self) -> None:
        import os

        state = _state()
        app = _build_app(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.streaming_latency.resolve_warehouse_id", AsyncMock(return_value="")
            ),
            patch.dict(os.environ, {"UC_CATALOG": _UC_CATALOG, "UC_SCHEMA": _UC_SCHEMA}),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/latency/zerobus?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical"
                )
        assert resp.status_code == 400
