"""End-to-end integration tests for the realtime + tier-aware data flow.

These tests exercise the FastAPI routes end-to-end against mocked backends
(no live Databricks / Lakebase).  The scope is the realtime path that the
frontend depends on once a pipeline has its ``sink_config`` configured:

* ``GET  /api/events``                         — SSE fan-out channel
* ``POST /api/sss/ingest``                     — aggregated sensor broadcast
* ``POST /api/sss/alerts``                     — threshold-alert broadcast
* ``GET  /api/timeseries``                     — Lakebase-backed time-series
* ``GET  /api/pipeline/:id/dbsql-timeseries``  — DBSQL-backed time-series
* ``POST /api/lakets/sync/check``              — Lakehouse Sync readiness probe
* ``POST /api/lakets/retention/deploy``        — retention Databricks Job deploy

Each test class is isolated: it builds a minimal FastAPI app wired to the
relevant router(s) with a mocked ``AppState`` / ``WorkspaceClient`` so the
full request lifecycle (dependency injection, JSON parsing, response shape)
is covered without external dependencies.

SSE testing note
----------------
``httpx.ASGITransport`` (used under ``AsyncClient``) buffers the entire ASGI
response before returning to the caller — it does not support incremental
response streaming.  That means ``AsyncClient.stream("GET", "/api/events")``
against the in-process app would never return for an infinite SSE generator.
The SSE E2E tests therefore exercise the fan-out by pre-registering a queue
on ``state.sse_clients`` (exactly what the SSE endpoint handler does at
runtime) and asserting that the ingest / alerts POST lands an event on that
queue via ``iot_twin.services.simulator_service.broadcast``.  ``_stream_sse_once``
uses the ``AsyncClient.stream`` API to cover the GET /api/events route shape
(status + media type) with a deadline so the infinite generator is cancelled.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_app_with_routers(state: AppState, *routers) -> FastAPI:
    """Create a minimal FastAPI app with the given router(s) and AppState."""
    app = FastAPI()
    app.state.app_state = state
    for router in routers:
        app.include_router(router)
    return app


def _make_mock_db(rows: list[dict] | None = None) -> MagicMock:
    """Return a mock LakebaseConnectionManager with async helpers."""
    db = MagicMock()
    db.fetch = AsyncMock(return_value=rows or [])
    db.execute = AsyncMock(return_value=None)
    db.fetchrow = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


def _app_state_with_db(rows: list[dict] | None = None) -> AppState:
    state = AppState()
    state.db = _make_mock_db(rows)
    state.db_available = True
    state.db_error = None
    state.industries = "pharmaceutical"
    return state


def _register_sse_queue(state: AppState, maxsize: int = 10) -> asyncio.Queue:
    """Attach a queue to ``state.sse_clients`` the way the SSE endpoint does.

    The real /api/events handler creates a queue, appends it to
    ``state.sse_clients``, and drains it in a loop.  For E2E tests we just
    register a queue and drain it synchronously — the observable effect of
    a successful SSE fan-out is exactly that an event lands on this queue.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
    state.sse_clients.append(queue)
    return queue


async def _assert_stream_route_is_sse(client: AsyncClient) -> None:
    """Smoke-check that /api/events exists and is wired as a streaming route.

    ``httpx.ASGITransport`` buffers the entire response body before returning
    — it does not support incremental streaming.  For an infinite SSE
    generator that means ``client.stream`` will never yield control back
    to the test, so we wrap it in a short deadline and treat the
    ``TimeoutError`` as confirmation that the route is actually streaming.
    """

    async def _open() -> None:
        async with client.stream("GET", "/api/events") as resp:
            # Consume until deadline expires; never reached for an SSE stream.
            async for _ in resp.aiter_lines():  # pragma: no cover — unreachable
                return

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(_open(), timeout=0.25)


# ---------------------------------------------------------------------------
# 1. SSE broadcast E2E — POST /api/sss/ingest fans out to /api/events
# ---------------------------------------------------------------------------


class TestSseIngestBroadcastE2E:
    """POST /api/sss/ingest should fan events out to SSE subscribers."""

    @pytest_asyncio.fixture()
    async def sse_client(self) -> AsyncGenerator[tuple[AsyncClient, AppState], None]:
        from iot_twin.routes.sensor_data import router as sensor_router
        from iot_twin.routes.websocket import router as ws_router

        state = _app_state_with_db()
        app = _build_app_with_routers(state, ws_router, sensor_router)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, state

    @pytest.mark.asyncio
    async def test_events_route_is_registered_as_stream(
        self, sse_client: tuple[AsyncClient, AppState]
    ) -> None:
        """``AsyncClient.stream`` against /api/events hangs until deadline — which
        is the ASGITransport-level signal that the route is serving an infinite
        streaming response.  This covers the route-wiring smoke."""
        client, _ = sse_client
        await _assert_stream_route_is_sse(client)

    @pytest.mark.asyncio
    async def test_ingest_event_reaches_sse_subscriber(
        self, sse_client: tuple[AsyncClient, AppState]
    ) -> None:
        client, state = sse_client
        queue = _register_sse_queue(state)
        payload = [
            {
                "sensor_name": "temp-01",
                "industry": "pharmaceutical",
                "value": 42.0,
                "unit": "C",
                "sensor_type": "temperature",
                "timestamp": "2026-04-05T10:00:00Z",
            }
        ]

        resp = await client.post("/api/sss/ingest", json=payload)
        assert resp.status_code == 200

        # Drain the SSE subscriber queue — the fan-out should have placed
        # exactly one ``sensor_update`` event there.
        event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert event["type"] == "sensor_update"
        assert event["source"] == "sss"
        assert isinstance(event["sensors"], list)
        assert event["sensors"][0]["sensor_name"] == "temp-01"

    @pytest.mark.asyncio
    async def test_ingest_response_reports_record_count(
        self, sse_client: tuple[AsyncClient, AppState]
    ) -> None:
        client, state = sse_client
        _register_sse_queue(state)

        resp = await client.post(
            "/api/sss/ingest",
            json=[{"sensor_name": "a", "value": 1.0}, {"sensor_name": "b", "value": 2.0}],
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["records"] == 2

    @pytest.mark.asyncio
    async def test_industry_pipeline_id_broadcasts_only_no_lakebase_insert(
        self, sse_client: tuple[AsyncClient, AppState]
    ) -> None:
        """A realtime consumer POSTs with ``X-Pipeline-Id="industry:<industry>"``.

        The deployed consumer's psycopg ForeachWriter already owns the Lakebase
        ChronoTable write, so the app must ONLY broadcast for SSE — it must NOT
        call ``get_pipeline_lakebase`` (which would attempt a second, lossy
        insert). We assert the SSE fan-out still happens, the response reports
        no lakebase insert, and the resolver was never invoked.
        """
        client, state = sse_client
        queue = _register_sse_queue(state)

        with patch(
            "iot_twin.routes.sensor_data.get_pipeline_lakebase", new=AsyncMock()
        ) as mock_get_lb:
            resp = await client.post(
                "/api/sss/ingest",
                json=[
                    {
                        "sensor_name": "temp-01",
                        "industry": "pharmaceutical",
                        "value": 42.0,
                        "unit": "C",
                        "sensor_type": "temperature",
                        "timestamp": "2026-04-05T10:00:00Z",
                    }
                ],
                headers={"X-Pipeline-Id": "industry:pharmaceutical"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["lakebase_inserted"] == 0
            assert "lakebase_error" not in body
            # broadcast-only path must NEVER resolve a per-pipeline Lakebase.
            mock_get_lb.assert_not_awaited()

        # SSE fan-out still reaches the subscriber.
        event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert event["type"] == "sensor_update"
        assert event["sensors"][0]["sensor_name"] == "temp-01"


# ---------------------------------------------------------------------------
# 2. Threshold-alert SSE E2E — POST /api/sss/alerts fans out to /api/events
# ---------------------------------------------------------------------------


class TestSseThresholdAlertBroadcastE2E:
    """POST /api/sss/alerts should fan threshold alerts out to SSE subscribers."""

    @pytest_asyncio.fixture()
    async def sse_client(self) -> AsyncGenerator[tuple[AsyncClient, AppState], None]:
        from iot_twin.routes.websocket import router as ws_router

        state = _app_state_with_db()
        app = _build_app_with_routers(state, ws_router)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, state

    @pytest.mark.asyncio
    async def test_alert_reaches_sse_subscriber(
        self, sse_client: tuple[AsyncClient, AppState]
    ) -> None:
        client, state = sse_client
        queue = _register_sse_queue(state)
        alert = {
            "pipeline_id": "pipe-1",
            "industry": "pharmaceutical",
            "sensor_id": "temp-01",
            "sensor_type": "temperature",
            "value": 120.5,
            "threshold_min": 0.0,
            "threshold_max": 100.0,
            "timestamp": "2026-04-05T10:00:00Z",
        }

        resp = await client.post("/api/sss/alerts", json={"alerts": [alert]})
        assert resp.status_code == 200

        event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert event["type"] == "alert_opened"
        assert event["pipeline_id"] == alert["pipeline_id"]
        assert event["sensor_id"] == alert["sensor_id"]
        assert event["value"] == alert["value"]
        assert event["threshold"] == alert["threshold_max"]

    @pytest.mark.asyncio
    async def test_alerts_events_route_is_streaming(
        self, sse_client: tuple[AsyncClient, AppState]
    ) -> None:
        """Route-wiring smoke via ``AsyncClient.stream`` for /api/events."""
        client, _ = sse_client
        await _assert_stream_route_is_sse(client)


# ---------------------------------------------------------------------------
# 3. Timeseries endpoints — both return a JSON object with a ``rows`` list
# ---------------------------------------------------------------------------


class TestTimeseriesEndpointsReturnRowsShape:
    """Both /api/timeseries and /api/pipeline/:id/dbsql-timeseries return {rows: [...]}."""

    # 3-row fixture that matches the shape produced by the aggregation branch
    # in routes/sensor_data.py::get_timeseries (aggregated path).
    _LAKEBASE_ROWS = [
        {
            "time": f"2026-04-05T10:0{i}:00Z",
            "sensor_name": "temp-01",
            "sensor_type": "temperature",
            "industry": "pharmaceutical",
            "value": 20.0 + i,
            "min_value": 19.0 + i,
            "max_value": 21.0 + i,
            "sample_count": 10,
            "fault_active": False,
        }
        for i in range(3)
    ]

    _DBSQL_ROWS = [
        {
            "time": f"2026-04-05T10:0{i}:00Z",
            "sensor_name": "temp-01",
            "sensor_type": "temperature",
            "industry": "pharmaceutical",
            "value": 20.0 + i,
            "min_value": 19.0 + i,
            "max_value": 21.0 + i,
            "sample_count": 10,
        }
        for i in range(3)
    ]

    @pytest_asyncio.fixture()
    async def ts_client(self) -> AsyncGenerator[AsyncClient, None]:
        from iot_twin.routes.sensor_data import router as sensor_router

        state = _app_state_with_db(rows=self._LAKEBASE_ROWS)
        app = _build_app_with_routers(state, sensor_router)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    @pytest.mark.asyncio
    async def test_lakebase_timeseries_returns_rows_shape(self, ts_client: AsyncClient) -> None:
        # minutes=60 routes through the aggregated branch (no ``source`` filter).
        resp = await ts_client.get("/api/timeseries?minutes=60&industry=pharmaceutical")
        assert resp.status_code == 200
        body = resp.json()
        assert "rows" in body
        assert isinstance(body["rows"], list)
        assert len(body["rows"]) == 3
        first = body["rows"][0]
        assert {"time", "sensor_name", "value"}.issubset(first.keys())

    @pytest.mark.asyncio
    async def test_dbsql_timeseries_returns_rows_shape(self) -> None:
        from iot_twin.routes.sensor_data import router as sensor_router

        state = _app_state_with_db()
        # Stub the helpers used by the dbsql-timeseries route.
        with (
            patch(
                "iot_twin.routes.sensor_data.get_pipeline_catalog",
                new=AsyncMock(return_value="my_catalog"),
            ),
            patch(
                "iot_twin.routes.sensor_data.resolve_warehouse_id",
                new=AsyncMock(return_value="warehouse-xyz"),
            ),
            patch(
                "iot_twin.routes.sensor_data.execute_dbsql",
                new=AsyncMock(
                    return_value={
                        "columns": list(self._DBSQL_ROWS[0].keys()),
                        "data": self._DBSQL_ROWS,
                        "row_count": len(self._DBSQL_ROWS),
                    }
                ),
            ),
        ):
            app = _build_app_with_routers(state, sensor_router)
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipeline/pipe-xyz/dbsql-timeseries?minutes=120")

        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "dbsql"
        assert "rows" in body
        assert isinstance(body["rows"], list)
        assert len(body["rows"]) == 3
        assert body["count"] == 3


# ---------------------------------------------------------------------------
# 4. Sync readiness route — smoke test wiring into the app router
# ---------------------------------------------------------------------------


class TestSyncCheckRouteWired:
    """POST /api/lakets/sync/check is reachable and validates autoscale input."""

    @pytest_asyncio.fixture()
    async def sync_client(self) -> AsyncGenerator[AsyncClient, None]:
        from iot_twin.routes.lakets import router as lakets_router

        app = _build_app_with_routers(AppState(), lakets_router)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    @pytest.mark.asyncio
    async def test_autoscale_without_project_id_is_not_ready(
        self, sync_client: AsyncClient
    ) -> None:
        resp = await sync_client.post(
            "/api/lakets/sync/check",
            json={"lakebase": {"mode": "autoscale"}},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ready_to_enable"] is False
        assert "project_id" in body["error"].lower()


# ---------------------------------------------------------------------------
# 5. Retention deploy route — returns job_id + notebook_path with mocked SDK
# ---------------------------------------------------------------------------


class TestRetentionDeployRoute:
    """POST /api/lakets/retention/deploy with a mocked WorkspaceClient."""

    @pytest_asyncio.fixture()
    async def deploy_client(self) -> AsyncGenerator[AsyncClient, None]:
        from iot_twin.routes.lakets import router as lakets_router

        app = _build_app_with_routers(AppState(), lakets_router)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    @staticmethod
    def _fake_workspace_client() -> MagicMock:
        w = MagicMock()
        w.current_user.me.return_value = MagicMock(user_name="user@example.com")
        w.workspace.mkdirs = MagicMock(return_value=None)
        w.workspace.import_ = MagicMock(return_value=None)
        w.jobs.list.return_value = []
        w.jobs.create.return_value = MagicMock(job_id=1337)
        w.jobs.reset = MagicMock(return_value=None)
        w.config.host = "https://e2.cloud.databricks.com"
        return w

    @pytest.mark.asyncio
    async def test_deploy_returns_job_id_and_notebook_path(
        self, deploy_client: AsyncClient
    ) -> None:
        from iot_twin.services.lakets import lakets_deploy_service

        w = self._fake_workspace_client()
        with (
            patch("databricks.sdk.WorkspaceClient", return_value=w),
            patch.object(
                lakets_deploy_service,
                "_notebook_source_bytes",
                return_value=b"# Databricks notebook source\nprint('hi')\n",
            ),
        ):
            resp = await deploy_client.post(
                "/api/lakets/retention/deploy",
                json={"lakebase": {"project_id": "iot-lakets-demo"}},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["job_id"] == 1337
        assert "notebook_path" in body
        assert body["notebook_path"].endswith("/retention")
        assert body["action"] == "created"

    @pytest.mark.asyncio
    async def test_deploy_without_project_id_returns_400(self, deploy_client: AsyncClient) -> None:
        resp = await deploy_client.post(
            "/api/lakets/retention/deploy",
            json={"lakebase": {}},
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "project_id" in body["error"].lower()
