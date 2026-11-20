"""GET /api/value/summary and /api/ops/summary: envelope, validation, scoping [qa]."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes.value import router as value_router

PID = str(uuid.uuid4())


class FakeDb:
    """Records the parameters of every query; returns one caught episode."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fail = fail

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        if self.fail:
            raise RuntimeError("connection reset")
        opened = datetime.now(UTC) - timedelta(hours=1)
        return [
            {
                "alert_id": "a1",
                "episode_opened_at": opened,
                "onset_raw": (opened - timedelta(seconds=3)).isoformat(),
                "acknowledged_at": opened + timedelta(seconds=4),
                "cleared_at": opened + timedelta(seconds=30),
                "peak_value": 66.0,
                "rule_id": "mfg-alert-spindle-temp",
                "severity": "crit",
                "fault_code": "VPP-SPINDLE3-TEMP",
                "threshold": 60.0,
                "first_decision_at": opened + timedelta(seconds=5),
                "contained_at": opened + timedelta(seconds=5),
                "quarantined_at": opened + timedelta(seconds=5),
                "cmm_at": None,
                "ncr_at": opened + timedelta(seconds=6),
                "maintenance_at": None,
            }
        ]

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any]:
        self.calls.append((sql, args))
        return {"stops": 0}


@pytest_asyncio.fixture()
async def client_db() -> AsyncGenerator[tuple[AsyncClient, FakeDb, AppState], None]:
    state = AppState()
    db = FakeDb()
    state.app_backend_db = db
    state.app_backend_available = True
    app = FastAPI()
    app.state.app_state = state
    app.include_router(value_router)
    with patch("iot_twin.routes.value._is_industry_deployment", AsyncMock(return_value=False)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client, db, state


def _q(**kw: Any) -> dict[str, Any]:
    return {"industry": "manufacturing", "pipeline_id": PID, "window": "7d", **kw}


class TestEnvelope:
    @pytest.mark.asyncio
    async def test_value_summary_envelope_c7940_smoke(self, client_db) -> None:
        """[C7940][smoke] Value summary answers success/data/error/meta with projection + actuals."""
        client, _, _ = client_db
        resp = await client.get("/api/value/summary", params=_q())
        assert resp.status_code == 200
        body = resp.json()
        assert set(body) == {"success", "data", "error", "meta"}
        assert body["success"] is True and body["error"] is None
        data = body["data"]
        assert {"projection", "actuals", "assumptions", "defaults", "fields"} <= set(data)
        assert data["actuals"]["caught_in_time"] == 1
        assert body["meta"]["pipeline_id"] == PID
        assert body["meta"]["data_available"] is True

    @pytest.mark.asyncio
    async def test_ops_summary_envelope_c7941_smoke(self, client_db) -> None:
        """[C7941][smoke] Ops summary carries TTD, NCRs, projected RFT and recent events."""
        client, _, _ = client_db
        resp = await client.get("/api/ops/summary", params=_q())
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["time_to_detect"]["p50"] == 3
        assert data["ncrs_opened"] == 1
        assert data["projected_rft"]["label"] == "Projected right-first-time"
        assert data["recent_events"][0]["caught_in_time"] is True

    @pytest.mark.asyncio
    async def test_overrides_apply_c7942_smoke(self, client_db) -> None:
        """[C7942][smoke] Assumption overrides change the projection and are listed."""
        client, _, _ = client_db
        resp = await client.get(
            "/api/value/summary", params=_q(capture_rate="1", inspection_cost="0")
        )
        data = resp.json()["data"]
        assert data["overridden"] == ["capture_rate", "inspection_cost"]
        assert data["projection"]["exposure_remaining"] == 0


class TestValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["/api/value/summary", "/api/ops/summary"])
    async def test_pipeline_id_required_c7943_smoke(self, client_db, path) -> None:
        """[C7943][smoke] Missing pipeline_id → 422 envelope and no query runs."""
        client, db, _ = client_db
        resp = await client.get(path, params={"industry": "manufacturing"})
        assert resp.status_code == 422
        body = resp.json()
        assert body["success"] is False and body["data"] is None
        assert "pipeline_id" in body["error"]
        assert db.calls == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("params", "field"),
        [
            (_q(capture_rate="1.2"), "capture_rate"),
            (_q(lines_in_scope="abc"), "lines_in_scope"),
            (_q(window="9y"), "window"),
            (_q(industry="banking"), "industry"),
            (_q(pipeline_id="industry:pharma"), "pipeline_id"),
            (_q(bogus="1"), "bogus"),
        ],
    )
    async def test_invalid_inputs_422_c7944_regression(self, client_db, params, field) -> None:
        """[C7944][regression] Invalid assumptions or scope → 422 with per-field details."""
        client, db, _ = client_db
        resp = await client.get("/api/value/summary", params=params)
        assert resp.status_code == 422
        body = resp.json()
        assert body["success"] is False
        assert body["meta"]["details"][0]["field"] == field
        assert db.calls == []

    @pytest.mark.asyncio
    async def test_ops_rejects_value_overrides_c7945_regression(self, client_db) -> None:
        """[C7945][regression] Ops summary does not silently accept value-model parameters."""
        client, _, _ = client_db
        resp = await client.get("/api/ops/summary", params=_q(capture_rate="0.5"))
        assert resp.status_code == 422


class TestScoping:
    @pytest.mark.asyncio
    async def test_every_query_filters_on_pipeline_c7946_smoke(self, client_db) -> None:
        """[C7946][smoke] Every query is parameterised and receives the pipeline keys first."""
        client, db, _ = client_db
        await client.get("/api/value/summary", params=_q())
        await client.get("/api/ops/summary", params=_q())
        assert db.calls
        for sql, args in db.calls:
            assert "pipeline_id = ANY($1::text[])" in sql
            assert args[0] == [PID]
            assert PID not in sql

    @pytest.mark.asyncio
    async def test_realtime_deployment_reads_alias_c7947_regression(self, client_db) -> None:
        """[C7947][regression] A bound realtime deployment also reads industry:<industry> alerts."""
        client, db, _ = client_db
        with patch("iot_twin.routes.value._is_industry_deployment", AsyncMock(return_value=True)):
            await client.get("/api/ops/summary", params=_q())
        assert db.calls[0][1][0] == [PID, "industry:manufacturing"]

    @pytest.mark.asyncio
    async def test_no_backend_is_empty_not_error_c7948_smoke(self, client_db) -> None:
        """[C7948][smoke] Without the app backend the summary is empty and data_available=false."""
        client, _, state = client_db
        state.app_backend_available = False
        resp = await client.get("/api/ops/summary", params=_q())
        assert resp.status_code == 200
        body = resp.json()
        assert body["meta"]["data_available"] is False
        assert body["data"]["episodes"] == 0

    @pytest.mark.asyncio
    async def test_query_failure_is_503_envelope_c7949_regression(self, client_db) -> None:
        """[C7949][regression] A failing query is a 503 envelope with no internals leaked."""
        client, _, state = client_db
        state.app_backend_db = FakeDb(fail=True)
        resp = await client.get("/api/value/summary", params=_q())
        assert resp.status_code == 503
        body = resp.json()
        assert body["success"] is False
        assert "connection reset" not in body["error"]

    @pytest.mark.asyncio
    async def test_identity_required_in_apps_runtime_c7950_regression(self, client_db) -> None:
        """[C7950][regression] Like the alert routes, an Apps runtime without identity is 401."""
        client, _, _ = client_db
        with (
            patch("iot_twin.core.auth.is_apps_runtime", return_value=True),
            patch("iot_twin.core.auth.anonymous_identity_allowed", return_value=False),
        ):
            resp = await client.get("/api/ops/summary", params=_q())
        assert resp.status_code == 401
