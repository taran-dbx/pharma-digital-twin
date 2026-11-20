"""Operator decisions recorded per alert episode [qa]."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import AlertRule, list_alert_rules
from iot_twin.models import app_schema
from iot_twin.routes.alerts import router as alerts_router
from iot_twin.services.alerts.alert_engine import AlertEngine
from iot_twin.services.alerts.alert_responses import AlertResponseStore, response_store_for
from iot_twin.services.production_control import ProductionControl
from tests.test_production_control import FakeWorkspace

SPINDLE = {
    "sensor_id": "mfg-sns-cnc_1_spindle_temp",
    "sensor_name": "cnc_1_spindle_temp",
    "industry": "manufacturing",
}


class Clock:
    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def __call__(self) -> datetime:
        return self.instant


class FakeResponseDb:
    """Implements the alert_response INSERT/SELECT contract, including the unique key."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    async def fetchrow(self, sql: str, *args):
        compact = " ".join(sql.split()).lower()
        if compact.startswith("insert into alert_response"):
            rid, alert_id, episode, action, recorded_by, reference = args
            if any(
                (r["alert_id"], r["episode_opened_at"], r["action"]) == (alert_id, episode, action)
                for r in self.rows
            ):
                return None
            row = {
                "id": rid,
                "alert_id": alert_id,
                "episode_opened_at": episode,
                "action": action,
                "recorded_by": recorded_by,
                "reference": reference,
                "created_at": datetime.now(UTC),
            }
            self.rows.append(row)
            return row
        if "from alert_response" in compact:
            alert_id, episode, action = args
            return next(
                (
                    r
                    for r in self.rows
                    if (r["alert_id"], r["episode_opened_at"], r["action"])
                    == (alert_id, episode, action)
                ),
                None,
            )
        return None

    async def fetch(self, sql: str, *args):
        alert_id, episode = args
        return [
            r for r in self.rows if r["alert_id"] == alert_id and r["episode_opened_at"] == episode
        ]


@pytest_asyncio.fixture()
async def cell() -> AsyncGenerator[tuple[AsyncClient, AppState, Clock, asyncio.Queue], None]:
    clock = Clock(datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC))
    state = AppState()
    state.alert_engine = AlertEngine(now_fn=clock)
    state.production_control = ProductionControl(workspace_factory=FakeWorkspace, now_fn=clock)
    queue: asyncio.Queue = asyncio.Queue()
    state.sse_clients.append(queue)
    app = FastAPI()
    app.state.app_state = state
    app.include_router(alerts_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True):
            yield client, state, clock, queue


def _instant_spindle_rule() -> AlertRule:
    rule = next(
        r for r in list_alert_rules("manufacturing") if r.rule_id == "mfg-alert-spindle-temp"
    )
    return replace(rule, duration_s=0.0)


async def _spindle_reading(state: AppState, value: float) -> list:
    return await state.alert_engine.evaluate(
        [{**SPINDLE, "value": value}],
        [_instant_spindle_rule()],
        industry="manufacturing",
        pipeline_id="industry:manufacturing",
    )


def _drain(queue: asyncio.Queue) -> list[dict]:
    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


@pytest.mark.asyncio
class TestAlertResponses:
    async def test_hold_batch_is_recorded_and_broadcast_c7420_smoke(self, cell) -> None:
        """[C7420][smoke] Holding the batch at PACK is recorded on the alert episode and sent over SSE."""
        client, state, _clock, queue = cell
        opened = await _spindle_reading(state, 66.3)
        alert_id = opened[0].alert.id
        _drain(queue)

        resp = await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "hold_batch"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is True
        assert body["type"] == "alert_response"
        assert body["response"]["station"] == "PACK"
        assert body["response"]["episode_opened_at"] == "2026-10-20T13:48:52+00:00"

        events = _drain(queue)
        assert [e["type"] for e in events] == ["alert_response"]
        assert events[0]["response"]["action"] == "hold_batch"

        listed = await client.get(f"/api/alerts/{alert_id}/responses")
        assert [r["action"] for r in listed.json()["responses"]] == ["hold_batch"]

    async def test_each_decision_is_recorded_once_per_episode_c7421_regression(self, cell) -> None:
        """[C7421][regression] Repeating a decision returns the original record without a second broadcast."""
        client, state, _clock, queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        first = await client.post(
            f"/api/alerts/{alert_id}/responses", json={"action": "send_to_inspect"}
        )
        _drain(queue)
        again = await client.post(
            f"/api/alerts/{alert_id}/responses", json={"action": "send_to_inspect"}
        )
        assert again.json()["created"] is False
        assert again.json()["response"]["id"] == first.json()["response"]["id"]
        assert _drain(queue) == []
        assert first.json()["response"]["reference"]["parts_since"] == "2026-10-20T13:48:52+00:00"

    async def test_next_heat_event_starts_without_decisions_c7422_defect(self, cell) -> None:
        """[C7422][defect] A re-opened alert does not inherit the previous episode's hold."""
        client, state, clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "hold_batch"})
        clock.instant += timedelta(seconds=56)
        await _spindle_reading(state, 53.0)
        clock.instant += timedelta(seconds=240)
        reopened = await _spindle_reading(state, 64.0)
        assert reopened[0].alert.id == alert_id

        listed = await client.get(f"/api/alerts/{alert_id}/responses")
        assert listed.json()["responses"] == []
        assert listed.json()["episode_opened_at"] == "2026-10-20T13:53:48+00:00"

    async def test_maintenance_request_links_last_service_c7423_regression(self, cell) -> None:
        """[C7423][regression] A maintenance request references Spindle 3's last catalog service."""
        client, state, _clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        resp = await client.post(
            f"/api/alerts/{alert_id}/responses", json={"action": "request_maintenance"}
        )
        reference = resp.json()["response"]["reference"]
        assert reference["station"] == "MILL"
        assert reference["asset"] == "CNC-1"
        assert reference["component"] == "Spindle 3"
        assert reference["last_service"]["notes"] == "Spindle bearing inspection"

    async def test_rejects_unknown_action_alert_and_non_admin_c7424_defect(self, cell) -> None:
        """[C7424][defect] Bad actions, unknown alerts and non-admin callers are refused."""
        client, state, _clock, queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        _drain(queue)
        bad = await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "stop_mill"})
        assert bad.status_code == 422
        missing = await client.post(
            "/api/alerts/does-not-exist/responses", json={"action": "hold_batch"}
        )
        assert missing.status_code == 404
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=False):
            denied = await client.post(
                f"/api/alerts/{alert_id}/responses", json={"action": "hold_batch"}
            )
        assert denied.status_code == 403
        assert response_store_for(state)._memory == {}
        assert _drain(queue) == []


@pytest.mark.asyncio
async def test_lakebase_path_dedupes_on_unique_key_c7425_regression() -> None:
    """[C7425][regression] The Lakebase path relies on the (alert, episode, action) key."""
    engine = AlertEngine(now_fn=Clock(datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC)))
    alert = (
        await engine.evaluate(
            [{**SPINDLE, "value": 66.3}],
            [_instant_spindle_rule()],
            industry="manufacturing",
        )
    )[0].alert
    store = AlertResponseStore()
    db = FakeResponseDb()
    first, created = await store.record(alert, "hold_batch", "op@example.com", db=db)
    second, created_again = await store.record(alert, "hold_batch", "other@example.com", db=db)
    assert (created, created_again) == (True, False)
    assert second.id == first.id
    assert second.recorded_by == "op@example.com"
    assert [r.action for r in await store.list_for(alert, db=db)] == ["hold_batch"]
    assert store._memory == {}


def test_alert_response_schema_is_constrained_c7426_smoke() -> None:
    """[C7426][smoke] Only the three decisions exist, once per alert episode."""
    ddl = " ".join(app_schema.ALERT_RESPONSE_DDL.split())
    assert "CREATE TABLE IF NOT EXISTS alert_response" in ddl
    assert "CHECK (action IN ('hold_batch', 'send_to_inspect', 'request_maintenance'))" in ddl
    assert "UNIQUE (alert_id, episode_opened_at, action)" in ddl
    assert "REFERENCES alert(id) ON DELETE CASCADE" in ddl
