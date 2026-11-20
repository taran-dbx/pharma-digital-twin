"""Cell production stop / resume and the alert history it feeds [qa]."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import AlertRule, list_alert_rules
from iot_twin.models import app_schema
from iot_twin.routes.alerts import router as alerts_router
from iot_twin.routes.production import router as production_router
from iot_twin.services.alerts.alert_engine import AlertEngine
from iot_twin.services.alerts.alert_history import alert_episodes
from iot_twin.services.production_control import ProductionControl

SPINDLE = {
    "sensor_id": "mfg-sns-cnc_1_spindle_temp",
    "sensor_name": "cnc_1_spindle_temp",
    "industry": "manufacturing",
}
APP_DIR = "/Workspace/Users/app-sp/.iot-digital-twin"
CONTROL_FILE = f"{APP_DIR}/production_control/manufacturing.json"


class Clock:
    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def __call__(self) -> datetime:
        return self.instant


class FakeWorkspace:
    """Records control-file uploads; ``fail`` simulates an unreachable workspace."""

    def __init__(self) -> None:
        self.files: dict[str, dict] = {}
        self.uploads = 0
        self.fail = False
        self.workspace = SimpleNamespace(mkdirs=lambda _path: None, upload=self._upload)
        self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="app-sp"))

    def _upload(self, path, content, *, format, overwrite) -> None:
        if self.fail:
            raise ConnectionError("workspace unreachable")
        assert overwrite is True
        self.uploads += 1
        self.files[path] = json.loads(content.read())


class FakeLakebase:
    """Minimal production_state / production_event / pipeline_deployment contract."""

    def __init__(self, deployed: list[str]) -> None:
        self.deployed = deployed
        self.state: dict | None = None
        self.events: list[dict] = []

    async def fetchrow(self, sql: str, *args):
        assert "from production_state" in " ".join(sql.split()).lower()
        return self.state

    async def fetch(self, sql: str, *args):
        compact = " ".join(sql.split()).lower()
        if "from pipeline_deployment" in compact:
            assert args == ("manufacturing",)
            return [{"notebook_path": path} for path in self.deployed]
        if "from production_event" in compact:
            return [e for e in self.events if e["created_at"] >= args[1]]
        raise AssertionError(sql)

    async def execute(self, sql: str, *args):
        compact = " ".join(sql.split()).lower()
        industry, state = args[0:2] if "production_state" in compact else args[1:3]
        if compact.startswith("insert into production_state"):
            _, _, assets, reason, alert_id, changed_by, changed_at = args
            self.state = {
                "industry": industry,
                "state": state,
                "assets": assets,
                "reason": reason,
                "alert_id": alert_id,
                "changed_by": changed_by,
                "changed_at": changed_at,
            }
        elif compact.startswith("insert into production_event"):
            rid, _, _, assets, reason, alert_id, changed_by, created_at = args
            self.events.append(
                {
                    "id": rid,
                    "industry": industry,
                    "state": state,
                    "assets": assets,
                    "reason": reason,
                    "alert_id": alert_id,
                    "changed_by": changed_by,
                    "created_at": created_at,
                }
            )
        else:
            raise AssertionError(sql)


@pytest_asyncio.fixture()
async def cell() -> AsyncGenerator[
    tuple[AsyncClient, AppState, FakeWorkspace, asyncio.Queue, Clock], None
]:
    # history routes window on the wall clock, so the cell's clock starts now
    clock = Clock(datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10))
    workspace = FakeWorkspace()
    state = AppState()
    state.alert_engine = AlertEngine(now_fn=clock)
    state.production_control = ProductionControl(workspace_factory=lambda: workspace, now_fn=clock)
    queue: asyncio.Queue = asyncio.Queue()
    state.sse_clients.append(queue)
    app = FastAPI()
    app.state.app_state = state
    app.include_router(alerts_router)
    app.include_router(production_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        with (
            patch("iot_twin.routes.production.app_permissions.is_admin", return_value=True),
            patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=True),
        ):
            yield client, state, workspace, queue, clock


def _drain(queue: asyncio.Queue) -> list[dict]:
    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


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


@pytest.mark.asyncio
class TestProductionControl:
    async def test_stop_tells_the_producer_and_broadcasts_c7430_smoke(self, cell) -> None:
        """[C7430][smoke] Stopping writes the control file, records the stop and notifies viewers."""
        client, _state, workspace, queue, clock = cell
        assert (await client.get("/api/industries/manufacturing/production")).json()[
            "state"
        ] == "running"

        resp = await client.post(
            "/api/industries/manufacturing/production",
            json={"state": "stopped", "reason": "inspection"},
        )
        assert resp.status_code == 200
        assert resp.json()["changed"] is True
        assert workspace.files[CONTROL_FILE]["state"] == "stopped"
        assert workspace.files[CONTROL_FILE]["assets"] == ["cnc_1", "assy_line_1"]

        events = _drain(queue)
        assert [e["type"] for e in events] == ["production_state"]
        assert events[0]["production"]["reason"] == "inspection"
        current = (await client.get("/api/industries/manufacturing/production")).json()
        assert (current["state"], current["changed_at"]) == ("stopped", clock.instant.isoformat())

    async def test_resume_clears_the_stop_and_keeps_history_c7431_regression(self, cell) -> None:
        """[C7431][regression] Resuming empties the stopped assets; both changes stay in history."""
        client, _state, workspace, _queue, clock = cell
        await client.post("/api/industries/manufacturing/production", json={"state": "stopped"})
        clock.instant += timedelta(seconds=95)
        resumed = await client.post(
            "/api/industries/manufacturing/production", json={"state": "running"}
        )
        assert resumed.json()["state"] == "running"
        assert workspace.files[CONTROL_FILE]["assets"] == []

        history = (await client.get("/api/industries/manufacturing/production/events")).json()
        assert [e["state"] for e in history["events"]] == ["stopped", "running"]

    async def test_repeat_stop_is_a_no_op_c7432_regression(self, cell) -> None:
        """[C7432][regression] A second stop neither rewrites the file nor re-broadcasts."""
        client, _state, workspace, queue, _clock = cell
        await client.post("/api/industries/manufacturing/production", json={"state": "stopped"})
        _drain(queue)
        again = await client.post(
            "/api/industries/manufacturing/production", json={"state": "stopped"}
        )
        assert again.json()["changed"] is False
        assert workspace.uploads == 1
        assert _drain(queue) == []

    async def test_unreachable_producer_does_not_claim_a_stop_c7433_defect(self, cell) -> None:
        """[C7433][defect] If the control file can't be written the cell is still reported running."""
        client, _state, workspace, queue, _clock = cell
        workspace.fail = True
        resp = await client.post(
            "/api/industries/manufacturing/production", json={"state": "stopped"}
        )
        assert resp.status_code == 502
        assert "workspace unreachable" in resp.json()["detail"]
        assert (await client.get("/api/industries/manufacturing/production")).json()[
            "state"
        ] == "running"
        assert _drain(queue) == []

    async def test_rejects_bad_state_unknown_cell_and_non_admin_c7434_defect(self, cell) -> None:
        """[C7434][defect] Bad states, industries without a cell, and non-admins are refused."""
        client, _state, workspace, _queue, _clock = cell
        bad = await client.post(
            "/api/industries/manufacturing/production", json={"state": "paused"}
        )
        assert bad.status_code == 422
        missing = await client.post(
            "/api/industries/pharmaceutical/production", json={"state": "stopped"}
        )
        assert missing.status_code == 404
        with patch("iot_twin.routes.production.app_permissions.is_admin", return_value=False):
            denied = await client.post(
                "/api/industries/manufacturing/production", json={"state": "stopped"}
            )
        assert denied.status_code == 403
        assert workspace.uploads == 0

    async def test_send_to_inspect_stops_the_cell_c7435_smoke(self, cell) -> None:
        """[C7435][smoke] Sending parts to inspection stops production; holding the batch does not."""
        client, state, workspace, queue, _clock = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        _drain(queue)

        held = await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "hold_batch"})
        assert "production" not in held.json()
        assert workspace.uploads == 0

        inspect = await client.post(
            f"/api/alerts/{alert_id}/responses", json={"action": "send_to_inspect"}
        )
        production = inspect.json()["production"]
        assert (production["state"], production["reason"], production["alert_id"]) == (
            "stopped",
            "inspection",
            alert_id,
        )
        assert workspace.files[CONTROL_FILE]["state"] == "stopped"
        assert [e["type"] for e in _drain(queue)] == [
            "alert_response",
            "alert_response",
            "production_state",
        ]


@pytest.mark.asyncio
async def test_lakebase_path_writes_beside_every_deployment_c7436_regression() -> None:
    """[C7436][regression] Each deployed pipeline folder gets the control file; state lives in Lakebase."""
    clock = Clock(datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC))
    workspace = FakeWorkspace()
    db = FakeLakebase(
        [
            "/Workspace/Users/app-sp/.iot-digital-twin/manufacturing_realtime_consumer_1",
            "/Workspace/Users/ops/.iot-digital-twin/manufacturing_microbatch_consumer_2",
        ]
    )
    control = ProductionControl(workspace_factory=lambda: workspace, now_fn=clock)

    change, changed = await control.set_state(
        "manufacturing", "stopped", actor="op@example.com", db=db
    )

    assert changed is True
    assert sorted(workspace.files) == [
        CONTROL_FILE,
        "/Workspace/Users/ops/.iot-digital-twin/production_control/manufacturing.json",
    ]
    assert (await control.current("manufacturing", db=db))["changed_by"] == "op@example.com"
    history = await control.history("manufacturing", clock.instant - timedelta(minutes=1), db=db)
    assert [c.id for c in history] == [change["id"]]
    assert control._memory == []


@pytest.mark.asyncio
async def test_alert_history_groups_an_episode_c7438_smoke(cell) -> None:
    """[C7438][smoke] History lists the heat episode with its stop, ahead of the alert-by-id route."""
    client, state, _workspace, _queue, clock = cell
    alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
    await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "send_to_inspect"})
    clock.instant += timedelta(seconds=20)
    await _spindle_reading(state, 71.4)

    resp = await client.get("/api/alerts/history", params={"industry": "manufacturing", "hours": 1})
    assert resp.status_code == 200
    body = resp.json()
    episode = body["episodes"][0]
    assert episode["alert_id"] == alert_id
    assert episode["status"] == "open"
    assert episode["threshold"] == 60.0
    assert [p["state"] for p in body["production"]] == ["stopped"]


class FakeHistoryDb:
    def __init__(self, opened: datetime) -> None:
        self.opened = opened

    async def fetch(self, sql: str, *args):
        compact = " ".join(sql.split()).lower()
        if "from alert_event" in compact:
            return [
                {
                    "alert_id": "a1",
                    "episode": self.opened.isoformat(),
                    "opened_seen": self.opened,
                    "acknowledged_at": self.opened + timedelta(seconds=12),
                    "acknowledged_by": "op@example.com",
                    "cleared_at": self.opened + timedelta(seconds=56),
                    "peak_value": 71.4,
                    "last_event_at": self.opened + timedelta(seconds=56),
                },
                {
                    "alert_id": "a1",
                    "episode": (self.opened + timedelta(minutes=5)).isoformat(),
                    "opened_seen": self.opened + timedelta(minutes=5),
                    "acknowledged_at": None,
                    "acknowledged_by": None,
                    "cleared_at": None,
                    "peak_value": 64.0,
                    "last_event_at": self.opened + timedelta(minutes=5, seconds=3),
                },
            ]
        if "from alert where" in compact:
            return [
                {
                    "id": "a1",
                    "rule_id": "mfg-alert-spindle-temp",
                    "sensor_id": SPINDLE["sensor_id"],
                    "severity": "crit",
                    "fault_code": "SPINDLE_OVERHEAT",
                    "threshold": 60.0,
                }
            ]
        if "from alert_response" in compact:
            return [
                {
                    "alert_id": "a1",
                    "episode_opened_at": self.opened,
                    "action": "send_to_inspect",
                    "recorded_by": "op@example.com",
                    "created_at": self.opened + timedelta(seconds=15),
                }
            ]
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_lakebase_history_splits_reopened_alert_into_episodes_c7439_regression() -> None:
    """[C7439][regression] A re-opened alert is two episodes; decisions stay with their own episode."""
    opened = datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC)
    episodes = await alert_episodes(
        AlertEngine(), "manufacturing", opened - timedelta(hours=1), db=FakeHistoryDb(opened)
    )
    assert [e["status"] for e in episodes] == ["open", "cleared"]
    latest, first = episodes
    assert latest["decisions"] == []
    assert first["peak_value"] == 71.4
    assert [d["station"] for d in first["decisions"]] == ["INSPECT"]
    assert first["acknowledged_by"] == "op@example.com"


def test_production_schema_is_constrained_c7440_smoke() -> None:
    """[C7440][smoke] One state row per industry and an append-only change log."""
    state_ddl = " ".join(app_schema.PRODUCTION_STATE_DDL.split())
    event_ddl = " ".join(app_schema.PRODUCTION_EVENT_DDL.split())
    assert "industry TEXT PRIMARY KEY" in state_ddl
    assert "CHECK (state IN ('running', 'stopped'))" in state_ddl
    assert "CHECK (state IN ('running', 'stopped'))" in event_ddl
    assert (
        "ON production_event (industry, created_at DESC)" in app_schema.PRODUCTION_EVENT_INDEXES[0]
    )
