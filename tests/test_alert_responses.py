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
from iot_twin.domain import lot_context
from iot_twin.domain.catalog import AlertRule, list_alert_rules
from iot_twin.models import app_schema
from iot_twin.routes.alerts import router as alerts_router
from iot_twin.services.alerts.alert_engine import AlertEngine
from iot_twin.services.alerts.alert_responses import (
    AlertResponseStore,
    ResponseAudit,
    response_store_for,
)
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
            rid, alert_id, episode, action, recorded_by, reference, *audit = args
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
            if audit:
                reason, comment, meaning, signed_at = audit
                row |= {
                    "reason": reason,
                    "comment": comment,
                    "signature_meaning": meaning,
                    "signed_at": signed_at,
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


def _signed(action: str, **extra) -> dict:
    """A decision body with the Part 11-style signing fields."""
    return {
        "action": action,
        "reason": "Spindle 3 outside its validated limit",
        "signature_meaning": "responsible",
        **extra,
    }


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

        resp = await client.post(f"/api/alerts/{alert_id}/responses", json=_signed("hold_batch"))
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
            f"/api/alerts/{alert_id}/responses", json=_signed("send_to_inspect")
        )
        _drain(queue)
        again = await client.post(
            f"/api/alerts/{alert_id}/responses", json=_signed("send_to_inspect")
        )
        assert again.json()["created"] is False
        assert again.json()["response"]["id"] == first.json()["response"]["id"]
        assert _drain(queue) == []
        assert first.json()["response"]["reference"]["parts_since"] == "2026-10-20T13:48:52+00:00"

    async def test_next_heat_event_starts_without_decisions_c7422_defect(self, cell) -> None:
        """[C7422][defect] A re-opened alert does not inherit the previous episode's hold."""
        client, state, clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        await client.post(f"/api/alerts/{alert_id}/responses", json=_signed("hold_batch"))
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
            f"/api/alerts/{alert_id}/responses", json=_signed("request_maintenance")
        )
        reference = resp.json()["response"]["reference"]
        assert reference["station"] == "MILL"
        assert reference["asset"] == "CNC-1 5-axis mill"
        assert reference["component"] == "Spindle 3"
        assert reference["last_service"]["notes"].startswith("Spindle bearing inspection")

    async def test_rejects_unknown_action_alert_and_non_admin_c7424_defect(self, cell) -> None:
        """[C7424][defect] Bad actions, unknown alerts and non-admin callers are refused."""
        client, state, _clock, queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        _drain(queue)
        bad = await client.post(f"/api/alerts/{alert_id}/responses", json=_signed("stop_mill"))
        assert bad.status_code == 422
        missing = await client.post(
            "/api/alerts/does-not-exist/responses", json=_signed("hold_batch")
        )
        assert missing.status_code == 404
        with patch("iot_twin.routes.alerts.app_permissions.is_admin", return_value=False):
            denied = await client.post(
                f"/api/alerts/{alert_id}/responses", json=_signed("hold_batch")
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
    audit = ResponseAudit(reason="excursion", signature_meaning="responsible")
    first, created = await store.record(alert, "hold_batch", "op@example.com", db=db, audit=audit)
    second, created_again = await store.record(
        alert, "hold_batch", "other@example.com", db=db, audit=audit
    )
    assert (created, created_again) == (True, False)
    assert second.id == first.id
    assert second.recorded_by == "op@example.com"
    assert [r.action for r in await store.list_for(alert, db=db)] == ["hold_batch"]
    assert store._memory == {}


def test_alert_response_schema_is_constrained_c7426_smoke() -> None:
    """[C7426][smoke] Only the med-device decisions (plus legacy names) exist, once per episode."""
    ddl = " ".join(app_schema.ALERT_RESPONSE_DDL.split())
    assert "CREATE TABLE IF NOT EXISTS alert_response" in ddl
    assert (
        "CHECK (action IN ('quarantine_lot', 'open_ncr', 'request_cmm_inspection', "
        "'request_maintenance', 'hold_batch', 'send_to_inspect'))"
    ) in ddl
    assert "UNIQUE (alert_id, episode_opened_at, action)" in ddl
    # Signed records outlive their alert: deletes are refused, not cascaded.
    assert "REFERENCES alert(id) ON DELETE RESTRICT" in ddl


@pytest.mark.asyncio
class TestMedtechDecisions:
    async def test_quarantine_lot_names_parts_at_risk_c7830_smoke(self, cell) -> None:
        """[C7830][smoke] A QA hold lists the parts cut during the excursion and the rest to release."""
        client, state, clock, queue = cell
        # Excursion opens at second 225 of a lot and clears at second 275.
        start = clock.instant.timestamp()
        start -= start % lot_context.LOT_CYCLE_S
        clock.instant = datetime.fromtimestamp(start + 225, UTC)
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        clock.instant = datetime.fromtimestamp(start + 275, UTC)
        await _spindle_reading(state, 53.0)
        _drain(queue)

        resp = await client.post(
            f"/api/alerts/{alert_id}/responses", json=_signed("quarantine_lot")
        )
        assert resp.status_code == 200
        reference = resp.json()["response"]["reference"]
        lot = lot_context.lot_id_at(start)
        assert resp.json()["response"]["station"] == "PACK"
        assert reference["hold"] == "QA hold"
        assert reference["lot_ids"] == [lot]
        assert reference["parts_at_risk"] == [f"{lot}-12", f"{lot}-13", f"{lot}-14"]
        assert reference["parts_released"] == 12
        assert reference["dhr_refs"] == [f"DHR-{lot}"]
        assert reference["udi_di"] == lot_context.UDI_DI
        assert reference["triggering_part"]["lot_id"] == lot
        assert reference["excursion"]["open"] is False
        assert [e["type"] for e in _drain(queue)] == ["alert_response"]

    async def test_open_ncr_carries_validation_refs_c7831_smoke(self, cell) -> None:
        """[C7831][smoke] The NCR cites the validated limit, OQ/PQ and awaits MRB disposition."""
        client, state, _clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        resp = await client.post(f"/api/alerts/{alert_id}/responses", json=_signed("open_ncr"))
        body = resp.json()["response"]
        reference = body["reference"]
        assert body["station"] == "INSPECT"
        assert reference["ncr_id"].startswith("NCR-20261020-")
        assert reference["fault_code"] == "VPP-SPINDLE3-TEMP"
        assert reference["validation"]["oq_ref"] == "OQ-TM-CNC1-014"
        assert reference["validation"]["pq_ref"] == "PQ-TM-HS12-022"
        assert reference["disposition"] == "Pending MRB disposition"
        assert reference["excursion"]["open"] is True
        assert reference["parts_at_risk"]

    async def test_cmm_inspection_stops_the_cell_c7832_smoke(self, cell) -> None:
        """[C7832][smoke] Requesting CMM inspection stops production and lists parts to measure."""
        client, state, _clock, queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        _drain(queue)
        resp = await client.post(
            f"/api/alerts/{alert_id}/responses", json=_signed("request_cmm_inspection")
        )
        body = resp.json()
        assert body["response"]["reference"]["parts_since"] == "2026-10-20T13:48:52+00:00"
        assert body["response"]["reference"]["parts_to_measure"]
        assert body["production"]["state"] == "stopped"
        assert body["production"]["reason"] == "CMM inspection"
        assert [e["type"] for e in _drain(queue)] == ["alert_response", "production_state"]

    async def test_every_decision_is_a_signed_record_c7833_smoke(self, cell) -> None:
        """[C7833][smoke] Actor, timestamp, reason, comment and signature meaning are recorded."""
        client, state, _clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        resp = await client.post(
            f"/api/alerts/{alert_id}/responses",
            json=_signed(
                "quarantine_lot",
                signature_meaning="approved",
                comment="  Hold the lot pending CMM  ",
            ),
            headers={"X-Forwarded-Email": "qa.lead@example.com"},
        )
        record = resp.json()["response"]
        assert record["reason"] == "Spindle 3 outside its validated limit"
        assert record["comment"] == "Hold the lot pending CMM"
        assert record["signature_meaning"] == "approved"
        assert record["signed_at"]
        assert record["recorded_by"]
        assert record["signature"] == (
            f"{record['recorded_by']} · approved · {record['signed_at']}"
        )
        listed = (await client.get(f"/api/alerts/{alert_id}/responses")).json()["responses"]
        assert listed[0]["signature_meaning"] == "approved"

    @pytest.mark.parametrize(
        ("body", "message"),
        [
            ({"action": "open_ncr", "signature_meaning": "approved"}, "reason is required"),
            ({"action": "open_ncr", "reason": "   ", "signature_meaning": "approved"}, "reason"),
            ({"action": "open_ncr", "reason": "x"}, "signature_meaning must be one of"),
            (
                {"action": "open_ncr", "reason": "x", "signature_meaning": "approve-ish"},
                "signature_meaning",
            ),
            (
                {"action": "open_ncr", "reason": "x" * 281, "signature_meaning": "approved"},
                "at most 280",
            ),
            (
                {
                    "action": "open_ncr",
                    "reason": "x",
                    "signature_meaning": "approved",
                    "comment": 7,
                },
                "comment must be text",
            ),
        ],
    )
    async def test_unsigned_decisions_are_refused_c7834_defect(self, cell, body, message) -> None:
        """[C7834][defect] A decision without a reason or a valid signature meaning is 422."""
        client, state, _clock, queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        _drain(queue)
        resp = await client.post(f"/api/alerts/{alert_id}/responses", json=body)
        assert resp.status_code == 422
        assert message in resp.json()["detail"]
        assert response_store_for(state)._memory == {}
        assert _drain(queue) == []

    async def test_non_object_body_is_refused_c7834_regression(self, cell) -> None:
        """[C7834][regression] A JSON array body is a 422, not a 500."""
        client, state, _clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        resp = await client.post(f"/api/alerts/{alert_id}/responses", json=["open_ncr"])
        assert resp.status_code == 422


class LegacyColumnsDb(FakeResponseDb):
    """A table the app identity could not upgrade: no audit columns."""

    async def fetchrow(self, sql: str, *args):
        if "signature_meaning" in sql:
            raise RuntimeError('column "reason" of relation "alert_response" does not exist')
        return await super().fetchrow(sql, *args)


@pytest.mark.asyncio
async def test_unupgraded_table_keeps_the_signature_in_reference_c7835_defect() -> None:
    """[C7835][defect] Without audit columns the signature survives in reference.audit."""
    engine = AlertEngine(now_fn=Clock(datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC)))
    alert = (
        await engine.evaluate(
            [{**SPINDLE, "value": 66.3}], [_instant_spindle_rule()], industry="manufacturing"
        )
    )[0].alert
    store = AlertResponseStore()
    db = LegacyColumnsDb()
    audit = ResponseAudit(reason="excursion", signature_meaning="reviewed", comment="see NCR")
    response, created = await store.record(alert, "open_ncr", "qa@example.com", db=db, audit=audit)
    assert created is True
    assert store._memory == {}
    assert db.rows[0]["reference"].count('"audit"') == 1
    assert response.reason == "excursion"
    assert response.signature_meaning == "reviewed"
    assert response.comment == "see NCR"
    assert "audit" not in response.reference
    (listed,) = await store.list_for(alert, db=db)
    assert listed.signature_meaning == "reviewed"


class StaleCheckDb(FakeResponseDb):
    """A table whose CHECK predates the med-device actions (asyncpg-style errors)."""

    class CheckViolationError(Exception):
        sqlstate = "23514"

    async def fetchrow(self, sql: str, *args):
        compact = " ".join(sql.split()).lower()
        if compact.startswith("insert into alert_response") and args[3] not in {
            "hold_batch",
            "send_to_inspect",
            "request_maintenance",
        }:
            raise self.CheckViolationError(
                'violates check constraint "alert_response_action_check"'
            )
        return await super().fetchrow(sql, *args)


@pytest.mark.asyncio
async def test_ncr_numbers_differ_per_episode_on_the_same_day_c7836_defect() -> None:
    """[C7836][defect] Two excursions of one alert row on one day get distinct NCR ids."""
    from iot_twin.services.alerts.alert_responses import ncr_number

    clock = Clock(datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC))
    engine = AlertEngine(now_fn=clock)
    rule = _instant_spindle_rule()
    first = (await engine.evaluate([{**SPINDLE, "value": 66.3}], [rule]))[0].alert
    clock.instant += timedelta(seconds=56)
    await engine.evaluate([{**SPINDLE, "value": 53.0}], [rule])
    clock.instant += timedelta(seconds=244)
    second = (await engine.evaluate([{**SPINDLE, "value": 64.0}], [rule]))[0].alert
    assert second.id == first.id
    assert ncr_number(first) == f"NCR-20261020-134852-{first.id.replace('-', '')[:4].upper()}"
    assert ncr_number(second) != ncr_number(first)


@pytest.mark.asyncio
class TestDurableSignedRecords:
    async def test_stale_check_stores_mapped_actions_and_refuses_ncr_c7837_defect(
        self, cell
    ) -> None:
        """[C7837][defect] No silent in-memory records: map to legacy names or 503."""
        client, state, _clock, queue = cell
        state.app_backend_available = True
        state.app_backend_db = StaleCheckDb()
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        _drain(queue)

        held = await client.post(
            f"/api/alerts/{alert_id}/responses", json=_signed("quarantine_lot")
        )
        assert held.status_code == 200
        assert held.json()["response"]["action"] == "quarantine_lot"
        assert state.app_backend_db.rows[0]["action"] == "hold_batch"

        ncr = await client.post(f"/api/alerts/{alert_id}/responses", json=_signed("open_ncr"))
        assert ncr.status_code == 503
        assert "not upgraded" in ncr.json()["detail"]
        assert response_store_for(state)._memory == {}
        assert [e["response"]["action"] for e in _drain(queue)] == ["quarantine_lot"]

    async def test_database_errors_are_503_not_memory_c7837_regression(self, cell) -> None:
        """[C7837][regression] A failing backend refuses the decision instead of faking it."""

        class DownDb:
            async def fetchrow(self, *_args):
                raise ConnectionError("connection reset")

            async def fetch(self, *_args):
                raise ConnectionError("connection reset")

        client, state, _clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        state.app_backend_available = True
        state.app_backend_db = DownDb()
        resp = await client.post(f"/api/alerts/{alert_id}/responses", json=_signed("open_ncr"))
        assert resp.status_code == 503
        assert response_store_for(state)._memory == {}

    async def test_legacy_clients_default_to_legacy_unsigned_c7838_regression(self, cell) -> None:
        """[C7838][regression] Older clients posting only a legacy action still work, marked unsigned."""
        client, state, _clock, _queue = cell
        alert_id = (await _spindle_reading(state, 66.3))[0].alert.id
        resp = await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "hold_batch"})
        assert resp.status_code == 200
        record = resp.json()["response"]
        assert record["signature_meaning"] == "legacy-unsigned"
        assert record["reason"] == "Recorded by a legacy client without a signature"
        assert record["signature"] is None
        new = await client.post(f"/api/alerts/{alert_id}/responses", json={"action": "open_ncr"})
        assert new.status_code == 422
        forged = await client.post(
            f"/api/alerts/{alert_id}/responses",
            json={"action": "open_ncr", "reason": "x", "signature_meaning": "legacy-unsigned"},
        )
        assert forged.status_code == 422


@pytest.mark.asyncio
class TestHoldAcrossUpgrade:
    async def _alert(self):
        engine = AlertEngine(now_fn=Clock(datetime(2026, 10, 20, 13, 48, 52, tzinfo=UTC)))
        return (
            await engine.evaluate(
                [{**SPINDLE, "value": 66.3}], [_instant_spindle_rule()], industry="manufacturing"
            )
        )[0].alert

    async def test_hold_stored_before_the_upgrade_is_not_duplicated_c7839_defect(self) -> None:
        """[C7839][defect] quarantine_lot after the upgrade finds the pre-upgrade hold_batch row."""
        alert = await self._alert()
        store = AlertResponseStore()
        audit = ResponseAudit(reason="excursion", signature_meaning="responsible")
        stale = StaleCheckDb()
        held, created = await store.record(alert, "quarantine_lot", "qa@x", db=stale, audit=audit)
        assert (created, stale.rows[0]["action"]) == (True, "hold_batch")

        upgraded = FakeResponseDb()
        upgraded.rows = stale.rows  # same table, CHECK now current
        again, created_again = await store.record(
            alert, "quarantine_lot", "other@x", db=upgraded, audit=audit
        )
        assert created_again is False
        assert again.id == held.id
        assert again.action == "quarantine_lot"
        assert len(upgraded.rows) == 1

    async def test_genuine_legacy_hold_is_returned_as_hold_batch_c7839_regression(self) -> None:
        """[C7839][regression] A legacy client's hold_batch counts as the episode's QA hold."""
        alert = await self._alert()
        store = AlertResponseStore()
        db = FakeResponseDb()
        legacy = ResponseAudit(reason="legacy", signature_meaning="legacy-unsigned")
        await store.record(alert, "hold_batch", "old-client", db=db, audit=legacy)
        audit = ResponseAudit(reason="excursion", signature_meaning="responsible")
        hold, created = await store.record(alert, "quarantine_lot", "qa@x", db=db, audit=audit)
        assert (created, hold.action, len(db.rows)) == (False, "hold_batch", 1)
        assert hold.signature() is None  # legacy-unsigned prints no signature line
