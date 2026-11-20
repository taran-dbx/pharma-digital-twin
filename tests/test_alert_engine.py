"""Deterministic alert engine tests [qa]."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import AlertRule, list_alert_rules
from iot_twin.services.alerts.alert_engine import (
    AlertEngine,
    AlertRecord,
    evaluate_and_emit,
    fingerprint_for,
)

pytestmark = pytest.mark.asyncio


def _catalog_spindle_rule() -> AlertRule:
    return next(
        rule
        for rule in list_alert_rules("manufacturing")
        if rule.rule_id == "mfg-alert-spindle-temp"
    )


def _spindle_rule(**overrides) -> AlertRule:
    """Catalog spindle rule that opens on a single reading unless overridden."""
    base = _catalog_spindle_rule()
    payload = {
        "rule_id": base.rule_id,
        "sensor_id": base.sensor_id,
        "operator": base.operator,
        "threshold": base.threshold,
        "severity": base.severity,
        "fault_code": base.fault_code,
        "duration_s": 0.0,
        "hysteresis": getattr(base, "hysteresis", 0.0),
        "correlate_sensor_ids": getattr(base, "correlate_sensor_ids", ()),
    }
    payload.update(overrides)
    return AlertRule(**payload)


def _reading(value: float, sensor_id: str = "mfg-sns-cnc_1_spindle_temp") -> dict:
    return {
        "sensor_id": sensor_id,
        "sensor_name": "cnc_1_spindle_temp",
        "industry": "manufacturing",
        "value": value,
    }


class FakeAlertDb:
    """In-memory Lakebase stand-in with fingerprint identity and CTE upserts."""

    def __init__(self) -> None:
        self.by_fp: dict[str, dict] = {}
        self.by_id: dict[str, dict] = {}
        self.events: list[dict] = []
        self.calls: list[tuple[str, str]] = []

    def _row(self, args: tuple) -> dict:
        version = int(args[18]) if len(args) > 18 and args[18] is not None else 0
        return {
            "id": str(args[0]),
            "fingerprint": args[1],
            "industry": args[2],
            "pipeline_id": args[3],
            "rule_id": args[4],
            "sensor_id": args[5],
            "sensor_ids": list(args[6] or []),
            "severity": args[7],
            "status": args[8],
            "fault_code": args[9],
            "value": args[10],
            "threshold": args[11],
            "opened_at": args[12],
            "updated_at": args[13],
            "acknowledged_at": args[14],
            "acknowledged_by": args[15],
            "cleared_at": args[16],
            "metadata": args[17] if len(args) > 17 else {},
            "version": version,
        }

    def _upsert(self, args: tuple) -> dict:
        proposed_id = str(args[0])
        fingerprint = args[1]
        existing = self.by_fp.get(fingerprint)
        row = self._row(args)
        proposed_status = row["status"]
        proposed_version = int(row.get("version") or 0)
        applied = True
        if existing is not None:
            current_status = existing["status"]
            current_version = int(existing.get("version") or 0)
            applied = (
                proposed_status == "cleared"
                or current_status == "cleared"
                or (
                    current_status != "cleared"
                    and not (current_status == "acknowledged" and proposed_status == "open")
                    and proposed_version >= current_version
                )
            )
            if not applied:
                canonical = dict(existing)
                canonical["applied"] = False
                return canonical
            row["id"] = existing["id"]
            if current_status != "cleared":
                row["opened_at"] = existing["opened_at"]
            if proposed_status != "open" or current_status != "cleared":
                row["version"] = current_version + 1
            existing.update(row)
            row = existing
        else:
            row["id"] = proposed_id
            row["version"] = 0
            self.by_fp[fingerprint] = row
            self.by_id[proposed_id] = row
        event_type_idx = 19 if len(args) > 21 else 18
        if len(args) > event_type_idx + 2:
            self.events.append(
                {
                    "alert_id": row["id"],
                    "event_type": args[event_type_idx],
                    "fingerprint": args[event_type_idx + 1],
                    "payload": args[event_type_idx + 2],
                }
            )
        self.by_id[row["id"]] = row
        out = dict(row)
        out["applied"] = True
        return out

    async def fetchrow(self, sql: str, *args):
        self.calls.append(("fetchrow", sql))
        compact = " ".join(sql.split()).lower()
        if "insert into alert" in compact and "insert into alert_event" in compact:
            return self._upsert(args)
        if "from alert where fingerprint" in compact:
            found = self.by_fp.get(args[0])
            return dict(found) if found else None
        if "from alert where id" in compact:
            found = self.by_id.get(str(args[0]))
            return dict(found) if found else None
        return None

    async def fetch(self, sql: str, *args):
        self.calls.append(("fetch", sql))
        status, industry, severity = (args + (None, None, None))[:3]
        rows = list(self.by_id.values())
        if status is None:
            rows = [row for row in rows if row["status"] in {"open", "acknowledged"}]
        else:
            rows = [row for row in rows if row["status"] == status]
        if industry:
            rows = [row for row in rows if row["industry"] == industry]
        if severity:
            rows = [row for row in rows if row["severity"] == severity]
        return [dict(row) for row in rows]

    async def execute(self, sql: str, *args):
        self.calls.append(("execute", sql))
        compact = " ".join(sql.split()).lower()
        if "insert into alert" in compact:
            self._upsert(args)
        return "OK"


class FakeClock:
    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def __call__(self) -> datetime:
        return self.instant

    def advance(self, seconds: float) -> None:
        self.instant = self.instant + timedelta(seconds=seconds)


class TestAlertEngineStateMachine:
    async def test_opens_alert_from_catalog_gt_rule_c7402_smoke(self) -> None:
        """[C7402][smoke] Catalog gt rule opens an alert when exceeded."""
        engine = AlertEngine()
        transitions = await engine.evaluate(
            [_reading(85.0)],
            [_spindle_rule()],
            industry="manufacturing",
        )
        assert len(transitions) == 1
        assert transitions[0].event_type == "alert_opened"
        assert transitions[0].alert.status == "open"
        assert transitions[0].alert.severity == "crit"
        assert transitions[0].alert.fault_code == "M-SPINDLE-OVERTEMP"

    async def test_does_not_open_when_below_threshold_c7402_regression(self) -> None:
        """[C7402][regression] Values inside the limit stay quiet."""
        engine = AlertEngine()
        transitions = await engine.evaluate(
            [_reading(60.0)],
            [_spindle_rule()],
            industry="manufacturing",
        )
        assert transitions == []
        assert await engine.list_alerts() == []

    async def test_empty_and_none_readings_are_safe_c7402_defect(self) -> None:
        """[C7402][defect] Empty, None, and non-numeric readings do not raise."""
        engine = AlertEngine()
        assert await engine.evaluate([], [_spindle_rule()]) == []
        assert await engine.evaluate(None, [_spindle_rule()]) == []  # type: ignore[arg-type]
        transitions = await engine.evaluate(
            [{"sensor_id": "mfg-sns-cnc_1_spindle_temp", "value": None}],
            [_spindle_rule()],
        )
        assert transitions == []
        transitions = await engine.evaluate(
            [{"sensor_id": "mfg-sns-cnc_1_spindle_temp", "value": "hot"}],
            [_spindle_rule()],
        )
        assert transitions == []

    async def test_duration_holds_open_until_elapsed_c7403_regression(self) -> None:
        """[C7403][regression] Duration must elapse before an alert opens."""
        clock = FakeClock(datetime(2026, 10, 13, tzinfo=UTC))
        engine = AlertEngine(now_fn=clock)
        rule = _spindle_rule(duration_s=10.0)
        first = await engine.evaluate([_reading(90.0)], [rule], industry="manufacturing")
        assert first == []
        clock.advance(9.0)
        still = await engine.evaluate([_reading(91.0)], [rule], industry="manufacturing")
        assert still == []
        clock.advance(1.0)
        opened = await engine.evaluate([_reading(92.0)], [rule], industry="manufacturing")
        assert len(opened) == 1
        assert opened[0].event_type == "alert_opened"

    async def test_duration_resets_when_condition_clears_c7403_defect(self) -> None:
        """[C7403][defect] Leaving the band before duration elapses resets pending."""
        clock = FakeClock(datetime(2026, 10, 13, tzinfo=UTC))
        engine = AlertEngine(now_fn=clock)
        rule = _spindle_rule(duration_s=10.0)
        await engine.evaluate([_reading(90.0)], [rule])
        clock.advance(5.0)
        await engine.evaluate([_reading(50.0)], [rule])
        clock.advance(10.0)
        later = await engine.evaluate([_reading(50.0)], [rule])
        assert later == []

    async def test_hysteresis_holds_until_clear_band_c7404_regression(self) -> None:
        """[C7404][regression] Hysteresis keeps an alert open in the deadband."""
        engine = AlertEngine()
        rule = _spindle_rule(hysteresis=2.0)
        opened = await engine.evaluate([_reading(85.0)], [rule])
        assert opened[0].event_type == "alert_opened"
        hold = await engine.evaluate([_reading(59.0)], [rule])
        assert hold == []
        assert (await engine.list_alerts())[0].status == "open"
        cleared = await engine.evaluate([_reading(58.0)], [rule])
        assert len(cleared) == 1
        assert cleared[0].event_type == "alert_cleared"
        assert cleared[0].alert.status == "cleared"

    async def test_catalog_spindle_rule_ignores_noise_spikes_c7415_defect(self) -> None:
        """[C7415][defect][regression] One noisy sample past 60 °C neither opens nor re-opens."""
        clock = FakeClock(datetime(2026, 10, 20, 13, 18, 30, tzinfo=UTC))
        engine = AlertEngine(now_fn=clock)
        rule = _catalog_spindle_rule()
        assert rule.duration_s == 2.0
        assert rule.action == "Inspection required"

        async def feed(values: list[float], step_s: float = 0.25) -> list[str]:
            events: list[str] = []
            for value in values:
                for transition in await engine.evaluate(
                    [_reading(value)], [rule], industry="manufacturing"
                ):
                    events.append(transition.event_type)
                clock.advance(step_s)
            return events

        assert await feed([58.4, 61.2, 57.9, 59.1]) == []
        assert await feed([61.0, 62.3, 61.8, 63.0, 62.7, 64.1, 63.5, 64.9]) == []
        assert await feed([65.2]) == ["alert_opened"]
        # Cooling: 54.7 clears, the 60.4 spike that re-opened it live is ignored.
        events = await feed([54.7, 60.4, 53.8, 52.1, 49.6, 47.0])
        assert events == ["alert_cleared"]
        assert await engine.list_alerts() == []

    async def test_batches_without_the_sensor_keep_duration_pending_c7415_defect(self) -> None:
        """[C7415][defect] Per-partition batches lacking the spindle reading do not reset the 2 s timer."""
        clock = FakeClock(datetime(2026, 10, 20, 13, 38, 50, tzinfo=UTC))
        engine = AlertEngine(now_fn=clock)
        rule = _catalog_spindle_rule()
        other = _reading(2.0, sensor_id="mfg-sns-cnc_1_vibration")
        other["sensor_name"] = "cnc_1_vibration"

        events: list[str] = []
        for step in range(10):
            batch = [_reading(70.0 + step * 0.1)] if step % 2 == 0 else [other]
            for transition in await engine.evaluate(batch, [rule], industry="manufacturing"):
                events.append(transition.event_type)
            clock.advance(0.25)

        assert events == ["alert_opened"]

    async def test_reopen_refreshes_opened_at_in_lakebase_c7416_defect(self) -> None:
        """[C7416][defect] A re-opened alert reports when this heat event started, not the first one."""
        first_open = datetime(2026, 10, 20, 13, 13, 49, tzinfo=UTC)
        clock = FakeClock(first_open)
        engine = AlertEngine(now_fn=clock)
        db = FakeAlertDb()
        rule = _spindle_rule()

        await engine.evaluate([_reading(61.2)], [rule], db=db)
        clock.advance(56.0)
        await engine.evaluate([_reading(54.0)], [rule], db=db)
        clock.advance(240.0)
        reopened = await engine.evaluate([_reading(60.1)], [rule], db=db)

        assert reopened[0].event_type == "alert_opened"
        second_open = first_open + timedelta(seconds=296)
        row = next(iter(db.by_fp.values()))
        assert row["opened_at"] == second_open
        listed = await AlertEngine().list_alerts(db=db)
        assert listed[0].opened_at == second_open
        assert listed[0].cleared_at is None
        assert any(
            "WHEN alert.status = 'cleared' THEN EXCLUDED.opened_at" in sql
            for _kind, sql in db.calls
        )

    async def test_same_fingerprint_is_idempotent_c7405_smoke(self) -> None:
        """[C7405][smoke] Repeat ticks with the same fingerprint do not re-open."""
        engine = AlertEngine()
        rule = _spindle_rule()
        first = await engine.evaluate([_reading(85.0)], [rule], pipeline_id="p1")
        second = await engine.evaluate([_reading(85.0)], [rule], pipeline_id="p1")
        assert first[0].event_type == "alert_opened"
        assert second == []
        assert fingerprint_for(rule.rule_id, [rule.sensor_id], "p1") == first[0].alert.fingerprint
        alerts = await engine.list_alerts()
        assert len(alerts) == 1

    async def test_value_change_emits_updated_c7405_regression(self) -> None:
        """[C7405][regression] A new value on an open alert emits alert_updated."""
        engine = AlertEngine()
        rule = _spindle_rule()
        await engine.evaluate([_reading(85.0)], [rule])
        updated = await engine.evaluate([_reading(95.0)], [rule])
        assert len(updated) == 1
        assert updated[0].event_type == "alert_updated"
        assert updated[0].alert.value == 95.0

    async def test_unicode_sensor_ids_fingerprint_stably_c7405_defect(self) -> None:
        """[C7405][defect] Unicode sensor ids produce a stable fingerprint."""
        rule = AlertRule(
            rule_id="uni-1",
            sensor_id="温度-1",
            operator="gt",
            threshold=1.0,
            severity="warn",
            fault_code="U-1",
        )
        a = fingerprint_for(rule.rule_id, [rule.sensor_id], "")
        b = fingerprint_for(rule.rule_id, [rule.sensor_id], "")
        assert a == b
        assert len(a) == 64

    async def test_multi_sensor_correlation_requires_all_c7406_regression(self) -> None:
        """[C7406][regression] Correlation rules open only when every sensor violates."""
        engine = AlertEngine()
        correlated = AlertRule(
            rule_id="mfg-alert-spindle-temp",
            sensor_id="mfg-sns-cnc_1_spindle_temp",
            operator="gt",
            threshold=80.0,
            severity="crit",
            fault_code="M-SPINDLE-OVERTEMP",
            correlate_sensor_ids=("mfg-sns-cnc_1_vibration",),
            correlate_operator="gt",
            correlate_threshold=8.0,
        )
        only_temp = await engine.evaluate(
            [
                _reading(90.0),
                {
                    "sensor_id": "mfg-sns-cnc_1_vibration",
                    "sensor_name": "cnc_1_vibration",
                    "value": 1.0,
                },
            ],
            [correlated],
        )
        assert only_temp == []
        both = await engine.evaluate(
            [
                _reading(90.0),
                {
                    "sensor_id": "mfg-sns-cnc_1_vibration",
                    "sensor_name": "cnc_1_vibration",
                    "value": 9.0,
                },
            ],
            [correlated],
        )
        assert len(both) == 1
        assert both[0].event_type == "alert_opened"
        assert "mfg-sns-cnc_1_vibration" in both[0].alert.sensor_ids
        assert "mfg-sns-cnc_1_spindle_temp" in both[0].alert.sensor_ids
        expected_fp = fingerprint_for(
            correlated.rule_id,
            [correlated.sensor_id, *correlated.correlate_sensor_ids],
            None,
        )
        assert both[0].alert.fingerprint == expected_fp

    async def test_correlated_clears_when_any_required_recovers_c7406_defect(self) -> None:
        """[C7406][defect] Any required correlated condition recovering past hysteresis clears the same fingerprint."""
        engine = AlertEngine()
        correlated = AlertRule(
            rule_id="mfg-alert-spindle-temp",
            sensor_id="mfg-sns-cnc_1_spindle_temp",
            operator="gt",
            threshold=80.0,
            severity="crit",
            fault_code="M-SPINDLE-OVERTEMP",
            hysteresis=2.0,
            correlate_sensor_ids=("mfg-sns-cnc_1_vibration",),
            correlate_operator="gt",
            correlate_threshold=8.0,
        )
        opened = await engine.evaluate(
            [
                _reading(90.0),
                {
                    "sensor_id": "mfg-sns-cnc_1_vibration",
                    "sensor_name": "cnc_1_vibration",
                    "value": 9.0,
                },
            ],
            [correlated],
        )
        fingerprint = opened[0].alert.fingerprint
        hold = await engine.evaluate(
            [
                _reading(79.0),
                {
                    "sensor_id": "mfg-sns-cnc_1_vibration",
                    "sensor_name": "cnc_1_vibration",
                    "value": 7.0,
                },
            ],
            [correlated],
        )
        assert hold == []
        assert (await engine.list_alerts())[0].status == "open"
        secondary_recovered = await engine.evaluate(
            [
                _reading(90.0),
                {
                    "sensor_id": "mfg-sns-cnc_1_vibration",
                    "sensor_name": "cnc_1_vibration",
                    "value": 1.0,
                },
            ],
            [correlated],
        )
        assert len(secondary_recovered) == 1
        assert secondary_recovered[0].event_type == "alert_cleared"
        assert secondary_recovered[0].alert.fingerprint == fingerprint

    async def test_correlated_open_and_clear_share_fingerprint_c7406_defect(self) -> None:
        """[C7406][defect] Correlated rules keep one fingerprint from open through clear."""
        engine = AlertEngine()
        correlated = AlertRule(
            rule_id="mfg-alert-spindle-temp",
            sensor_id="mfg-sns-cnc_1_spindle_temp",
            operator="gt",
            threshold=80.0,
            severity="crit",
            fault_code="M-SPINDLE-OVERTEMP",
            hysteresis=2.0,
            correlate_sensor_ids=("mfg-sns-cnc_1_vibration",),
            correlate_operator="gt",
            correlate_threshold=8.0,
        )
        readings_open = [
            _reading(90.0),
            {
                "sensor_id": "mfg-sns-cnc_1_vibration",
                "sensor_name": "cnc_1_vibration",
                "value": 9.0,
            },
        ]
        opened = await engine.evaluate(readings_open, [correlated])
        assert opened[0].event_type == "alert_opened"
        fingerprint = opened[0].alert.fingerprint
        alert_id = opened[0].alert.id
        cleared = await engine.evaluate(
            [
                _reading(70.0),
                {
                    "sensor_id": "mfg-sns-cnc_1_vibration",
                    "sensor_name": "cnc_1_vibration",
                    "value": 1.0,
                },
            ],
            [correlated],
        )
        assert len(cleared) == 1
        assert cleared[0].event_type == "alert_cleared"
        assert cleared[0].alert.fingerprint == fingerprint
        assert cleared[0].alert.id == alert_id

    async def test_works_in_memory_when_db_unavailable_c7407_smoke(self) -> None:
        """[C7407][smoke] Engine stores alerts in memory when no DB is passed."""
        engine = AlertEngine()
        await engine.evaluate([_reading(85.0)], [_spindle_rule()])
        alerts = await engine.list_alerts(db=None)
        assert len(alerts) == 1
        assert alerts[0].status == "open"

    async def test_persists_when_db_available_c7407_regression(self) -> None:
        """[C7407][regression] Open transitions persist alert and event atomically."""
        engine = AlertEngine()
        db = FakeAlertDb()
        await engine.evaluate([_reading(85.0)], [_spindle_rule()], db=db)
        sqls = [sql for _kind, sql in db.calls]
        joined = "\n".join(sqls)
        assert "INSERT INTO alert" in joined
        assert "INSERT INTO alert_event" in joined
        assert any("INSERT INTO alert" in sql and "INSERT INTO alert_event" in sql for sql in sqls)
        assert db.by_fp
        assert db.events

    async def test_restart_rehydrates_persisted_id_c7407_defect(self) -> None:
        """[C7407][defect] After restart, in-memory/SSE ids match Lakebase fingerprint identity."""
        db = FakeAlertDb()
        first = AlertEngine()
        opened = await first.evaluate([_reading(85.0)], [_spindle_rule()], db=db, pipeline_id="p1")
        persisted_id = opened[0].alert.id
        assert db.by_id[persisted_id]["fingerprint"] == opened[0].alert.fingerprint

        restarted = AlertEngine()
        again = await restarted.evaluate(
            [_reading(85.0)], [_spindle_rule()], db=db, pipeline_id="p1"
        )
        assert again == []
        listed = await restarted.list_alerts(db=db, status="open")
        assert listed[0].id == persisted_id

        await first.evaluate([_reading(50.0)], [_spindle_rule()], db=db, pipeline_id="p1")
        third = AlertEngine()
        reopened = await third.evaluate(
            [_reading(90.0)], [_spindle_rule()], db=db, pipeline_id="p1"
        )
        assert reopened[0].event_type == "alert_opened"
        assert reopened[0].alert.id == persisted_id

    async def test_stale_open_write_cannot_regress_acknowledged_c7407_defect(self) -> None:
        """[C7407][defect] A stale open/update CTE never overwrites acknowledged; cleared still wins."""
        db = FakeAlertDb()
        engine = AlertEngine()
        opened = await engine.evaluate(
            [_reading(85.0)], [_spindle_rule()], db=db, pipeline_id="stale-p"
        )
        alert_id = opened[0].alert.id
        ack = await engine.acknowledge(alert_id, user="ops@example.com", db=db)
        assert ack is not None
        assert db.by_id[alert_id]["status"] == "acknowledged"
        compact = " ".join(sql for kind, sql in db.calls if kind == "fetchrow").lower()
        assert "version" in compact
        assert "acknowledged" in compact

        ghost = AlertEngine()
        row = db.by_id[alert_id]
        ghost._store(
            AlertRecord(
                id=row["id"],
                fingerprint=row["fingerprint"],
                industry=row["industry"],
                pipeline_id=row["pipeline_id"],
                rule_id=row["rule_id"],
                sensor_id=row["sensor_id"],
                sensor_ids=list(row["sensor_ids"]),
                severity=row["severity"],
                status="open",
                fault_code=row["fault_code"],
                value=row["value"],
                threshold=row["threshold"],
                opened_at=row["opened_at"],
                updated_at=row["updated_at"],
                version=0,
            )
        )
        stale_result = await ghost.evaluate(
            [_reading(95.0)], [_spindle_rule()], db=db, pipeline_id="stale-p"
        )
        assert db.by_id[alert_id]["status"] == "acknowledged"
        memory = ghost.get_alert_sync(alert_id)
        assert memory is not None
        assert memory.status == "acknowledged"
        assert memory.id == alert_id
        assert stale_result == [] or stale_result[0].alert.status == "acknowledged"

        cleared = await engine.evaluate(
            [_reading(50.0)], [_spindle_rule()], db=db, pipeline_id="stale-p"
        )
        assert cleared[0].event_type == "alert_cleared"
        assert db.by_id[alert_id]["status"] == "cleared"

    async def test_engine_lock_serializes_same_fingerprint_c7407_regression(self) -> None:
        """[C7407][regression] Concurrent evaluate/ack mutations share one fingerprint identity."""
        engine = AlertEngine()
        rule = _spindle_rule()
        first, second = await asyncio.gather(
            engine.evaluate([_reading(85.0)], [rule], pipeline_id="p-lock"),
            engine.evaluate([_reading(85.0)], [rule], pipeline_id="p-lock"),
        )
        opened = [item for batch in (first, second) for item in batch]
        assert len(opened) == 1
        alerts = await engine.list_alerts()
        assert len(alerts) == 1
        ack_task = engine.acknowledge(alerts[0].id, user="ops@example.com")
        update_task = engine.evaluate([_reading(95.0)], [rule], pipeline_id="p-lock")
        ack, updated = await asyncio.gather(ack_task, update_task)
        assert engine.get_alert_sync(alerts[0].id) is not None
        assert len(await engine.list_alerts()) == 1
        assert ack is not None or updated

    async def test_list_defaults_to_active_states_c7408_regression(self) -> None:
        """[C7408][regression] Default list/snapshot skips cleared history unless filtered."""
        engine = AlertEngine()
        rule = _spindle_rule()
        await engine.evaluate([_reading(85.0)], [rule], pipeline_id="cleared-pipe")
        await engine.evaluate([_reading(50.0)], [rule], pipeline_id="cleared-pipe")
        await engine.evaluate([_reading(85.0)], [rule], pipeline_id="open-pipe")
        defaulted = await engine.list_alerts()
        assert [item.status for item in defaulted] == ["open"]
        assert defaulted[0].pipeline_id == "open-pipe"
        cleared = await engine.list_alerts(status="cleared")
        assert len(cleared) == 1
        assert cleared[0].status == "cleared"
        db = FakeAlertDb()
        await engine.list_alerts(db=db)
        list_sql = next(sql for kind, sql in db.calls if kind == "fetch")
        compact = " ".join(list_sql.split()).lower()
        assert "open" in compact and "acknowledged" in compact

    async def test_acknowledge_emits_lifecycle_event_c7408_smoke(self) -> None:
        """[C7408][smoke] Acknowledge moves open → acknowledged."""
        engine = AlertEngine()
        opened = await engine.evaluate([_reading(85.0)], [_spindle_rule()])
        alert_id = opened[0].alert.id
        ack = await engine.acknowledge(alert_id, user="ops@example.com")
        assert ack is not None
        assert ack.event_type == "alert_acknowledged"
        assert ack.alert.status == "acknowledged"
        assert ack.alert.acknowledged_by == "ops@example.com"

    async def test_acknowledge_unknown_id_returns_none_c7408_defect(self) -> None:
        """[C7408][defect] Unknown or empty alert ids return None."""
        engine = AlertEngine()
        assert await engine.acknowledge("", user="ops@example.com") is None
        assert await engine.acknowledge("missing", user="ops@example.com") is None

    async def test_ingest_threshold_alerts_open_from_max_c7410_smoke(self) -> None:
        """[C7410][smoke] SSS threshold payloads open lifecycle alerts."""
        engine = AlertEngine()
        transitions = await engine.ingest_threshold_alerts(
            [
                {
                    "pipeline_id": "pipe-1",
                    "sensor_id": "temp-01",
                    "sensor_type": "temperature",
                    "value": 120.5,
                    "threshold_min": 0.0,
                    "threshold_max": 100.0,
                    "timestamp": "2026-04-05T10:00:00Z",
                }
            ]
        )
        assert len(transitions) == 1
        assert transitions[0].event_type == "alert_opened"
        assert transitions[0].alert.sensor_id == "temp-01"
        assert transitions[0].alert.pipeline_id == "pipe-1"

    async def test_electric_power_readings_use_energy_rules_c7402_regression(self) -> None:
        """[C7402][regression] Simulator electric_power ticks resolve to energy rules."""
        state = AppState()
        state.alert_engine = AlertEngine()
        transitions = await evaluate_and_emit(
            state,
            [
                {
                    "sensor_id": "energy-sns-gen_1_frequency",
                    "sensor_name": "gen_1_frequency",
                    "industry": "electric_power",
                    "value": 49.2,
                }
            ],
            industry="electric_power",
        )
        assert len(transitions) == 1
        assert transitions[0].event_type == "alert_opened"
        assert transitions[0].alert.industry == "energy"
        assert transitions[0].alert.fault_code == "E-FREQ-LOW"
