"""AlertEngine.ingest_model_scores: predicted alerts with threshold-rule semantics [qa]."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from iot_twin.ml.risk_contract import (
    ALERT_KIND_BREACHED,
    ALERT_KIND_PREDICTED,
    MODEL_FAULT_CODE,
    MODEL_RULE_ID,
    RISK_METADATA_KEYS,
    RISK_SENSOR_ID,
    RISK_SENSOR_IDS,
)
from iot_twin.services.alerts.alert_engine import AlertEngine, fingerprint_for
from iot_twin.services.alerts.alert_sse import EVENT_CLEARED, EVENT_OPENED, EVENT_UPDATED
from tests.test_alert_engine import FakeAlertDb

pytestmark = pytest.mark.asyncio

T0 = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
TOP = [{"feature": "temp_slope", "contribution": 1.2, "value": 9.5}]
LOT = {"lot_id": "TM-HS12-261120-001", "part_serial": "TM-HS12-261120-001-03"}


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now

    def tick(self, seconds: float = 5.0) -> None:
        self.now += timedelta(seconds=seconds)


def _score(risk: float, **overrides) -> dict:
    payload = {
        "industry": "manufacturing",
        "pipeline_id": "p1",
        "risk_score": risk,
        "threshold": 0.6,
        "hysteresis": 0.15,
        "model_name": "cat.pharma_twin.spindle_thermal_risk",
        "model_version": "3",
        "top_features": TOP,
        "horizon_s": 120.0,
        "scored_at": "2026-11-20T12:00:00+00:00",
        "lot": LOT,
    }
    payload.update(overrides)
    return payload


async def test_opens_a_predicted_alert_with_the_documented_metadata() -> None:
    engine = AlertEngine(now_fn=Clock())
    [transition] = await engine.ingest_model_scores([_score(0.8123)])
    alert = transition.alert
    assert transition.event_type == EVENT_OPENED
    assert alert.rule_id == MODEL_RULE_ID
    assert alert.fault_code == MODEL_FAULT_CODE
    assert alert.severity == "warn"
    assert alert.value == 0.81 and alert.threshold == 0.6
    assert alert.sensor_id == RISK_SENSOR_ID and alert.sensor_ids == list(RISK_SENSOR_IDS)
    assert alert.fingerprint == fingerprint_for(MODEL_RULE_ID, [RISK_SENSOR_ID], "p1")
    meta = alert.metadata
    assert set(RISK_METADATA_KEYS) <= set(meta)
    assert meta["alert_kind"] == ALERT_KIND_PREDICTED
    assert meta["risk_score"] == 0.8123
    assert meta["model_name"] == "cat.pharma_twin.spindle_thermal_risk"
    assert meta["model_version"] == "3"
    assert meta["top_features"] == TOP
    assert meta["lot_id"] == LOT["lot_id"] and meta["part_serial"] == LOT["part_serial"]
    # No excursion has happened yet, so no onset is recorded.
    assert "excursion_started_at" not in meta
    as_dict = alert.to_dict()
    assert as_dict["alert_kind"] == ALERT_KIND_PREDICTED
    assert as_dict["risk_score"] == 0.8123
    assert transition.extra == {"alert_kind": "predicted", "risk_score": 0.8123}


async def test_below_threshold_never_opens() -> None:
    engine = AlertEngine(now_fn=Clock())
    assert await engine.ingest_model_scores([_score(0.59)]) == []
    assert await engine.list_alerts() == []


async def test_repeated_scores_update_instead_of_duplicating() -> None:
    clock = Clock()
    engine = AlertEngine(now_fn=clock)
    [opened] = await engine.ingest_model_scores([_score(0.7)])
    clock.tick()
    assert await engine.ingest_model_scores([_score(0.701)]) == []  # same rounded value
    clock.tick()
    [updated] = await engine.ingest_model_scores([_score(0.9, model_version="4")])
    assert updated.event_type == EVENT_UPDATED
    assert updated.alert.id == opened.alert.id
    assert updated.alert.metadata["risk_score"] == 0.9
    assert updated.alert.metadata["model_version"] == "4"
    assert len(await engine.list_alerts()) == 1


async def test_hysteresis_band_holds_then_clears_then_reopens() -> None:
    clock = Clock()
    engine = AlertEngine(now_fn=clock)
    [opened] = await engine.ingest_model_scores([_score(0.8)])
    clock.tick()
    # 0.5 is under the cut-off but inside the 0.15 band: no event, still open.
    assert await engine.ingest_model_scores([_score(0.5)]) == []
    [still_open] = await engine.list_alerts()
    assert still_open.status == "open"
    clock.tick()
    [cleared] = await engine.ingest_model_scores([_score(0.44)])
    assert cleared.event_type == EVENT_CLEARED
    assert cleared.alert.metadata["risk_score"] == 0.44
    clock.tick()
    [reopened] = await engine.ingest_model_scores([_score(0.75)])
    assert reopened.event_type == EVENT_OPENED
    assert reopened.alert.id == opened.alert.id  # same fingerprint, new episode


async def test_model_and_threshold_alerts_do_not_share_a_fingerprint() -> None:
    from tests.test_alert_engine import _reading, _spindle_rule

    engine = AlertEngine(now_fn=Clock())
    [predicted] = await engine.ingest_model_scores([_score(0.9)])
    [breached] = await engine.evaluate(
        [_reading(66.0)], [_spindle_rule()], industry="manufacturing", pipeline_id="p1"
    )
    assert predicted.alert.fingerprint != breached.alert.fingerprint
    assert predicted.alert.id != breached.alert.id
    assert len(await engine.list_alerts()) == 2


async def test_threshold_alerts_report_breached() -> None:
    from tests.test_alert_engine import _reading, _spindle_rule

    engine = AlertEngine(now_fn=Clock())
    [opened] = await engine.evaluate(
        [_reading(66.0)], [_spindle_rule()], industry="manufacturing", pipeline_id="p1"
    )
    as_dict = opened.alert.to_dict()
    assert as_dict["alert_kind"] == ALERT_KIND_BREACHED
    assert as_dict["risk_score"] is None
    assert "excursion_started_at" in opened.alert.metadata


async def test_bad_scores_are_ignored_and_batches_are_bounded() -> None:
    engine = AlertEngine(now_fn=Clock())
    assert await engine.ingest_model_scores([{"risk_score": "x"}, "nope", {}]) == []  # type: ignore[list-item]
    many = [_score(0.9, pipeline_id=f"p{i}") for i in range(250)]
    assert len(await engine.ingest_model_scores(many)) == 200


async def test_defaults_apply_when_threshold_missing() -> None:
    engine = AlertEngine(now_fn=Clock())
    [opened] = await engine.ingest_model_scores(
        [_score(0.65, threshold=None, hysteresis=None, top_features="bad", lot=None)]
    )
    assert opened.alert.threshold == 0.22  # DEFAULT_THRESHOLD
    assert opened.alert.metadata["top_features"] == []
    # Without lot context on the score, the lot comes from the cell's lot clock.
    assert opened.alert.metadata["lot_id"].startswith("TM-HS12-")


async def test_persists_and_merges_metadata_through_lakebase() -> None:
    clock = Clock()
    db = FakeAlertDb()
    engine = AlertEngine(now_fn=clock)
    [opened] = await engine.ingest_model_scores([_score(0.8)], db=db)
    stored = db.by_fp[opened.alert.fingerprint]
    assert json.loads(stored["metadata"])["risk_score"] == 0.8
    assert db.events and db.events[-1]["event_type"] == "opened"
    payload = json.loads(db.events[-1]["payload"])
    assert payload["alert_kind"] == "predicted" and payload["risk_score"] == 0.8
    assert payload["metadata"]["model_version"] == "3"
    clock.tick()
    await engine.ingest_model_scores([_score(0.95)], db=db)
    assert json.loads(db.by_fp[opened.alert.fingerprint]["metadata"])["risk_score"] == 0.95


async def test_upsert_sql_refreshes_metadata_and_resets_it_on_reopen() -> None:
    captured: list[str] = []

    class SqlSpy(FakeAlertDb):
        async def fetchrow(self, sql: str, *args):  # type: ignore[override]
            captured.append(sql)
            return await super().fetchrow(sql, *args)

    engine = AlertEngine(now_fn=Clock())
    await engine.ingest_model_scores([_score(0.8)], db=SqlSpy())
    upsert = next(sql for sql in captured if "ON CONFLICT (fingerprint) DO UPDATE" in sql)
    assert upsert.count("metadata = CASE") == 1
    assert "WHEN alert.status = 'cleared' THEN EXCLUDED.metadata" in upsert
    # Only the model alert merges new metadata on update; the literal must track
    # MODEL_RULE_ID.
    assert f"WHEN alert.rule_id = '{MODEL_RULE_ID}'" in upsert
    assert "COALESCE(alert.metadata, '{}'::jsonb) || EXCLUDED.metadata" in upsert
