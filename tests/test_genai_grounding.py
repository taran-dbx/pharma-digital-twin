"""Grounding-set assembly for the AI action plan [qa]."""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from iot_twin.services.alerts.alert_responses import AlertResponse
from iot_twin.services.genai import grounding as g
from tests._genai_fixtures import NOW, OPENED, RISK, make_alert


class FakeDb:
    def __init__(self, rows=None, fail: Exception | None = None) -> None:
        self.rows = rows or []
        self.fail = fail
        self.calls: list[tuple] = []

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        if self.fail:
            raise self.fail
        return self.rows


def _response(alert_id: str) -> AlertResponse:
    return AlertResponse(
        id=str(uuid.uuid4()),
        alert_id=alert_id,
        episode_opened_at=OPENED,
        action="quarantine_lot",
        recorded_by="qa@example.com",
        created_at=NOW,
        reference={"hold": "QA hold"},
        reason="excursion",
        signature_meaning="approved",
    )


@pytest.mark.asyncio
async def test_every_source_is_cited_with_a_stable_id() -> None:
    alert = make_alert()
    event_id = str(uuid.uuid4())
    db = FakeDb(
        [
            {
                "id": event_id,
                "event_type": "opened",
                "created_at": OPENED,
                "payload": '{"value": 64.2}',
            }
        ]
    )

    async def runner(sql: str):
        return [{"bucket_start": "2026-11-20T10:04:10Z", "max_value": 64.2, "n": 10}]

    sql = g.readings_sql("`c`.`s`.`t`", "cnc_1_spindle_temp", OPENED, NOW, None)
    response = _response(alert.id)
    sources = g.GroundingSources(
        db=db, responses=[response], readings_sql=sql, readings_runner=runner
    )
    grounding = await g.assemble_grounding(alert, sources, NOW)

    sources_seen = {item.source for item in grounding.items}
    assert sources_seen == {
        "alert",
        "alert_event",
        "alert_response",
        "readings",
        "lot",
        "projected_rft",
        "alert_rule",
        "maintenance",
        "risk_score",
    }
    ids = grounding.ids
    assert f"alert:{alert.id}" in ids
    assert f"alert_event:{event_id}" in ids
    assert f"alert_response:{response.id}" in ids
    assert "alert_rule:mfg-alert-spindle-temp" in ids
    assert "maintenance:mfg-mnt-001" in ids
    assert f"risk_score:{alert.id}" in ids
    readings = grounding.by_source("readings")
    assert readings[0].id.startswith("readings:") and readings[0].id.endswith("#0")
    assert grounding.gaps == ()
    # Same inputs -> same rows -> same digest (the audit's grounding_hash).
    again = await g.assemble_grounding(alert, sources, NOW)
    assert again.digest() == grounding.digest()


@pytest.mark.asyncio
async def test_validated_limit_and_oq_pq_refs_come_from_the_catalog() -> None:
    grounding = await g.assemble_grounding(make_alert(), g.GroundingSources(), NOW)
    rule = grounding.by_source("alert_rule")[0].data
    assert rule["threshold"] == 60.0
    assert rule["validated_limit"] == "60 °C"
    assert rule["oq_ref"] == "OQ-TM-CNC1-014"
    assert rule["pq_ref"] == "PQ-TM-HS12-022"
    assert rule["process_spec"].startswith("PS-TM-MILL-HS-003")


@pytest.mark.asyncio
async def test_lots_cover_the_window_from_the_excursion_onset() -> None:
    alert = make_alert()
    grounding = await g.assemble_grounding(alert, g.GroundingSources(), NOW)
    start = OPENED - timedelta(seconds=2)
    assert grounding.window["start"] == start.isoformat()
    lots = grounding.by_source("lot")
    assert lots, "a Spindle 3 excursion touches at least one lot"
    serials = [s for lot in lots for s in lot.data["parts_at_risk"]]
    assert all(s.startswith(lot.data["lot_id"]) for lot in lots for s in lot.data["parts_at_risk"])
    rft = grounding.by_source("projected_rft")[0].data
    assert rft["parts_at_risk"] == len(serials)
    assert "Projected right-first-time" in rft["definition"]
    alert_row = grounding.by_source("alert")[0].data
    assert alert_row["ncr_id_if_opened"].startswith("NCR-20261120-")


@pytest.mark.asyncio
async def test_risk_score_is_optional_and_read_defensively() -> None:
    grounding = await g.assemble_grounding(make_alert(risk=False), g.GroundingSources(), NOW)
    assert not grounding.by_source("risk_score")
    assert "no model risk score on this alert" in grounding.gaps

    odd = make_alert(
        risk=False,
        metadata={"risk_score": "not-a-number", "top_features": "{broken"},
    )
    grounding = await g.assemble_grounding(odd, g.GroundingSources(), NOW)
    assert not grounding.by_source("risk_score")

    mapped = make_alert(risk=False, metadata={"risk_score": "0.5", "top_features": {"a": 1}})
    item = (await g.assemble_grounding(mapped, g.GroundingSources(), NOW)).by_source("risk_score")[
        0
    ]
    assert item.data["risk_score"] == 0.5
    assert item.data["top_features"] == [{"feature": "a", "contribution": 1}]
    assert item.data["model_name"] is None


@pytest.mark.asyncio
async def test_risk_score_from_the_parallel_model_alert() -> None:
    alert = make_alert(risk=False)
    model_alert = make_alert(
        rule_id=g.RISK_RULE_ID, fingerprint="fp-model", metadata=dict(RISK), opened_at=OPENED
    )
    sources = g.GroundingSources(other_alerts=[alert, model_alert])
    grounding = await g.assemble_grounding(alert, sources, NOW)
    risk = grounding.by_source("risk_score")
    assert [r.id for r in risk] == [f"risk_score:{model_alert.id}"]
    assert risk[0].data["model_version"] == "3"


@pytest.mark.asyncio
async def test_unavailable_sources_become_gaps_not_errors() -> None:
    async def failing(sql: str):
        raise RuntimeError("warehouse down")

    sources = g.GroundingSources(
        db=FakeDb(fail=RuntimeError("pg down")), readings_sql="SELECT 1", readings_runner=failing
    )
    grounding = await g.assemble_grounding(make_alert(), sources, NOW)
    assert "alert_event history read failed" in grounding.gaps
    assert "telemetry readings query failed" in grounding.gaps
    assert grounding.by_source("alert")


def test_readings_sql_is_bounded_and_quoted() -> None:
    sql = g.readings_sql("`c`.`s`.`t`", "x'y", OPENED, NOW, "industry = 'manufacturing'")
    assert "sensor_name = 'x''y'" in sql
    assert f"timestamp_seconds({int(OPENED.timestamp())})" in sql
    assert "AND industry = 'manufacturing'" in sql
    assert sql.endswith(f"LIMIT {g.MAX_READING_BUCKETS}")
    lo, hi = g.reading_window(make_alert(), NOW + timedelta(hours=5))
    assert (hi - lo).total_seconds() <= g.MAX_WINDOW_S


@pytest.mark.asyncio
async def test_untrusted_text_is_clipped() -> None:
    alert = make_alert(risk=False, metadata={"risk_score": 0.2, "model_name": "x" * 2000})
    grounding = await g.assemble_grounding(alert, g.GroundingSources(), NOW)
    name = grounding.by_source("risk_score")[0].data["model_name"]
    assert len(name) <= g.MAX_TEXT_CHARS + 1
