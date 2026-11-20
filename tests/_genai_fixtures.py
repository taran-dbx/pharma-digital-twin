"""Shared builders for the AI action-plan tests: an alert, a fake endpoint, a valid plan."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from iot_twin.services.alerts.alert_engine import AlertRecord
from iot_twin.services.genai.grounding import GroundingSet

NOW = datetime(2026, 11, 20, 10, 5, 0, tzinfo=UTC)
OPENED = NOW - timedelta(seconds=50)
RISK = {
    "risk_score": 0.87,
    "model_name": "pharma_twin.ml.spindle_risk",
    "model_version": "3",
    "top_features": [{"feature": "spindle_temp_slope", "contribution": 0.41}],
}


def make_alert(*, risk: bool = True, opened: datetime = OPENED, **overrides: Any) -> AlertRecord:
    metadata: dict[str, Any] = {
        "validated_limit": "60 °C",
        "oq_ref": "OQ-TM-CNC1-014",
        "lot_id": "TM-HS12-261120-121",
        "part_serial": "TM-HS12-261120-121-03",
        "excursion_started_at": (opened - timedelta(seconds=2)).isoformat(),
        **(RISK if risk else {}),
    }
    base = {
        "id": str(uuid.uuid4()),
        "fingerprint": "fp-1",
        "industry": "manufacturing",
        "pipeline_id": "industry:manufacturing",
        "rule_id": "mfg-alert-spindle-temp",
        "sensor_id": "mfg-sns-cnc_1_spindle_temp",
        "sensor_ids": ["mfg-sns-cnc_1_spindle_temp"],
        "severity": "crit",
        "status": "open",
        "fault_code": "VPP-SPINDLE3-TEMP",
        "value": 64.2,
        "threshold": 60.0,
        "opened_at": opened,
        "updated_at": opened,
        "metadata": metadata,
    }
    return AlertRecord(**{**base, **overrides})


def reply(text: str, *, model: str = "claude-sonnet-5-5", usage: tuple[int, int] = (1200, 400)):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=text))],
        model=model,
        served_model_name="databricks-claude-sonnet-5-5",
        usage=SimpleNamespace(
            prompt_tokens=usage[0], completion_tokens=usage[1], total_tokens=sum(usage)
        ),
    )


class FakeServing:
    """``client.serving_endpoints.query`` returning queued answers or raising queued errors."""

    def __init__(self, *outcomes: Any) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def query(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, BaseException):
            raise outcome
        if callable(outcome):
            return outcome()
        return outcome


def fake_client(*outcomes: Any) -> SimpleNamespace:
    return SimpleNamespace(serving_endpoints=FakeServing(*outcomes))


def _first(grounding: GroundingSet, source: str) -> str:
    return grounding.by_source(source)[0].id


def valid_plan(grounding: GroundingSet) -> dict[str, Any]:
    """A plan whose every citation, number and identifier is grounded."""
    alert_id = _first(grounding, "alert")
    rule_id = _first(grounding, "alert_rule")
    lots = grounding.by_source("lot")
    lot_ids = [lot.data["lot_id"] for lot in lots]
    serials = [s for lot in lots for s in lot.data["parts_at_risk"]]
    lot_cites = [lot.id for lot in lots]
    risk = [item.id for item in grounding.by_source("risk_score")]
    maintenance = _first(grounding, "maintenance")
    return {
        "summary": {
            "text": "Spindle 3 peaked at 64.2 °C against its validated limit of 60 °C (+4.2 °C).",
            "citations": [alert_id, rule_id],
        },
        "disposition": {
            "quarantine": {
                "lots": lot_ids,
                "serials": serials,
                "rationale": f"{len(serials)} parts were cut while outside the validated limit.",
                "citations": lot_cites,
            },
            "cmm_inspection": {
                "scope": "Measure stem taper and neck geometry on every part at risk.",
                "serials": serials,
                "citations": [*lot_cites, rule_id],
            },
            "ncr": {
                "required": True,
                "rationale": "Validated parameter excursion.",
                "citations": [rule_id],
            },
            "maintenance_request": {
                "required": True,
                "rationale": "Last spindle inspection re-verified thermal compensation.",
                "citations": [maintenance],
            },
        },
        "risk_narrative": {
            "text": "The model rates this episode 87% likely to be a genuine thermal drift.",
            "citations": risk or [alert_id],
        },
        "steps": [
            {
                "order": 1,
                "owner_role": "QA",
                "action": "Place the lots at risk on QA hold.",
                "response_action": "quarantine_lot",
                "citations": lot_cites,
            },
            {
                "order": 2,
                "owner_role": "Maintenance",
                "action": "Inspect Spindle 3 bearings and thermal compensation.",
                "response_action": "request_maintenance",
                "citations": [maintenance],
            },
        ],
    }


def plan_text(grounding: GroundingSet, **changes: Any) -> str:
    return json.dumps({**valid_plan(grounding), **changes})
