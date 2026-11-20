"""The model-alert contract shared by the scorer, the alert engine, the UI and Genie.

A model alert is an ordinary alert row (same table, fingerprint and lifecycle)
with ``rule_id = MODEL_RULE_ID``. ``value`` is the risk score and ``threshold``
is the cut-off. Its metadata carries the keys in ``RISK_METADATA_KEYS``; the
parallel GenAI work reads them. See docs/ml-risk-model.md.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from iot_twin.domain.catalog import AlertRule

MODEL_RULE_ID = "medtech-model-spindle-risk"
MODEL_FAULT_CODE = "PRED-SPINDLE3-THERMAL"
MODEL_SEVERITY = "warn"
MODEL_ACTION = (
    "Predicted thermal excursion: check coolant and spindle load, and prepare a "
    "QA hold before Spindle 3 reaches its validated limit"
)

ALERT_KIND_PREDICTED = "predicted"
ALERT_KIND_BREACHED = "breached"

# The alert attaches to Spindle 3; the fingerprint covers all of its sensors.
RISK_SENSOR_ID = "mfg-sns-cnc_1_spindle_temp"
RISK_SENSOR_IDS: tuple[str, ...] = (
    "mfg-sns-cnc_1_spindle_temp",
    "mfg-sns-cnc_1_vibration",
    "mfg-sns-cnc_1_spindle_power",
    "mfg-sns-cnc_1_spindle_speed",
)

# Names consistent with RESOURCE_PREFIX (default pharma-twin / pharma_twin).
DEFAULT_ENDPOINT_NAME = "pharma-twin-spindle-risk"
DEFAULT_MODEL_SCHEMA = "pharma_twin"
DEFAULT_MODEL_SHORT_NAME = "spindle_thermal_risk"
CHAMPION_ALIAS = "champion"

# Cut-off the champion training run chose on validation (decision_threshold metric).
DEFAULT_THRESHOLD = 0.22
DEFAULT_HYSTERESIS = 0.15
TOP_FEATURES_K = 3

RISK_METADATA_KEYS: tuple[str, ...] = (
    "alert_kind",
    "risk_score",
    "risk_threshold",
    "model_name",
    "model_version",
    "top_features",
    "horizon_s",
    "scored_at",
)


def alert_kind_for(rule_id: str | None, metadata: Mapping[str, Any] | None = None) -> str:
    """``predicted`` for model alerts, ``breached`` for limit / threshold rules."""
    if rule_id == MODEL_RULE_ID:
        return ALERT_KIND_PREDICTED
    if metadata and metadata.get("alert_kind") == ALERT_KIND_PREDICTED:
        return ALERT_KIND_PREDICTED
    return ALERT_KIND_BREACHED


def risk_score_of(rule_id: str | None, metadata: Mapping[str, Any] | None) -> float | None:
    if alert_kind_for(rule_id, metadata) != ALERT_KIND_PREDICTED or not metadata:
        return None
    try:
        return float(metadata["risk_score"])
    except (KeyError, TypeError, ValueError):
        return None


def risk_alert_rule(
    threshold: float = DEFAULT_THRESHOLD,
    hysteresis: float = DEFAULT_HYSTERESIS,
    duration_s: float = 0.0,
) -> AlertRule:
    """The model alert as an engine rule: opens at ``risk >= threshold``."""
    return AlertRule(
        rule_id=MODEL_RULE_ID,
        sensor_id=RISK_SENSOR_ID,
        operator="gte",
        threshold=float(threshold),
        severity=MODEL_SEVERITY,
        fault_code=MODEL_FAULT_CODE,
        duration_s=float(duration_s),
        hysteresis=float(hysteresis),
        action=MODEL_ACTION,
        metadata=(
            ("parameter_type", "predicted_process_risk"),
            ("parameter", "Spindle 3 thermal-excursion risk"),
            ("validated_limit", "60 °C"),
        ),
    )


def model_full_name(catalog: str, schema: str = DEFAULT_MODEL_SCHEMA) -> str:
    return f"{catalog}.{schema}.{DEFAULT_MODEL_SHORT_NAME}"
