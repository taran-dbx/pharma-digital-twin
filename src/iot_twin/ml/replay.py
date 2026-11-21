"""Replay live-format telemetry through the scorer's feature path (pure Python).

Used by ``scripts/replay_risk_endpoint.py`` to measure early warning through a
real serving endpoint when the updated producer is not live yet. The scoring
instants and feature rows follow ``SpindleRiskScorer``: one round every
``interval_s``, features from the last ``buffer_s`` of readings through
``build_features`` (the serving function), hours since maintenance from the
catalog.
"""

from __future__ import annotations

import bisect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from iot_twin.ml.lead_metrics import EventReport, alert_episodes, breach_episodes, event_report
from iot_twin.ml.spindle_features import (
    WINDOW_S,
    SpindleSeries,
    build_features,
    reading_epoch,
)
from iot_twin.scenario_engine.spindle_scenario import SPINDLE_CYCLE_S, spindle_cycle_plan

SCORER_INTERVAL_S = 5.0
SCORER_BUFFER_S = 2 * WINDOW_S


@dataclass(frozen=True)
class ReplayRow:
    at: float
    features: Mapping[str, float]


def replay_rows(
    readings: Sequence[Mapping[str, Any]],
    *,
    limit_c: float,
    last_maintenance_at: float | None,
    interval_s: float = SCORER_INTERVAL_S,
    buffer_s: float = SCORER_BUFFER_S,
) -> list[ReplayRow]:
    """Feature rows at every scoring instant that has a full window."""
    ordered = sorted(readings, key=lambda reading: reading_epoch(reading) or 0.0)
    stamps = [reading_epoch(reading) or 0.0 for reading in ordered]
    if not stamps:
        return []
    rows: list[ReplayRow] = []
    at = stamps[0] + WINDOW_S
    while at <= stamps[-1]:
        lo = bisect.bisect_right(stamps, at - buffer_s)
        hi = bisect.bisect_right(stamps, at)
        features = build_features(
            ordered[lo:hi], at=at, limit_c=limit_c, last_maintenance_at=last_maintenance_at
        )
        if features is not None:
            rows.append(ReplayRow(at=at, features=features))
        at += interval_s
    return rows


def replay_report(
    readings: Sequence[Mapping[str, Any]],
    times: Sequence[float],
    scores: Sequence[float],
    *,
    open_at: float,
    clear_at: float,
    limit_c: float,
    hysteresis_c: float,
    rule_delay_s: float,
    interval_s: float = SCORER_INTERVAL_S,
) -> tuple[EventReport, list[dict[str, Any]]]:
    """Event report for one replay, plus one record per breach with its scenario plan."""
    series = SpindleSeries.from_readings(readings)
    breaches = breach_episodes(
        *series.points["temp"], limit_c=limit_c, hysteresis_c=hysteresis_c, duration_s=rule_delay_s
    )
    alerts = alert_episodes(times, scores, open_at=open_at, clear_at=clear_at)
    start, end = min(times), max(times) + interval_s
    in_period = [episode for episode in breaches if start <= episode[0] < end]
    report = event_report(in_period, alerts, start=start, end=end, rule_delay_s=rule_delay_s)
    details = []
    for (onset, cleared), lead in zip(in_period, report.leads, strict=True):
        plan = spindle_cycle_plan(int(onset // SPINDLE_CYCLE_S))
        details.append(
            {
                "onset_s_into_cycle": round(onset % SPINDLE_CYCLE_S, 1),
                "cleared_after_s": None if cleared is None else round(cleared - onset, 1),
                "lead_s": None if lead is None else round(lead, 1),
                "plan_kind": plan.kind,
                "precursor": plan.precursor,
                "precursor_lead_s": round(plan.lead_s, 1) if plan.precursor != "none" else None,
            }
        )
    return report, details


def warned_share_by_precursor(details: Sequence[Mapping[str, Any]], min_lead_s: float) -> dict:
    """Breaches and the share warned >= ``min_lead_s`` per precursor type."""
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for detail in details:
        groups.setdefault(str(detail["precursor"]), []).append(detail)
    return {
        name: {
            "breaches": len(items),
            "warned_30s": sum(
                1 for d in items if d["lead_s"] is not None and d["lead_s"] >= min_lead_s
            ),
        }
        for name, items in sorted(groups.items())
    }
