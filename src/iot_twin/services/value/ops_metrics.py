"""Operational metrics for the Head of Manufacturing Operations (pure, no I/O).

Totals come from the full-window SQL in ``episodes.py``; the distributions and
the recent-events table use the latest ``MAX_SAMPLES`` / ``RECENT_EVENTS``
episodes and say so in ``meta``.

* **time to detect (TTD)** = ``opened_at - onset``.
* **time to acknowledge (TTA)** = first of (acknowledge event, first signed
  decision) - ``opened_at``; a signed decision implies acknowledgement.
* **time to disposition** = first containment decision (quarantine or CMM) - ``opened_at``.
* **MTTR** = mean ``cleared_at - opened_at`` over cleared episodes.
* **projected right-first-time** = ``PROJECTED_RFT_DEFINITION`` with lots and
  parts deduped across the window's excursions.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from iot_twin.domain.lot_context import PROJECTED_RFT_DEFINITION, PROJECTED_RFT_LABEL
from iot_twin.services.value.episodes import Episode, EpisodeWindow, WindowTotals


def percentile(values: Sequence[float], pct: float) -> float | None:
    """Linear-interpolated percentile (``pct`` in 0..100), as Postgres percentile_cont."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def projected_rft(totals: WindowTotals) -> dict[str, Any]:
    """(parts started - parts at risk) / parts started over the distinct lots touched."""
    started = totals.parts_started
    result: dict[str, Any] = {
        "label": PROJECTED_RFT_LABEL,
        "definition": PROJECTED_RFT_DEFINITION,
        "pct": None,
        "parts_started": started,
        "parts_at_risk": totals.parts_at_risk,
        "lots": totals.lots_touched,
    }
    if started:
        result["pct"] = round(100.0 * (started - totals.parts_at_risk) / started, 2)
    return result


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def recent_event(episode: Episode) -> dict[str, Any]:
    decisions = [
        name
        for name, at in (
            ("quarantine_lot", episode.quarantined_at),
            ("request_cmm_inspection", episode.cmm_at),
            ("open_ncr", episode.ncr_at),
            ("request_maintenance", episode.maintenance_at),
        )
        if at is not None
    ]
    return {
        "alert_id": episode.alert_id,
        "rule_id": episode.rule_id,
        "fault_code": episode.fault_code,
        "severity": episode.severity,
        "lot_id": episode.lot_id,
        "onset_at": _iso(episode.onset_at),
        "onset_source": episode.onset_source,
        "opened_at": _iso(episode.opened_at),
        "cleared_at": _iso(episode.cleared_at),
        "deadline": _iso(episode.deadline),
        "deadline_before_alert": episode.deadline_before_alert,
        "peak_value": episode.peak_value,
        "threshold": episode.threshold,
        "time_to_detect_s": round(episode.time_to_detect_s, 3),
        "time_to_acknowledge_s": episode.time_to_acknowledge_s,
        "time_to_disposition_s": episode.time_to_disposition_s,
        "parts_at_risk": len(episode.disposition["parts_at_risk"]),
        "caught_in_time": episode.caught_in_time,
        "decisions": decisions,
    }


def ops_summary(window: EpisodeWindow) -> dict[str, Any]:
    t = window.totals
    return {
        "episodes": t.episodes,
        "open_episodes": t.open_episodes,
        "time_to_detect": {
            **t.ttd,
            "from_first_reading": t.onset_from_reading,
            "from_rule_duration": t.episodes - t.onset_from_reading,
            "samples": [round(v, 3) for v in window.ttd_samples],
        },
        "time_to_acknowledge": {**t.tta, "samples": [round(v, 3) for v in window.tta_samples]},
        "time_to_disposition": t.ttdisp,
        "mttr": t.restore,
        "ncrs_opened": t.ncrs_opened,
        "cmm_inspections": t.cmm_inspections,
        "lots_quarantined_events": t.quarantine_events,
        "maintenance_requests": t.maintenance_requests,
        "production_stops": t.production_stops,
        "caught_in_time": t.caught_in_time,
        "deadline_before_alert": t.deadline_before_alert,
        "projected_rft": projected_rft(t),
        "recent_events": [recent_event(e) for e in window.recent],
    }
