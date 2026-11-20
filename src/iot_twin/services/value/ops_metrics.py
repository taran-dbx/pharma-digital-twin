"""Operational metrics for the Head of Manufacturing Operations (pure, no I/O).

All metrics come from the episodes of one pipeline over a window (see
``episodes.py`` for onset, containment window and caught-in-time):

* **time to detect (TTD)** = ``opened_at - onset``.
* **time to acknowledge (TTA)** = first of (acknowledge event, first signed
  decision) - ``opened_at``; a signed decision implies acknowledgement.
* **time to disposition** = first containment decision (quarantine or CMM) - ``opened_at``.
* **MTTR** = mean ``cleared_at - opened_at`` over cleared episodes: the time to
  bring Spindle 3 back inside its validated limit.
* **projected right-first-time (RFT)** = (parts started - parts at risk) /
  parts started, over the lots the excursions touched. "Projected" because no
  CMM results exist yet; parts at risk are those cut in the containment window.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from iot_twin.domain.lot_context import PARTS_PER_LOT
from iot_twin.services.value.episodes import Episode

RECENT_EVENTS = 20
MAX_SAMPLES = 200


def percentile(values: Sequence[float], pct: float) -> float | None:
    """Linear-interpolated percentile (``pct`` in 0..100); None when empty."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def duration_stats(values: Iterable[float | None]) -> dict[str, Any]:
    present = [float(v) for v in values if v is not None]
    if not present:
        return {"count": 0, "mean": None, "p50": None, "p90": None, "max": None}
    return {
        "count": len(present),
        "mean": round(sum(present) / len(present), 3),
        "p50": round(percentile(present, 50) or 0.0, 3),
        "p90": round(percentile(present, 90) or 0.0, 3),
        "max": round(max(present), 3),
    }


def projected_rft(episodes: Iterable[Episode]) -> dict[str, Any]:
    """(parts started - parts at risk) / parts started over the lots touched."""
    lots: set[str] = set()
    at_risk: set[str] = set()
    for episode in episodes:
        disposition = episode.disposition
        lots.update(disposition["lot_ids"])
        at_risk.update(disposition["parts_at_risk"])
    started = len(lots) * PARTS_PER_LOT
    if started == 0:
        return {"label": "projected RFT", "pct": None, "parts_started": 0, "parts_at_risk": 0}
    at_risk_count = min(len(at_risk), started)
    return {
        "label": "projected RFT",
        "pct": round((started - at_risk_count) / started * 100, 2),
        "parts_started": started,
        "parts_at_risk": at_risk_count,
    }


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
        "peak_value": episode.peak_value,
        "threshold": episode.threshold,
        "time_to_detect_s": round(episode.time_to_detect_s, 3),
        "time_to_acknowledge_s": episode.time_to_acknowledge_s,
        "time_to_disposition_s": episode.time_to_disposition_s,
        "parts_at_risk": len(episode.disposition["parts_at_risk"]),
        "caught_in_time": episode.caught_in_time,
        "decisions": decisions,
    }


def ops_summary(episodes: Iterable[Episode], production_stops: int = 0) -> dict[str, Any]:
    items = sorted(episodes, key=lambda e: e.opened_at, reverse=True)
    ttd = [e.time_to_detect_s for e in items]
    tta = [e.time_to_acknowledge_s for e in items]
    return {
        "episodes": len(items),
        "open_episodes": sum(1 for e in items if e.cleared_at is None),
        "time_to_detect": {
            **duration_stats(ttd),
            "from_first_reading": sum(1 for e in items if e.onset_source == "first_reading"),
            "from_rule_duration": sum(1 for e in items if e.onset_source != "first_reading"),
            "samples": [round(v, 3) for v in ttd[:MAX_SAMPLES]],
        },
        "time_to_acknowledge": {
            **duration_stats(tta),
            "samples": [round(v, 3) for v in tta[:MAX_SAMPLES] if v is not None],
        },
        "time_to_disposition": duration_stats(e.time_to_disposition_s for e in items),
        "mttr": duration_stats(e.time_to_restore_s for e in items),
        "ncrs_opened": sum(1 for e in items if e.ncr_at is not None),
        "cmm_inspections": sum(1 for e in items if e.cmm_at is not None),
        "lots_quarantined_events": sum(1 for e in items if e.quarantined_at is not None),
        "maintenance_requests": sum(1 for e in items if e.maintenance_at is not None),
        "production_stops": production_stops,
        "caught_in_time": sum(1 for e in items if e.caught_in_time),
        "projected_rft": projected_rft(items),
        "recent_events": [recent_event(e) for e in items[:RECENT_EVENTS]],
    }
