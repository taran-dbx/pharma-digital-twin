"""Operational metrics for the Head of Manufacturing Operations (pure, no I/O).

Totals come from the full-window SQL in ``episodes.py``; the distributions and
the recent-events table use the latest ``MAX_SAMPLES`` / ``RECENT_EVENTS``
episodes and say so in ``meta``.

* **time to detect (TTD)** = ``opened_at - onset``.
* **time to acknowledge (TTA)** = first of (acknowledge event, first signed
  decision) - ``opened_at``; a signed decision implies acknowledgement.
* **time to disposition** = first containment decision (quarantine or CMM) - ``opened_at``.
* **MTTR** = mean ``cleared_at - opened_at`` over cleared episodes.
* Every duration statistic covers only the episodes whose end event exists
  (never zero-filled) and reports that coverage: e.g. ``acknowledged_count`` of
  ``episodes_total``, ``unacknowledged_count``, the age of the oldest open
  episode still waiting (``oldest_open_unacknowledged_age_s``, capped at
  ``open_age_cap_s`` = ``max_open_excursion_s``), how many waiting episodes are
  past that cap, and ``clock_skew_count`` (end event stamped before the start).
* ``duration_stats`` / ``open_waiting`` are the Python references the parity
  tests compare with the SQL; the summary itself always uses the SQL totals.
* **projected right-first-time** = ``PROJECTED_RFT_DEFINITION`` with lots and
  parts deduped across the window's excursions.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import datetime
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


def duration_stats(values: Iterable[float | None]) -> dict[str, Any]:
    """Python reference for the SQL duration statistics (``episodes._stats``).

    Not a runtime path: the summary always uses the SQL totals. The parity
    tests feed it each episode's ``Episode.signed_durations`` and check it
    equals the SQL block, so the two definitions cannot drift.

    ``None`` (the end event never happened) is excluded, never counted as 0 s.
    A negative value (end event stamped before the start: clock skew) is
    counted in ``clock_skew_count`` and clamps to 0, as ``GREATEST(x, 0)``.
    """
    signed = [float(v) for v in values if v is not None]
    measured = [max(0.0, v) for v in signed]

    def r(value: float | None) -> float | None:
        return round(value, 3) if value is not None else None

    return {
        "count": len(measured),
        "mean": r(sum(measured) / len(measured)) if measured else None,
        "p50": r(percentile(measured, 50)),
        "p90": r(percentile(measured, 90)),
        "max": r(max(measured)) if measured else None,
        "clock_skew_count": sum(1 for v in signed if v < 0),
    }


def open_waiting(
    episodes: Iterable[Episode],
    now: datetime,
    measure: Callable[[Episode], float | None],
    cap_s: float,
) -> tuple[float | None, int]:
    """Python reference for the SQL ``oldest_open_*`` columns (parity tests only).

    Returns the age of the oldest still-open episode whose ``measure`` is
    missing, clamped to [0, ``cap_s``] (``max_open_excursion_s``), or None
    when there is none; and how many such episodes are older than the cap.
    """
    ages = [
        (now - e.opened_at).total_seconds()
        for e in episodes
        if e.cleared_at is None and measure(e) is None
    ]
    if not ages:
        return None, 0
    return min(max(0.0, max(ages)), cap_s), sum(1 for a in ages if a > cap_s)


def _coverage(stats: dict[str, Any], total: int, measured: str, missing: str) -> dict[str, Any]:
    """A statistic plus how many episodes it covers (``measured``) and leaves out."""
    count = int(stats.get("count") or 0)
    return {**stats, "episodes_total": total, measured: count, missing: max(0, total - count)}


def _age(value: float | None) -> float | None:
    return round(value, 3) if value is not None else None


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
            "episodes_total": t.episodes,
            "from_first_reading": t.onset_from_reading,
            "from_rule_duration": t.episodes - t.onset_from_reading,
            "samples": [round(v, 3) for v in window.ttd_samples],
        },
        "time_to_acknowledge": {
            **_coverage(t.tta, t.episodes, "acknowledged_count", "unacknowledged_count"),
            "oldest_open_unacknowledged_age_s": _age(t.oldest_open_unacknowledged_s),
            "open_unacknowledged_past_cap_count": t.open_unacknowledged_past_cap,
            "open_age_cap_s": t.open_age_cap_s,
            "samples": [round(v, 3) for v in window.tta_samples],
        },
        "time_to_disposition": {
            **_coverage(t.ttdisp, t.episodes, "dispositioned_count", "undispositioned_count"),
            "oldest_open_undispositioned_age_s": _age(t.oldest_open_undispositioned_s),
            "open_undispositioned_past_cap_count": t.open_undispositioned_past_cap,
            "open_age_cap_s": t.open_age_cap_s,
        },
        "mttr": _coverage(t.restore, t.episodes, "cleared_count", "open_count"),
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
