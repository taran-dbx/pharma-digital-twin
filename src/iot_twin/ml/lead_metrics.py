"""Per-breach early-warning metrics for the thermal-risk model (pure Python).

These follow the app's alert semantics rather than per-row classification:

* **Breach**: the threshold rule's episode. It opens once the temperature stays
  over the limit for ``duration_s``; its onset is the first over-limit reading of
  that run (``excursion_started_at``). It clears at ``limit - hysteresis``.
  At the live sample rate (~25 Hz) single noisy samples over the limit do not
  count; a sustained run does.
* **Predicted alert**: opens at ``score >= open_at`` and clears at
  ``score <= clear_at`` (the model's shipped cut-off and clear level), scored
  every few seconds like the app's scorer.
* **Lead** of a breach: onset minus the open time of the predicted alert that is
  still open at the onset and opened at most ``lookback_s`` before it. No such
  alert means the breach was missed (no lead).
* **False alarm**: a predicted alert that opens outside any breach and is not
  followed by a breach onset within ``lookback_s``.
* **Chance baseline**: the same alert episodes circularly shifted in time by
  many offsets. It keeps the alerting rate and durations but breaks their
  alignment with the episode cycle, so "warned >= 30 s ahead" by luck is visible.
"""

from __future__ import annotations

import bisect
from collections.abc import Sequence
from dataclasses import dataclass

DEFAULT_LOOKBACK_S = 240.0
EARLY_WARNING_S = 30.0
CHANCE_OFFSETS = 200

Episode = tuple[float, float | None]


def breach_episodes(
    times: Sequence[float],
    values: Sequence[float],
    *,
    limit_c: float,
    hysteresis_c: float,
    duration_s: float,
) -> list[Episode]:
    """``(onset, cleared_at)`` of each threshold-rule episode (``cleared_at`` None if still open)."""
    episodes: list[Episode] = []
    pending: float | None = None
    onset: float | None = None
    for stamp, value in zip(times, values, strict=True):
        if onset is None:
            if value > limit_c:
                pending = stamp if pending is None else pending
                if stamp - pending >= duration_s:
                    onset, pending = pending, None
            else:
                pending = None
        elif value <= limit_c - hysteresis_c:
            episodes.append((onset, stamp))
            onset = None
    if onset is not None:
        episodes.append((onset, None))
    return episodes


def alert_episodes(
    times: Sequence[float], scores: Sequence[float], *, open_at: float, clear_at: float
) -> list[Episode]:
    """Predicted-alert episodes with open / clear hysteresis, from scores in time order."""
    if clear_at > open_at:
        raise ValueError("clear_at must not exceed open_at")
    episodes: list[Episode] = []
    opened: float | None = None
    for stamp, score in sorted(zip(times, scores, strict=True)):
        if opened is None and score >= open_at:
            opened = stamp
        elif opened is not None and score <= clear_at:
            episodes.append((opened, stamp))
            opened = None
    if opened is not None:
        episodes.append((opened, None))
    return episodes


def _open_at(episode: Episode, at: float) -> bool:
    opened, cleared = episode
    return opened < at and (cleared is None or cleared >= at)


def breach_leads(
    onsets: Sequence[float], alerts: Sequence[Episode], *, lookback_s: float = DEFAULT_LOOKBACK_S
) -> list[float | None]:
    """Lead (s) per breach onset, or None when no predicted alert covered it in time."""
    leads: list[float | None] = []
    for onset in onsets:
        covering = [
            episode[0]
            for episode in alerts
            if _open_at(episode, onset) and onset - episode[0] <= lookback_s
        ]
        leads.append(onset - min(covering) if covering else None)
    return leads


def false_alarms(
    alerts: Sequence[Episode],
    breaches: Sequence[Episode],
    *,
    lookback_s: float = DEFAULT_LOOKBACK_S,
) -> list[Episode]:
    """Predicted alerts that open outside every breach and precede no breach onset."""
    onsets = sorted(onset for onset, _ in breaches)
    alarms: list[Episode] = []
    for episode in alerts:
        opened = episode[0]
        inside = any(
            onset <= opened and (cleared is None or opened <= cleared)
            for onset, cleared in breaches
        )
        if inside:
            continue
        idx = bisect.bisect_left(onsets, opened)
        if idx < len(onsets) and onsets[idx] - opened <= lookback_s:
            continue
        alarms.append(episode)
    return alarms


def open_fraction(alerts: Sequence[Episode], *, start: float, end: float) -> float:
    """Share of ``[start, end)`` with a predicted alert open (an alert stuck open scores 1)."""
    if end <= start:
        return 0.0
    covered = 0.0
    for opened, cleared in alerts:
        lo, hi = max(start, opened), min(end, end if cleared is None else cleared)
        covered += max(0.0, hi - lo)
    return min(1.0, covered / (end - start))


def _quantile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def _warned_share(leads: Sequence[float | None], min_lead_s: float) -> float:
    if not leads:
        return 0.0
    return sum(1 for lead in leads if lead is not None and lead >= min_lead_s) / len(leads)


def shifted(alerts: Sequence[Episode], offset: float, start: float, span: float) -> list[Episode]:
    """Alert episodes moved ``offset`` seconds later, wrapping around ``[start, start + span)``."""
    moved: list[Episode] = []
    for opened, cleared in alerts:
        new_open = start + (opened - start + offset) % span
        new_clear = None if cleared is None else new_open + (cleared - opened)
        moved.append((new_open, new_clear))
    return sorted(moved, key=lambda episode: episode[0])


def chance_warned_counts(
    onsets: Sequence[float],
    alerts: Sequence[Episode],
    *,
    start: float,
    span: float,
    min_lead_s: float = EARLY_WARNING_S,
    lookback_s: float = DEFAULT_LOOKBACK_S,
    offsets: int = CHANCE_OFFSETS,
) -> tuple[int, ...]:
    """Breaches "warned >= min_lead_s" under each of ``offsets`` evenly spaced circular shifts.

    Offset ``k`` moves the alerts by ``span * k / (offsets + 1)``, so pooling
    runs offset by offset keeps one joint shift per draw.
    """
    if span <= 0 or offsets <= 0:
        return ()
    counts = []
    for k in range(1, offsets + 1):
        moved = shifted(alerts, span * k / (offsets + 1), start, span)
        leads = breach_leads(onsets, moved, lookback_s=lookback_s)
        counts.append(sum(1 for lead in leads if lead is not None and lead >= min_lead_s))
    return tuple(counts)


def _chance_summary(counts: Sequence[int], breaches: int) -> tuple[float, float]:
    if not counts or not breaches:
        return 0.0, 0.0
    shares = [count / breaches for count in counts]
    return sum(shares) / len(shares), _quantile(shares, 0.95)


@dataclass(frozen=True)
class EventReport:
    breaches: int
    warned: int
    warned_30s: int
    median_lead_s: float
    lead_p25_s: float
    lead_p75_s: float
    alerts: int
    false_alarms: int
    hours: float
    rule_lead_s: float
    leads: tuple[float | None, ...]
    chance_counts: tuple[int, ...]
    alert_open_s: float = 0.0

    @property
    def chance_warned_30s_mean(self) -> float:
        return _chance_summary(self.chance_counts, self.breaches)[0]

    @property
    def chance_warned_30s_p95(self) -> float:
        return _chance_summary(self.chance_counts, self.breaches)[1]

    @property
    def warned_30s_share(self) -> float:
        return self.warned_30s / self.breaches if self.breaches else 0.0

    @property
    def false_alarms_per_hour(self) -> float:
        return self.false_alarms / self.hours if self.hours else 0.0

    @property
    def alert_open_fraction(self) -> float:
        return self.alert_open_s / (self.hours * 3600.0) if self.hours else 0.0

    def as_metrics(self, prefix: str = "") -> dict[str, float]:
        return {
            f"{prefix}breaches": float(self.breaches),
            f"{prefix}breaches_warned": float(self.warned),
            f"{prefix}breaches_warned_30s": float(self.warned_30s),
            f"{prefix}warned_30s_share": self.warned_30s_share,
            f"{prefix}median_lead_s": self.median_lead_s,
            f"{prefix}lead_p25_s": self.lead_p25_s,
            f"{prefix}lead_p75_s": self.lead_p75_s,
            f"{prefix}predicted_alerts": float(self.alerts),
            f"{prefix}false_alarms": float(self.false_alarms),
            f"{prefix}false_alarms_per_hour": self.false_alarms_per_hour,
            f"{prefix}hours": self.hours,
            f"{prefix}chance_warned_30s_mean": self.chance_warned_30s_mean,
            f"{prefix}chance_warned_30s_p95": self.chance_warned_30s_p95,
            f"{prefix}rule_lead_s": self.rule_lead_s,
            f"{prefix}alert_open_fraction": self.alert_open_fraction,
        }


def event_report(
    breaches: Sequence[Episode],
    alerts: Sequence[Episode],
    *,
    start: float,
    end: float,
    rule_delay_s: float,
    lookback_s: float = DEFAULT_LOOKBACK_S,
    min_lead_s: float = EARLY_WARNING_S,
    offsets: int = CHANCE_OFFSETS,
) -> EventReport:
    """Per-breach lead distribution, false alarms and the chance baseline over ``[start, end)``.

    Every report uses the same number of chance ``offsets`` so runs can be pooled.

    The threshold rule opens ``rule_delay_s`` after each onset, so its lead is
    ``-rule_delay_s`` for every breach.
    """
    onsets = [onset for onset, _ in breaches]
    leads = breach_leads(onsets, alerts, lookback_s=lookback_s)
    warned = [lead for lead in leads if lead is not None]
    return EventReport(
        breaches=len(onsets),
        warned=len(warned),
        warned_30s=sum(1 for lead in warned if lead >= min_lead_s),
        median_lead_s=_quantile(warned, 0.5),
        lead_p25_s=_quantile(warned, 0.25),
        lead_p75_s=_quantile(warned, 0.75),
        alerts=len(alerts),
        false_alarms=len(false_alarms(alerts, breaches, lookback_s=lookback_s)),
        hours=max(0.0, end - start) / 3600.0,
        rule_lead_s=-float(rule_delay_s),
        leads=tuple(leads),
        chance_counts=chance_warned_counts(
            onsets,
            alerts,
            start=start,
            span=end - start,
            min_lead_s=min_lead_s,
            lookback_s=lookback_s,
            offsets=offsets,
        ),
        alert_open_s=open_fraction(alerts, start=start, end=end) * max(0.0, end - start),
    )


def combine(reports: Sequence[EventReport]) -> EventReport:
    """Pool per-run reports; chance counts are summed offset by offset."""
    leads = tuple(lead for report in reports for lead in report.leads)
    warned = [lead for lead in leads if lead is not None]
    counts = [report.chance_counts for report in reports if report.chance_counts]
    pooled = tuple(sum(column) for column in zip(*counts, strict=True)) if counts else ()
    return EventReport(
        breaches=sum(report.breaches for report in reports),
        warned=len(warned),
        warned_30s=sum(report.warned_30s for report in reports),
        median_lead_s=_quantile(warned, 0.5),
        lead_p25_s=_quantile(warned, 0.25),
        lead_p75_s=_quantile(warned, 0.75),
        alerts=sum(report.alerts for report in reports),
        false_alarms=sum(report.false_alarms for report in reports),
        hours=sum(report.hours for report in reports),
        rule_lead_s=reports[0].rule_lead_s if reports else 0.0,
        leads=leads,
        chance_counts=pooled,
        alert_open_s=sum(report.alert_open_s for report in reports),
    )
