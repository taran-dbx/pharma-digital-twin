"""Labelled training rows from synthetic runs or real telemetry (pure Python)."""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from iot_twin.ml.lead_metrics import (
    EventReport,
    alert_episodes,
    breach_episodes,
    combine,
    event_report,
)
from iot_twin.ml.spindle_features import (
    DEFAULT_HORIZON_S,
    WINDOW_S,
    SpindleSeries,
    build_labels,
    excursion_onsets,
    feature_rows,
    sample_times,
)

DEFAULT_STEP_S = 5.0


@dataclass(frozen=True)
class LabelledRow:
    run_id: str
    at: float
    features: Mapping[str, float]
    label: int


@dataclass(frozen=True)
class TrainingSet:
    rows: tuple[LabelledRow, ...]
    series_by_run: Mapping[str, SpindleSeries]
    data_origin: str
    # Threshold-rule episodes per run (live-format data); empty for other sources.
    breaches_by_run: Mapping[str, tuple[tuple[float, float | None], ...]] = field(
        default_factory=dict
    )

    @property
    def positives(self) -> int:
        return sum(row.label for row in self.rows)


@dataclass(frozen=True)
class RunInput:
    run_id: str
    readings: Sequence[Mapping[str, Any]]
    last_maintenance_at: float | None


def labelled_rows(
    series: SpindleSeries,
    *,
    run_id: str,
    last_maintenance_at: float | None,
    limit_c: float,
    horizon_s: float = DEFAULT_HORIZON_S,
    step_s: float = DEFAULT_STEP_S,
    window_s: float = WINDOW_S,
) -> list[LabelledRow]:
    """Feature rows with forward-looking labels; in-excursion rows are dropped."""
    rows = feature_rows(
        series,
        sample_times(series, step_s, window_s),
        limit_c=limit_c,
        last_maintenance_at=last_maintenance_at,
        window_s=window_s,
    )
    labels = build_labels(series, [at for at, _ in rows], limit_c=limit_c, horizon_s=horizon_s)
    return [
        LabelledRow(run_id=run_id, at=at, features=features, label=label)
        for (at, features), label in zip(rows, labels, strict=True)
        if label is not None
    ]


def breach_labels(
    times: Sequence[float],
    breaches: Sequence[tuple[float, float | None]],
    *,
    horizon_s: float = DEFAULT_HORIZON_S,
) -> list[int | None]:
    """1 when a breach onset falls in ``(at, at + horizon_s]``; ``None`` inside a breach.

    Labels follow the threshold rule's episodes (``lead_metrics.breach_episodes``)
    rather than single over-limit samples, which at the live ~25 Hz rate include
    noise spikes that never open a breach alert.
    """
    onsets = sorted(onset for onset, _ in breaches)
    labels: list[int | None] = []
    for at in times:
        if any(onset <= at and (cleared is None or at <= cleared) for onset, cleared in breaches):
            labels.append(None)
            continue
        idx = bisect.bisect_right(onsets, at)
        labels.append(int(idx < len(onsets) and onsets[idx] - at <= horizon_s))
    return labels


def breach_labelled_rows(
    series: SpindleSeries,
    breaches: Sequence[tuple[float, float | None]],
    *,
    run_id: str,
    last_maintenance_at: float | None,
    limit_c: float,
    horizon_s: float = DEFAULT_HORIZON_S,
    step_s: float = DEFAULT_STEP_S,
    window_s: float = WINDOW_S,
) -> list[LabelledRow]:
    """Feature rows labelled by breach episodes; rows inside a breach are dropped."""
    rows = feature_rows(
        series,
        sample_times(series, step_s, window_s),
        limit_c=limit_c,
        last_maintenance_at=last_maintenance_at,
        window_s=window_s,
    )
    labels = breach_labels([at for at, _ in rows], breaches, horizon_s=horizon_s)
    return [
        LabelledRow(run_id=run_id, at=at, features=features, label=label)
        for (at, features), label in zip(rows, labels, strict=True)
        if label is not None
    ]


def build_training_set(
    runs: Iterable[RunInput],
    *,
    data_origin: str,
    limit_c: float,
    horizon_s: float = DEFAULT_HORIZON_S,
    step_s: float = DEFAULT_STEP_S,
    window_s: float = WINDOW_S,
) -> TrainingSet:
    rows: list[LabelledRow] = []
    series_by_run: dict[str, SpindleSeries] = {}
    for run in runs:
        series = SpindleSeries.from_readings(run.readings)
        series_by_run[run.run_id] = series
        rows.extend(
            labelled_rows(
                series,
                run_id=run.run_id,
                last_maintenance_at=run.last_maintenance_at,
                limit_c=limit_c,
                horizon_s=horizon_s,
                step_s=step_s,
                window_s=window_s,
            )
        )
    rows.sort(key=lambda row: row.at)
    return TrainingSet(rows=tuple(rows), series_by_run=series_by_run, data_origin=data_origin)


def time_split(
    rows: Sequence[LabelledRow], test_fraction: float, embargo_s: float = 0.0
) -> tuple[list[LabelledRow], list[LabelledRow]]:
    """Split on time: every test row is later than every training row.

    Training rows within ``embargo_s`` before the boundary are dropped: their
    feature window or label horizon overlaps the test period. Use
    ``window_s + horizon_s``.
    """
    if not 0.0 < test_fraction < 1.0:
        raise ValueError("test_fraction must be in (0, 1)")
    ordered = sorted(rows, key=lambda row: row.at)
    cut = int(len(ordered) * (1.0 - test_fraction))
    if cut <= 0 or cut >= len(ordered):
        raise ValueError("not enough rows for a time-based split")
    boundary = ordered[cut].at
    train = [row for row in ordered if row.at < boundary - max(0.0, embargo_s)]
    test = [row for row in ordered if row.at >= boundary]
    if not train:
        raise ValueError("embargo leaves no training rows")
    return train, test


def onsets_in_period(
    training_set: TrainingSet,
    test_rows: Sequence[LabelledRow],
    *,
    limit_c: float,
    hysteresis_c: float,
) -> list[float]:
    """Excursion onsets inside the test period of every run it covers."""
    if not test_rows:
        return []
    start = min(row.at for row in test_rows)
    onsets: list[float] = []
    for run_id in sorted({row.run_id for row in test_rows}):
        series = training_set.series_by_run[run_id]
        onsets.extend(
            onset
            for onset in excursion_onsets(series, limit_c=limit_c, hysteresis_c=hysteresis_c)
            if onset >= start
        )
    return sorted(onsets)


def period_event_report(
    training_set: TrainingSet,
    test_rows: Sequence[LabelledRow],
    scores: Sequence[float],
    *,
    open_at: float,
    clear_at: float,
    limit_c: float,
    hysteresis_c: float,
    rule_delay_s: float,
    step_s: float = DEFAULT_STEP_S,
) -> EventReport:
    """Per-breach lead, false alarms and chance baseline over the test period, per run.

    Scores are replayed through the predicted alert's open / clear hysteresis in
    time order, one lane per run, as the app's scorer would see them.
    """
    by_run: dict[str, list[tuple[float, float]]] = {}
    for row, score in zip(test_rows, scores, strict=True):
        by_run.setdefault(row.run_id, []).append((row.at, float(score)))
    reports: list[EventReport] = []
    for run_id in sorted(by_run):
        pairs = sorted(by_run[run_id])
        start, end = pairs[0][0], pairs[-1][0] + step_s
        breaches = training_set.breaches_by_run.get(run_id)
        if breaches is None:
            breaches = tuple(
                breach_episodes(
                    *training_set.series_by_run[run_id].points["temp"],
                    limit_c=limit_c,
                    hysteresis_c=hysteresis_c,
                    duration_s=rule_delay_s,
                )
            )
        in_period = [episode for episode in breaches if start <= episode[0] < end]
        alerts = alert_episodes(
            [at for at, _ in pairs],
            [score for _, score in pairs],
            open_at=open_at,
            clear_at=clear_at,
        )
        reports.append(
            event_report(in_period, alerts, start=start, end=end, rule_delay_s=rule_delay_s)
        )
    return combine(reports)


DEFAULT_FALSE_ALARM_BUDGET_PER_H = 3.0
# Steady running: this far from the next breach onset and past the last clear.
CALM_LOOKAHEAD_S = 240.0
CALM_SETTLE_S = 60.0
CALM_CLEAR_QUANTILE = 0.9


def calm_mask(
    training_set: TrainingSet,
    rows: Sequence[LabelledRow],
    *,
    lookahead_s: float = CALM_LOOKAHEAD_S,
    settle_s: float = CALM_SETTLE_S,
) -> list[bool]:
    """Rows of steady running: no breach onset within ``lookahead_s`` ahead, none open,
    and at least ``settle_s`` after the last breach cleared.

    Uses only the temperature record (breach episodes), not the scenario plan, so
    the same definition works on real telemetry. Precursor-like false alarms that
    fade without a breach count as calm: the model should not keep an alert open
    through them.
    """
    mask: list[bool] = []
    for row in rows:
        calm = True
        for onset, cleared in training_set.breaches_by_run.get(row.run_id, ()):
            end = float("inf") if cleared is None else cleared
            if onset - lookahead_s <= row.at <= end + settle_s:
                calm = False
                break
        mask.append(calm)
    return mask


DEFAULT_CLEAR_BANDS = (0.05, 0.10, 0.15, 0.20)


@dataclass(frozen=True)
class EventThreshold:
    threshold: float
    clear_band: float
    rule: str
    warned_30s_share: float
    false_alarms_per_hour: float


def choose_event_threshold(
    training_set: TrainingSet,
    rows: Sequence[LabelledRow],
    scores: Sequence[float],
    *,
    limit_c: float,
    hysteresis_c: float,
    rule_delay_s: float,
    false_alarm_budget_per_h: float = DEFAULT_FALSE_ALARM_BUDGET_PER_H,
    clear_bands: Sequence[float] = DEFAULT_CLEAR_BANDS,
    min_clear: float = 0.0,
    step_s: float = DEFAULT_STEP_S,
    grid: Sequence[float] | None = None,
) -> EventThreshold:
    """Cut-off and clear band that warn the most breaches >= 30 s ahead within a budget.

    Run on validation rows only. Each (cut-off, band) pair is replayed through
    the predicted alert's open / clear hysteresis (clear = cut-off - band). Ties
    go to fewer false alarms, then the higher cut-off, then the narrower band.
    When no pair meets ``false_alarm_budget_per_h``, the pair with the fewest
    false alarms per hour wins. Pairs whose clear level is below ``min_clear``
    (use a high quantile of calm-running scores, see ``calm_mask``) are skipped:
    such an alert would not clear while the spindle runs calm, and a stuck-open
    alert "warns" of whatever comes next by luck.
    """
    candidates = list(grid) if grid is not None else [i / 100 for i in range(10, 96)]
    best: tuple[tuple[float, float, float, float], EventThreshold] | None = None
    fallback: tuple[tuple[float, float, float], EventThreshold] | None = None
    for threshold in candidates:
        for band in clear_bands:
            if threshold - band < min_clear:
                continue
            report = period_event_report(
                training_set,
                rows,
                scores,
                open_at=threshold,
                clear_at=max(0.01, threshold - band),
                limit_c=limit_c,
                hysteresis_c=hysteresis_c,
                rule_delay_s=rule_delay_s,
                step_s=step_s,
            )
            alarms = report.false_alarms_per_hour
            within = alarms <= false_alarm_budget_per_h
            rule = (
                f"max_warned_30s_at_false_alarms_per_h<={false_alarm_budget_per_h:g}"
                if within
                else "min_false_alarms_per_h"
            )
            choice = EventThreshold(threshold, band, rule, report.warned_30s_share, alarms)
            if within:
                key = (report.warned_30s_share, -alarms, threshold, -band)
                if best is None or key > best[0]:
                    best = (key, choice)
            fallback_key = (-alarms, threshold, -band)
            if fallback is None or fallback_key > fallback[0]:
                fallback = (fallback_key, choice)
    if best is not None:
        return best[1]
    if fallback is None:
        raise ValueError("no threshold candidates")
    return fallback[1]
