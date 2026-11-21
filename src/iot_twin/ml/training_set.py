"""Labelled training rows from synthetic runs or real telemetry (pure Python)."""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from iot_twin.ml.lead_metrics import (
    CYCLE_S,
    EventReport,
    alert_episodes,
    breach_episodes,
    clock_alerts,
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
    threshold_rule_delay_s,
    threshold_rule_hysteresis_c,
    validated_limit_c,
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


def run_breaches(
    training_set: TrainingSet,
    run_id: str,
    *,
    limit_c: float | None = None,
    hysteresis_c: float | None = None,
    rule_delay_s: float | None = None,
) -> tuple[tuple[float, float | None], ...]:
    """Threshold-rule episodes of one run: stored for live-format data, else from its series."""
    stored = training_set.breaches_by_run.get(run_id)
    if stored is not None:
        return stored
    series = training_set.series_by_run.get(run_id)
    if series is None:
        return ()
    return tuple(
        breach_episodes(
            *series.points["temp"],
            limit_c=validated_limit_c() if limit_c is None else limit_c,
            hysteresis_c=threshold_rule_hysteresis_c() if hysteresis_c is None else hysteresis_c,
            duration_s=threshold_rule_delay_s() if rule_delay_s is None else rule_delay_s,
        )
    )


def _run_spans(
    rows: Sequence[LabelledRow], scores: Sequence[float] | None, step_s: float
) -> dict[str, tuple[float, float, list[tuple[float, float]]]]:
    by_run: dict[str, list[tuple[float, float]]] = {}
    values = scores if scores is not None else [0.0] * len(rows)
    for row, score in zip(rows, values, strict=True):
        by_run.setdefault(row.run_id, []).append((row.at, float(score)))
    spans = {}
    for run_id, pairs in by_run.items():
        ordered = sorted(pairs)
        spans[run_id] = (ordered[0][0], ordered[-1][0] + step_s, ordered)
    return spans


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
    significance: bool = True,
    seed: int = 0,
) -> EventReport:
    """Per-breach lead, false alarms, cycle counts and nulls over the test period, per run.

    The permutation null of run ``i`` (sorted by run id) uses seed ``seed + i``.

    Scores are replayed through the predicted alert's open / clear hysteresis in
    time order, one lane per run, as the app's scorer would see them.
    """
    reports: list[EventReport] = []
    spans = sorted(_run_spans(test_rows, scores, step_s).items())
    for index, (run_id, (start, end, pairs)) in enumerate(spans):
        breaches = run_breaches(
            training_set,
            run_id,
            limit_c=limit_c,
            hysteresis_c=hysteresis_c,
            rule_delay_s=rule_delay_s,
        )
        in_period = [episode for episode in breaches if start <= episode[0] < end]
        alerts = alert_episodes(
            [at for at, _ in pairs],
            [score for _, score in pairs],
            open_at=open_at,
            clear_at=clear_at,
        )
        reports.append(
            event_report(
                in_period,
                alerts,
                start=start,
                end=end,
                rule_delay_s=rule_delay_s,
                significance=significance,
                seed=seed + index,
            )
        )
    return combine(reports)


def period_alerts(
    rows: Sequence[LabelledRow],
    scores: Sequence[float],
    *,
    open_at: float,
    clear_at: float,
    step_s: float = DEFAULT_STEP_S,
) -> dict[str, tuple[float, float, list[tuple[float, float | None]]]]:
    """``{run_id: (start, end, predicted-alert episodes)}`` of scored rows."""
    out = {}
    for run_id, (start, end, pairs) in _run_spans(rows, scores, step_s).items():
        out[run_id] = (
            start,
            end,
            alert_episodes(
                [at for at, _ in pairs],
                [score for _, score in pairs],
                open_at=open_at,
                clear_at=clear_at,
            ),
        )
    return out


@dataclass(frozen=True)
class ClockWindow:
    open_offset_s: float
    duration_s: float


def period_clock_report(
    training_set: TrainingSet,
    rows: Sequence[LabelledRow],
    window: ClockWindow,
    *,
    rule_delay_s: float,
    step_s: float = DEFAULT_STEP_S,
    significance: bool = False,
) -> EventReport:
    """The sensor-blind clock baseline (one fixed in-cycle window) over the rows' period."""
    reports: list[EventReport] = []
    for run_id, (start, end, _) in sorted(_run_spans(rows, None, step_s).items()):
        breaches = run_breaches(training_set, run_id, rule_delay_s=rule_delay_s)
        in_period = [episode for episode in breaches if start <= episode[0] < end]
        alerts = clock_alerts(
            start, end, open_offset_s=window.open_offset_s, duration_s=window.duration_s
        )
        reports.append(
            event_report(
                in_period,
                alerts,
                start=start,
                end=end,
                rule_delay_s=rule_delay_s,
                significance=significance,
            )
        )
    return combine(reports)


def choose_clock_window(
    training_set: TrainingSet,
    rows: Sequence[LabelledRow],
    *,
    max_duty: float,
    rule_delay_s: float,
    step_s: float = DEFAULT_STEP_S,
    offset_step_s: float = 10.0,
    duration_step_s: float = 10.0,
) -> ClockWindow:
    """Best fixed in-cycle window (most breaches warned >= 30 s ahead, then fewer false
    alarms) whose duty is at most ``max_duty``. Run on validation rows only."""
    longest = max(duration_step_s, min(CYCLE_S, max_duty * CYCLE_S))
    best: tuple[tuple[float, float], ClockWindow] | None = None
    offset = 0.0
    while offset < CYCLE_S:
        duration = duration_step_s
        while duration <= longest + 1e-9:
            window = ClockWindow(offset, duration)
            report = period_clock_report(
                training_set, rows, window, rule_delay_s=rule_delay_s, step_s=step_s
            )
            key = (report.warned_30s_share, -report.false_alarms_per_hour)
            if best is None or key > best[0]:
                best = (key, window)
            duration += duration_step_s
        offset += offset_step_s
    assert best is not None
    return best[1]


DEFAULT_FALSE_ALARM_BUDGET_PER_H = 3.0
# Steady running: this far from the next breach onset and past the last clear.
CALM_LOOKAHEAD_S = 240.0
CALM_SETTLE_S = 60.0
CALM_CLEAR_QUANTILE = 0.9
# The cut-off search keeps the clear level at least this far above the gate's
# steady-running p90, so a champion does not flip on the next run's validation
# sample. (p95 was tried first: its top tail is fading false-alarm precursors,
# not calm running, and it pushed v9's cut-off to 0.91, where it never alerts.)
SEARCH_CLEAR_MARGIN = 0.02


def scenario_calm(at: float, *, window_s: float = WINDOW_S, step_s: float = DEFAULT_STEP_S) -> bool:
    """True when the simulated scenario has no precursor or heat episode anywhere in
    the feature window ``(at - window_s, at]``: ground-truth calm running."""
    from iot_twin.scenario_engine.spindle_scenario import (
        SPINDLE_CYCLE_S,
        spindle_cycle_plan,
        spindle_heat_phase,
        spindle_precursor_level,
    )

    t = at - window_s
    while t <= at + 1e-9:
        plan = spindle_cycle_plan(int(t // SPINDLE_CYCLE_S))
        if spindle_precursor_level(t) > 0.0 or spindle_heat_phase(t, plan)[1] > 0.0:
            return False
        t += step_s
    return True


def calm_mask(
    training_set: TrainingSet,
    rows: Sequence[LabelledRow],
    *,
    lookahead_s: float = CALM_LOOKAHEAD_S,
    settle_s: float = CALM_SETTLE_S,
) -> list[bool]:
    """Rows of calm running, for the steady-running clear floor.

    Every row must be at least ``settle_s`` after the last breach cleared. For
    live-format (simulated) data the scenario is known, so a row is calm only
    when its feature window holds no precursor and no heat episode
    (``scenario_calm``). Without the scenario (real telemetry) it falls back to
    the temperature record: no breach onset within ``lookahead_s`` ahead. That
    fallback also counts fading precursor-like false alarms as calm, which made
    the floor swing with how many of them a validation sample held (v4's p90
    ranged 0.18-0.69 over four fresh samples).
    """
    simulated = training_set.data_origin == "live_format_simulated"
    mask: list[bool] = []
    breaches: dict[str, tuple[tuple[float, float | None], ...]] = {}
    for row in rows:
        calm = True
        for onset, cleared in breaches.setdefault(
            row.run_id, run_breaches(training_set, row.run_id)
        ):
            end = float("inf") if cleared is None else cleared
            lead = 0.0 if simulated else lookahead_s
            if onset - lead <= row.at <= end + settle_s:
                calm = False
                break
        if calm and simulated:
            calm = scenario_calm(row.at)
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
    """Cut-off and clear band with the best discrimination within a false-alarm budget.

    Run on validation rows only. Each (cut-off, band) pair is replayed through
    the predicted alert's open / clear hysteresis (clear = cut-off - band).
    Discrimination is breaches warned >= 30 s ahead minus breach-free cycles
    with a false alarm, so an alert that is simply open more often does not
    win. Ties go to more breaches warned, fewer false alarms, then the higher
    cut-off.
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
                significance=False,
            )
            alarms = report.false_alarms_per_hour
            within = alarms <= false_alarm_budget_per_h
            rule = (
                f"max_discrimination_at_false_alarms_per_h<={false_alarm_budget_per_h:g}"
                if within
                else "min_false_alarms_per_h"
            )
            choice = EventThreshold(threshold, band, rule, report.warned_30s_share, alarms)
            if within:
                key = (report.discrimination, report.warned_30s_share, -alarms, threshold)
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


def steady_clear_floor(
    training_set: TrainingSet,
    rows: Sequence[LabelledRow],
    scores: Sequence[float],
    *,
    quantile_q: float = CALM_CLEAR_QUANTILE,
) -> float:
    """``quantile_q`` of the scores in steady running (``calm_mask``); 0.0 if none."""
    steady = sorted(
        float(score)
        for score, calm in zip(scores, calm_mask(training_set, rows), strict=True)
        if calm
    )
    if not steady:
        return 0.0
    pos = (len(steady) - 1) * quantile_q
    lo = int(pos)
    hi = min(lo + 1, len(steady) - 1)
    return steady[lo] + (steady[hi] - steady[lo]) * (pos - lo)


SCENARIO_TAG = "scenario_id"


def is_scenario_telemetry(readings: Iterable[Mapping[str, Any]]) -> bool:
    """True when any reading carries the simulator's ``metadata.scenario_id`` tag.

    The live producer stamps ``scenario_id`` on Spindle 3 temperature readings,
    so ``manufacturing_realtime`` holding such rows is simulated telemetry, not
    a real machine; ``--data-source auto`` must not train on it as ``real``.
    ``metadata`` may be a mapping or a JSON string.
    """
    import json

    for reading in readings:
        metadata = reading.get("metadata")
        if isinstance(metadata, str):
            if SCENARIO_TAG not in metadata:
                continue
            try:
                metadata = json.loads(metadata)
            except ValueError:
                continue
        if isinstance(metadata, Mapping) and metadata.get(SCENARIO_TAG):
            return True
    return False
