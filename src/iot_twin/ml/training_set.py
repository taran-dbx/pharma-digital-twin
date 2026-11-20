"""Labelled training rows from synthetic runs or real telemetry (pure Python)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

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
    rows: Sequence[LabelledRow], test_fraction: float
) -> tuple[list[LabelledRow], list[LabelledRow]]:
    """Split on time: every test row is later than every training row."""
    if not 0.0 < test_fraction < 1.0:
        raise ValueError("test_fraction must be in (0, 1)")
    ordered = sorted(rows, key=lambda row: row.at)
    cut = int(len(ordered) * (1.0 - test_fraction))
    if cut <= 0 or cut >= len(ordered):
        raise ValueError("not enough rows for a time-based split")
    boundary = ordered[cut].at
    train = [row for row in ordered if row.at < boundary]
    test = [row for row in ordered if row.at >= boundary]
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
