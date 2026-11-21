"""Windowed Spindle 3 features and forward-looking labels.

This module is the single source of truth for the thermal-risk model's inputs,
in the same way ``SENSOR_FIELD_MAP`` is for the reading schema:

* ``SPINDLE_SIGNALS`` maps model signals to catalog sensor names.
* ``FEATURE_SPEC`` lists every model input, in order. ``FEATURE_NAMES``, the
  MLflow signature, the training frame and the serving request all derive from it.
* ``features_at`` computes one feature row. The training job calls it for every
  sample time of a series; the app's scorer calls it on its recent-readings buffer.

Spindle load is the cutting torque derived from power and speed
(``T[N·m] = 9549 · P[kW] / n[rpm]``); the mill has no separate load sensor.
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from iot_twin.domain.catalog import get_domain, list_alert_rules

INDUSTRY = "manufacturing"
SPINDLE_RULE_ID = "mfg-alert-spindle-temp"
SPINDLE_COMPONENT_ID = "mfg-comp-spindle"

# Feature window and the minimum readings per signal inside it.
WINDOW_S = 60.0
MIN_POINTS = 3
# Default label horizon: an excursion within the next N seconds counts as positive.
DEFAULT_HORIZON_S = 120.0

# hours_since_maintenance is clipped to the range the model was trained on, at
# train and at serve time (the catalog fixture keeps ageing in a long demo).
MAX_MAINTENANCE_AGE_H = 3000.0

_TORQUE_FACTOR = 9549.0
# Below this speed the spindle is idle and torque is undefined.
MIN_SPEED_RPM = 100.0


@dataclass(frozen=True)
class SignalSpec:
    signal: str
    sensor_name: str
    unit: str


SPINDLE_SIGNALS: tuple[SignalSpec, ...] = (
    SignalSpec("temp", "cnc_1_spindle_temp", "°C"),
    SignalSpec("vibration", "cnc_1_vibration", "mm/s"),
    SignalSpec("power", "cnc_1_spindle_power", "kW"),
    SignalSpec("speed", "cnc_1_spindle_speed", "RPM"),
)
SIGNAL_BY_SENSOR: dict[str, str] = {spec.sensor_name: spec.signal for spec in SPINDLE_SIGNALS}
SPINDLE_SENSOR_NAMES: tuple[str, ...] = tuple(spec.sensor_name for spec in SPINDLE_SIGNALS)
TEMP_SENSOR = "cnc_1_spindle_temp"
LOAD_SIGNAL = "load"


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    signal: str
    stat: str
    unit: str
    description: str


_WINDOW_SIGNALS: tuple[tuple[str, str, str], ...] = (
    ("temp", "°C", "spindle temperature"),
    ("vibration", "mm/s", "spindle vibration"),
    ("load", "N·m", "spindle load (torque from power and speed)"),
    ("power", "kW", "spindle power"),
)


def _window_specs() -> tuple[FeatureSpec, ...]:
    specs: list[FeatureSpec] = []
    for signal, unit, label in _WINDOW_SIGNALS:
        specs.append(
            FeatureSpec(f"{signal}_mean", signal, "mean", unit, f"Rolling mean of {label}")
        )
        specs.append(
            FeatureSpec(
                f"{signal}_slope",
                signal,
                "slope",
                f"{unit}/min",
                f"Least-squares slope of {label} per minute",
            )
        )
        specs.append(FeatureSpec(f"{signal}_max", signal, "max", unit, f"Rolling max of {label}"))
    return tuple(specs)


FEATURE_SPEC: tuple[FeatureSpec, ...] = (
    *_window_specs(),
    FeatureSpec("temp_last", "temp", "last", "°C", "Latest spindle temperature"),
    FeatureSpec(
        "temp_margin_c",
        "temp",
        "margin",
        "°C",
        "Validated limit minus the latest temperature (negative means over the limit)",
    ),
    FeatureSpec(
        "hours_since_maintenance",
        "maintenance",
        "maintenance",
        "h",
        "Hours since the last Spindle 3 maintenance completed, clipped to 0-3000 h",
    ),
)
FEATURE_NAMES: tuple[str, ...] = tuple(spec.name for spec in FEATURE_SPEC)


# ---------------------------------------------------------------------------
# Catalog lookups (validated limit, rule timing, maintenance)
# ---------------------------------------------------------------------------
def _spindle_rule() -> Any:
    for rule in list_alert_rules(INDUSTRY):
        if rule.rule_id == SPINDLE_RULE_ID:
            return rule
    raise KeyError(SPINDLE_RULE_ID)


def validated_limit_c() -> float:
    """Spindle 3 validated thermal limit, from the catalog rule."""
    return float(_spindle_rule().threshold)


def threshold_rule_delay_s() -> float:
    """How long the threshold rule waits above the limit before it opens."""
    return float(getattr(_spindle_rule(), "duration_s", 0.0) or 0.0)


def threshold_rule_hysteresis_c() -> float:
    return float(getattr(_spindle_rule(), "hysteresis", 0.0) or 0.0)


def _parse_iso(value: str) -> float | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def last_spindle_maintenance_epoch() -> float | None:
    """Completion time of the latest Spindle 3 maintenance event in the catalog."""
    stamps = [
        _parse_iso(event.completed_at)
        for event in get_domain(INDUSTRY).maintenance_history
        if event.component_id == SPINDLE_COMPONENT_ID and event.completed_at
    ]
    found = [stamp for stamp in stamps if stamp is not None]
    return max(found) if found else None


# ---------------------------------------------------------------------------
# Readings → per-signal series
# ---------------------------------------------------------------------------
def _as_epoch(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, datetime):
        stamp = value if value.tzinfo else value.replace(tzinfo=UTC)
        return stamp.timestamp()
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return _parse_iso(value.strip())
    return None


def reading_epoch(reading: Mapping[str, Any]) -> float | None:
    """Device time of a reading: ``timestamp`` (epoch s) or ``event_time``."""
    stamp = _as_epoch(reading.get("timestamp"))
    return stamp if stamp is not None else _as_epoch(reading.get("event_time"))


def _reading_value(reading: Mapping[str, Any]) -> float | None:
    try:
        value = float(reading.get("value"))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _torque(power_kw: float, speed_rpm: float) -> float | None:
    if speed_rpm < MIN_SPEED_RPM:
        return None
    return _TORQUE_FACTOR * power_kw / speed_rpm


@dataclass(frozen=True)
class SpindleSeries:
    """Time-sorted ``(times, values)`` per signal, including derived load."""

    points: Mapping[str, tuple[tuple[float, ...], tuple[float, ...]]]

    @classmethod
    def from_readings(cls, readings: Iterable[Mapping[str, Any]]) -> SpindleSeries:
        raw: dict[str, list[tuple[float, float]]] = {spec.signal: [] for spec in SPINDLE_SIGNALS}
        for reading in readings:
            if not isinstance(reading, Mapping):
                continue
            signal = SIGNAL_BY_SENSOR.get(str(reading.get("sensor_name") or ""))
            if signal is None:
                continue
            stamp = reading_epoch(reading)
            value = _reading_value(reading)
            if stamp is None or value is None:
                continue
            raw[signal].append((stamp, value))
        points = {signal: _unzip(sorted(items)) for signal, items in raw.items()}
        points[LOAD_SIGNAL] = _unzip(_load_points(points["power"], points["speed"]))
        return cls(points=points)

    def window(self, signal: str, start: float, end: float) -> tuple[Sequence[float], ...]:
        """Points with ``start < t <= end``."""
        times, values = self.points.get(signal, ((), ()))
        lo = bisect.bisect_right(times, start)
        hi = bisect.bisect_right(times, end)
        return times[lo:hi], values[lo:hi]

    def latest_time(self, signal: str = "temp") -> float | None:
        times = self.points.get(signal, ((), ()))[0]
        return times[-1] if times else None


def _unzip(items: Sequence[tuple[float, float]]) -> tuple[tuple[float, ...], tuple[float, ...]]:
    return tuple(t for t, _ in items), tuple(v for _, v in items)


def _load_points(
    power: tuple[tuple[float, ...], tuple[float, ...]],
    speed: tuple[tuple[float, ...], tuple[float, ...]],
) -> list[tuple[float, float]]:
    """Pair each power reading with the latest speed reading at or before it."""
    speed_times, speed_values = speed
    out: list[tuple[float, float]] = []
    for stamp, kw in zip(*power, strict=True):
        idx = bisect.bisect_right(speed_times, stamp) - 1
        if idx < 0:
            continue
        torque = _torque(kw, speed_values[idx])
        if torque is not None:
            out.append((stamp, torque))
    return out


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------
def _slope_per_min(times: Sequence[float], values: Sequence[float]) -> float:
    n = len(times)
    mean_t = sum(times) / n
    mean_v = sum(values) / n
    var_t = sum((t - mean_t) ** 2 for t in times)
    if var_t <= 0:
        return 0.0
    cov = sum((t - mean_t) * (v - mean_v) for t, v in zip(times, values, strict=True))
    return cov / var_t * 60.0


def _window_stat(stat: str, times: Sequence[float], values: Sequence[float]) -> float:
    if stat == "mean":
        return sum(values) / len(values)
    if stat == "slope":
        return _slope_per_min(times, values)
    if stat == "max":
        return max(values)
    raise ValueError(f"unknown window stat {stat!r}")


def features_at(
    series: SpindleSeries,
    at: float,
    *,
    limit_c: float,
    last_maintenance_at: float | None,
    window_s: float = WINDOW_S,
) -> dict[str, float] | None:
    """One feature row at ``at`` from the ``window_s`` seconds before it.

    Returns ``None`` when any windowed signal has fewer than ``MIN_POINTS``
    readings: the model is never scored on a partial window.
    """
    windows = {}
    for signal, _unit, _label in _WINDOW_SIGNALS:
        times, values = series.window(signal, at - window_s, at)
        if len(values) < MIN_POINTS:
            return None
        windows[signal] = (times, values)
    row: dict[str, float] = {}
    for spec in FEATURE_SPEC:
        if spec.stat in {"mean", "slope", "max"}:
            times, values = windows[spec.signal]
            row[spec.name] = _window_stat(spec.stat, times, values)
        elif spec.stat == "last":
            row[spec.name] = windows["temp"][1][-1]
        elif spec.stat == "margin":
            row[spec.name] = limit_c - windows["temp"][1][-1]
        elif spec.stat == "maintenance":
            age = 0.0 if last_maintenance_at is None else (at - last_maintenance_at) / 3600.0
            row[spec.name] = min(MAX_MAINTENANCE_AGE_H, max(0.0, age))
    return {name: round(row[name], 6) for name in FEATURE_NAMES}


def build_features(
    readings: Iterable[Mapping[str, Any]],
    *,
    at: float | None = None,
    limit_c: float | None = None,
    last_maintenance_at: float | None = None,
    window_s: float = WINDOW_S,
) -> dict[str, float] | None:
    """Feature row for the latest spindle state in ``readings`` (the serving path)."""
    series = SpindleSeries.from_readings(readings)
    when = at if at is not None else series.latest_time()
    if when is None:
        return None
    return features_at(
        series,
        when,
        limit_c=validated_limit_c() if limit_c is None else limit_c,
        last_maintenance_at=last_maintenance_at,
        window_s=window_s,
    )


def feature_rows(
    series: SpindleSeries,
    sample_times: Iterable[float],
    *,
    limit_c: float,
    last_maintenance_at: float | None,
    window_s: float = WINDOW_S,
) -> list[tuple[float, dict[str, float]]]:
    """Feature rows at each sample time that has a full window (the training path)."""
    rows: list[tuple[float, dict[str, float]]] = []
    for at in sample_times:
        row = features_at(
            series,
            at,
            limit_c=limit_c,
            last_maintenance_at=last_maintenance_at,
            window_s=window_s,
        )
        if row is not None:
            rows.append((at, row))
    return rows


def sample_times(series: SpindleSeries, step_s: float, window_s: float = WINDOW_S) -> list[float]:
    """Evenly spaced sample times from the first full window to the last reading."""
    times = series.points["temp"][0]
    if not times or step_s <= 0:
        return []
    start, end = times[0] + window_s, times[-1]
    count = int((end - start) // step_s) + 1 if end >= start else 0
    return [start + i * step_s for i in range(count)]


# ---------------------------------------------------------------------------
# Labels and excursion events
# ---------------------------------------------------------------------------
def exceedance_times(series: SpindleSeries, limit_c: float) -> tuple[float, ...]:
    times, values = series.points["temp"]
    return tuple(t for t, v in zip(times, values, strict=True) if v > limit_c)


def label_at(at: float, exceed: Sequence[float], horizon_s: float) -> int:
    """1 when the temperature exceeds the limit within ``(at, at + horizon_s]``."""
    idx = bisect.bisect_right(exceed, at)
    return int(idx < len(exceed) and exceed[idx] - at <= horizon_s)


def build_labels(
    series: SpindleSeries,
    times: Sequence[float],
    *,
    limit_c: float,
    horizon_s: float = DEFAULT_HORIZON_S,
) -> list[int | None]:
    """Forward-looking labels; ``None`` while already over the limit.

    Rows inside an excursion are excluded from training: the threshold rule
    already handles them and the model's job is to warn before the trip.
    """
    exceed = exceedance_times(series, limit_c)
    temp_times, temp_values = series.points["temp"]
    labels: list[int | None] = []
    for at in times:
        idx = bisect.bisect_right(temp_times, at) - 1
        if idx >= 0 and temp_values[idx] > limit_c:
            labels.append(None)
            continue
        labels.append(label_at(at, exceed, horizon_s))
    return labels


def excursion_onsets(series: SpindleSeries, *, limit_c: float, hysteresis_c: float) -> list[float]:
    """Start times of excursions, with the threshold rule's clear-band hysteresis."""
    onsets: list[float] = []
    inside = False
    for stamp, value in zip(*series.points["temp"], strict=True):
        if not inside and value > limit_c:
            onsets.append(stamp)
            inside = True
        elif inside and value <= limit_c - hysteresis_c:
            inside = False
    return onsets


def lead_time_report(
    times: Sequence[float],
    scores: Sequence[float],
    onsets: Sequence[float],
    *,
    threshold: float,
    horizon_s: float,
    rule_delay_s: float,
) -> dict[str, float]:
    """Lead time the model gains over the threshold rule, per excursion.

    The onset is the first over-limit reading (what the alert engine records as
    ``excursion_started_at``); the rule opens ``rule_delay_s`` after it. The model
    "fires" at the first sample in ``[onset - horizon_s, onset)`` with
    ``score >= threshold``. ``lead_before_onset`` is onset minus that; the gain
    vs the rule adds the rule's delay. Missed events count as 0 s.
    """
    pairs = sorted(zip(times, scores, strict=True))
    sorted_times = [t for t, _ in pairs]
    gains: list[float] = []
    before: list[float] = []
    early = 0
    for onset in onsets:
        lo = bisect.bisect_left(sorted_times, onset - horizon_s)
        hi = bisect.bisect_left(sorted_times, onset)
        fired = [t for t, s in pairs[lo:hi] if s >= threshold]
        if fired:
            early += 1
            gains.append(onset + rule_delay_s - fired[0])
            before.append(onset - fired[0])
        else:
            gains.append(0.0)
            before.append(0.0)
    events = len(onsets)
    ordered = sorted(gains)
    median = 0.0
    if ordered:
        mid = len(ordered) // 2
        median = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0
    return {
        "excursion_events": float(events),
        "events_predicted_early": float(early),
        "early_detection_rate": early / events if events else 0.0,
        "mean_lead_time_gain_s": sum(gains) / events if events else 0.0,
        "median_lead_time_gain_s": median,
        "mean_lead_before_onset_s": sum(before) / events if events else 0.0,
    }
