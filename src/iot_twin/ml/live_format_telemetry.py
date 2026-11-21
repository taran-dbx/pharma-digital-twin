"""Live-format Spindle 3 telemetry: the live producer's own code path, offline.

Training data for the thermal-risk model, and the endpoint replay, must look
like what the app scorer sees live. This module drives the same pieces the
realtime producer runs, only on a virtual clock:

* ``iot_simulator`` ``SensorSimulator`` objects for the four spindle sensors,
  calibrated exactly like the producer's ``_calibrate_simulator`` (noise in
  sensor units, random simulator faults off);
* the shared scenario ``apply_spindle_scenario`` from
  ``iot_twin.scenario_engine.spindle_scenario``, the source the producer
  notebook embeds verbatim;
* the live sample rate: the producer sends 500 records/s over 19 manufacturing
  sensors, so each spindle sensor reports about 25 times a second (measured
  25.7 Hz on the live pipeline). Window statistics such as ``temp_max`` depend
  on it, so training at 1 Hz would not match live.

The other 15 sensors are independent of the spindle and are not simulated.
The scenario physics is SIMULATED (see the module docstring of
``spindle_scenario``); readings are tagged ``data_origin=live_format_simulated``.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from iot_twin.ml.lead_metrics import breach_episodes
from iot_twin.ml.spindle_features import (
    DEFAULT_HORIZON_S,
    INDUSTRY,
    MAX_MAINTENANCE_AGE_H,
    SPINDLE_SENSOR_NAMES,
    WINDOW_S,
    SpindleSeries,
)
from iot_twin.ml.training_set import (
    DEFAULT_STEP_S,
    LabelledRow,
    TrainingSet,
    breach_labelled_rows,
)
from iot_twin.scenario_engine.spindle_scenario import SPINDLE_CYCLE_S, apply_spindle_scenario

DATA_ORIGIN = "live_format_simulated"
LIVE_RATE_HZ = 25.0
DEFAULT_START_EPOCH = 1_796_000_000.0
# Held-out replay cycles start well after any training run.
REPLAY_START_EPOCH = 1_798_000_000.0


@dataclass(frozen=True)
class LiveFormatRun:
    run_id: str
    readings: tuple[dict[str, Any], ...]
    last_maintenance_at: float


class _VirtualTime:
    """Stands in for the ``time`` module inside ``iot_simulator.sensor_models``."""

    def __init__(self, start: float) -> None:
        self.now = start

    def time(self) -> float:
        return self.now


@contextmanager
def _simulator_clock(start: float, seed: int) -> Iterator[_VirtualTime]:
    """Run ``SensorSimulator`` on a virtual clock and a seeded RNG, then restore both."""
    from iot_simulator import sensor_models

    clock = _VirtualTime(start)
    saved_time, saved_random = sensor_models.time, sensor_models.random
    sensor_models.time = clock  # type: ignore[assignment]
    sensor_models.random = random.Random(seed)  # type: ignore[assignment]
    try:
        yield clock
    finally:
        sensor_models.time = saved_time
        sensor_models.random = saved_random


def _calibrated(config: Any) -> Any:
    """The producer's ``_calibrate_simulator`` for one sensor config."""
    update: dict[str, float] = {"anomaly_probability": 0.0}
    if config.nominal_value and config.noise_std != 0.01:
        update["noise_std"] = config.noise_std / abs(config.nominal_value)
    return config.model_copy(update=update)


def spindle_simulators() -> list[Any]:
    """Calibrated simulators for the spindle sensors (call inside ``_simulator_clock``)."""
    from iot_simulator.sensor_models import INDUSTRY_SENSORS, IndustryType, SensorSimulator

    configs = {
        cfg.name: cfg
        for cfg in INDUSTRY_SENSORS[IndustryType.MANUFACTURING]
        if cfg.name in SPINDLE_SENSOR_NAMES
    }
    missing = set(SPINDLE_SENSOR_NAMES) - set(configs)
    if missing:
        raise KeyError(f"iot_simulator catalog lacks spindle sensors: {sorted(missing)}")
    return [SensorSimulator(_calibrated(configs[name])) for name in SPINDLE_SENSOR_NAMES]


def _payload(sensor: Any, value: float, epoch: float) -> dict[str, Any]:
    cfg = sensor.config
    return {
        "sensor_name": cfg.name,
        "industry": INDUSTRY,
        "value": value,
        "unit": cfg.unit,
        "timestamp": epoch,
        "min_value": cfg.min_value,
        "max_value": cfg.max_value,
        "nominal_value": cfg.nominal_value,
        "fault_active": False,
        "metadata": {},
    }


def live_format_readings(
    *, start: float, duration_s: float, rate_hz: float = LIVE_RATE_HZ, seed: int = 0
) -> list[dict[str, Any]]:
    """Spindle readings as the live producer emits them, ``rate_hz`` ticks per second."""
    if rate_hz <= 0 or duration_s <= 0:
        raise ValueError("rate_hz and duration_s must be positive")
    scenario_rng = random.Random(seed + 1)
    step = 1.0 / rate_hz
    ticks = int(duration_s * rate_hz)
    readings: list[dict[str, Any]] = []
    with _simulator_clock(start, seed) as clock:
        sensors = spindle_simulators()
        for tick in range(ticks):
            epoch = start + tick * step
            clock.now = epoch
            for sensor in sensors:
                payload = _payload(sensor, sensor.update(), epoch)
                out = apply_spindle_scenario(payload, scenario_rng)
                readings.append(
                    {**out, "metadata": {**out["metadata"], "data_origin": DATA_ORIGIN}}
                )
    return readings


def generate_live_format_runs(
    *,
    n_runs: int,
    duration_s: float,
    seed: int,
    start_epoch: float = DEFAULT_START_EPOCH,
    rate_hz: float = LIVE_RATE_HZ,
) -> Iterator[LiveFormatRun]:
    """Consecutive runs (one cycle apart); ``hours_since_maintenance`` is random per run.

    The live producer has no wear model, so wear does not change the signals
    here either: the feature is present (the scorer sends it) but uninformative.
    """
    rng = random.Random(seed)
    for index in range(n_runs):
        start = start_epoch + index * (duration_s + SPINDLE_CYCLE_S)
        hours = rng.uniform(0.0, MAX_MAINTENANCE_AGE_H)
        readings = live_format_readings(
            start=start, duration_s=duration_s, rate_hz=rate_hz, seed=seed * 1000 + index
        )
        yield LiveFormatRun(
            run_id=f"live-format-{seed}-{index:03d}",
            readings=tuple(readings),
            last_maintenance_at=start - hours * 3600.0,
        )


def _temperature_only(series: SpindleSeries) -> SpindleSeries:
    """Keep only what the onset and lead-time metrics need (memory at 25 Hz)."""
    return SpindleSeries(points={"temp": series.points["temp"]})


def build_live_format_training_set(
    *,
    n_runs: int,
    duration_s: float,
    seed: int,
    limit_c: float,
    hysteresis_c: float,
    rule_delay_s: float,
    horizon_s: float = DEFAULT_HORIZON_S,
    step_s: float = DEFAULT_STEP_S,
    start_epoch: float = DEFAULT_START_EPOCH,
    rate_hz: float = LIVE_RATE_HZ,
) -> TrainingSet:
    """Labelled rows from live-format runs, built run by run to bound memory.

    Labels and the kept breach episodes (``breaches_by_run``) follow the
    threshold rule: over ``limit_c`` for ``rule_delay_s``, clear at
    ``limit_c - hysteresis_c``.
    """
    rows: list[LabelledRow] = []
    series_by_run: dict[str, SpindleSeries] = {}
    breaches_by_run: dict[str, tuple[tuple[float, float | None], ...]] = {}
    for run in generate_live_format_runs(
        n_runs=n_runs, duration_s=duration_s, seed=seed, start_epoch=start_epoch, rate_hz=rate_hz
    ):
        series = SpindleSeries.from_readings(run.readings)
        breaches = tuple(
            breach_episodes(
                *series.points["temp"],
                limit_c=limit_c,
                hysteresis_c=hysteresis_c,
                duration_s=rule_delay_s,
            )
        )
        breaches_by_run[run.run_id] = breaches
        rows.extend(
            breach_labelled_rows(
                series,
                breaches,
                run_id=run.run_id,
                last_maintenance_at=run.last_maintenance_at,
                limit_c=limit_c,
                horizon_s=horizon_s,
                step_s=step_s,
                window_s=WINDOW_S,
            )
        )
        series_by_run[run.run_id] = _temperature_only(series)
    rows.sort(key=lambda row: row.at)
    return TrainingSet(
        rows=tuple(rows),
        series_by_run=series_by_run,
        data_origin=DATA_ORIGIN,
        breaches_by_run=breaches_by_run,
    )


__all__ = [
    "DATA_ORIGIN",
    "LIVE_RATE_HZ",
    "REPLAY_START_EPOCH",
    "LiveFormatRun",
    "build_live_format_training_set",
    "generate_live_format_runs",
    "live_format_readings",
    "spindle_simulators",
]
