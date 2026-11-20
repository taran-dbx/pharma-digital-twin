"""SYNTHETIC labelled Spindle 3 telemetry for training the thermal-risk model.

Live ``manufacturing_realtime`` telemetry is usually too short and too regular
to train on (one scheduled heat event every five minutes). This generator
builds longer, more varied runs from the repo's own sources:

* sensor nominal / noise / range / cycles from the ``iot_simulator`` catalog
  (noise is in sensor units, as the producer's ``_calibrate_simulator`` emits it),
* the producer's scheduled heat event (``SPINDLE_HEAT_SCENARIO`` in
  ``client/src/lib/manufacturing-scenario.ts``), on half of the cycles and with a
  random peak, so some ramps stay under the limit,
* the ``ScenarioEngine`` degraded / failed / recovering phase effects and its lot
  metadata (on a quarter of the runs, 15 min phases),
* spindle wear: hotter and rougher the longer since maintenance, and on most heat
  events a bearing-friction precursor (vibration and load rise first).

Every reading is tagged ``data_origin=synthetic``. Nothing here is a record of
a real machine; the precursor physics is a modelling assumption, documented in
docs/ml-risk-model.md.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any

from iot_twin.ml.spindle_features import INDUSTRY, SPINDLE_SIGNALS
from iot_twin.scenario_engine.engine import ScenarioEngine

DATA_ORIGIN = "synthetic"

# Mirrors SPINDLE_HEAT_SCENARIO in client/src/lib/manufacturing-scenario.ts.
HEAT_CYCLE_S = 300
HEAT_NORMAL_S = 210
HEAT_RAMP_S = 30
HEAT_HOLD_S = 30
HEAT_PEAK_DELTA_C = 25.0

# Share of 300 s cycles with a heat event, and its random peak as a fraction
# of HEAT_PEAK_DELTA_C (some ramps stay under the limit).
_EVENT_PROBABILITY = 0.5
_PEAK_RANGE = (0.3, 1.1)
_PRECURSOR_PROBABILITY = 0.65
_PRECURSOR_LEAD_S = (45.0, 150.0)
_MAX_MAINTENANCE_AGE_H = 3000.0
_DEFAULT_START_EPOCH = 1_790_000_000.0
# ScenarioEngine phases (normal → degraded → failed → recovering) on a share of
# runs, each phase this long, so degradation builds for minutes before failure.
_PHASE_RUN_PROBABILITY = 0.25
_PHASE_SECONDS = 900.0


@dataclass(frozen=True)
class SensorProfile:
    sensor_name: str
    unit: str
    nominal: float
    noise: float
    low: float
    high: float
    cyclic: bool
    period_s: float
    amplitude: float


@dataclass(frozen=True)
class SyntheticRun:
    run_id: str
    readings: tuple[dict[str, Any], ...]
    last_maintenance_at: float
    phases_enabled: bool


def simulator_profiles() -> dict[str, SensorProfile]:
    """Spindle sensor profiles from the iot_simulator catalog."""
    from iot_simulator.sensor_models import INDUSTRY_SENSORS, IndustryType

    wanted = {spec.sensor_name for spec in SPINDLE_SIGNALS}
    profiles: dict[str, SensorProfile] = {}
    for cfg in INDUSTRY_SENSORS[IndustryType.MANUFACTURING]:
        if cfg.name not in wanted:
            continue
        profiles[cfg.name] = SensorProfile(
            sensor_name=cfg.name,
            unit=cfg.unit,
            nominal=float(cfg.nominal_value),
            noise=float(cfg.noise_std),
            low=float(cfg.min_value),
            high=float(cfg.max_value),
            cyclic=bool(cfg.cyclic),
            period_s=float(cfg.cycle_period_seconds),
            amplitude=float(cfg.cycle_amplitude),
        )
    missing = wanted - set(profiles)
    if missing:
        raise KeyError(f"iot_simulator catalog lacks spindle sensors: {sorted(missing)}")
    return profiles


def heat_delta_at(epoch: float, peak_c: float) -> float:
    """The producer's scheduled heat-event offset at ``epoch`` for a given peak."""
    t = epoch % HEAT_CYCLE_S
    if t < HEAT_NORMAL_S:
        return 0.0
    t -= HEAT_NORMAL_S
    if t < HEAT_RAMP_S:
        return peak_c * t / HEAT_RAMP_S
    t -= HEAT_RAMP_S
    if t < HEAT_HOLD_S:
        return peak_c
    t -= HEAT_HOLD_S
    cool_s = HEAT_CYCLE_S - HEAT_NORMAL_S - HEAT_RAMP_S - HEAT_HOLD_S
    return peak_c * max(0.0, 1.0 - t / cool_s)


@dataclass(frozen=True)
class _HeatCycle:
    peak_c: float
    precursor_s: float | None
    vib_rise: float
    load_rise: float


def _heat_cycle(rng: random.Random) -> _HeatCycle:
    if rng.random() >= _EVENT_PROBABILITY:
        return _HeatCycle(0.0, None, 0.0, 0.0)
    peak = HEAT_PEAK_DELTA_C * rng.uniform(*_PEAK_RANGE)
    if rng.random() < _PRECURSOR_PROBABILITY:
        return _HeatCycle(peak, rng.uniform(*_PRECURSOR_LEAD_S), rng.uniform(1.5, 3.5), 0.12)
    return _HeatCycle(peak, None, 0.0, 0.0)


def _precursor_ramp(epoch: float, cycle: _HeatCycle) -> float:
    """0→1 from ``precursor_s`` before the ramp starts, 1 through the event, then 0."""
    if cycle.precursor_s is None:
        return 0.0
    t = epoch % HEAT_CYCLE_S
    ramp_start = float(HEAT_NORMAL_S)
    event_end = float(HEAT_NORMAL_S + HEAT_RAMP_S + HEAT_HOLD_S)
    if ramp_start - cycle.precursor_s <= t < ramp_start:
        return (t - (ramp_start - cycle.precursor_s)) / cycle.precursor_s
    if ramp_start <= t < event_end:
        return 1.0
    return 0.0


def _base_value(profile: SensorProfile, epoch: float, phase_offset: float) -> float:
    value = profile.nominal
    if profile.cyclic:
        angle = 2 * math.pi * epoch / profile.period_s + phase_offset
        value += math.sin(angle) * profile.amplitude * profile.nominal
    return value


def _reading(profile: SensorProfile, epoch: float, value: float, run_id: str) -> dict[str, Any]:
    return {
        "sensor_name": profile.sensor_name,
        "industry": INDUSTRY,
        "value": value,
        "unit": profile.unit,
        "timestamp": epoch,
        "min_value": profile.low,
        "max_value": profile.high,
        "nominal_value": profile.nominal,
        "fault_active": False,
        "metadata": {"data_origin": DATA_ORIGIN, "run_id": run_id},
    }


def _run_readings(
    run_id: str,
    rng: random.Random,
    profiles: dict[str, SensorProfile],
    *,
    start: float,
    duration_s: int,
    wear: float,
    engine: ScenarioEngine | None,
) -> list[dict[str, Any]]:
    offsets = {name: rng.uniform(0, 2 * math.pi) for name in profiles}
    cycles: dict[int, _HeatCycle] = {}
    readings: list[dict[str, Any]] = []
    for step in range(duration_s):
        epoch = start + step
        cycle = cycles.setdefault(int(epoch // HEAT_CYCLE_S), _heat_cycle(rng))
        friction = _precursor_ramp(epoch, cycle)
        batch: list[dict[str, Any]] = []
        for name, profile in profiles.items():
            value = _base_value(profile, epoch, offsets[name]) + rng.gauss(0, profile.noise)
            if name == "cnc_1_spindle_temp":
                value += 4.0 * wear + heat_delta_at(epoch, cycle.peak_c)
            elif name == "cnc_1_vibration":
                value += 0.6 * wear + friction * cycle.vib_rise
            elif name == "cnc_1_spindle_power":
                value *= 1.0 + friction * cycle.load_rise
            batch.append(_reading(profile, epoch, value, run_id))
        if engine is not None:
            batch = engine.wrap_records(batch)
        for record in batch:
            profile = profiles[record["sensor_name"]]
            record["value"] = min(profile.high, max(profile.low, float(record["value"])))
            record["metadata"] = {**record["metadata"], "data_origin": DATA_ORIGIN}
        readings.extend(batch)
    return readings


def generate_runs(
    *,
    n_runs: int = 24,
    duration_s: int = 7200,
    seed: int = 7,
    start_epoch: float = _DEFAULT_START_EPOCH,
) -> list[SyntheticRun]:
    """Deterministic synthetic runs at 1 Hz per spindle sensor."""
    rng = random.Random(seed)
    profiles = simulator_profiles()
    runs: list[SyntheticRun] = []
    for index in range(n_runs):
        run_id = f"synthetic-{seed}-{index:03d}"
        start = start_epoch + index * (duration_s + HEAT_CYCLE_S)
        hours = rng.uniform(0.0, _MAX_MAINTENANCE_AGE_H)
        phases = rng.random() < _PHASE_RUN_PROBABILITY
        engine = (
            ScenarioEngine(
                INDUSTRY, seed=seed + index, scenario_id=run_id, phase_seconds=_PHASE_SECONDS
            )
            if phases
            else None
        )
        readings = _run_readings(
            run_id,
            rng,
            profiles,
            start=start,
            duration_s=duration_s,
            wear=hours / _MAX_MAINTENANCE_AGE_H,
            engine=engine,
        )
        runs.append(
            SyntheticRun(
                run_id=run_id,
                readings=tuple(readings),
                last_maintenance_at=start - hours * 3600.0,
                phases_enabled=phases,
            )
        )
    return runs
