"""SIMULATED Spindle 3 thermal-runaway scenario, shared by the live producer and training.

This file is the single generator for the Spindle 3 heat episodes:

* the realtime producer notebook embeds this source verbatim
  (``client/src/lib/generated/spindle-scenario-python.ts``, kept in sync by
  ``scripts/sync_spindle_scenario.py`` and a test) and calls
  ``apply_spindle_scenario`` on every reading before it is sent;
* the risk model's live-format training data and the endpoint replay
  (``ml/live_format_telemetry.py`` in this repo) import it and call the same function.

The physics is SIMULATED. It is a plausible stand-in for what precedes spindle
thermal runaway on a real mill: bearing preload/wear friction shows up as rising
vibration and spindle power (so torque) and a slow temperature creep, tens of
seconds to minutes before the temperature takes off. Nothing here is measured
behaviour of a real machine; see docs/ml-risk-model.md.

Every 300 s lot cycle draws a plan from a deterministic per-cycle PRNG (so the
dashboard can show when the next excursion is due):

* 70 % of cycles have a heat excursion: the ramp starts at a random point
  30-210 s into the cycle (real spindles are not periodic, so the clock alone
  must not predict it) and peaks 20-29 °C over the baseline. Of those, 55 %
  have a strong precursor, 25 % a weak one and 20 % none at all.
* 15 % are false alarms: a precursor-like rise that fades without an
  excursion (60 %), or a warm bump of 1-2.5 °C that stays under the limit (40 %).
* 15 % are quiet.

Each precursor has a random lead (30-180 s before the ramp; it may start in
the previous cycle, never overlapping that cycle's episode), amplitude, onset
shape and a slow wobble, plus per-sample noise from the caller's RNG. It is a
noisy, variable tell, not a clean deterministic one.

Hard bounds, so the scenario never trips another catalog rule:

* the vibration precursor adds at most ``SPINDLE_VIBRATION_PRECURSOR_MAX``
  (noise included) and a reading it touches is capped at
  ``SPINDLE_VIBRATION_CEILING``, below ``mfg-alert-vibration`` (8.0 mm/s);
* in a false-alarm cycle the temperature offset is at most
  ``spindle_max_temp_offset(plan)`` (creep and bump use the envelope without
  wobble), which keeps a 4-sigma simulator sample under 60 °C.

Stdlib only and no ``from __future__`` import: the producer notebook runs this
source inside a larger cell.
"""

import functools
import math
import random
from typing import Any, NamedTuple

SPINDLE_SCENARIO_ID = "spindle-3-heat"
SPINDLE_TEMP_SENSOR = "cnc_1_spindle_temp"
SPINDLE_VIBRATION_SENSOR = "cnc_1_vibration"
SPINDLE_POWER_SENSOR = "cnc_1_spindle_power"
SPINDLE_SCENARIO_SENSORS = (SPINDLE_TEMP_SENSOR, SPINDLE_VIBRATION_SENSOR, SPINDLE_POWER_SENSOR)

SPINDLE_CYCLE_S = 300
SPINDLE_RAMP_START_RANGE_S = (30.0, 210.0)
SPINDLE_RAMP_S = 30
SPINDLE_HOLD_S = 30
SPINDLE_COOL_S = 30
SPINDLE_EPISODE_S = SPINDLE_RAMP_S + SPINDLE_HOLD_S + SPINDLE_COOL_S
SPINDLE_PEAK_DELTA_C = 25.0

# Per-cycle plan. client/src/lib/manufacturing-scenario.ts ports the PRNG and
# the first two draws (excursion or not, ramp start) for the dashboard
# countdown; a test checks that both agree.
SPINDLE_PLAN_SEED = 20261121
SPINDLE_PLAN_DRAWS = 16
SPINDLE_EXCURSION_SHARE = 0.70
SPINDLE_FALSE_ALARM_SHARE = 0.15
SPINDLE_STRONG_PRECURSOR_SHARE = 0.55
SPINDLE_WEAK_PRECURSOR_SHARE = 0.25
SPINDLE_FADE_ALARM_SHARE = 0.60
SPINDLE_LEAD_RANGE_S = (30.0, 180.0)
SPINDLE_PEAK_RANGE = (0.80, 1.15)
SPINDLE_STRONG_SCALE = (0.60, 1.00)
SPINDLE_WEAK_SCALE = (0.15, 0.35)
SPINDLE_GAMMA_RANGE = (0.5, 2.0)
SPINDLE_FADE_RANGE_S = (20.0, 60.0)
SPINDLE_BUMP_RANGE_C = (1.0, 2.5)
SPINDLE_JITTER_RANGE = (0.6, 1.4)
# Precursor amplitude at scale 1: vibration (mm/s), power (fraction), temperature creep (°C).
SPINDLE_VIBRATION_RISE = 3.0
SPINDLE_POWER_RISE = 0.15
SPINDLE_TEMP_CREEP_C = 2.5
# Hard caps (see the module docstring).
SPINDLE_VIBRATION_PRECURSOR_MAX = 4.0
SPINDLE_VIBRATION_CEILING = 7.5
SPINDLE_WOBBLE_MAX = 1.5

_MASK32 = 0xFFFFFFFF


def _imul(a: int, b: int) -> int:
    return (a * b) & _MASK32


def spindle_cycle_draws(index: int, count: int) -> tuple[float, ...]:
    """``count`` uniform [0, 1) draws for cycle ``index`` (mulberry32, as in the TS port)."""
    state = (SPINDLE_PLAN_SEED ^ _imul(index & _MASK32, 0x9E3779B1)) & _MASK32
    draws = []
    for _ in range(count):
        state = (state + 0x6D2B79F5) & _MASK32
        t = _imul(state ^ (state >> 15), state | 1)
        t = ((t + _imul(t ^ (t >> 7), t | 61)) & _MASK32) ^ t
        draws.append(((t ^ (t >> 14)) & _MASK32) / 4294967296.0)
    return tuple(draws)


def _between(draw: float, bounds: tuple[float, float]) -> float:
    return bounds[0] + (bounds[1] - bounds[0]) * draw


class SpindleCyclePlan(NamedTuple):
    index: int
    kind: str  # "excursion" | "false_alarm" | "quiet"
    precursor: str  # "strong" | "weak" | "none" | "fade" | "bump"
    ramp_start_s: float  # into the cycle; the precursor peaks here
    lead_s: float  # effective lead (clipped so it never overlaps the previous episode)
    gamma: float
    vibration_rise: float
    power_rise: float
    temp_creep_c: float
    peak_c: float
    fade_s: float
    bump_c: float
    wobble: tuple[float, float, float, float]


def _kind(draw: float) -> str:
    if draw < SPINDLE_EXCURSION_SHARE:
        return "excursion"
    if draw < SPINDLE_EXCURSION_SHARE + SPINDLE_FALSE_ALARM_SHARE:
        return "false_alarm"
    return "quiet"


def spindle_cycle_kind(index: int) -> str:
    return _kind(spindle_cycle_draws(index, 1)[0])


def spindle_ramp_start(index: int) -> float:
    """Seconds into cycle ``index`` where its episode (ramp or false-alarm peak) starts."""
    return _between(spindle_cycle_draws(index, 2)[1], SPINDLE_RAMP_START_RANGE_S)


def _precursor_kind(kind: str, draw: float) -> str:
    if kind == "excursion":
        if draw < SPINDLE_STRONG_PRECURSOR_SHARE:
            return "strong"
        if draw < SPINDLE_STRONG_PRECURSOR_SHARE + SPINDLE_WEAK_PRECURSOR_SHARE:
            return "weak"
        return "none"
    if kind == "false_alarm":
        return "fade" if draw < SPINDLE_FADE_ALARM_SHARE else "bump"
    return "none"


@functools.lru_cache(maxsize=256)
def _episode_end_s(index: int) -> float:
    """Seconds into cycle ``index`` when its episode is over (0 for a quiet cycle)."""
    d = spindle_cycle_draws(index, SPINDLE_PLAN_DRAWS)
    kind = _kind(d[0])
    start = _between(d[1], SPINDLE_RAMP_START_RANGE_S)
    if kind == "excursion":
        return start + SPINDLE_EPISODE_S
    if kind == "false_alarm":
        return start + _between(d[10], SPINDLE_FADE_RANGE_S)
    return 0.0


@functools.lru_cache(maxsize=256)
def spindle_cycle_plan(index: int) -> SpindleCyclePlan:
    """The deterministic plan of cycle ``index`` (``epoch // SPINDLE_CYCLE_S``)."""
    d = spindle_cycle_draws(index, SPINDLE_PLAN_DRAWS)
    kind = _kind(d[0])
    start = _between(d[1], SPINDLE_RAMP_START_RANGE_S)
    precursor = _precursor_kind(kind, d[2])
    if precursor == "strong":
        scale = _between(d[3], SPINDLE_STRONG_SCALE)
    elif precursor in ("weak", "bump"):
        scale = _between(d[3], SPINDLE_WEAK_SCALE)
    elif precursor == "fade":
        # A fading false alarm can look as strong as a real precursor.
        scale = _between(d[3], (SPINDLE_WEAK_SCALE[0], SPINDLE_STRONG_SCALE[1]))
    else:
        scale = 0.0
    # The precursor may start in the previous cycle, but only after its episode ended.
    gap = start + SPINDLE_CYCLE_S - _episode_end_s(index - 1)
    return SpindleCyclePlan(
        index=index,
        kind=kind,
        precursor=precursor,
        ramp_start_s=start,
        lead_s=min(_between(d[4], SPINDLE_LEAD_RANGE_S), gap),
        gamma=_between(d[5], SPINDLE_GAMMA_RANGE),
        vibration_rise=SPINDLE_VIBRATION_RISE * scale * _between(d[6], SPINDLE_JITTER_RANGE),
        power_rise=SPINDLE_POWER_RISE * scale * _between(d[7], SPINDLE_JITTER_RANGE),
        temp_creep_c=SPINDLE_TEMP_CREEP_C * scale * _between(d[8], SPINDLE_JITTER_RANGE),
        peak_c=SPINDLE_PEAK_DELTA_C * _between(d[9], SPINDLE_PEAK_RANGE)
        if kind == "excursion"
        else 0.0,
        fade_s=_between(d[10], SPINDLE_FADE_RANGE_S),
        bump_c=_between(d[11], SPINDLE_BUMP_RANGE_C) if precursor == "bump" else 0.0,
        wobble=(_between(d[12], (15.0, 40.0)), _between(d[13], (4.0, 10.0)), d[14], d[15]),
    )


def spindle_max_temp_offset(plan: SpindleCyclePlan) -> float:
    """Upper bound of the temperature offset a false-alarm or quiet cycle can add."""
    if plan.kind == "excursion":
        return plan.peak_c + plan.temp_creep_c
    return plan.temp_creep_c + plan.bump_c


def spindle_heat_phase(epoch: float, plan: SpindleCyclePlan) -> tuple[str, float]:
    """``(phase, temperature offset °C)`` of the cycle's heat episode."""
    if plan.kind != "excursion":
        return "normal", 0.0
    t = epoch - plan.index * SPINDLE_CYCLE_S - plan.ramp_start_s
    if t < 0:
        return "normal", 0.0
    if t < SPINDLE_RAMP_S:
        return "heating", plan.peak_c * t / SPINDLE_RAMP_S
    t -= SPINDLE_RAMP_S
    if t < SPINDLE_HOLD_S:
        return "overheat", plan.peak_c
    t -= SPINDLE_HOLD_S
    if t < SPINDLE_COOL_S:
        return "cooling", plan.peak_c * (1.0 - t / SPINDLE_COOL_S)
    return "normal", 0.0


def _envelope(epoch: float, plan: SpindleCyclePlan) -> float:
    """Precursor envelope of ``plan`` in [0, 1], before wobble (0 outside its episode)."""
    if plan.precursor == "none" or plan.kind == "quiet":
        return 0.0
    t = epoch - plan.index * SPINDLE_CYCLE_S - plan.ramp_start_s
    if t < -plan.lead_s:
        return 0.0
    if t < 0:
        return ((t + plan.lead_s) / plan.lead_s) ** plan.gamma
    if plan.kind == "excursion":
        hot = SPINDLE_RAMP_S + SPINDLE_HOLD_S
        return 1.0 if t < hot else max(0.0, 1.0 - (t - hot) / SPINDLE_COOL_S)
    return max(0.0, 1.0 - t / plan.fade_s)


def _active_plan(epoch: float) -> tuple[SpindleCyclePlan, float]:
    """The plan whose precursor or episode covers ``epoch``, with its envelope."""
    index = int(epoch // SPINDLE_CYCLE_S)
    plan = spindle_cycle_plan(index)
    level = _envelope(epoch, plan)
    if level == 0.0:
        upcoming = spindle_cycle_plan(index + 1)
        ahead = _envelope(epoch, upcoming)
        if ahead > 0.0:
            return upcoming, ahead
    return plan, level


def _wobble(epoch: float, plan: SpindleCyclePlan) -> float:
    p1, p2, phase1, phase2 = plan.wobble
    return (
        1.0
        + 0.3 * math.sin(2 * math.pi * (epoch / p1 + phase1))
        + 0.2 * math.sin(2 * math.pi * (epoch / p2 + phase2))
    )


def spindle_precursor_level(epoch: float) -> float:
    """Precursor level at ``epoch`` in [0, SPINDLE_WOBBLE_MAX] (envelope times wobble)."""
    plan, envelope = _active_plan(epoch)
    if envelope == 0.0:
        return 0.0
    return min(SPINDLE_WOBBLE_MAX, max(0.0, envelope * _wobble(epoch, plan)))


def spindle_offsets(
    sensor_name: str, epoch: float, rng: random.Random
) -> tuple[float, float, str, float | None]:
    """``(offset, factor, temperature phase, ceiling)`` for one reading."""
    heat_plan = spindle_cycle_plan(int(epoch // SPINDLE_CYCLE_S))
    phase, heat = spindle_heat_phase(epoch, heat_plan)
    plan, envelope = _active_plan(epoch)
    if envelope == 0.0:
        return (heat if sensor_name == SPINDLE_TEMP_SENSOR else 0.0), 1.0, phase, None
    level = min(SPINDLE_WOBBLE_MAX, max(0.0, envelope * _wobble(epoch, plan)))
    if sensor_name == SPINDLE_TEMP_SENSOR:
        # Creep and bump follow the envelope without wobble: bounded by
        # spindle_max_temp_offset.
        return heat + (plan.temp_creep_c + plan.bump_c) * envelope, 1.0, phase, None
    if sensor_name == SPINDLE_VIBRATION_SENSOR:
        rise = plan.vibration_rise * level + rng.gauss(0.0, 0.15 * plan.vibration_rise * level)
        offset = min(SPINDLE_VIBRATION_PRECURSOR_MAX, max(0.0, rise))
        return offset, 1.0, phase, SPINDLE_VIBRATION_CEILING
    if sensor_name == SPINDLE_POWER_SENSOR:
        return 0.0, 1.0 + plan.power_rise * level + rng.gauss(0.0, 0.01 * level), phase, None
    return 0.0, 1.0, phase, None


def apply_spindle_scenario(payload: dict, rng: random.Random) -> dict:
    """A new payload with the scenario applied (other sensors are returned unchanged)."""
    name = str(payload.get("sensor_name") or "")
    if name not in SPINDLE_SCENARIO_SENSORS:
        return payload
    offset, factor, phase, ceiling = spindle_offsets(name, float(payload["timestamp"]), rng)
    value: Any = float(payload["value"]) * factor + offset
    low, high = payload.get("min_value"), payload.get("max_value")
    if ceiling is not None:
        value = min(ceiling, value)
    if high is not None:
        value = min(float(high), value)
    if low is not None:
        value = max(float(low), value)
    updated = {**payload, "value": value}
    if name == SPINDLE_TEMP_SENSOR:
        updated["fault_active"] = phase != "normal"
        updated["metadata"] = {
            **(payload.get("metadata") or {}),
            "scenario_id": SPINDLE_SCENARIO_ID,
            "phase": phase,
        }
    return updated
