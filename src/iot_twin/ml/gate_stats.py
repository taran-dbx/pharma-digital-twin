"""Cycle-level outcomes, plan-mix standardisation and bootstrap CIs for the promotion gate.

Per-row and per-breach metrics on a few hours of a 300 s schedule are noisy:
one 4 h replay holds about 48 cycles. The gate therefore works on **cycles**:

* every full cycle is one record: its scenario plan group (``strong`` /
  ``weak`` / ``none`` excursions, ``fade`` / ``bump`` false alarms, ``quiet``),
  whether it had a breach, whether that breach was warned >= 30 s ahead, and
  how many false alarms opened in it (each flap counts);
* records from the held-out test split and the fresh replay are pooled;
* **standardised discrimination** re-weights the per-group rates to the
  scenario's nominal plan mix, so a sample that happens to hold many fading
  false alarms does not move the gate: ``sum_w(warned rate over excursion
  groups) - sum_w(false-alarm-cycle rate over breach-free groups)``;
* CIs come from a stratified bootstrap over cycles (resampled within each
  group), so every reported number carries its 95 % interval.

A plan-aware *oracle* that alerts at the start of every precursor (real or
fading) and never otherwise warns all strong and weak breaches and raises a
false alarm in every fade and bump cycle: its standardised discrimination is
``0.80 - 0.50 = 0.30``. It is a *reference point*, not a ceiling: a model can
beat it by alerting on a fade less often than on a real precursor. The gate
asks for a lower 95 % bound above 0.

The stratified CIs resample cycles within plan groups, so they are
conditional on the realised plan mix; ``block_bootstrap`` resamples whole
held-out periods and is reported alongside.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from iot_twin.ml.lead_metrics import (
    CYCLE_S,
    EARLY_WARNING_S,
    Episode,
    breach_leads,
    false_alarms,
    full_cycles,
)

EXCURSION_GROUPS = ("strong", "weak", "none")
FREE_GROUPS = ("fade", "bump", "quiet")
# Nominal plan mix of spindle_scenario: excursions 55/25/20; breach-free cycles
# fade 0.15*0.6, bump 0.15*0.4, quiet 0.15 (normalised to 0.3 / 0.2 / 0.5).
NOMINAL_WEIGHTS: Mapping[str, float] = {
    "strong": 0.55,
    "weak": 0.25,
    "none": 0.20,
    "fade": 0.30,
    "bump": 0.20,
    "quiet": 0.50,
}
ORACLE_DISCRIMINATION = 0.30
BOOTSTRAP_DRAWS = 2000


@dataclass(frozen=True)
class CycleOutcome:
    group: str
    breach: bool
    warned_30s: bool
    false_alarms: int
    period: int = 0  # held-out run or replay hour the cycle belongs to


def _warned(lead: float | None, min_lead_s: float) -> bool:
    return lead is not None and lead >= min_lead_s


def cycle_outcomes(
    breaches: Sequence[Episode],
    alerts: Sequence[Episode],
    *,
    start: float,
    end: float,
    group_of: Callable[[int], str],
    min_lead_s: float = EARLY_WARNING_S,
    cycle_s: float = CYCLE_S,
    period: int = 0,
) -> list[CycleOutcome]:
    """One record per full cycle in ``[start, end)``; ``group_of(index)`` names its plan group."""
    onsets = [onset for onset, _ in breaches]
    leads = dict(zip(onsets, breach_leads(onsets, alerts), strict=True))
    alarms: dict[int, int] = {}
    for opened, _ in false_alarms(alerts, breaches):
        index = int(opened // cycle_s)
        alarms[index] = alarms.get(index, 0) + 1
    records: list[CycleOutcome] = []
    for index in full_cycles(start, end, cycle_s):
        in_cycle = [onset for onset in onsets if int(onset // cycle_s) == index]
        warned = any(_warned(leads[onset], min_lead_s) for onset in in_cycle)
        records.append(
            CycleOutcome(
                group=group_of(index),
                breach=bool(in_cycle),
                warned_30s=warned,
                false_alarms=alarms.get(index, 0),
                period=period,
            )
        )
    return records


def _rate(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _weighted(rates: Mapping[str, float | None], groups: Sequence[str]) -> float:
    present: list[tuple[float, float]] = []
    for group in groups:
        rate = rates.get(group)
        if rate is not None and group in NOMINAL_WEIGHTS:
            present.append((NOMINAL_WEIGHTS[group], rate))
    total = sum(weight for weight, _ in present)
    return sum(weight * rate for weight, rate in present) / total if total else 0.0


def summarise(records: Sequence[CycleOutcome], cycle_s: float = CYCLE_S) -> dict[str, float]:
    """Point estimates over cycle records (raw and plan-mix standardised)."""
    breach = [r for r in records if r.breach]
    free = [r for r in records if not r.breach]
    hours = len(records) * cycle_s / 3600.0
    warned = _rate([float(r.warned_30s) for r in breach]) or 0.0
    alarm_share = _rate([float(r.false_alarms > 0) for r in free]) or 0.0
    by_group: dict[str, float | None] = {}
    for group in EXCURSION_GROUPS:
        by_group[group] = _rate([float(r.warned_30s) for r in breach if r.group == group])
    for group in FREE_GROUPS:
        by_group[group] = _rate([float(r.false_alarms > 0) for r in free if r.group == group])
    warned_std = _weighted(by_group, EXCURSION_GROUPS)
    alarm_std = _weighted(by_group, FREE_GROUPS)
    return {
        "cycles": float(len(records)),
        "breaches": float(len(breach)),
        "warned_30s_share": warned,
        "false_alarm_cycle_share": alarm_share,
        "false_alarms_per_hour": sum(r.false_alarms for r in records) / hours if hours else 0.0,
        "discrimination": warned - alarm_share,
        "warned_30s_share_std": warned_std,
        "false_alarm_cycle_share_std": alarm_std,
        "discrimination_std": warned_std - alarm_std,
    }


def bootstrap(
    records: Sequence[CycleOutcome],
    *,
    draws: int = BOOTSTRAP_DRAWS,
    seed: int = 0,
    cycle_s: float = CYCLE_S,
) -> dict[str, tuple[float, float, float]]:
    """``{metric: (estimate, lo95, hi95)}`` from a bootstrap stratified by plan group."""
    point = summarise(records, cycle_s)
    strata: dict[str, list[CycleOutcome]] = {}
    for record in records:
        strata.setdefault(record.group, []).append(record)
    rng = random.Random(seed)
    samples: dict[str, list[float]] = {key: [] for key in point}
    for _ in range(draws):
        resampled = [rng.choice(items) for items in strata.values() for _ in items]
        for key, value in summarise(resampled, cycle_s).items():
            samples[key].append(value)
    out: dict[str, tuple[float, float, float]] = {}
    for key, values in samples.items():
        ordered = sorted(values)
        lo = ordered[int(0.025 * (len(ordered) - 1))]
        hi = ordered[int(round(0.975 * (len(ordered) - 1)))]
        out[key] = (point[key], lo, hi)
    return out


def group_counts(records: Sequence[CycleOutcome]) -> dict[str, dict[str, int]]:
    """Per plan group: cycles, breaches, warned >= 30 s, cycles with a false alarm, false alarms."""
    table: dict[str, dict[str, int]] = {}
    for record in records:
        row = table.setdefault(
            record.group,
            {
                "cycles": 0,
                "breaches": 0,
                "warned_30s": 0,
                "false_alarm_cycles": 0,
                "false_alarms": 0,
            },
        )
        row["cycles"] += 1
        row["breaches"] += int(record.breach)
        row["warned_30s"] += int(record.warned_30s)
        row["false_alarm_cycles"] += int(record.false_alarms > 0)
        row["false_alarms"] += record.false_alarms
    return dict(sorted(table.items()))


def paired_difference(
    challenger: Sequence[CycleOutcome],
    champion: Sequence[CycleOutcome],
    *,
    metric: str = "discrimination_std",
    draws: int = BOOTSTRAP_DRAWS,
    seed: int = 0,
    cycle_s: float = CYCLE_S,
) -> tuple[float, float, float]:
    """``(difference, lo95, hi95)`` of ``metric`` (challenger minus champion), paired.

    Both lists hold the same cycles in the same order (one record per cycle per
    model); each bootstrap draw resamples cycle indices within plan groups and
    applies them to both models, so the shared cycle-to-cycle noise cancels.
    """
    if len(challenger) != len(champion):
        raise ValueError("paired records must cover the same cycles")
    point = summarise(challenger, cycle_s)[metric] - summarise(champion, cycle_s)[metric]
    strata: dict[str, list[int]] = {}
    for index, record in enumerate(challenger):
        strata.setdefault(record.group, []).append(index)
    rng = random.Random(seed)
    diffs = []
    for _ in range(draws):
        picks = [rng.choice(indices) for indices in strata.values() for _ in indices]
        a = summarise([challenger[i] for i in picks], cycle_s)[metric]
        b = summarise([champion[i] for i in picks], cycle_s)[metric]
        diffs.append(a - b)
    diffs.sort()
    return (
        point,
        diffs[int(0.025 * (len(diffs) - 1))],
        diffs[int(round(0.975 * (len(diffs) - 1)))],
    )


def block_bootstrap(
    records: Sequence[CycleOutcome],
    *,
    draws: int = BOOTSTRAP_DRAWS,
    seed: int = 0,
    cycle_s: float = CYCLE_S,
) -> dict[str, tuple[float, float, float]]:
    """``{metric: (estimate, lo95, hi95)}`` resampling whole periods (runs / replay hours).

    Cycles inside one run share its noise and drift, so a cycle-level bootstrap
    can be optimistic; this one resamples the held-out periods with replacement.
    It also lets the plan mix vary, so it is not conditional on the realised mix.
    """
    point = summarise(records, cycle_s)
    periods: dict[int, list[CycleOutcome]] = {}
    for record in records:
        periods.setdefault(record.period, []).append(record)
    blocks = list(periods.values())
    rng = random.Random(seed)
    samples: dict[str, list[float]] = {key: [] for key in point}
    for _ in range(draws):
        resampled = [record for _ in blocks for record in rng.choice(blocks)]
        for key, value in summarise(resampled, cycle_s).items():
            samples[key].append(value)
    out: dict[str, tuple[float, float, float]] = {}
    for key, values in samples.items():
        ordered = sorted(values)
        out[key] = (
            point[key],
            ordered[int(0.025 * (len(ordered) - 1))],
            ordered[int(round(0.975 * (len(ordered) - 1)))],
        )
    return out


def as_rows(records: Sequence[CycleOutcome]) -> list[list[object]]:
    return [[r.group, r.breach, r.warned_30s, r.false_alarms, r.period] for r in records]


def from_rows(rows: Sequence[Sequence[object]]) -> list[CycleOutcome]:
    out = []
    for row in rows:
        group, breach, warned, alarms = row[0], row[1], row[2], row[3]
        period = row[4] if len(row) > 4 else 0
        out.append(
            CycleOutcome(str(group), bool(breach), bool(warned), int(alarms), int(period))  # type: ignore[call-overload]
        )
    return out
