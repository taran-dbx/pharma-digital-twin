"""Threshold selection and confusion metrics (pure Python, used by the training job)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


def precision_recall_at(
    labels: Sequence[int], scores: Sequence[float], threshold: float
) -> dict[str, float]:
    tp = fp = fn = 0
    for label, score in zip(labels, scores, strict=True):
        predicted = score >= threshold
        if predicted and label:
            tp += 1
        elif predicted:
            fp += 1
        elif label:
            fn += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def quantile(values: Sequence[float], q: float) -> float:
    """Linear-interpolated quantile (``q`` in [0, 1]); 0.0 for no values."""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    pos = (len(ordered) - 1) * min(1.0, max(0.0, q))
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def clear_threshold_for(cutoff: float, hysteresis: float, floor: float = 0.01) -> float:
    """Clear level for a predicted alert: ``hysteresis`` below the cut-off, >= ``floor``."""
    return round(max(floor, cutoff - hysteresis), 4)


WEAR_BANDS_H: tuple[tuple[float, float], ...] = ((0.0, 1000.0), (1000.0, 2000.0), (2000.0, 3000.0))


CALM_MIN_MARGIN_C = 8.0
CALM_MAX_SLOPE_C_PER_MIN = 5.0


def is_calm(label: int, margin_c: float, slope_c_per_min: float) -> bool:
    """Steady running: no excursion ahead, well under the limit, temperature flat.

    Label-0 rows also include sub-limit heat ramps (near misses) that rightly
    score high; they are not calm running.
    """
    return (
        label == 0
        and margin_c >= CALM_MIN_MARGIN_C
        and abs(slope_c_per_min) < CALM_MAX_SLOPE_C_PER_MIN
    )


def calm_score_check(
    scores: Sequence[float],
    calm_mask: Sequence[bool],
    wear_h: Sequence[float],
    *,
    cutoff: float,
    clear: float,
    q: float = 0.99,
    bands: Sequence[tuple[float, float]] = WEAR_BANDS_H,
) -> tuple[dict[str, float], list[str]]:
    """High-percentile calm scores (``calm_mask``, see ``is_calm``), overall and per
    wear band, vs the cut-off itself.

    A calm p99 at or above the cut-off means calm running opens predicted
    alerts; a calm p90 above the clear level means they will not clear. Returns
    ``(metrics, problems)``; empty bands are skipped.
    """
    rows = list(zip(scores, calm_mask, wear_h, strict=True))
    calm = [score for score, steady, _ in rows if steady]
    pct = int(round(q * 100))
    metrics = {
        "val_calm_score_p50": quantile(calm, 0.5),
        "val_calm_score_p90": quantile(calm, 0.9),
        f"val_calm_score_p{pct}": quantile(calm, q),
    }
    problems: list[str] = []
    if calm and metrics[f"val_calm_score_p{pct}"] >= cutoff:
        problems.append(
            f"calm p{pct} {metrics[f'val_calm_score_p{pct}']:.3f} >= cut-off {cutoff:.3f}"
        )
    if calm and metrics["val_calm_score_p90"] > clear:
        problems.append(f"calm p90 {metrics['val_calm_score_p90']:.3f} > clear level {clear:.3f}")
    last = len(bands) - 1
    for index, (lo, hi) in enumerate(bands):
        band = [
            score
            for score, steady, wear in rows
            if steady and lo <= wear and (wear < hi or (index == last and wear <= hi))
        ]
        if not band:
            continue
        value = quantile(band, q)
        metrics[f"val_calm_score_p{pct}_wear_{int(lo)}_{int(hi)}h"] = value
        if value >= cutoff:
            problems.append(
                f"calm p{pct} {value:.3f} at {int(lo)}-{int(hi)} h wear >= cut-off {cutoff:.3f}"
            )
    return metrics, problems


def two_classes(labels: Sequence[int]) -> bool:
    """ROC-AUC / PR-AUC are undefined on a single-class split."""
    return len(set(labels)) == 2


def choose_threshold(
    labels: Sequence[int],
    scores: Sequence[float],
    *,
    min_precision: float,
    grid: Sequence[float] | None = None,
) -> tuple[float, str]:
    """Highest-recall cut-off whose precision is at least ``min_precision``.

    A predicted alert that is usually wrong would teach operators to ignore it,
    so precision is the constraint. When no cut-off reaches it, fall back to the
    best F1. Returns ``(threshold, rule)``.
    """
    candidates = list(grid) if grid is not None else [i / 100 for i in range(5, 100)]
    best: tuple[float, float] | None = None
    best_f1: tuple[float, float] | None = None
    for threshold in candidates:
        stats = precision_recall_at(labels, scores, threshold)
        if stats["precision"] >= min_precision and (best is None or stats["recall"] > best[1]):
            best = (threshold, stats["recall"])
        if best_f1 is None or stats["f1"] > best_f1[1]:
            best_f1 = (threshold, stats["f1"])
    if best is not None:
        return best[0], f"max_recall_at_precision>={min_precision}"
    if best_f1 is None:
        raise ValueError("no threshold candidates")
    return best_f1[0], "max_f1"


# A challenger may raise false alarms by at most this much per hour of running.
FALSE_ALARM_TOLERANCE_PER_H = 1.0
# Qualification: what any version must show on pooled held-out cycles (the
# test split plus a fresh replay; see iot_twin.ml.gate_stats).
SIGNIFICANCE_P = 0.05
FALSE_ALARM_CEILING_PER_H = 3.0  # the design budget, counting every flap
GATE_METRIC = "discrimination_std_lo95"
# Input invariance: re-scored with a non-signal input (hours_since_maintenance)
# at INVARIANCE_HOURS, the same cycles must give the same answer within these.
INVARIANCE_HOURS = (1500.0, 2500.0, 3000.0)
INVARIANCE_DISCRIMINATION_TOL = 0.03
INVARIANCE_FALSE_ALARM_TOL_PER_H = 0.3


def qualifies(metrics: Mapping[str, float]) -> tuple[bool, list[str]]:
    """Absolute checks for one model: ``(passes, problems)``. All are blocking.

    * ``clear_threshold`` >= ``steady_clear_floor`` (steady-running p90 on
      validation), else the alert would stay open through calm running;
    * pooled ``false_alarms_per_hour`` <= ``FALSE_ALARM_CEILING_PER_H``;
    * pooled ``warned_30s_share`` > ``clock_warned_30s_share`` (the best fixed
      in-cycle window chosen on validation at this model's alert duty);
    * pooled cycle-permutation ``permutation_p`` < ``SIGNIFICANCE_P``;
    * ``discrimination_std_lo95`` > 0: the lower 95 % bootstrap bound of
      discrimination standardised to the scenario's nominal plan mix;
    * input invariance: re-scored with ``hours_since_maintenance`` at each of
      ``INVARIANCE_HOURS`` (it carries no signal in the simulated scenario and
      the live scorer sends a value that grows by 24 h a day), the ranges of
      standardised discrimination and false alarms per hour stay within
      ``INVARIANCE_DISCRIMINATION_TOL`` and ``INVARIANCE_FALSE_ALARM_TOL_PER_H``.
    """
    problems: list[str] = []
    disc_range = metrics["invariance_discrimination_std_range"]
    fa_range = metrics["invariance_false_alarms_per_hour_range"]
    if disc_range > INVARIANCE_DISCRIMINATION_TOL or fa_range > INVARIANCE_FALSE_ALARM_TOL_PER_H:
        problems.append(
            "not invariant to hours_since_maintenance "
            f"({'/'.join(f'{h:g}' for h in INVARIANCE_HOURS)} h): discrimination range "
            f"{disc_range:.3f} (tol {INVARIANCE_DISCRIMINATION_TOL}), false alarms/h range "
            f"{fa_range:.2f} (tol {INVARIANCE_FALSE_ALARM_TOL_PER_H})"
        )
    if metrics["clear_threshold"] < metrics["steady_clear_floor"]:
        problems.append(
            f"clear level {metrics['clear_threshold']:.3f} < steady-running p90 "
            f"{metrics['steady_clear_floor']:.3f}"
        )
    if metrics["false_alarms_per_hour"] > FALSE_ALARM_CEILING_PER_H:
        problems.append(
            f"false alarms {metrics['false_alarms_per_hour']:.2f}/h > "
            f"{FALSE_ALARM_CEILING_PER_H:g}/h"
        )
    if metrics["warned_30s_share"] <= metrics["clock_warned_30s_share"]:
        problems.append(
            f"warned>=30s {metrics['warned_30s_share']:.3f} <= clock baseline "
            f"{metrics['clock_warned_30s_share']:.3f}"
        )
    if metrics["permutation_p"] >= SIGNIFICANCE_P:
        problems.append(f"cycle-permutation p {metrics['permutation_p']:.4f} >= {SIGNIFICANCE_P}")
    if metrics[GATE_METRIC] <= 0.0:
        problems.append(
            f"standardised discrimination lower 95 % bound {metrics[GATE_METRIC]:.3f} <= 0"
        )
    return not problems, problems


def challenger_beats_champion(
    challenger: Mapping[str, float],
    champion: Mapping[str, float] | None,
    *,
    paired: tuple[float, float, float] | None = None,
    false_alarm_tolerance_per_h: float = FALSE_ALARM_TOLERANCE_PER_H,
) -> tuple[bool, str]:
    """Promotion rule on the same pooled held-out cycles: ``(beats, reason)``.

    The challenger must pass ``qualifies``. A champion that fails it is beaten
    by any qualifying challenger. Against a qualifying champion the improvement
    must be real, not noise: ``paired`` is the paired-bootstrap difference
    (challenger minus champion) of standardised discrimination on the same
    cycles, ``(estimate, lo95, hi95)``, and its lower bound must be above 0;
    false alarms must stay within ``false_alarm_tolerance_per_h``. Ties keep
    the incumbent.
    """
    ok, problems = qualifies(challenger)
    if not ok:
        return False, "challenger does not qualify: " + "; ".join(problems)
    if champion is None:
        return True, "challenger qualifies; no champion to compare against"
    champion_ok, champion_problems = qualifies(champion)
    if not champion_ok:
        return True, "champion does not qualify: " + "; ".join(champion_problems)
    if paired is None:
        return False, "no paired comparison available; keeping the qualifying champion"
    better = paired[1] > 0.0
    alarms = (
        challenger["false_alarms_per_hour"]
        <= champion["false_alarms_per_hour"] + false_alarm_tolerance_per_h
    )
    summary = (
        f"paired discrimination_std difference {paired[0]:+.3f} "
        f"(95 % CI {paired[1]:+.3f} to {paired[2]:+.3f}), false alarms/h "
        f"{challenger['false_alarms_per_hour']:.2f} vs {champion['false_alarms_per_hour']:.2f}"
    )
    return better and alarms, summary


def best_by_gate(table: Mapping[str, Mapping[str, float]]) -> str | None:
    """Version with the highest gate metric (ties: the newer version)."""
    if not table:
        return None
    return max(table, key=lambda version: (table[version][GATE_METRIC], int(version)))


def neutral_winner(table: Mapping[str, Mapping[str, float]]) -> str | None:
    """Champion by neutral selection: among versions that pass ``qualifies``, the
    highest standardised discrimination (point estimate) on the shared pooled
    cycles; ties go to fewer false alarms, then the newer version. Incumbency
    plays no part. ``None`` when no version qualifies."""
    passing = [v for v, metrics in table.items() if qualifies(metrics)[0]]
    if not passing:
        return None
    return max(
        passing,
        key=lambda v: (
            table[v]["discrimination_std"],
            -table[v]["false_alarms_per_hour"],
            int(v),
        ),
    )
