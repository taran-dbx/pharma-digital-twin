"""Threshold selection and confusion metrics (pure Python, used by the training job)."""

from __future__ import annotations

from collections.abc import Sequence


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
