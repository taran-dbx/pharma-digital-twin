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
