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
