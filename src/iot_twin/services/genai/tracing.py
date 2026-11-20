"""Optional MLflow tracing of each action-plan call.

The Lakebase audit row is the system of record. When ``mlflow`` (or
``mlflow-tracing``) is installed and ``GENAI_MLFLOW_TRACING=1``, each call is
also recorded as an MLflow trace span (inputs: alert id, prompt hash, citation
ids; outputs: grounding check, usage, latency) tagged with the audit plan id.
Without the package or the flag this is a no-op, and a tracing failure never
affects the plan.
"""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

TRACING_ENV = "GENAI_MLFLOW_TRACING"


def _mlflow() -> Any | None:
    if os.getenv(TRACING_ENV, "").strip().lower() not in {"1", "true", "yes"}:
        return None
    try:
        return importlib.import_module("mlflow")
    except ImportError:
        logger.info("GENAI_MLFLOW_TRACING is set but mlflow is not installed; skipping traces")
        return None


def _noop(_outputs: dict[str, Any]) -> None:
    return None


@contextmanager
def trace_span(name: str, inputs: dict[str, Any]) -> Iterator[Callable[[dict[str, Any]], None]]:
    """Yield a callback that records the span outputs (no-op when tracing is off)."""
    mlflow = _mlflow()
    if mlflow is None:
        yield _noop
        return
    try:
        manager = mlflow.start_span(name=name, span_type="CHAT_MODEL")
        span = manager.__enter__()
        span.set_inputs(inputs)
    except Exception as exc:  # noqa: BLE001 - tracing must never break the call
        logger.warning("mlflow trace start failed: %s", exc)
        yield _noop
        return

    def record(outputs: dict[str, Any]) -> None:
        try:
            span.set_outputs(outputs)
        except Exception as exc:  # noqa: BLE001
            logger.warning("mlflow trace outputs failed: %s", exc)

    try:
        yield record
    finally:
        try:
            manager.__exit__(None, None, None)
        except Exception as exc:  # noqa: BLE001
            logger.warning("mlflow trace end failed: %s", exc)
