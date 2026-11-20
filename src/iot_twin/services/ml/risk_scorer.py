"""Score live Spindle 3 readings with the thermal-risk model and raise predicted alerts.

Readings reach the scorer through ``evaluate_and_emit`` (the same call that runs
the threshold rules). The scorer keeps a short buffer of spindle readings,
builds features with ``iot_twin.ml.spindle_features`` (the code the training job
uses), and at most every ``interval_s`` queries the ``pharma-twin-spindle-risk``
serving endpoint. Each call has a timeout and goes through a circuit breaker, so
a cold (scale-to-zero) or failing endpoint never blocks ingest. Scores go to
``AlertEngine.ingest_model_scores``, which owns fingerprint and hysteresis.

Disabled when ``RISK_ENDPOINT_NAME`` is unset (local runs, tests, workspaces
without the endpoint).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from iot_twin.domain.catalog import to_record_industry
from iot_twin.domain.lot_context import lot_context_at, lot_fields
from iot_twin.ml.risk_contract import (
    DEFAULT_HYSTERESIS,
    DEFAULT_THRESHOLD,
)
from iot_twin.ml.spindle_features import (
    DEFAULT_HORIZON_S,
    INDUSTRY,
    SPINDLE_SENSOR_NAMES,
    TEMP_SENSOR,
    WINDOW_S,
    build_features,
    last_spindle_maintenance_epoch,
    reading_epoch,
    validated_limit_c,
)
from iot_twin.services.ml.circuit_breaker import CircuitBreaker

logger = logging.getLogger("iot_digital_twin.services.risk_scorer")

ENDPOINT_ENV = "RISK_ENDPOINT_NAME"
MODEL_NAME_ENV = "RISK_MODEL_NAME"
_BUFFER_MAX = 10_000
_VERSION_TTL_S = 300.0


def _env_float(name: str, default: float, low: float, high: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("%s=%r is not a number; using %s", name, raw, default)
        return default
    if not low <= value <= high:
        logger.warning("%s=%s outside [%s, %s]; using %s", name, value, low, high, default)
        return default
    return value


@dataclass(frozen=True)
class RiskScorerConfig:
    endpoint_name: str
    model_name: str
    threshold: float = DEFAULT_THRESHOLD
    hysteresis: float = DEFAULT_HYSTERESIS
    interval_s: float = 5.0
    timeout_s: float = 3.0
    breaker_failures: int = 3
    breaker_cooldown_s: float = 60.0
    horizon_s: float = DEFAULT_HORIZON_S
    window_s: float = WINDOW_S

    @classmethod
    def from_env(cls) -> RiskScorerConfig | None:
        endpoint = os.getenv(ENDPOINT_ENV, "").strip()
        if not endpoint:
            return None
        return cls(
            endpoint_name=endpoint,
            model_name=os.getenv(MODEL_NAME_ENV, "").strip(),
            threshold=_env_float("RISK_ALERT_THRESHOLD", DEFAULT_THRESHOLD, 0.01, 0.99),
            hysteresis=_env_float("RISK_ALERT_HYSTERESIS", DEFAULT_HYSTERESIS, 0.0, 0.5),
            interval_s=_env_float("RISK_SCORE_INTERVAL_S", 5.0, 1.0, 3600.0),
            timeout_s=_env_float("RISK_QUERY_TIMEOUT_S", 3.0, 0.1, 60.0),
            breaker_failures=int(_env_float("RISK_BREAKER_FAILURES", 3, 1, 100)),
            breaker_cooldown_s=_env_float("RISK_BREAKER_COOLDOWN_S", 60.0, 1.0, 3600.0),
            horizon_s=_env_float("RISK_HORIZON_S", DEFAULT_HORIZON_S, 1.0, 3600.0),
        )


@dataclass(frozen=True)
class RiskScore:
    risk_score: float
    top_features: tuple[Mapping[str, Any], ...]
    model_name: str
    model_version: str
    at: float
    lot: Mapping[str, str] = field(default_factory=dict)

    def as_ingest(self, config: RiskScorerConfig, pipeline_id: str | None) -> dict[str, Any]:
        """The payload ``AlertEngine.ingest_model_scores`` accepts."""
        return {
            "industry": INDUSTRY,
            "pipeline_id": pipeline_id,
            "risk_score": self.risk_score,
            "threshold": config.threshold,
            "hysteresis": config.hysteresis,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "top_features": [dict(item) for item in self.top_features],
            "horizon_s": config.horizon_s,
            "scored_at": datetime.fromtimestamp(self.at, UTC).isoformat(),
            "lot": dict(self.lot),
        }


class EndpointResponseError(ValueError):
    """The endpoint answered with something that is not a risk prediction."""


def parse_prediction(predictions: Any) -> tuple[float, tuple[dict[str, Any], ...]]:
    """``(risk_score, top_features)`` from a serving response's predictions."""
    if isinstance(predictions, Mapping) and "predictions" in predictions:
        predictions = predictions["predictions"]
    if isinstance(predictions, Sequence) and not isinstance(predictions, str) and predictions:
        first = predictions[0]
    else:
        raise EndpointResponseError("empty predictions")
    if isinstance(first, Mapping):
        raw_score, raw_top = first.get("risk_score"), first.get("top_features")
    else:
        raw_score, raw_top = first, None
    try:
        score = float(raw_score)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise EndpointResponseError(f"risk_score is not numeric: {raw_score!r}") from exc
    if not 0.0 <= score <= 1.0:
        raise EndpointResponseError(f"risk_score outside [0, 1]: {score}")
    if isinstance(raw_top, str):
        try:
            raw_top = json.loads(raw_top)
        except json.JSONDecodeError:
            raw_top = []
    top = tuple(dict(item) for item in raw_top or () if isinstance(item, Mapping))
    return score, top


_workspace_client: Any = None


def _client() -> Any:
    global _workspace_client
    if _workspace_client is None:
        from databricks.sdk import WorkspaceClient

        _workspace_client = WorkspaceClient()
    return _workspace_client


def sdk_query(endpoint: str, record: Mapping[str, float]) -> Any:
    """Blocking ``serving_endpoints.query`` with one dataframe record."""
    response = _client().serving_endpoints.query(name=endpoint, dataframe_records=[dict(record)])
    return response.predictions


def sdk_served_model(endpoint: str) -> tuple[str | None, str | None]:
    """UC model name and version the endpoint currently serves."""
    found = _client().serving_endpoints.get(endpoint)
    entities = (found.config.served_entities if found.config else None) or []
    if not entities:
        return None, None
    return entities[0].entity_name, entities[0].entity_version


class SpindleRiskScorer:
    def __init__(
        self,
        config: RiskScorerConfig,
        *,
        query_fn: Callable[[str, Mapping[str, float]], Any] = sdk_query,
        served_model_fn: Callable[[str], tuple[str | None, str | None]] = sdk_served_model,
        now_fn: Callable[[], float] = time.monotonic,
        limit_c: float | None = None,
        last_maintenance_at: float | None = None,
    ) -> None:
        self.config = config
        self._query = query_fn
        self._served_model = served_model_fn
        self._now = now_fn
        self.breaker = CircuitBreaker(config.breaker_failures, config.breaker_cooldown_s, now_fn)
        self.limit_c = validated_limit_c() if limit_c is None else limit_c
        self.last_maintenance_at = (
            last_spindle_maintenance_epoch() if last_maintenance_at is None else last_maintenance_at
        )
        self._buffer: deque[dict[str, Any]] = deque(maxlen=_BUFFER_MAX)
        self._last_started: float | None = None
        self._inflight = False
        self._model: tuple[str, str, float] | None = None
        self.tasks: set[asyncio.Task] = set()
        self.stats: dict[str, Any] = {"calls": 0, "failures": 0, "last_score": None}

    # -- buffer -------------------------------------------------------------
    def observe(self, readings: Sequence[Mapping[str, Any]] | None) -> int:
        """Buffer the spindle readings; returns how many were added."""
        added = 0
        for reading in readings or ():
            if not isinstance(reading, Mapping):
                continue
            if str(reading.get("sensor_name") or "") not in SPINDLE_SENSOR_NAMES:
                continue
            if reading_epoch(reading) is None:
                continue
            self._buffer.append(dict(reading))
            added += 1
        if added:
            self._prune()
        return added

    def _prune(self) -> None:
        latest = max((reading_epoch(r) or 0.0) for r in self._buffer)
        horizon = latest - self.config.window_s * 2
        while self._buffer and (reading_epoch(self._buffer[0]) or 0.0) < horizon:
            self._buffer.popleft()

    def due(self) -> bool:
        if self._inflight:
            return False
        last = self._last_started
        return last is None or self._now() - last >= self.config.interval_s

    # -- scoring ------------------------------------------------------------
    def _lot_for(self, at: float) -> dict[str, str]:
        for reading in reversed(self._buffer):
            if reading.get("sensor_name") == TEMP_SENSOR:
                found = lot_fields(_metadata(reading))
                if found:
                    return found
                break
        return lot_context_at(at)

    def _model_identity(self) -> tuple[str, str]:
        cached = self._model
        if cached is not None and self._now() - cached[2] < _VERSION_TTL_S:
            return cached[0], cached[1]
        name, version = self.config.model_name or None, None
        try:
            served_name, version = self._served_model(self.config.endpoint_name)
            name = served_name or name
        except Exception as exc:
            logger.warning(
                "risk endpoint %s: served model lookup failed: %s", self.config.endpoint_name, exc
            )
        identity = (name or "unknown", str(version) if version else "unknown")
        self._model = (*identity, self._now())
        return identity

    async def score_once(self) -> RiskScore | None:
        """Score the latest full window, or ``None`` (no window, breaker open, error)."""
        self._last_started = self._now()
        features = build_features(
            self._buffer, limit_c=self.limit_c, last_maintenance_at=self.last_maintenance_at
        )
        if features is None:
            return None
        if not self.breaker.allow():
            self.stats["skipped_open"] = self.stats.get("skipped_open", 0) + 1
            return None
        self._inflight = True
        self.stats["calls"] += 1
        try:
            raw = await asyncio.wait_for(
                asyncio.to_thread(self._query, self.config.endpoint_name, features),
                timeout=self.config.timeout_s,
            )
            score, top = parse_prediction(raw)
        except Exception as exc:
            self.breaker.record_failure()
            self.stats["failures"] += 1
            self.stats["last_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
            logger.warning(
                "risk endpoint %s failed (%s, breaker=%s)",
                self.config.endpoint_name,
                self.stats["last_error"],
                self.breaker.state,
            )
            return None
        finally:
            self._inflight = False
        self.breaker.record_success()
        name, version = await asyncio.to_thread(self._model_identity)
        at = max(reading_epoch(r) or 0.0 for r in self._buffer)
        self.stats["last_score"] = score
        return RiskScore(score, top, name, version, at, self._lot_for(at))


def _metadata(reading: Mapping[str, Any]) -> dict[str, Any]:
    raw = reading.get("metadata")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def scorer_for(state: Any) -> SpindleRiskScorer | None:
    """The process-wide scorer, built from env on first use (``False`` = disabled)."""
    scorer = getattr(state, "risk_scorer", None)
    if isinstance(scorer, SpindleRiskScorer):
        return scorer
    if scorer is None:
        config = RiskScorerConfig.from_env()
        built = SpindleRiskScorer(config) if config else None
        state.risk_scorer = built if built is not None else False
        return built
    return None


async def score_and_emit(
    state: Any, scorer: SpindleRiskScorer, pipeline_id: str | None
) -> list[Any]:
    """One scoring round: query the endpoint and feed the alert engine."""
    from iot_twin.services.alerts.alert_engine import engine_for
    from iot_twin.services.alerts.alert_snapshot_sync import mark_snapshot_dirty
    from iot_twin.services.alerts.alert_sse import build_lifecycle_payload, enqueue_alert_events

    result = await scorer.score_once()
    if result is None:
        return []
    db = state.app_backend_db if getattr(state, "app_backend_available", False) else None
    transitions = await engine_for(state).ingest_model_scores(
        [result.as_ingest(scorer.config, pipeline_id)], db=db
    )
    enqueue_alert_events(
        state,
        [build_lifecycle_payload(t.event_type, t.alert, t.extra) for t in transitions],
    )
    if transitions:
        mark_snapshot_dirty(state)
    return transitions


async def _guarded(state: Any, scorer: SpindleRiskScorer, pipeline_id: str | None) -> list[Any]:
    try:
        return await score_and_emit(state, scorer, pipeline_id)
    except Exception:
        logger.exception("risk scoring round failed")
        return []


def observe_and_score(
    state: Any,
    readings: Sequence[Mapping[str, Any]] | None,
    *,
    industry: str | None,
    pipeline_id: str | None,
) -> asyncio.Task | None:
    """Buffer readings and, when due, start a background scoring round."""
    if not industry or to_record_industry(str(industry)) != INDUSTRY:
        return None
    scorer = scorer_for(state)
    if scorer is None or not scorer.observe(readings) or not scorer.due():
        return None
    scorer._last_started = scorer._now()
    task = asyncio.get_running_loop().create_task(_guarded(state, scorer, pipeline_id))
    scorer.tasks.add(task)
    task.add_done_callback(scorer.tasks.discard)
    return task
