"""Score live Spindle 3 readings with the thermal-risk model and raise predicted alerts.

Readings reach the scorer through ``evaluate_and_emit`` (the same call that runs
the threshold rules). The scorer keeps one *lane* per pipeline: a short buffer
of that pipeline's spindle readings and its own scoring timer, so pipelines
never mix. It builds features with ``iot_twin.ml.spindle_features`` (the code the
training job uses), and at most every ``interval_s`` per lane it queries the
``pharma-twin-spindle-risk`` serving endpoint.

A cold (scale-to-zero) or hung endpoint must never starve the app:

* calls run on the scorer's own single-worker executor, never the default
  ``asyncio.to_thread`` pool that Lakebase credential refresh uses;
* the scorer's ``WorkspaceClient`` has ``http_timeout_seconds`` and
  ``retry_timeout_seconds`` equal to the query timeout;
* one call at a time: the scorer stays busy until the worker thread actually
  returns, even after the await has timed out;
* a circuit breaker stops calls after repeated failures.

The served model returns its own ``decision_threshold`` / ``clear_threshold``;
``RISK_ALERT_THRESHOLD`` / ``RISK_ALERT_HYSTERESIS`` override them. Scores go
to ``AlertEngine.ingest_model_scores``, which owns fingerprint and hysteresis.
Disabled when ``RISK_ENDPOINT_NAME`` is unset.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import os
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from iot_twin.domain.catalog import to_record_industry
from iot_twin.domain.lot_context import lot_context_at, lot_fields
from iot_twin.ml.risk_contract import CHAMPION_ALIAS, DEFAULT_HYSTERESIS, DEFAULT_THRESHOLD
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
# RISK_ALERT_THRESHOLD=model (or unset) uses the served model's cut-off.
MODEL_THRESHOLD = "model"
_BUFFER_MAX = 10_000
_MAX_LANES = 32
_VERSION_TTL_S = 300.0


def _env_float(name: str, default: float | None, low: float, high: float) -> float | None:
    raw = os.getenv(name, "").strip()
    if not raw or raw.lower() == MODEL_THRESHOLD:
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


def _env_number(name: str, default: float, low: float, high: float) -> float:
    value = _env_float(name, default, low, high)
    return default if value is None else value


@dataclass(frozen=True)
class RiskScorerConfig:
    endpoint_name: str
    model_name: str
    # None = use the served model's decision_threshold / clear level.
    threshold: float | None = None
    hysteresis: float | None = None
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
            threshold=_env_float("RISK_ALERT_THRESHOLD", None, 0.01, 0.99),
            hysteresis=_env_float("RISK_ALERT_HYSTERESIS", None, 0.0, 0.5),
            interval_s=_env_number("RISK_SCORE_INTERVAL_S", 5.0, 1.0, 3600.0),
            timeout_s=_env_number("RISK_QUERY_TIMEOUT_S", 3.0, 0.1, 60.0),
            breaker_failures=int(_env_number("RISK_BREAKER_FAILURES", 3, 1, 100)),
            breaker_cooldown_s=_env_number("RISK_BREAKER_COOLDOWN_S", 60.0, 1.0, 3600.0),
            horizon_s=_env_number("RISK_HORIZON_S", DEFAULT_HORIZON_S, 1.0, 3600.0),
        )


@dataclass(frozen=True)
class Prediction:
    risk_score: float
    top_features: tuple[dict[str, Any], ...] = ()
    decision_threshold: float | None = None
    clear_threshold: float | None = None


@dataclass(frozen=True)
class RiskScore:
    risk_score: float
    top_features: tuple[Mapping[str, Any], ...]
    model_name: str
    model_version: str
    at: float
    lot: Mapping[str, str] = field(default_factory=dict)
    pipeline_id: str | None = None
    decision_threshold: float | None = None
    clear_threshold: float | None = None

    def thresholds(self, config: RiskScorerConfig) -> tuple[float, float]:
        """``(cut-off, hysteresis)``: env override, else the model's, else defaults."""
        cutoff = config.threshold
        if cutoff is None:
            cutoff = self.decision_threshold or DEFAULT_THRESHOLD
        hysteresis = config.hysteresis
        if hysteresis is None:
            if self.clear_threshold is not None and self.clear_threshold < cutoff:
                hysteresis = cutoff - self.clear_threshold
            else:
                hysteresis = DEFAULT_HYSTERESIS
        return float(cutoff), round(float(hysteresis), 4)

    def as_ingest(self, config: RiskScorerConfig) -> dict[str, Any]:
        """The payload ``AlertEngine.ingest_model_scores`` accepts."""
        cutoff, hysteresis = self.thresholds(config)
        return {
            "industry": INDUSTRY,
            "pipeline_id": self.pipeline_id,
            "risk_score": self.risk_score,
            "threshold": cutoff,
            "hysteresis": hysteresis,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "top_features": [dict(item) for item in self.top_features],
            "horizon_s": config.horizon_s,
            "scored_at": datetime.fromtimestamp(self.at, UTC).isoformat(),
            "lot": dict(self.lot),
        }


class EndpointResponseError(ValueError):
    """The endpoint answered with something that is not a risk prediction."""


def _optional_unit(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if 0.0 < number < 1.0 else None


def parse_prediction(predictions: Any) -> Prediction:
    """The first prediction of a serving response."""
    if isinstance(predictions, Mapping) and "predictions" in predictions:
        predictions = predictions["predictions"]
    if isinstance(predictions, Sequence) and not isinstance(predictions, str) and predictions:
        first = predictions[0]
    else:
        raise EndpointResponseError("empty predictions")
    record: Mapping[str, Any] = first if isinstance(first, Mapping) else {"risk_score": first}
    raw_score = record.get("risk_score")
    try:
        score = float(raw_score)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise EndpointResponseError(f"risk_score is not numeric: {raw_score!r}") from exc
    if not 0.0 <= score <= 1.0:
        raise EndpointResponseError(f"risk_score outside [0, 1]: {score}")
    raw_top = record.get("top_features")
    if isinstance(raw_top, str):
        try:
            raw_top = json.loads(raw_top)
        except json.JSONDecodeError:
            raw_top = []
    top = tuple(dict(item) for item in raw_top or () if isinstance(item, Mapping))
    return Prediction(
        risk_score=score,
        top_features=top,
        decision_threshold=_optional_unit(record.get("decision_threshold")),
        clear_threshold=_optional_unit(record.get("clear_threshold")),
    )


_clients: dict[int, Any] = {}


def _client(timeout_s: float) -> Any:
    """A WorkspaceClient whose HTTP timeout and retry budget equal ``timeout_s``."""
    seconds = max(1, math.ceil(timeout_s))
    if seconds not in _clients:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config

        _clients[seconds] = WorkspaceClient(
            config=Config(http_timeout_seconds=seconds, retry_timeout_seconds=seconds)
        )
    return _clients[seconds]


def sdk_query(endpoint: str, record: Mapping[str, float], timeout_s: float) -> Any:
    """Blocking ``serving_endpoints.query`` with one dataframe record."""
    response = _client(timeout_s).serving_endpoints.query(
        name=endpoint, dataframe_records=[dict(record)]
    )
    return response.predictions


def sdk_served_model(endpoint: str, timeout_s: float) -> tuple[str | None, str | None]:
    """UC model name and version the endpoint currently serves."""
    found = _client(timeout_s).serving_endpoints.get(endpoint)
    entities = (found.config.served_entities if found.config else None) or []
    if not entities:
        return None, None
    return entities[0].entity_name, entities[0].entity_version


def sdk_champion_version(model_name: str, timeout_s: float) -> str | None:
    """Version behind the ``champion`` alias (needs read access on the UC model)."""
    found = _client(timeout_s).model_versions.get_by_alias(model_name, CHAMPION_ALIAS)
    return None if found.version is None else str(found.version)


@dataclass
class _Lane:
    """Per-pipeline buffer and scoring timer."""

    buffer: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=_BUFFER_MAX))
    last_started: float | None = None
    last_seen: float = 0.0


def _lane_key(pipeline_id: str | None) -> str:
    return "" if pipeline_id is None else str(pipeline_id)


class SpindleRiskScorer:
    def __init__(
        self,
        config: RiskScorerConfig,
        *,
        query_fn: Callable[[str, Mapping[str, float], float], Any] = sdk_query,
        served_model_fn: Callable[[str, float], tuple[str | None, str | None]] = sdk_served_model,
        champion_fn: Callable[[str, float], str | None] = sdk_champion_version,
        now_fn: Callable[[], float] = time.monotonic,
        limit_c: float | None = None,
        last_maintenance_at: float | None = None,
    ) -> None:
        self.config = config
        self._query = query_fn
        self._served_model = served_model_fn
        self._champion = champion_fn
        self._now = now_fn
        self.breaker = CircuitBreaker(config.breaker_failures, config.breaker_cooldown_s, now_fn)
        self.limit_c = validated_limit_c() if limit_c is None else limit_c
        self.last_maintenance_at = (
            last_spindle_maintenance_epoch() if last_maintenance_at is None else last_maintenance_at
        )
        self._lanes: dict[str, _Lane] = {}
        # One worker: a hung call can block only the scorer, never the app.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="risk-scorer")
        self.inflight = False
        self._model: tuple[str, str, float] | None = None
        self._warned_version: str | None = None
        self._warned_lookup = False
        self._warned_thresholds: str | None = None
        # True while the endpoint serves a version other than @champion.
        self.off_champion = False
        self.tasks: set[asyncio.Task] = set()
        self.stats: dict[str, Any] = {"calls": 0, "failures": 0, "last_score": None}

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    # -- lanes / buffers ----------------------------------------------------
    def _lane(self, pipeline_id: str | None) -> _Lane:
        key = _lane_key(pipeline_id)
        lane = self._lanes.get(key)
        if lane is None:
            if len(self._lanes) >= _MAX_LANES:
                stale = min(self._lanes, key=lambda k: self._lanes[k].last_seen)
                self._lanes.pop(stale)
            lane = self._lanes[key] = _Lane()
        lane.last_seen = self._now()
        return lane

    def buffer(self, pipeline_id: str | None) -> list[dict[str, Any]]:
        lane = self._lanes.get(_lane_key(pipeline_id))
        return list(lane.buffer) if lane else []

    def observe(
        self, readings: Sequence[Mapping[str, Any]] | None, pipeline_id: str | None = None
    ) -> int:
        """Buffer this pipeline's spindle readings; returns how many were added."""
        wanted = [
            dict(reading)
            for reading in readings or ()
            if isinstance(reading, Mapping)
            and str(reading.get("sensor_name") or "") in SPINDLE_SENSOR_NAMES
            and reading_epoch(reading) is not None
        ]
        if not wanted:
            return 0
        lane = self._lane(pipeline_id)
        lane.buffer.extend(wanted)
        latest = max((reading_epoch(r) or 0.0) for r in lane.buffer)
        horizon = latest - self.config.window_s * 2
        while lane.buffer and (reading_epoch(lane.buffer[0]) or 0.0) < horizon:
            lane.buffer.popleft()
        return len(wanted)

    def due(self, pipeline_id: str | None = None) -> bool:
        if self.inflight:
            return False
        lane = self._lanes.get(_lane_key(pipeline_id))
        last = lane.last_started if lane else None
        return last is None or self._now() - last >= self.config.interval_s

    def mark_started(self, pipeline_id: str | None) -> None:
        self._lane(pipeline_id).last_started = self._now()

    # -- endpoint calls -----------------------------------------------------
    async def _call(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Run ``fn`` on the scorer's executor; busy until the thread returns.

        ``inflight`` is cleared by the worker thread itself, in a ``finally``
        that runs before the thread hands its result back. So on the normal path
        it is already False when this coroutine resumes, on every Python version
        (no reliance on when an asyncio done-callback runs). After a timeout the
        abandoned thread keeps it True until the call really ends.
        """
        loop = asyncio.get_running_loop()
        self.inflight = True
        call = functools.partial(self._run_worker, fn, *args, self.config.timeout_s)
        future = loop.run_in_executor(self._executor, call)
        return await asyncio.wait_for(asyncio.shield(future), timeout=self.config.timeout_s)

    def _run_worker(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Worker-thread body: the call, then release the busy flag."""
        try:
            return fn(*args)
        except Exception as exc:
            logger.debug("risk endpoint call finished with %r", exc)
            raise
        finally:
            self.inflight = False

    async def _model_identity(self) -> tuple[str, str]:
        cached = self._model
        if cached is not None and self._now() - cached[2] < _VERSION_TTL_S:
            return cached[0], cached[1]
        name, version = self.config.model_name or None, None
        try:
            served_name, version = await self._call(self._served_model, self.config.endpoint_name)
            name = served_name or name
        except Exception as exc:
            logger.warning(
                "risk endpoint %s: served model lookup failed: %s", self.config.endpoint_name, exc
            )
        identity = (name or "unknown", str(version) if version else "unknown")
        self._model = (*identity, self._now())
        if name and version:
            await self._check_champion(name, str(version))
        return identity

    async def _check_champion(self, name: str, version: str) -> None:
        """Warn when the endpoint does not serve the champion (e.g. after bundle deploy)."""
        try:
            champion = await self._call(self._champion, name)
        except Exception as exc:
            if not self._warned_lookup:
                self._warned_lookup = True
                logger.warning(
                    "risk endpoint %s: cannot read @%s of %s (%s); the served-vs-champion "
                    "check is off. Grant the app service principal EXECUTE on the model "
                    "to enable it (docs/ml-risk-model.md)",
                    self.config.endpoint_name,
                    CHAMPION_ALIAS,
                    name,
                    exc,
                )
            return
        self.stats["champion_version"] = champion
        self.off_champion = bool(champion and champion != version)
        if champion and champion != version and self._warned_version != version:
            self._warned_version = version
            logger.warning(
                "risk endpoint %s serves %s v%s but @%s is v%s; redeploy with "
                "--var risk_model_version=%s or rerun the training job",
                self.config.endpoint_name,
                name,
                version,
                CHAMPION_ALIAS,
                champion,
                champion,
            )

    def _lot_for(self, lane: _Lane, at: float) -> dict[str, str]:
        for reading in reversed(lane.buffer):
            if reading.get("sensor_name") == TEMP_SENSOR:
                found = lot_fields(_metadata(reading))
                if found:
                    return found
                break
        return lot_context_at(at)

    async def score_once(self, pipeline_id: str | None = None) -> RiskScore | None:
        """Score the pipeline's latest full window, or ``None`` (no window, busy, error)."""
        lane = self._lane(pipeline_id)
        lane.last_started = self._now()
        if self.inflight:
            self.stats["skipped_busy"] = self.stats.get("skipped_busy", 0) + 1
            return None
        features = build_features(
            lane.buffer, limit_c=self.limit_c, last_maintenance_at=self.last_maintenance_at
        )
        if features is None:
            return None
        if not self.breaker.allow():
            self.stats["skipped_open"] = self.stats.get("skipped_open", 0) + 1
            return None
        self.stats["calls"] += 1
        try:
            raw = await self._call(self._query, self.config.endpoint_name, features)
            prediction = parse_prediction(raw)
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
        self.breaker.record_success()
        name, version = await self._model_identity()
        at = max(reading_epoch(r) or 0.0 for r in lane.buffer)
        self.stats["last_score"] = prediction.risk_score
        decision, clear = self._embedded_thresholds(prediction, version)
        return RiskScore(
            risk_score=prediction.risk_score,
            top_features=prediction.top_features,
            model_name=name,
            model_version=version,
            at=at,
            lot=self._lot_for(lane, at),
            pipeline_id=pipeline_id,
            decision_threshold=decision,
            clear_threshold=clear,
        )

    def _embedded_thresholds(
        self, prediction: Prediction, version: str
    ) -> tuple[float | None, float | None]:
        """The served version's cut-off / clear level, unless it is not the champion.

        Off-champion thresholds were tuned for another model version, so they are
        ignored (contract defaults apply) with one warning that names them.
        ``RISK_ALERT_THRESHOLD`` still overrides either way.
        """
        if not self.off_champion or prediction.decision_threshold is None:
            return prediction.decision_threshold, prediction.clear_threshold
        if self._warned_thresholds != version:
            self._warned_thresholds = version
            logger.warning(
                "risk endpoint %s serves v%s, not @%s v%s: ignoring its cut-off %.2f / clear "
                "%s and using %.2f / %.2f (set RISK_ALERT_THRESHOLD to pin one)",
                self.config.endpoint_name,
                version,
                CHAMPION_ALIAS,
                self.stats.get("champion_version"),
                prediction.decision_threshold,
                prediction.clear_threshold,
                DEFAULT_THRESHOLD,
                DEFAULT_THRESHOLD - DEFAULT_HYSTERESIS,
            )
        self.stats["ignored_thresholds_from"] = version
        return None, None


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
    """One scoring round for one pipeline: query the endpoint, feed the alert engine."""
    from iot_twin.services.alerts.alert_engine import engine_for
    from iot_twin.services.alerts.alert_snapshot_sync import mark_snapshot_dirty
    from iot_twin.services.alerts.alert_sse import build_lifecycle_payload, enqueue_alert_events

    result = await scorer.score_once(pipeline_id)
    if result is None:
        return []
    db = state.app_backend_db if getattr(state, "app_backend_available", False) else None
    transitions = await engine_for(state).ingest_model_scores(
        [result.as_ingest(scorer.config)], db=db
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
    """Buffer a pipeline's readings and, when its lane is due, start a scoring round."""
    if not industry or to_record_industry(str(industry)) != INDUSTRY:
        return None
    scorer = scorer_for(state)
    if scorer is None or not scorer.observe(readings, pipeline_id) or not scorer.due(pipeline_id):
        return None
    scorer.mark_started(pipeline_id)
    task = asyncio.get_running_loop().create_task(_guarded(state, scorer, pipeline_id))
    scorer.tasks.add(task)
    task.add_done_callback(scorer.tasks.discard)
    return task
