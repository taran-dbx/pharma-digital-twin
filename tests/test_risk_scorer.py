"""App-side thermal-risk scorer with a mocked serving endpoint (offline) [qa]."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from types import SimpleNamespace

import pytest

from iot_twin.core.state import AppState
from iot_twin.ml.risk_contract import MODEL_RULE_ID
from iot_twin.ml.spindle_features import FEATURE_NAMES
from iot_twin.services.alerts.alert_engine import engine_for, evaluate_and_emit
from iot_twin.services.ml import risk_scorer as rs
from iot_twin.services.ml.circuit_breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker

T0 = 1_800_000_000.0
LOT = {"lot_id": "TM-HS12-270115-007", "part_serial": "TM-HS12-270115-007-04"}


def _readings(seconds: int = 30, start: float = T0, temp: float = 55.0) -> list[dict]:
    out: list[dict] = []
    for i in range(seconds):
        t = start + i
        out += [
            {
                "sensor_name": "cnc_1_spindle_temp",
                "value": temp + i * 0.1,
                "timestamp": t,
                "industry": "manufacturing",
                "metadata": LOT,
            },
            {"sensor_name": "cnc_1_vibration", "value": 2.5, "timestamp": t},
            {"sensor_name": "cnc_1_spindle_power", "value": 20.0, "timestamp": t},
            {"sensor_name": "cnc_1_spindle_speed", "value": 3500.0, "timestamp": t},
        ]
    return out


class FakeEndpoint:
    def __init__(self, score: float = 0.83, fail: Exception | None = None, delay: float = 0.0):
        self.score = score
        self.fail = fail
        self.delay = delay
        self.records: list[dict] = []
        self.timeouts: list[float] = []
        self.threads: list[str] = []

    def __call__(self, endpoint: str, record, timeout_s: float):
        self.records.append(dict(record))
        self.timeouts.append(timeout_s)
        self.threads.append(threading.current_thread().name)
        if self.delay:
            time.sleep(self.delay)
        if self.fail:
            raise self.fail
        top = [{"feature": "temp_slope", "contribution": 0.9, "value": 6.0}]
        return [
            {
                "risk_score": self.score,
                "top_features": json.dumps(top),
                "decision_threshold": 0.22,
                "clear_threshold": 0.17,
            }
        ]


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _config(**overrides) -> rs.RiskScorerConfig:
    values = {"endpoint_name": "pharma-twin-spindle-risk", "model_name": "cat.pharma_twin.m"}
    values.update(overrides)
    return rs.RiskScorerConfig(**values)


def _scorer(endpoint: FakeEndpoint, clock: Clock | None = None, **config) -> rs.SpindleRiskScorer:
    return rs.SpindleRiskScorer(
        _config(**config),
        query_fn=endpoint,
        served_model_fn=lambda name, timeout: ("cat.pharma_twin.spindle_thermal_risk", "2"),
        champion_fn=lambda name, timeout: "2",
        now_fn=clock or Clock(),
        limit_c=60.0,
        last_maintenance_at=T0 - 3600,
    )


class TestConfig:
    def test_disabled_without_endpoint(self, monkeypatch) -> None:
        monkeypatch.delenv("RISK_ENDPOINT_NAME", raising=False)
        assert rs.RiskScorerConfig.from_env() is None

    def test_reads_env_and_rejects_bad_values(self, monkeypatch) -> None:
        monkeypatch.setenv("RISK_ENDPOINT_NAME", "pharma-twin-spindle-risk")
        monkeypatch.setenv("RISK_MODEL_NAME", "cat.pharma_twin.spindle_thermal_risk")
        monkeypatch.setenv("RISK_ALERT_THRESHOLD", "0.35")
        monkeypatch.setenv("RISK_QUERY_TIMEOUT_S", "abc")
        monkeypatch.setenv("RISK_ALERT_HYSTERESIS", "2")
        config = rs.RiskScorerConfig.from_env()
        assert config is not None
        assert config.threshold == 0.35
        assert config.timeout_s == 3.0  # not a number → default
        assert config.hysteresis is None  # out of range → the model's clear level
        assert config.model_name == "cat.pharma_twin.spindle_thermal_risk"

    def test_model_keyword_defers_to_the_served_model(self, monkeypatch) -> None:
        monkeypatch.setenv("RISK_ENDPOINT_NAME", "pharma-twin-spindle-risk")
        monkeypatch.setenv("RISK_ALERT_THRESHOLD", "model")
        config = rs.RiskScorerConfig.from_env()
        assert config is not None and config.threshold is None

    def test_threshold_precedence(self) -> None:
        score = rs.RiskScore(0.5, (), "m", "2", T0, decision_threshold=0.3, clear_threshold=0.24)
        assert score.thresholds(_config()) == (0.3, 0.06)  # the model's
        assert score.thresholds(_config(threshold=0.5, hysteresis=0.1)) == (0.5, 0.1)  # env
        bare = rs.RiskScore(0.5, (), "m", "2", T0)
        assert bare.thresholds(_config()) == (0.47, 0.05)  # contract defaults

    def test_sdk_client_uses_the_query_timeout_for_http_and_retries(self, monkeypatch) -> None:
        import databricks.sdk as sdk
        import databricks.sdk.config as sdk_config

        seen: dict = {}

        class FakeConfig:
            def __init__(self, **kwargs):
                seen.update(kwargs)

        class FakeClient:
            def __init__(self, config=None):
                seen["config"] = config

        monkeypatch.setattr(sdk_config, "Config", FakeConfig)
        monkeypatch.setattr(sdk, "WorkspaceClient", FakeClient)
        monkeypatch.setattr(rs, "_clients", {})
        client = rs._client(2.2)
        assert seen["http_timeout_seconds"] == 3 and seen["retry_timeout_seconds"] == 3
        assert rs._client(2.9) is client  # cached per timeout


class TestParse:
    def test_record_with_json_top_features_and_thresholds(self) -> None:
        parsed = rs.parse_prediction(
            {
                "predictions": [
                    {
                        "risk_score": 0.4,
                        "top_features": '[{"feature": "x"}]',
                        "decision_threshold": 0.22,
                        "clear_threshold": 0.17,
                    }
                ]
            }
        )
        assert parsed == rs.Prediction(0.4, ({"feature": "x"},), 0.22, 0.17)

    def test_bare_scores_and_list_top_features(self) -> None:
        assert rs.parse_prediction([0.2]) == rs.Prediction(0.2)
        parsed = rs.parse_prediction([{"risk_score": "0.9", "top_features": [{"a": 1}, 3]}])
        assert parsed.risk_score == 0.9 and parsed.top_features == ({"a": 1},)
        assert parsed.decision_threshold is None

    @pytest.mark.parametrize(
        "bad", [[], None, "x", [{"risk_score": "nan?"}], [{"risk_score": 1.5}], {"predictions": []}]
    )
    def test_rejects_non_predictions(self, bad) -> None:
        with pytest.raises(rs.EndpointResponseError):
            rs.parse_prediction(bad)


class TestBreaker:
    def test_opens_then_half_opens_then_closes(self) -> None:
        clock = Clock()
        breaker = CircuitBreaker(2, 30.0, clock)
        breaker.record_failure()
        assert breaker.state == CLOSED and breaker.allow()
        breaker.record_failure()
        assert breaker.state == OPEN and not breaker.allow()
        clock.now += 30
        assert breaker.allow() and breaker.state == HALF_OPEN
        assert not breaker.allow()  # one trial at a time
        breaker.record_failure()
        assert breaker.state == OPEN
        clock.now += 30
        assert breaker.allow()
        breaker.record_success()
        assert breaker.state == CLOSED and breaker.consecutive_failures == 0


@pytest.mark.asyncio
class TestScorer:
    async def test_scores_with_the_shared_feature_row(self) -> None:
        endpoint = FakeEndpoint()
        scorer = _scorer(endpoint)
        assert scorer.observe(_readings()) == 120
        result = await scorer.score_once()
        assert result is not None
        assert result.risk_score == 0.83
        assert result.model_name == "cat.pharma_twin.spindle_thermal_risk"
        assert result.model_version == "2"
        assert result.lot["lot_id"] == LOT["lot_id"]
        assert list(endpoint.records[0]) == list(FEATURE_NAMES)
        from iot_twin.ml.spindle_features import build_features

        expected = build_features(_readings(), limit_c=60.0, last_maintenance_at=T0 - 3600)
        assert endpoint.records[0] == expected

    async def test_no_call_without_a_full_window(self) -> None:
        endpoint = FakeEndpoint()
        scorer = _scorer(endpoint)
        scorer.observe(_readings(2))
        assert await scorer.score_once() is None
        assert endpoint.records == []

    async def test_ignores_other_sensors_and_prunes_old_readings(self) -> None:
        scorer = _scorer(FakeEndpoint())
        assert (
            scorer.observe([{"sensor_name": "hvac_zone_1_temp", "value": 1, "timestamp": T0}]) == 0
        )
        scorer.observe(_readings(10, start=T0))
        scorer.observe(_readings(10, start=T0 + 1000))
        assert min(r["timestamp"] for r in scorer.buffer(None)) >= T0 + 1000 - 120

    async def test_errors_open_the_breaker(self) -> None:
        clock = Clock()
        endpoint = FakeEndpoint(fail=RuntimeError("503 endpoint scaling from zero"))
        scorer = _scorer(endpoint, clock, breaker_failures=2)
        scorer.observe(_readings())
        assert await scorer.score_once() is None
        assert await scorer.score_once() is None
        assert scorer.breaker.state == OPEN
        calls = len(endpoint.records)
        assert await scorer.score_once() is None
        assert len(endpoint.records) == calls  # breaker open: no call
        clock.now += scorer.config.breaker_cooldown_s
        endpoint.fail = None
        assert (await scorer.score_once()) is not None
        assert scorer.breaker.state == CLOSED

    async def test_hung_call_keeps_the_scorer_busy_until_the_thread_returns(self) -> None:
        release = threading.Event()

        def hung(endpoint, record, timeout_s):
            release.wait(5)
            return [{"risk_score": 0.5}]

        scorer = rs.SpindleRiskScorer(
            _config(timeout_s=0.05),
            query_fn=hung,
            served_model_fn=lambda name, timeout: ("m", "2"),
            champion_fn=lambda name, timeout: "2",
            now_fn=Clock(),
            limit_c=60.0,
            last_maintenance_at=T0,
        )
        scorer.observe(_readings())
        assert await scorer.score_once() is None
        assert "TimeoutError" in scorer.stats["last_error"]
        # The await timed out but the worker thread is still running.
        assert scorer.inflight and not scorer.due()
        assert await scorer.score_once() is None
        assert scorer.stats["skipped_busy"] == 1
        # The default to_thread pool stays free while the scorer is stuck.
        assert await asyncio.wait_for(asyncio.to_thread(lambda: "free"), 1) == "free"
        release.set()
        for _ in range(100):
            if not scorer.inflight:
                break
            await asyncio.sleep(0.01)
        assert not scorer.inflight
        scorer.close()

    async def test_busy_flag_is_clear_when_a_call_returns(self) -> None:
        """Deterministic on every Python: no awaiting task sees a stale busy flag."""
        endpoint = FakeEndpoint(fail=RuntimeError("503"))
        scorer = _scorer(endpoint, breaker_failures=10)  # keep the breaker closed
        scorer.observe(_readings())
        for _ in range(3):
            assert await scorer.score_once() is None
            assert not scorer.inflight  # cleared before score_once resumed
        assert len(endpoint.records) == 3
        assert "skipped_busy" not in scorer.stats
        endpoint.fail = None
        assert await scorer.score_once() is not None
        assert not scorer.inflight

    async def test_calls_run_on_the_dedicated_executor_with_the_timeout(self) -> None:
        endpoint = FakeEndpoint()
        scorer = _scorer(endpoint, timeout_s=2.5)
        scorer.observe(_readings())
        assert await scorer.score_once() is not None
        assert endpoint.threads[0].startswith("risk-scorer")
        assert endpoint.timeouts == [2.5]

    async def test_pipelines_have_separate_buffers_and_timers(self) -> None:
        clock = Clock()
        endpoint = FakeEndpoint()
        scorer = _scorer(endpoint, clock, interval_s=5.0)
        hot = _readings(temp=58.0)
        calm = _readings(temp=40.0)
        # Interleave the two pipelines' batches.
        for i in range(0, len(hot), 8):
            scorer.observe(hot[i : i + 8], "p-hot")
            scorer.observe(calm[i : i + 8], "p-calm")
        assert {r["value"] for r in scorer.buffer("p-hot") if "temp" in r["sensor_name"]} == {
            r["value"] for r in hot if "temp" in r["sensor_name"]
        }
        hot_score = await scorer.score_once("p-hot")
        assert not scorer.due("p-hot") and scorer.due("p-calm")
        calm_score = await scorer.score_once("p-calm")
        assert hot_score is not None and calm_score is not None
        assert hot_score.pipeline_id == "p-hot" and calm_score.pipeline_id == "p-calm"
        assert endpoint.records[0]["temp_last"] == pytest.approx(58.0 + 2.9)
        assert endpoint.records[1]["temp_last"] == pytest.approx(40.0 + 2.9)

    async def test_warns_when_the_endpoint_is_not_on_the_champion(self, caplog) -> None:
        scorer = rs.SpindleRiskScorer(
            _config(),
            query_fn=FakeEndpoint(),
            served_model_fn=lambda name, timeout: ("cat.pharma_twin.spindle_thermal_risk", "1"),
            champion_fn=lambda name, timeout: "3",
            now_fn=Clock(),
            limit_c=60.0,
            last_maintenance_at=T0,
        )
        scorer.observe(_readings())
        with caplog.at_level("WARNING"):
            result = await scorer.score_once()
        assert result is not None and result.model_version == "1"
        assert "risk_model_version=3" in caplog.text
        assert scorer.stats["champion_version"] == "3"

    async def test_bad_response_counts_as_failure(self) -> None:
        scorer = _scorer(FakeEndpoint(score=7.0))
        scorer.observe(_readings())
        assert await scorer.score_once() is None
        assert scorer.stats["failures"] == 1

    async def test_model_lookup_failure_falls_back_to_configured_name(self) -> None:
        def broken(name: str, timeout_s: float):
            raise RuntimeError("no CAN_VIEW")

        scorer = rs.SpindleRiskScorer(
            _config(),
            query_fn=FakeEndpoint(),
            served_model_fn=broken,
            now_fn=Clock(),
            limit_c=60.0,
            last_maintenance_at=None,
        )
        scorer.observe(_readings())
        result = await scorer.score_once()
        assert result is not None
        assert (result.model_name, result.model_version) == ("cat.pharma_twin.m", "unknown")

    async def test_interval_throttles_rounds(self) -> None:
        clock = Clock()
        scorer = _scorer(FakeEndpoint(), clock, interval_s=5.0)
        assert scorer.due()
        scorer.observe(_readings())
        await scorer.score_once()
        assert not scorer.due()
        clock.now += 5
        assert scorer.due()


def _state_with(scorer) -> AppState:
    state = AppState()
    state.risk_scorer = scorer
    return state


@pytest.mark.asyncio
class TestEndToEnd:
    async def test_readings_through_evaluate_and_emit_raise_a_predicted_alert(self) -> None:
        scorer = _scorer(FakeEndpoint(score=0.91), threshold=0.6)
        state = _state_with(scorer)
        queue: asyncio.Queue = asyncio.Queue()
        state.sse_clients.append(queue)
        await evaluate_and_emit(state, _readings(), industry="manufacturing", pipeline_id="p1")
        assert scorer.tasks
        await asyncio.gather(*list(scorer.tasks))
        alerts = await engine_for(state).list_alerts()
        predicted = [a for a in alerts if a.rule_id == MODEL_RULE_ID]
        assert len(predicted) == 1
        meta = predicted[0].metadata
        assert meta["alert_kind"] == "predicted" and meta["risk_score"] == 0.91
        assert meta["model_version"] == "2" and meta["lot_id"] == LOT["lot_id"]
        assert meta["top_features"][0]["feature"] == "temp_slope"
        events = [queue.get_nowait() for _ in range(queue.qsize())]
        opened = [e for e in events if e.get("rule_id") == MODEL_RULE_ID]
        assert opened and opened[0]["type"] == "alert_opened"
        assert opened[0]["alert_kind"] == "predicted" and opened[0]["risk_score"] == 0.91

    async def test_low_risk_raises_nothing(self) -> None:
        scorer = _scorer(FakeEndpoint(score=0.1))
        state = _state_with(scorer)
        task = rs.observe_and_score(state, _readings(), industry="manufacturing", pipeline_id=None)
        assert task is not None
        assert await task == []

    async def test_other_industries_and_disabled_scorer_are_ignored(self, monkeypatch) -> None:
        scorer = _scorer(FakeEndpoint())
        state = _state_with(scorer)
        assert rs.observe_and_score(state, _readings(), industry="energy", pipeline_id=None) is None
        assert not scorer.buffer(None)
        monkeypatch.delenv("RISK_ENDPOINT_NAME", raising=False)
        empty = AppState()
        assert (
            rs.observe_and_score(empty, _readings(), industry="manufacturing", pipeline_id=None)
            is None
        )
        assert empty.risk_scorer is False
        mocked = SimpleNamespace(risk_scorer=object())
        assert rs.scorer_for(mocked) is None

    async def test_scoring_round_failure_is_contained(self) -> None:
        scorer = _scorer(FakeEndpoint())
        state = _state_with(scorer)

        async def boom(*args, **kwargs):
            raise RuntimeError("engine down")

        state.alert_engine = SimpleNamespace(ingest_model_scores=boom)
        task = rs.observe_and_score(state, _readings(), industry="manufacturing", pipeline_id=None)
        assert task is not None
        assert await task == []


@pytest.mark.asyncio
class TestChampionGuards:
    def _scorer(self, served: str, champion) -> rs.SpindleRiskScorer:
        def champion_fn(name, timeout):
            if isinstance(champion, Exception):
                raise champion
            return champion

        return rs.SpindleRiskScorer(
            _config(),
            query_fn=FakeEndpoint(score=0.5),
            served_model_fn=lambda name, timeout: ("cat.pharma_twin.spindle_thermal_risk", served),
            champion_fn=champion_fn,
            now_fn=Clock(),
            limit_c=60.0,
            last_maintenance_at=T0,
        )

    async def test_off_champion_thresholds_are_ignored_with_a_named_warning(self, caplog) -> None:
        scorer = self._scorer("1", "4")
        scorer.observe(_readings())
        with caplog.at_level("WARNING"):
            result = await scorer.score_once()
        assert result is not None and scorer.off_champion
        # The endpoint returned 0.22 / 0.17; they are dropped for the contract defaults.
        assert result.decision_threshold is None and result.clear_threshold is None
        assert result.thresholds(_config()) == (0.47, 0.05)
        assert "ignoring its cut-off 0.22 / clear 0.17" in caplog.text
        assert result.as_ingest(_config(threshold=0.3))["threshold"] == 0.3  # env still wins

    async def test_champion_thresholds_are_used(self) -> None:
        scorer = self._scorer("4", "4")
        scorer.observe(_readings())
        result = await scorer.score_once()
        assert result is not None and not scorer.off_champion
        assert (result.decision_threshold, result.clear_threshold) == (0.22, 0.17)

    async def test_lookup_failure_warns_once(self, caplog) -> None:
        scorer = self._scorer("4", PermissionError("no EXECUTE on the model"))
        scorer.observe(_readings())
        with caplog.at_level("WARNING"):
            assert await scorer.score_once() is not None
            scorer._model = None  # force the next round to look up again
            assert await scorer.score_once() is not None
        assert caplog.text.count("served-vs-champion check is off") == 1
        assert "EXECUTE" in caplog.text


@pytest.mark.asyncio
class TestRoundIsolation:
    async def test_second_pipeline_cannot_slip_in_between_query_and_lookup(self) -> None:
        """Regression: p1's query thread finished and freed the worker before p1
        resumed; p2's round started a (hung) call in that gap, p1's served-model
        lookup queued behind it, timed out and cached version 'unknown'."""
        loop = asyncio.get_running_loop()
        release = threading.Event()
        rounds: dict[str, asyncio.Task] = {}
        scorer: rs.SpindleRiskScorer

        def query(endpoint, record, timeout_s):
            if record["temp_last"] > 50:  # p1 ("p-hot"): fast, but opens the gap
                loop.call_soon_threadsafe(
                    lambda: rounds.setdefault("p2", loop.create_task(scorer.score_once("p-calm")))
                )
                return [{"risk_score": 0.6}]
            release.wait(5)  # p2: a hung endpoint call
            return [{"risk_score": 0.1}]

        scorer = rs.SpindleRiskScorer(
            _config(timeout_s=0.2),
            query_fn=query,
            served_model_fn=lambda name, timeout: ("cat.pharma_twin.spindle_thermal_risk", "4"),
            champion_fn=lambda name, timeout: "4",
            now_fn=Clock(),
            limit_c=60.0,
            last_maintenance_at=T0,
        )
        scorer.observe(_readings(temp=58.0), "p-hot")
        scorer.observe(_readings(temp=40.0), "p-calm")
        try:
            first = await scorer.score_once("p-hot")
            second = await rounds["p2"]
        finally:
            release.set()
        assert first is not None
        assert first.model_version == "4"  # not 'unknown' from a starved lookup
        assert second is None and scorer.stats["skipped_busy"] == 1
        for _ in range(100):
            if not scorer.inflight:
                break
            await asyncio.sleep(0.01)
        assert not scorer.inflight
        scorer.close()

    async def test_unknown_identity_is_only_cached_briefly(self) -> None:
        clock = Clock()
        lookups: list[int] = []

        def served(name, timeout):
            lookups.append(1)
            if len(lookups) == 1:
                raise RuntimeError("endpoint metadata unavailable")
            return "cat.pharma_twin.spindle_thermal_risk", "4"

        scorer = rs.SpindleRiskScorer(
            _config(interval_s=1.0),
            query_fn=FakeEndpoint(),
            served_model_fn=served,
            champion_fn=lambda name, timeout: "4",
            now_fn=clock,
            limit_c=60.0,
            last_maintenance_at=T0,
        )
        scorer.observe(_readings())
        first = await scorer.score_once()
        assert first is not None and first.model_version == "unknown"
        clock.now += rs.UNKNOWN_IDENTITY_TTL_S - 1
        assert (await scorer.score_once()).model_version == "unknown"  # still cached
        clock.now += 2
        assert (await scorer.score_once()).model_version == "4"  # looked up again
        assert len(lookups) == 2
        clock.now += rs.UNKNOWN_IDENTITY_TTL_S + 1
        await scorer.score_once()
        assert len(lookups) == 2  # a known identity keeps the long TTL
