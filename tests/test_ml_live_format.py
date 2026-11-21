"""Live-format training data: the producer's code path offline, and train/serve feature parity."""

from __future__ import annotations

import json
import random
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
WHEEL = ROOT / "lib" / "iot_data_simulator-0.1.0-py3-none-any.whl"
CLIENT = ROOT / "client"
SCENARIO_TS = CLIENT / "src" / "lib" / "manufacturing-scenario.ts"
ESBUILD = CLIENT / "node_modules" / ".bin" / "esbuild"

pytestmark = pytest.mark.skipif(not WHEEL.exists(), reason="simulator wheel unavailable")
if str(WHEEL) not in sys.path:
    sys.path.insert(0, str(WHEEL))

from iot_twin.ml import live_format_telemetry as lf  # noqa: E402
from iot_twin.ml.replay import replay_rows  # noqa: E402
from iot_twin.ml.spindle_features import (  # noqa: E402
    FEATURE_NAMES,
    SPINDLE_SENSOR_NAMES,
    SpindleSeries,
    build_features,
    feature_rows,
)
from iot_twin.ml.training_set import time_split  # noqa: E402
from iot_twin.scenario_engine.spindle_scenario import apply_spindle_scenario  # noqa: E402

START = 1_796_000_100.0  # a cycle boundary is irrelevant here


@pytest.fixture(scope="module")
def readings() -> list[dict]:
    return lf.live_format_readings(start=START, duration_s=600, seed=4)


class TestLiveFormatReadings:
    def test_emits_the_four_spindle_sensors_at_the_live_rate(self, readings) -> None:
        names = {r["sensor_name"] for r in readings}
        assert names == set(SPINDLE_SENSOR_NAMES)
        per_sensor = len(readings) / len(names)
        assert per_sensor == pytest.approx(600 * lf.LIVE_RATE_HZ)
        assert all(r["metadata"]["data_origin"] == lf.DATA_ORIGIN for r in readings)

    def test_is_deterministic_for_a_seed(self) -> None:
        a = lf.live_format_readings(start=START, duration_s=20, seed=1)
        b = lf.live_format_readings(start=START, duration_s=20, seed=1)
        c = lf.live_format_readings(start=START, duration_s=20, seed=2)
        assert a == b
        assert [r["value"] for r in a] != [r["value"] for r in c]

    def test_noise_is_calibrated_like_the_producer(self, readings) -> None:
        speed = [r["value"] for r in readings if r["sensor_name"] == "cnc_1_spindle_speed"]
        assert 3000 < statistics.mean(speed) < 4000  # nominal 3500 RPM, not clamp noise
        temps = [r["value"] for r in readings if r["sensor_name"] == "cnc_1_spindle_temp"]
        assert min(temps) >= 25.0 and max(temps) <= 75.0

    def test_restores_the_simulator_clock_and_rng(self) -> None:
        from iot_simulator import sensor_models

        before = (sensor_models.time, sensor_models.random)
        lf.live_format_readings(start=START, duration_s=2, seed=1)
        assert (sensor_models.time, sensor_models.random) == before
        assert abs(sensor_models.time.time() - time.time()) < 5

    def test_rejects_non_positive_rate(self) -> None:
        with pytest.raises(ValueError):
            lf.live_format_readings(start=START, duration_s=10, rate_hz=0)


class TestFeatureParity:
    def test_training_path_equals_the_scorer_path(self, readings) -> None:
        series = SpindleSeries.from_readings(readings)
        times = [START + 120.0 + 5.0 * i for i in range(40)]
        training = dict(feature_rows(series, times, limit_c=60.0, last_maintenance_at=START - 3600))
        for at in times:
            buffer = [r for r in readings if at - 120.0 < r["timestamp"] <= at]
            served = build_features(buffer, at=at, limit_c=60.0, last_maintenance_at=START - 3600)
            assert served == training[at]
            assert list(served) == list(FEATURE_NAMES)

    def test_replay_rows_are_the_scorer_features(self, readings) -> None:
        rows = replay_rows(readings, limit_c=60.0, last_maintenance_at=START - 3600)
        assert rows[0].at == pytest.approx(START + 60.0)
        assert all(b.at - a.at == pytest.approx(5.0) for a, b in zip(rows, rows[1:], strict=False))
        sample = rows[10]
        buffer = [r for r in readings if sample.at - 120.0 < r["timestamp"] <= sample.at]
        assert sample.features == build_features(
            buffer, at=sample.at, limit_c=60.0, last_maintenance_at=START - 3600
        )


class TestTrainingSet:
    def test_rows_labels_and_breaches_with_a_time_split(self) -> None:
        ts = lf.build_live_format_training_set(
            n_runs=2,
            duration_s=900,
            seed=5,
            limit_c=60.0,
            hysteresis_c=5.0,
            rule_delay_s=2.0,
            rate_hz=5.0,
        )
        assert ts.data_origin == lf.DATA_ORIGIN
        assert set(ts.breaches_by_run) == set(ts.series_by_run)
        assert set(ts.series_by_run["live-format-5-000"].points) == {"temp"}
        assert 0 < ts.positives < len(ts.rows)
        assert {row.features["hours_since_maintenance"] for row in ts.rows} == {
            lf.PINNED_MAINTENANCE_H
        }
        onsets = sorted(o for eps in ts.breaches_by_run.values() for o, _ in eps)
        for row in ts.rows:
            ahead = any(0 < onset - row.at <= 120.0 for onset in onsets)
            assert row.label == int(ahead)
        train, test = time_split(list(ts.rows), 0.25, embargo_s=180.0)
        assert max(r.at for r in train) < min(r.at for r in test) - 180.0


@pytest.mark.skipif(
    shutil.which("node") is None or not ESBUILD.exists(), reason="node or esbuild unavailable"
)
class TestProducerParity:
    def test_rendered_producer_applies_the_same_scenario(self) -> None:
        bundle = subprocess.run(
            [str(ESBUILD), str(SCENARIO_TS), "--bundle", "--format=cjs", "--platform=node"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        script = (
            f"{bundle}\nconst m = module.exports;"
            "process.stdout.write(JSON.stringify(m.producerSignalPython('manufacturing')));"
        )
        python = json.loads(
            subprocess.run(
                ["node", "-e", script], check=True, capture_output=True, text=True
            ).stdout
        )
        import logging
        import threading

        ns: dict = {
            "json": json,
            "time": time,
            "threading": threading,
            "logger": logging.getLogger("t"),
        }
        exec(compile(python, "<producer>", "exec"), ns)
        for name in ("cnc_1_spindle_temp", "cnc_1_vibration", "cnc_1_spindle_power"):
            for offset in range(0, 300, 7):
                payload = {
                    "sensor_name": name,
                    "value": 10.0,
                    "timestamp": 1_796_000_100.0 + offset,
                    "min_value": 0.0,
                    "max_value": 75.0,
                    "metadata": {},
                }
                ns["_scenario_rng"] = random.Random(offset)
                live = ns["_apply_scenario"](dict(payload))
                shared = apply_spindle_scenario(dict(payload), random.Random(offset))
                assert live["value"] == shared["value"]
                assert live.get("fault_active") == shared.get("fault_active")
                assert {k: v for k, v in live["metadata"].items() if k in shared["metadata"]} == (
                    shared["metadata"]
                )


def test_scorer_buffer_holds_two_windows_at_the_live_rate() -> None:
    from iot_twin.services.ml import risk_scorer

    live_hz = 25.7  # measured per spindle sensor on the live pipeline
    needed = 2 * 60.0 * len(SPINDLE_SENSOR_NAMES) * live_hz
    assert needed <= risk_scorer._BUFFER_MAX
    assert int(2 * 60 * 4 * risk_scorer.SCORER_MAX_RATE_HZ) == risk_scorer._BUFFER_MAX
