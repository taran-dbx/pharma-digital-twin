"""Thermal-risk features, labels and evaluation helpers (pure Python, offline)."""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from iot_twin.ml import spindle_features as sf
from iot_twin.ml.evaluation import choose_threshold, precision_recall_at
from iot_twin.ml.training_set import (
    LabelledRow,
    RunInput,
    build_training_set,
    onsets_in_period,
    time_split,
)

ROOT = Path(__file__).resolve().parents[1]
LIMIT = 60.0
T0 = 1_800_000_000.0


def _series_readings(
    seconds: int,
    *,
    temp=lambda t: 45.0,
    vibration=lambda t: 2.0,
    power=lambda t: 20.0,
    speed=lambda t: 3500.0,
    start: float = T0,
) -> list[dict]:
    readings: list[dict] = []
    for i in range(seconds):
        t = start + i
        for name, fn in (
            ("cnc_1_spindle_temp", temp),
            ("cnc_1_vibration", vibration),
            ("cnc_1_spindle_power", power),
            ("cnc_1_spindle_speed", speed),
        ):
            readings.append({"sensor_name": name, "value": fn(i), "timestamp": t})
    return readings


class TestFeatureSpec:
    def test_feature_names_derive_from_the_spec_in_order(self) -> None:
        assert tuple(spec.name for spec in sf.FEATURE_SPEC) == sf.FEATURE_NAMES
        assert len(set(sf.FEATURE_NAMES)) == len(sf.FEATURE_NAMES)

    def test_spec_covers_the_contracted_features(self) -> None:
        for signal in ("temp", "vibration", "load", "power"):
            for stat in ("mean", "slope", "max"):
                assert f"{signal}_{stat}" in sf.FEATURE_NAMES
        assert "temp_margin_c" in sf.FEATURE_NAMES
        assert "hours_since_maintenance" in sf.FEATURE_NAMES

    def test_signals_are_catalog_sensors(self) -> None:
        from iot_twin.domain.catalog import sensor_by_name

        for spec in sf.SPINDLE_SIGNALS:
            node = sensor_by_name("manufacturing", spec.sensor_name)
            assert node is not None and node.component_id == sf.SPINDLE_COMPONENT_ID

    def test_validated_limit_and_maintenance_come_from_the_catalog(self) -> None:
        assert sf.validated_limit_c() == LIMIT
        assert sf.threshold_rule_delay_s() == 2.0
        assert sf.threshold_rule_hysteresis_c() == 5.0
        expected = datetime(2026, 9, 1, 12, tzinfo=UTC).timestamp()
        assert sf.last_spindle_maintenance_epoch() == expected


class TestFeatures:
    def test_window_stats(self) -> None:
        readings = _series_readings(61, temp=lambda i: 40.0 + i / 6.0)  # +10 °C/min
        row = sf.build_features(readings, limit_c=LIMIT, last_maintenance_at=T0 - 7200)
        assert row is not None
        assert row["temp_slope"] == pytest.approx(10.0, rel=1e-6)
        assert row["temp_max"] == pytest.approx(50.0)
        assert row["temp_last"] == pytest.approx(50.0)
        assert row["temp_margin_c"] == pytest.approx(10.0)
        assert row["vibration_mean"] == pytest.approx(2.0)
        assert row["vibration_slope"] == pytest.approx(0.0)
        assert row["hours_since_maintenance"] == pytest.approx(2.0 + 60 / 3600)
        assert list(row) == list(sf.FEATURE_NAMES)

    def test_load_is_torque_from_power_and_speed(self) -> None:
        readings = _series_readings(30, power=lambda i: 21.0, speed=lambda i: 3000.0)
        row = sf.build_features(readings, limit_c=LIMIT)
        assert row is not None
        assert row["load_mean"] == pytest.approx(9549.0 * 21.0 / 3000.0)

    def test_idle_spindle_has_no_load_and_no_row(self) -> None:
        readings = _series_readings(30, speed=lambda i: 0.0)
        assert sf.build_features(readings, limit_c=LIMIT) is None

    def test_partial_window_is_not_scored(self) -> None:
        readings = _series_readings(2)
        assert sf.build_features(readings, limit_c=LIMIT) is None
        no_vibration = [r for r in _series_readings(30) if r["sensor_name"] != "cnc_1_vibration"]
        assert sf.build_features(no_vibration, limit_c=LIMIT) is None

    def test_event_time_iso_and_datetime_are_accepted(self) -> None:
        readings = _series_readings(20)
        as_iso = [
            {
                **r,
                "timestamp": None,
                "event_time": datetime.fromtimestamp(r["timestamp"], UTC).isoformat(),
            }
            for r in readings
        ]
        as_dt = [
            {**r, "timestamp": None, "event_time": datetime.fromtimestamp(r["timestamp"], UTC)}
            for r in readings
        ]
        base = sf.build_features(readings, limit_c=LIMIT)
        assert base == sf.build_features(as_iso, limit_c=LIMIT)
        assert base == sf.build_features(as_dt, limit_c=LIMIT)

    def test_non_spindle_and_bad_readings_are_ignored(self) -> None:
        readings = _series_readings(20)
        noisy = [
            *readings,
            {"sensor_name": "hvac_zone_1_temp", "value": 99.0, "timestamp": T0 + 5},
            {"sensor_name": "cnc_1_spindle_temp", "value": "nan", "timestamp": T0 + 6},
            {"sensor_name": "cnc_1_spindle_temp", "value": None, "timestamp": T0 + 7},
            {"sensor_name": "cnc_1_spindle_temp", "value": 1.0},
            "not-a-mapping",
        ]
        assert sf.build_features(noisy, limit_c=LIMIT) == sf.build_features(readings, limit_c=LIMIT)

    def test_train_serve_parity(self) -> None:
        """The training path (feature_rows over a long series) equals the serving
        path (build_features on just the recent window) at every sample time."""
        readings = _series_readings(
            400,
            temp=lambda i: 45.0 + 20.0 * math.sin(i / 40.0),
            vibration=lambda i: 2.0 + (i % 17) / 10.0,
            power=lambda i: 18.0 + (i % 11),
        )
        series = sf.SpindleSeries.from_readings(readings)
        maint = T0 - 3600 * 100
        times = sf.sample_times(series, 5.0)
        rows = sf.feature_rows(series, times, limit_c=LIMIT, last_maintenance_at=maint)
        assert len(rows) == len(times) > 50
        for at, train_row in rows[::7]:
            recent = [r for r in readings if at - sf.WINDOW_S < r["timestamp"] <= at]
            serve_row = sf.build_features(
                list(reversed(recent)), at=at, limit_c=LIMIT, last_maintenance_at=maint
            )
            assert serve_row == train_row


class TestLabels:
    def _ramp_series(self) -> sf.SpindleSeries:
        # 45 °C, then from t=200 s +1 °C/s: over 60 °C first at t=216 s, down at 300 s.
        def temp(i: int) -> float:
            if 200 <= i < 300:
                return 45.0 + (i - 200)
            return 45.0

        return sf.SpindleSeries.from_readings(_series_readings(400, temp=temp))

    def test_label_looks_forward_by_the_horizon(self) -> None:
        series = self._ramp_series()
        times = [T0 + 90, T0 + 96, T0 + 100, T0 + 215]
        labels = sf.build_labels(series, times, limit_c=LIMIT, horizon_s=120.0)
        # 216 - 96 = 120 is inside the horizon, 216 - 90 = 126 is not.
        assert labels == [0, 1, 1, 1]

    def test_rows_inside_an_excursion_are_excluded(self) -> None:
        series = self._ramp_series()
        labels = sf.build_labels(series, [T0 + 216, T0 + 250, T0 + 299], limit_c=LIMIT)
        assert labels == [None, None, None]

    def test_after_the_excursion_the_label_is_zero(self) -> None:
        series = self._ramp_series()
        assert sf.build_labels(series, [T0 + 320], limit_c=LIMIT) == [0]

    def test_onsets_use_the_rule_hysteresis(self) -> None:
        values = [50, 61, 58, 61, 54, 61, 50]
        readings = _series_readings(len(values), temp=lambda i: values[i])
        series = sf.SpindleSeries.from_readings(readings)
        onsets = sf.excursion_onsets(series, limit_c=LIMIT, hysteresis_c=5.0)
        # 58 is inside the clear band: one episode until 54, then a second one.
        assert onsets == [T0 + 1, T0 + 5]

    def test_lead_time_report(self) -> None:
        times = [100.0, 105.0, 110.0, 115.0, 300.0, 305.0]
        scores = [0.1, 0.7, 0.8, 0.9, 0.2, 0.3]
        report = sf.lead_time_report(
            times, scores, [120.0, 310.0], threshold=0.6, horizon_s=60.0, rule_delay_s=2.0
        )
        assert report["excursion_events"] == 2
        assert report["events_predicted_early"] == 1
        assert report["early_detection_rate"] == 0.5
        # First event: rule opens at 122, model fired at 105 → 17 s; second missed → 0.
        assert report["mean_lead_time_gain_s"] == pytest.approx(8.5)
        assert report["median_lead_time_gain_s"] == pytest.approx(8.5)


class TestTrainingSet:
    def test_build_and_time_split(self) -> None:
        def temp(i: int) -> float:
            return 70.0 if 300 <= (i % 600) < 330 else 45.0

        readings = _series_readings(3000, temp=temp)
        ts = build_training_set(
            [RunInput("r1", readings, T0 - 3600)], data_origin="test", limit_c=LIMIT
        )
        assert ts.rows and 0 < ts.positives < len(ts.rows)
        assert all(row.label in (0, 1) for row in ts.rows)
        train, test = time_split(list(ts.rows), 0.25)
        assert max(r.at for r in train) < min(r.at for r in test)
        onsets = onsets_in_period(ts, test, limit_c=LIMIT, hysteresis_c=5.0)
        assert onsets and min(onsets) >= min(r.at for r in test)

    def test_time_split_rejects_bad_input(self) -> None:
        rows = [LabelledRow("r", float(i), {}, 0) for i in range(3)]
        with pytest.raises(ValueError):
            time_split(rows, 1.5)
        with pytest.raises(ValueError):
            time_split(rows[:1], 0.5)


class TestEvaluation:
    def test_precision_recall(self) -> None:
        stats = precision_recall_at([1, 1, 0, 0], [0.9, 0.4, 0.8, 0.1], 0.5)
        assert stats == {"precision": 0.5, "recall": 0.5, "f1": 0.5}

    def test_choose_threshold_prefers_recall_under_a_precision_floor(self) -> None:
        labels = [1, 1, 1, 0, 0, 0]
        scores = [0.9, 0.8, 0.3, 0.7, 0.2, 0.1]
        threshold, rule = choose_threshold(
            labels, scores, min_precision=0.8, grid=[0.25, 0.5, 0.75]
        )
        # 0.25 → precision 0.75, 0.5 → 0.67, 0.75 → 1.0 (recall 2/3).
        assert (threshold, rule) == (0.75, "max_recall_at_precision>=0.8")

    def test_choose_threshold_falls_back_to_f1(self) -> None:
        threshold, rule = choose_threshold([1, 0], [0.4, 0.6], min_precision=0.99, grid=[0.3, 0.5])
        assert rule == "max_f1" and threshold == 0.3


class TestSynthetic:
    def test_generator_is_deterministic_tagged_and_labelled(self) -> None:
        from iot_twin.ml.synthetic_telemetry import DATA_ORIGIN, generate_runs

        runs = generate_runs(n_runs=2, duration_s=900, seed=3)
        again = generate_runs(n_runs=2, duration_s=900, seed=3)
        assert [r.readings[:20] for r in runs] == [r.readings[:20] for r in again]
        assert all(rd["metadata"]["data_origin"] == DATA_ORIGIN for r in runs for rd in r.readings)
        names = {rd["sensor_name"] for r in runs for rd in r.readings}
        assert names == set(sf.SPINDLE_SENSOR_NAMES)
        ts = build_training_set(
            [RunInput(r.run_id, r.readings, r.last_maintenance_at) for r in runs],
            data_origin=DATA_ORIGIN,
            limit_c=LIMIT,
        )
        assert ts.rows and ts.positives > 0

    def test_heat_schedule_mirrors_the_producer_constants(self) -> None:
        from iot_twin.ml import synthetic_telemetry as st

        text = (ROOT / "client" / "src" / "lib" / "manufacturing-scenario.ts").read_text()
        block = re.search(r"SPINDLE_HEAT_SCENARIO = \{(.*?)\}", text, re.S)
        assert block
        values = dict(re.findall(r"(\w+): ([\d.]+)", block.group(1)))
        assert float(values["cycleSeconds"]) == st.HEAT_CYCLE_S
        assert float(values["normalSeconds"]) == st.HEAT_NORMAL_S
        assert float(values["rampSeconds"]) == st.HEAT_RAMP_S
        assert float(values["holdSeconds"]) == st.HEAT_HOLD_S
        assert float(values["peakDeltaC"]) == st.HEAT_PEAK_DELTA_C

    def test_heat_delta_shape(self) -> None:
        from iot_twin.ml.synthetic_telemetry import heat_delta_at

        base = 1_800_000_100.0 - (1_800_000_100.0 % 300)
        assert heat_delta_at(base + 100, 25.0) == 0.0
        assert heat_delta_at(base + 225, 25.0) == pytest.approx(12.5)
        assert heat_delta_at(base + 250, 25.0) == 25.0
        assert heat_delta_at(base + 285, 25.0) == pytest.approx(12.5)

    def test_profiles_come_from_the_simulator_catalog(self) -> None:
        from iot_twin.ml.synthetic_telemetry import simulator_profiles

        profiles = simulator_profiles()
        assert profiles["cnc_1_spindle_temp"].nominal == 45.0
        assert profiles["cnc_1_spindle_temp"].high == 75.0
        assert set(profiles) == set(sf.SPINDLE_SENSOR_NAMES)


class TestReviewFixes:
    def test_maintenance_age_is_clipped_to_the_training_range(self) -> None:
        readings = _series_readings(20)
        old = sf.build_features(readings, limit_c=LIMIT, last_maintenance_at=T0 - 3260 * 3600)
        assert old is not None
        assert old["hours_since_maintenance"] == sf.MAX_MAINTENANCE_AGE_H
        future = sf.build_features(readings, limit_c=LIMIT, last_maintenance_at=T0 + 3600)
        assert future is not None and future["hours_since_maintenance"] == 0.0

    def test_synthetic_runs_stay_inside_the_clip_range(self) -> None:
        from iot_twin.ml.synthetic_telemetry import generate_runs

        for run in generate_runs(n_runs=6, duration_s=120, seed=11):
            start = min(r["timestamp"] for r in run.readings)
            assert 0 <= (start - run.last_maintenance_at) / 3600 <= sf.MAX_MAINTENANCE_AGE_H

    def test_time_split_embargo_drops_rows_next_to_the_boundary(self) -> None:
        rows = [LabelledRow("r", float(t), {}, t % 2) for t in range(0, 1000, 5)]
        train, test = time_split(rows, 0.25, embargo_s=180.0)
        boundary = min(r.at for r in test)
        assert max(r.at for r in train) < boundary - 180.0
        assert len(train) + len(test) < len(rows)
        with pytest.raises(ValueError):
            time_split(rows[:10], 0.5, embargo_s=1e6)

    def test_quantile_clear_level_and_class_guard(self) -> None:
        from iot_twin.ml.evaluation import clear_threshold_for, quantile, two_classes

        assert quantile([0.0, 1.0], 0.5) == 0.5
        assert quantile([3.0, 1.0, 2.0], 0.9) == pytest.approx(2.8)
        assert quantile([], 0.5) == 0.0
        assert clear_threshold_for(0.22, 0.05) == 0.17
        assert clear_threshold_for(0.03, 0.05) == 0.01
        assert two_classes([0, 1, 1]) and not two_classes([0, 0])


class TestCalmScoreCheck:
    def test_is_calm_excludes_near_misses(self) -> None:
        from iot_twin.ml.evaluation import is_calm

        assert is_calm(0, 12.0, 1.0)
        assert not is_calm(1, 12.0, 1.0)
        assert not is_calm(0, 4.0, 1.0)  # close to the limit
        assert not is_calm(0, 12.0, 9.0)  # a ramp that stays under the limit

    def test_p99_per_wear_band_against_the_cut_off(self) -> None:
        from iot_twin.ml.evaluation import calm_score_check

        scores = [0.1] * 99 + [0.2] + [0.1] * 90 + [0.6] * 10  # worn band: p99 = 0.6
        wear = [500.0] * 100 + [2500.0] * 100
        metrics, problems = calm_score_check(scores, [True] * 200, wear, cutoff=0.47, clear=0.42)
        assert metrics["val_calm_score_p99_wear_0_1000h"] < 0.47
        assert metrics["val_calm_score_p99_wear_2000_3000h"] > 0.47
        assert "1000_2000" not in " ".join(metrics)  # empty band skipped
        assert any("2000-3000 h" in p for p in problems)

    def test_no_problems_for_quiet_calm_scores_and_mask_is_respected(self) -> None:
        from iot_twin.ml.evaluation import calm_score_check

        scores = [0.1] * 50 + [0.9] * 50
        mask = [True] * 50 + [False] * 50
        metrics, problems = calm_score_check(scores, mask, [3000.0] * 100, cutoff=0.47, clear=0.42)
        assert problems == [] and metrics["val_calm_score_p99"] == pytest.approx(0.1)
        assert "val_calm_score_p99_wear_2000_3000h" in metrics  # 3000 h is in the top band
