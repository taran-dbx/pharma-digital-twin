"""The shared, SIMULATED Spindle 3 scenario: randomness bounds, mix, caps, embedding and TS parity."""

from __future__ import annotations

import json
import random
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

import pytest

from iot_twin.domain.catalog import list_alert_rules
from iot_twin.ml.lead_metrics import breach_episodes
from iot_twin.ml.live_format_telemetry import live_format_readings
from iot_twin.ml.spindle_features import SpindleSeries
from iot_twin.scenario_engine import spindle_scenario as sc

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "client"
SCENARIO_TS = CLIENT / "src" / "lib" / "manufacturing-scenario.ts"
ESBUILD = CLIENT / "node_modules" / ".bin" / "esbuild"
WHEEL = ROOT / "lib" / "iot_data_simulator-0.1.0-py3-none-any.whl"
FIRST = 1_796_000_000 // sc.SPINDLE_CYCLE_S
PLANS = [sc.spindle_cycle_plan(i) for i in range(FIRST, FIRST + 20_000)]
# Simulator temperature: nominal 45 °C, drift up to ±5 %, noise sigma 2 °C.
TEMP_4_SIGMA_MAX = 45.0 * 1.05 + 4 * 2.0

needs_node = pytest.mark.skipif(
    shutil.which("node") is None or not ESBUILD.exists(),
    reason="node or the client esbuild is unavailable",
)


def _share(predicate) -> float:
    return sum(1 for plan in PLANS if predicate(plan)) / len(PLANS)


def _rule(rule_id: str):
    return next(r for r in list_alert_rules("manufacturing") if r.rule_id == rule_id)


class TestCyclePlan:
    def test_plan_is_deterministic_per_cycle(self) -> None:
        assert sc.spindle_cycle_plan(FIRST + 7) == sc.spindle_cycle_plan(FIRST + 7)
        assert sc.spindle_cycle_plan(FIRST + 7) != sc.spindle_cycle_plan(FIRST + 8)

    def test_cycle_mix_matches_the_documented_shares(self) -> None:
        assert _share(lambda p: p.kind == "excursion") == pytest.approx(0.70, abs=0.015)
        assert _share(lambda p: p.kind == "false_alarm") == pytest.approx(0.15, abs=0.01)
        assert _share(lambda p: p.kind == "quiet") == pytest.approx(0.15, abs=0.01)

    def test_some_excursions_have_weak_or_no_precursor(self) -> None:
        excursions = [p for p in PLANS if p.kind == "excursion"]
        shares = {
            name: sum(1 for p in excursions if p.precursor == name) / len(excursions)
            for name in ("strong", "weak", "none")
        }
        assert shares["strong"] == pytest.approx(0.55, abs=0.02)
        assert shares["weak"] == pytest.approx(0.25, abs=0.02)
        assert shares["none"] == pytest.approx(0.20, abs=0.02)

    def test_false_alarms_split_into_fades_and_warm_bumps(self) -> None:
        alarms = [p for p in PLANS if p.kind == "false_alarm"]
        fades = sum(1 for p in alarms if p.precursor == "fade") / len(alarms)
        assert fades == pytest.approx(0.60, abs=0.03)
        assert {p.precursor for p in alarms} == {"fade", "bump"}

    def test_ramp_start_is_jittered_across_the_cycle(self) -> None:
        starts = [p.ramp_start_s for p in PLANS]
        assert min(starts) >= 30.0 and max(starts) <= 210.0
        assert statistics.pstdev(starts) > 45.0  # ~52 s for U(30, 210): no fixed phase
        thirds = [sum(1 for s in starts if lo <= s < lo + 60) / len(starts) for lo in (30, 90, 150)]
        assert all(share == pytest.approx(1 / 3, abs=0.02) for share in thirds)
        assert all(
            p.ramp_start_s + sc.SPINDLE_EPISODE_S <= sc.SPINDLE_CYCLE_S
            for p in PLANS
            if p.kind == "excursion"
        )

    def test_randomised_lead_amplitude_and_peak_stay_in_bounds(self) -> None:
        leads = [p.lead_s for p in PLANS]
        assert min(leads) >= 30.0 and max(leads) <= 180.0
        assert statistics.pstdev(leads) > 35.0  # spread, not a fixed tell
        peaks = [p.peak_c for p in PLANS if p.kind == "excursion"]
        assert min(peaks) >= 20.0 and max(peaks) <= 28.75
        assert all(p.peak_c == 0.0 for p in PLANS if p.kind != "excursion")
        for plan in PLANS:
            assert 0.0 <= plan.vibration_rise <= sc.SPINDLE_VIBRATION_RISE * 1.4 + 1e-9
            assert 0.0 <= plan.power_rise <= sc.SPINDLE_POWER_RISE * 1.4 + 1e-9
            assert 0.5 <= plan.gamma <= 2.0
        strong = [p.vibration_rise for p in PLANS if p.precursor == "strong"]
        weak = [p.vibration_rise for p in PLANS if p.precursor == "weak"]
        assert statistics.mean(strong) > 2 * statistics.mean(weak)
        assert all(p.vibration_rise == 0.0 for p in PLANS if p.precursor == "none")

    def test_precursor_never_overlaps_the_previous_cycles_episode(self) -> None:
        for prev, plan in zip(PLANS, PLANS[1:], strict=False):
            start = plan.index * 300 + plan.ramp_start_s - plan.lead_s
            prev_end = prev.index * 300 + sc._episode_end_s(prev.index)
            assert start >= prev_end - 1e-6

    def test_only_excursion_cycles_heat_up(self) -> None:
        for plan in PLANS[:400]:
            phases = {
                sc.spindle_heat_phase(plan.index * 300 + t, plan)[0] for t in range(0, 300, 2)
            }
            if plan.kind == "excursion":
                assert phases == {"normal", "heating", "overheat", "cooling"}
            else:
                assert phases == {"normal"}

    def test_precursor_envelope_starts_lead_s_before_the_ramp(self) -> None:
        plan = next(p for p in PLANS if p.precursor == "strong" and p.ramp_start_s > 190)
        ramp = plan.index * sc.SPINDLE_CYCLE_S + plan.ramp_start_s
        assert sc.spindle_precursor_level(ramp - plan.lead_s - 1) == 0.0
        assert sc.spindle_precursor_level(ramp - 1) > 0.3
        for t in range(300):
            assert 0.0 <= sc.spindle_precursor_level(plan.index * 300 + t) <= sc.SPINDLE_WOBBLE_MAX

    def test_an_early_ramp_takes_its_precursor_from_the_previous_cycle(self) -> None:
        plan = next(
            p
            for p in PLANS
            if p.precursor == "strong" and p.ramp_start_s < 60 and p.lead_s > p.ramp_start_s + 10
        )
        assert sc.spindle_precursor_level(plan.index * 300 - 5.0) > 0.0


class TestCaps:
    def test_vibration_precursor_stays_below_the_vibration_alert(self) -> None:
        rule = _rule("mfg-alert-vibration")
        assert rule.threshold > sc.SPINDLE_VIBRATION_CEILING
        rng = random.Random(7)
        strongest = sorted(PLANS, key=lambda p: -p.vibration_rise)[:20]
        peak = 0.0
        for plan in strongest:
            for tenth in range(0, 3000):
                at = plan.index * 300 + tenth / 10
                payload = {
                    "sensor_name": sc.SPINDLE_VIBRATION_SENSOR,
                    "value": 3.5,
                    "timestamp": at,
                }
                peak = max(peak, sc.apply_spindle_scenario(payload, rng)["value"])
        assert peak <= sc.SPINDLE_VIBRATION_CEILING < rule.threshold
        # Analytic: a 6-sigma simulator reading (2.1 + 6 * 0.25) plus the capped offset.
        assert rule.threshold > 2.0 * 1.05 + 6 * 0.25 + sc.SPINDLE_VIBRATION_PRECURSOR_MAX

    def test_false_alarm_cycles_stay_clearly_under_the_thermal_limit(self) -> None:
        limit = _rule("mfg-alert-spindle-temp").threshold
        alarms = [p for p in PLANS if p.kind != "excursion"]
        worst = max(sc.spindle_max_temp_offset(p) for p in alarms)
        assert TEMP_4_SIGMA_MAX + worst < limit - 1.0
        for plan in alarms[:200]:
            nxt = sc.spindle_cycle_plan(plan.index + 1)
            bound = max(sc.spindle_max_temp_offset(plan), nxt.temp_creep_c + nxt.bump_c)
            for t in range(0, 300, 3):
                offset, *_ = sc.spindle_offsets(
                    sc.SPINDLE_TEMP_SENSOR, plan.index * 300 + t, random.Random(0)
                )
                assert offset <= bound + 1e-9


class TestApplyScenario:
    def _payload(self, name: str, value: float, at: float) -> dict:
        return {
            "sensor_name": name,
            "value": value,
            "timestamp": at,
            "min_value": 0.0,
            "max_value": 75.0,
            "metadata": {"lot_id": "L1"},
        }

    def test_returns_a_new_payload_and_keeps_existing_metadata(self) -> None:
        plan = next(p for p in PLANS if p.kind == "excursion")
        at = plan.index * 300 + plan.ramp_start_s + 40.0
        payload = self._payload(sc.SPINDLE_TEMP_SENSOR, 45.0, at)
        out = sc.apply_spindle_scenario(payload, random.Random(1))
        assert payload["value"] == 45.0 and "phase" not in payload["metadata"]
        assert out["metadata"] == {
            "lot_id": "L1",
            "scenario_id": "spindle-3-heat",
            "phase": "overheat",
        }
        assert out["fault_active"] is True
        assert out["value"] <= 75.0

    def test_precursor_noise_comes_from_the_callers_rng(self) -> None:
        plan = next(p for p in PLANS if p.precursor == "strong")
        at = plan.index * 300 + plan.ramp_start_s - 5.0
        payload = self._payload(sc.SPINDLE_VIBRATION_SENSOR, 2.0, at)
        a = sc.apply_spindle_scenario(payload, random.Random(1))["value"]
        b = sc.apply_spindle_scenario(payload, random.Random(1))["value"]
        c = sc.apply_spindle_scenario(payload, random.Random(2))["value"]
        assert a == b != c
        assert a > 2.0

    def test_other_sensors_pass_through_unchanged(self) -> None:
        payload = self._payload("cnc_1_coolant_flow", 12.0, FIRST * 300.0)
        assert sc.apply_spindle_scenario(payload, random.Random(0)) is payload


@pytest.mark.skipif(not WHEEL.exists(), reason="simulator wheel unavailable")
class TestLiveFormatEpisodes:
    def test_breaches_only_in_excursion_cycles_and_vibration_never_alerts(self) -> None:
        if str(WHEEL) not in sys.path:
            sys.path.insert(0, str(WHEEL))
        start = FIRST * 300.0
        readings = live_format_readings(start=start, duration_s=2 * 3600, seed=3, rate_hz=10)
        temp = SpindleSeries.from_readings(readings).points["temp"]
        breaches = breach_episodes(*temp, limit_c=60.0, hysteresis_c=5.0, duration_s=2.0)
        kinds = [sc.spindle_cycle_plan(int(onset // 300)).kind for onset, _ in breaches]
        expected = sum(
            1 for i in range(FIRST, FIRST + 24) if sc.spindle_cycle_plan(i).kind == "excursion"
        )
        assert set(kinds) == {"excursion"}
        assert len(breaches) == expected  # one breach per excursion, none from noise
        vibration = [
            r["value"] for r in readings if r["sensor_name"] == sc.SPINDLE_VIBRATION_SENSOR
        ]
        assert max(vibration) < _rule("mfg-alert-vibration").threshold


class TestEmbedding:
    def test_generated_ts_module_is_in_sync(self) -> None:
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "sync_spindle_scenario.py"), "--check"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout

    def test_module_source_runs_inside_a_notebook_cell(self) -> None:
        source = (ROOT / "src" / "iot_twin" / "scenario_engine" / "spindle_scenario.py").read_text()
        assert "\nfrom __future__ import" not in source
        assert "# COMMAND ----------" not in source
        namespace: dict = {}
        exec(compile("x = 1\n" + source, "<cell>", "exec"), namespace)
        assert namespace["spindle_cycle_kind"](FIRST) == sc.spindle_cycle_kind(FIRST)


@needs_node
class TestDashboardParity:
    def _ts(self, expression: str):
        bundle = subprocess.run(
            [str(ESBUILD), str(SCENARIO_TS), "--bundle", "--format=cjs", "--platform=node"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        script = f"{bundle}\nconst m = module.exports;process.stdout.write(JSON.stringify({expression}));"
        out = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
        return json.loads(out.stdout)

    def test_ts_port_agrees_with_python_on_excursions_and_ramp_starts(self) -> None:
        indices = list(range(FIRST, FIRST + 3000))
        ts = self._ts(f"{json.dumps(indices)}.map((i) => m.spindleCycleHasExcursion(i))")
        assert ts == [sc.spindle_cycle_kind(i) == "excursion" for i in indices]
        starts = self._ts(f"{json.dumps(indices[:200])}.map((i) => m.spindleRampStart(i))")
        assert starts == pytest.approx([sc.spindle_ramp_start(i) for i in indices[:200]])
        assert [sc.spindle_ramp_start(i) for i in indices[:200]] == [
            sc.spindle_cycle_plan(i).ramp_start_s for i in indices[:200]
        ]

    def test_countdown_points_at_the_next_excursion_ramp(self) -> None:
        constants = self._ts("m.SPINDLE_HEAT_SCENARIO")
        assert constants["planSeed"] == sc.SPINDLE_PLAN_SEED
        assert constants["excursionShare"] == sc.SPINDLE_EXCURSION_SHARE
        assert constants["cycleSeconds"] == sc.SPINDLE_CYCLE_S
        assert constants["rampStartRangeSeconds"] == list(sc.SPINDLE_RAMP_START_RANGE_S)
        quiet = next(p.index for p in PLANS if p.kind != "excursion")
        at = quiet * 300 + 250
        state = self._ts(f"m.spindleHeatStateAt({at})")
        nxt = next(
            i for i in range(quiet + 1, quiet + 64) if sc.spindle_cycle_kind(i) == "excursion"
        )
        assert state["phase"] == "normal"
        assert state["secondsToNextEvent"] == pytest.approx(
            nxt * 300 + sc.spindle_ramp_start(nxt) - at
        )
        hot = next(p for p in PLANS if p.kind == "excursion")
        mid = hot.index * 300 + hot.ramp_start_s + 45
        assert self._ts(f"m.spindleHeatStateAt({mid})")["phase"] == "overheat"
        assert sc.spindle_heat_phase(mid, hot)[0] == "overheat"
