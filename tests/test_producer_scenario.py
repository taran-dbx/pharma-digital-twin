"""Behavioural tests for the producer's signal calibration and Spindle 3 heat event.

The Python fragment is rendered from ``client/src/lib/manufacturing-scenario.ts``
with esbuild + node, then executed against the vendored simulator wheel.
"""

from __future__ import annotations

import json
import logging
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from iot_twin.domain.catalog import list_alert_rules

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "client"
SCENARIO_TS = CLIENT / "src" / "lib" / "manufacturing-scenario.ts"
PRODUCER_TS = CLIENT / "src" / "lib" / "data-producer-notebook.ts"
WHEEL = ROOT / "lib" / "iot_data_simulator-0.1.0-py3-none-any.whl"
ESBUILD = CLIENT / "node_modules" / ".bin" / "esbuild"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None or not ESBUILD.exists() or not WHEEL.exists(),
    reason="node, client esbuild, or the simulator wheel is unavailable",
)


def _render(industry: str) -> dict:
    bundle = subprocess.run(
        [str(ESBUILD), str(SCENARIO_TS), "--bundle", "--format=cjs", "--platform=node"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    script = (
        f"{bundle}\nconst m = module.exports;"
        f"process.stdout.write(JSON.stringify({{python: m.producerSignalPython({json.dumps(industry)}),"
        " scenario: m.SPINDLE_HEAT_SCENARIO}));"
    )
    out = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def _namespace(industry: str) -> tuple[dict, dict]:
    if str(WHEEL) not in sys.path:
        sys.path.insert(0, str(WHEEL))
    rendered = _render(industry)
    ns: dict = {
        "json": json,
        "time": time,
        "threading": threading,
        "logger": logging.getLogger(f"producer.{industry}"),
    }
    exec(compile(rendered["python"], f"<producer:{industry}>", "exec"), ns)
    return ns, rendered["scenario"]


def _wait_for(predicate, timeout: float = 6.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def _spindle_samples(ns: dict, ticks: int) -> list[float]:
    from iot_simulator import Simulator

    sim = Simulator(industries=["manufacturing"], update_rate_hz=5)
    ns["_calibrate_simulator"](sim, disable_random_faults=ns["SCENARIO_ENABLED"])
    values: list[float] = []
    for _ in range(ticks):
        for record in sim._generator.tick():
            if record.sensor_name == "cnc_1_spindle_temp":
                values.append(record.value)
                assert record.fault_active is False
    return values


def _spindle_limit() -> tuple[float, float]:
    rule = next(
        r for r in list_alert_rules("manufacturing") if r.rule_id == "mfg-alert-spindle-temp"
    )
    return rule.threshold, rule.hysteresis


class TestProducerSignal:
    def test_calibrated_spindle_noise_matches_catalog_sigma_c7320_defect(self) -> None:
        """[C7320][defect][regression] Spindle temp is ~45 ±2 °C, not 25/75 clamp noise."""
        ns, _ = _namespace("manufacturing")
        values = _spindle_samples(ns, 2000)
        clamped = sum(1 for v in values if v in (25.0, 75.0)) / len(values)
        assert clamped < 0.01
        assert 43.0 < statistics.mean(values) < 47.0
        assert 1.0 < statistics.pstdev(values) < 3.5

    def test_normal_phase_stays_below_clear_band_c7321_regression(self) -> None:
        """[C7321][regression] Normal readings never reach the alert clear band."""
        ns, _ = _namespace("manufacturing")
        threshold, hysteresis = _spindle_limit()
        values = _spindle_samples(ns, 3000)
        assert max(values) < threshold - hysteresis + 2.0
        assert max(values) < threshold

    def test_heat_event_crosses_limit_then_recovers_c7322_smoke(self) -> None:
        """[C7322][smoke] The scheduled event crosses the limit and returns to normal."""
        ns, s = _namespace("manufacturing")
        threshold, hysteresis = _spindle_limit()
        cycle_start = 1_800_000_000 - (1_800_000_000 % s["cycleSeconds"])

        def at(offset: float, value: float = 45.0) -> dict:
            payload = {
                "sensor_name": "cnc_1_spindle_temp",
                "value": value,
                "max_value": 75.0,
                "timestamp": cycle_start + offset,
                "metadata": {},
            }
            return ns["_apply_scenario"](payload)

        normal = at(10)
        assert normal["value"] == 45.0
        assert normal["fault_active"] is False
        assert normal["metadata"] == {"scenario_id": s["id"], "phase": "normal"}

        overheat_start = s["normalSeconds"] + s["rampSeconds"]
        overheat = at(overheat_start + 1)
        assert overheat["metadata"]["phase"] == "overheat"
        assert overheat["fault_active"] is True
        assert overheat["value"] - 3 * 2.0 > threshold

        cooled = at(s["cycleSeconds"] - 1)
        assert cooled["metadata"]["phase"] == "cooling"
        assert cooled["value"] < threshold - hysteresis

        assert at(overheat_start + 1, value=74.0)["value"] == 75.0

    def test_other_sensors_are_untouched_c7323_regression(self) -> None:
        """[C7323][regression] Only Spindle 3 temperature carries the heat event."""
        ns, s = _namespace("manufacturing")
        payload = {
            "sensor_name": "cnc_1_vibration",
            "value": 2.0,
            "timestamp": float(s["normalSeconds"] + 40),
        }
        assert ns["_apply_scenario"](dict(payload)) == payload

    def test_non_manufacturing_has_no_scenario_c7324_regression(self) -> None:
        """[C7324][regression] Other industries get noise calibration only."""
        ns, _ = _namespace("pharmaceutical")
        assert ns["SCENARIO_ENABLED"] is False
        payload = {"sensor_name": "cnc_1_spindle_temp", "value": 45.0, "timestamp": 0.0}
        assert ns["_apply_scenario"](dict(payload)) == payload

    def test_msk_iam_producer_applies_scenario_after_device_stamp_c7325_regression(self) -> None:
        """[C7325][regression] The live producer stamps device time, then applies the event."""
        source = PRODUCER_TS.read_text()
        stamp = source.index(
            'payload["timestamp"] = time.time()\n        payload = _apply_scenario(payload)'
        )
        assert stamp > 0
        assert "${producerSignalPython(industry)}" in source
        assert "_calibrate_simulator(sim, disable_random_faults=SCENARIO_ENABLED)" in source


def _gated_namespace(control) -> dict:
    """Manufacturing producer whose control reads come from a local file instead of the Workspace API."""
    ns, _ = _namespace("manufacturing")
    ns["PRODUCTION_POLL_S"] = 0.05

    def download():
        if not control.exists():
            raise ns["_ControlNotFound"]("control file missing")
        return control.read_bytes()

    ns["_download_production_control"] = download
    return ns


class TestProductionGate:
    def test_stopped_cell_machines_stop_emitting_c7441_smoke(self, tmp_path) -> None:
        """[C7441][smoke] A stop silences CNC-1 and the assembly line; utilities keep streaming; resume restores."""
        control = tmp_path / "manufacturing.json"
        ns = _gated_namespace(control)
        spindle = {"sensor_name": "cnc_1_spindle_temp"}
        line = {"sensor_name": "assy_line_1_speed"}
        compressor = {"sensor_name": "compressor_1_pressure"}
        assert ns["_production_allows"](spindle)

        control.write_text(json.dumps({"state": "stopped", "assets": ["cnc_1", "assy_line_1"]}))
        assert _wait_for(lambda: not ns["_production_allows"](spindle))
        assert not ns["_production_allows"](line)
        assert ns["_production_allows"](compressor)

        control.write_text(json.dumps({"state": "running", "assets": []}))
        assert _wait_for(lambda: ns["_production_allows"](spindle))

    def test_missing_or_corrupt_control_file_fails_open_c7442_defect(self, tmp_path) -> None:
        """[C7442][defect] Without a readable control file the producer keeps every sensor streaming."""
        control = tmp_path / "manufacturing.json"
        ns = _gated_namespace(control)
        assert ns["_read_production_control"]() == ()
        control.write_text("{not json")
        assert ns["_read_production_control"]() == ()
        control.write_text("[]")
        assert ns["_read_production_control"]() == ()
        assert ns["_production_allows"]({"sensor_name": "cnc_1_spindle_temp"})

    def test_failed_read_keeps_the_cell_stopped_c7444_defect(self, tmp_path) -> None:
        """[C7444][defect] A Workspace API blip during a stop does not restart the cell."""
        control = tmp_path / "manufacturing.json"
        ns = _gated_namespace(control)
        spindle = {"sensor_name": "cnc_1_spindle_temp"}
        control.write_text(json.dumps({"state": "stopped", "assets": ["cnc_1", "assy_line_1"]}))
        assert _wait_for(lambda: not ns["_production_allows"](spindle))

        def unavailable():
            raise ConnectionError("workspace API unavailable")

        ns["_download_production_control"] = unavailable
        time.sleep(0.3)
        assert not ns["_production_allows"](spindle)

    def test_gate_runs_before_send_and_other_industries_always_emit_c7443_regression(self) -> None:
        """[C7443][regression] The live producer drops stopped records before stamping; other industries are ungated."""
        source = PRODUCER_TS.read_text()
        gate = source.index("if not _production_allows(payload):\n            continue")
        assert gate < source.index(
            'payload["timestamp"] = time.time()\n        payload = _apply_scenario(payload)'
        )
        ns, _ = _namespace("pharmaceutical")
        assert ns["_production_allows"]({"sensor_name": "cnc_1_spindle_temp"}) is True
        assert "PRODUCTION_CONTROL_FILE" not in ns
