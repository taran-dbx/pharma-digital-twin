"""Tests for the repository-owned scenario engine wrapper."""

from __future__ import annotations

from types import SimpleNamespace

from iot_twin.domain.catalog import CURATED_DOMAIN_INDUSTRIES
from iot_twin.scenario_engine import CadenceClock, ScenarioEngine, cadence_due


def _record(**overrides) -> SimpleNamespace:
    base = dict(
        sensor_name="cnc_1_spindle_temp",
        industry="manufacturing",
        value=45.0,
        unit="°C",
        sensor_type="temperature",
        timestamp=1_700_000_000.0,
        min_value=20.0,
        max_value=90.0,
        nominal_value=45.0,
        fault_active=False,
        metadata={},
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class TestScenarioPhases:
    def test_seeded_phases_are_deterministic_c7309_regression(self) -> None:
        """[C7309][regression] Same seed and timestamp yield the same phase."""
        left = ScenarioEngine(industry="manufacturing", seed=7, scenario_id="line-fault")
        right = ScenarioEngine(industry="manufacturing", seed=7, scenario_id="line-fault")
        stamps = [1_700_000_000.0 + offset * 90 for offset in range(12)]
        assert [left.phase_at(ts) for ts in stamps] == [right.phase_at(ts) for ts in stamps]
        assert set(left.phase_at(ts) for ts in stamps) <= {
            "normal",
            "degraded",
            "failed",
            "recovering",
        }
        other = ScenarioEngine(industry="manufacturing", seed=8, scenario_id="line-fault")
        assert [left.phase_at(ts) for ts in stamps] != [other.phase_at(ts) for ts in stamps]

    def test_wrap_attaches_stable_metadata_keys_c7310_regression(self) -> None:
        """[C7310][regression] Wrapped records carry hierarchy and scenario metadata."""
        engine = ScenarioEngine(industry="manufacturing", seed=1, scenario_id="spindle-overtemp")
        wrapped = engine.wrap_records([_record()])[0]
        for key in ("site_id", "asset_id", "component_id", "scenario_id", "fault_code"):
            assert key in wrapped.metadata
        assert wrapped.metadata["site_id"] == "mfg-site-detroit"
        assert wrapped.metadata["asset_id"] == "mfg-asset-cnc-1"
        assert wrapped.metadata["component_id"] == "mfg-comp-spindle"
        assert wrapped.metadata["scenario_id"] == "spindle-overtemp"

    def test_energy_records_keep_catalog_industry_c7302_regression(self) -> None:
        """[C7302][regression] Electric-power ticks are rewritten to energy."""
        engine = ScenarioEngine(industry="energy", seed=3, scenario_id="grid-sag")
        wrapped = engine.wrap_records(
            [_record(sensor_name="gt_1_power_output", industry="electric_power", value=80.0)]
        )[0]
        assert wrapped.industry == "energy"
        assert wrapped.metadata["site_id"] == "energy-site-north-grid"

    def test_failed_phase_correlates_related_sensors_c7311_regression(self) -> None:
        """[C7311][regression] Failed phase moves correlated sensors together."""
        engine = ScenarioEngine(industry="manufacturing", seed=0, scenario_id="bearing-fail")
        failed_at = next(
            ts
            for ts in (1_700_000_000.0 + i * 15 for i in range(80))
            if engine.phase_at(ts) == "failed"
        )
        temp = engine.wrap_records([_record(timestamp=failed_at, value=45.0)])[0]
        vibe = engine.wrap_records(
            [_record(sensor_name="cnc_1_vibration", timestamp=failed_at, value=2.0)]
        )[0]
        power = engine.wrap_records(
            [_record(sensor_name="cnc_1_spindle_power", timestamp=failed_at, value=20.0)]
        )[0]
        assert temp.value > 45.0
        assert vibe.value > 2.0
        assert power.value < 20.0
        assert temp.fault_active is True
        assert temp.metadata["fault_code"]

    def test_normal_phase_leaves_nominal_values_c7311_smoke(self) -> None:
        """[C7311][smoke] Normal phase does not inject a fault code."""
        engine = ScenarioEngine(industry="manufacturing", seed=0, scenario_id="bearing-fail")
        normal_at = next(
            ts
            for ts in (1_700_000_000.0 + i * 15 for i in range(80))
            if engine.phase_at(ts) == "normal"
        )
        wrapped = engine.wrap_records([_record(timestamp=normal_at, value=45.0)])[0]
        assert wrapped.value == 45.0
        assert wrapped.fault_active is False
        assert wrapped.metadata["fault_code"] == ""

    def test_every_curated_industry_can_wrap_c7311_smoke(self) -> None:
        """[C7311][smoke] Scenario wrapping is defined for all four industries."""
        samples = {
            "manufacturing": "cnc_1_spindle_temp",
            "pharmaceutical": "bioreactor_1_temp",
            "energy": "gt_1_exhaust_temp",
            "automotive": "robot_1_weld_current",
        }
        assert set(samples) == set(CURATED_DOMAIN_INDUSTRIES)
        for industry, sensor_name in samples.items():
            engine = ScenarioEngine(industry=industry, seed=0)
            wrapped = engine.wrap_records(
                [_record(sensor_name=sensor_name, industry=industry, value=10.0)]
            )[0]
            assert wrapped.metadata["site_id"]
            assert wrapped.metadata["asset_id"]
            assert wrapped.industry == industry

    def test_wrap_records_respects_catalog_cadence_c7312_regression(self) -> None:
        """[C7312][regression] wrap_records emits only sensors that are due on the catalog clock."""
        engine = ScenarioEngine(industry="manufacturing", seed=1, scenario_id="cadence")
        first = engine.wrap_records([_record(timestamp=10.0, value=45.0)])
        assert len(first) == 1
        skipped = engine.wrap_records([_record(timestamp=10.01, value=46.0)])
        assert skipped == []
        later = engine.wrap_records([_record(timestamp=10.2, value=47.0)])
        assert len(later) == 1
        assert later[0].value == 47.0
        counters = (
            engine.cadence_metrics()
            if hasattr(engine, "cadence_metrics")
            else {
                "emitted": getattr(engine, "emitted_count", None),
                "dropped": getattr(engine, "dropped_count", None),
            }
        )
        emitted = counters.get("emitted") if isinstance(counters, dict) else None
        dropped = counters.get("dropped") if isinstance(counters, dict) else None
        assert emitted == 2
        assert dropped == 1

    def test_energy_on_wire_after_cadence_filter_c7302_smoke(self) -> None:
        """[C7302][smoke] Due energy ticks keep industry=energy on the wire."""
        engine = ScenarioEngine(industry="energy", seed=3, scenario_id="grid-sag")
        wrapped = engine.wrap_records(
            [
                _record(
                    sensor_name="gt_1_power_output",
                    industry="electric_power",
                    timestamp=0.0,
                    value=80.0,
                )
            ]
        )
        assert len(wrapped) == 1
        assert wrapped[0].industry == "energy"


class TestCadenceHelpers:
    def test_cadence_due_respects_sensor_hz_c7312_regression(self) -> None:
        """[C7312][regression] Cadence helpers skip sensors that are not due."""
        hz = 2.0
        assert cadence_due("cnc_1_spindle_temp", now=10.0, last_emitted_at=None, hz=hz)
        assert not cadence_due("cnc_1_spindle_temp", now=10.2, last_emitted_at=10.0, hz=hz)
        assert cadence_due("cnc_1_spindle_temp", now=10.5, last_emitted_at=10.0, hz=hz)

    def test_cadence_clock_uses_catalog_hz_c7312_smoke(self) -> None:
        """[C7312][smoke] CadenceClock reads per-sensor hz from the domain catalog."""
        clock = CadenceClock(industry="manufacturing")
        assert clock.due("cnc_1_spindle_temp", now=0.0)
        assert not clock.due("cnc_1_spindle_temp", now=0.01)
        later = clock.interval_s("cnc_1_spindle_temp")
        assert later > 0
        assert clock.due("cnc_1_spindle_temp", now=later)
