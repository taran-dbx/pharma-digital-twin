"""Tests for iot_twin.services.industry.industry_templates — curated industry data ported from
the original design reference (kept outside this repo)."""

from __future__ import annotations

from iot_twin.services.industry.industry_templates import (
    CURATED_INDUSTRIES,
    get_template,
    mode_config,
)


def test_four_curated_industries():
    assert CURATED_INDUSTRIES == ["manufacturing", "pharmaceutical", "energy", "automotive"]


def test_get_template_pharma_has_kpis_and_twin():
    t = get_template("pharmaceutical")
    assert t is not None
    assert t.twin_type == "vessel"
    assert len(t.kpis) >= 3
    assert "reactor" in " ".join(k["k"].lower() for k in t.kpis)


def test_manufacturing_kpis_have_no_invented_values_c7230_defect():
    """[C7230][defect][regression] Manufacturing KPIs show "—" until live data arrives."""
    template = get_template("manufacturing")

    assert template is not None
    assert [k["k"] for k in template.kpis] == [
        "Spindle 3 temperature",
        "Operating limit",
        "Spindle 3 status",
        "Open alerts",
        "Last device event",
    ]
    assert all(k["v"] == "—" for k in template.kpis)
    assert not any(k["k"] in {"Production", "OEE", "Utilization"} for k in template.kpis)


def test_get_template_unknown_returns_none():
    assert get_template("banking") is None


def test_templates_use_real_simulator_sensor_names_c7313_regression():
    """[C7313][regression] Curated templates list real simulator sensor names."""
    expected = {
        "manufacturing": {
            "cnc_1_spindle_speed",
            "assy_line_1_speed",
            "cnc_1_vibration",
            "cnc_1_coolant_temp",
            "press_1_pressure",
            "compressor_1_pressure",
        },
        "pharmaceutical": {
            "bioreactor_1_temp",
            "bioreactor_1_ph",
            "bioreactor_1_dissolved_oxygen",
            "cleanroom_1_pressure",
            "tablet_weight",
            "autoclave_chamber_temp",
        },
        "energy": {
            "gt_1_power_output",
            "gen_1_frequency",
            "transformer_1_load",
            "gen_1_voltage",
            "st_1_power_output",
            "gen_1_bearing_temp",
        },
        "automotive": {
            "robot_1_weld_current",
            "torque_wrench_1_torque",
            "paint_booth_humidity",
            "paint_booth_temp",
            "assembly_line_speed",
            "agv_1_battery_level",
        },
    }
    for industry, names in expected.items():
        template = get_template(industry)
        assert template is not None
        assert names <= set(template.sensors)
        for sensor in template.sensors:
            assert " " not in sensor
            assert "·" not in sensor


def test_mode_config_flags():
    realtime_desc = mode_config("realtime")["desc"]
    assert isinstance(realtime_desc, str) and realtime_desc
    assert realtime_desc.startswith("Spark Structured Streaming")
    assert mode_config("realtime") == {
        "id": "realtime",
        "name": "Real-time",
        "to": "Lakebase + LakeTS",
        "desc": realtime_desc,
        "uses_kafka": True,
        "uses_lakebase": True,
        "has_metrics": True,
    }
    assert mode_config("zerobus")["uses_kafka"] is False
    assert mode_config("zerobus")["uses_lakebase"] is False
    assert mode_config("zerobus")["has_metrics"] is False
    assert mode_config("microbatch")["has_metrics"] is True
    assert mode_config("microbatch")["uses_lakebase"] is False
