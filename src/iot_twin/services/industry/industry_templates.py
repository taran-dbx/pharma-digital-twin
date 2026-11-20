"""Curated industry templates and streaming-mode metadata.

This is a straight Python port of the ``IND``, ``MODES``, and ``MODEMETA``
data objects embedded in the original design reference
(kept outside this repo; ``<script type="text/x-dc">``
block). It is the single source of truth for the four curated industries the
gallery/setup UI presents, and for the three streaming modes (realtime,
microbatch, zerobus) a pipeline can run in.

Values here are transcribed from the design where the design gives exact
strings (``name``, ``accent``, ``tag``, ``lede``, mode ``desc``/``to``).
KPI keys may be normalized when they are also frontend live-data contracts.
``sensors`` is derived from each industry's ``stream`` array in the design —
the first element of every ``[name, value, unit, alert]`` tuple.
"""

from __future__ import annotations

from dataclasses import dataclass, field

CURATED_INDUSTRIES: list[str] = ["manufacturing", "pharmaceutical", "energy", "automotive"]

MODES: list[str] = ["realtime", "microbatch", "zerobus"]


@dataclass(frozen=True)
class IndustryTemplate:
    id: str
    name: str
    accent: str
    icon: str
    twin_type: str
    tag: str
    lede: str
    sensors: list[str] = field(default_factory=list)
    kpis: list[dict] = field(default_factory=list)
    default_hz: int = 0


# Ported verbatim from IND in the original design reference.
_TEMPLATES: dict[str, IndustryTemplate] = {
    "manufacturing": IndustryTemplate(
        id="manufacturing",
        name="Manufacturing",
        accent="#e86247",
        icon="factory",
        twin_type="line",
        tag="Assembly line · CNC · robotics",
        lede=(
            "Real-time visibility into your production line — station "
            "throughput, spindle load, OEE and fault detection across the "
            "floor."
        ),
        # Real simulator sensor names (SSE / twin overlay keys).
        sensors=[
            "cnc_1_spindle_speed",
            "assy_line_1_speed",
            "cnc_1_vibration",
            "cnc_1_coolant_temp",
            "press_1_pressure",
            "compressor_1_pressure",
        ],
        # Every tile is backed by a live reading or the alert rail; "—" until
        # the first SSE event arrives. No derived production metrics.
        kpis=[
            {"k": "Spindle 3 temperature", "v": "—", "u": "°C"},
            {"k": "Operating limit", "v": "—", "u": "°C"},
            {"k": "Spindle 3 status", "v": "—", "u": ""},
            {"k": "Open alerts", "v": "—", "u": ""},
            {"k": "Last device event", "v": "—", "u": ""},
        ],
        default_hz=40,
    ),
    "pharmaceutical": IndustryTemplate(
        id="pharmaceutical",
        name="Pharmaceutical",
        accent="#2db0a0",
        icon="pharma",
        twin_type="vessel",
        tag="Bioreactor · WFI · batch yield",
        lede=(
            "Process integrity for regulated production — reactor "
            "temperature, pressure, WFI flow and batch yield with "
            "tamper-evident history."
        ),
        # Real simulator sensor names (the SSE feed keys the live stream + KPI
        # tiles by these). Kept in sync with KPI_SENSOR_MAP in
        # `client/src/pages/mode-dashboard-page.tsx`.
        sensors=[
            "bioreactor_1_temp",
            "bioreactor_1_ph",
            "bioreactor_1_dissolved_oxygen",
            "cleanroom_1_pressure",
            "tablet_weight",
            "autoclave_chamber_temp",
        ],
        # Curated KPI tiles — each label maps (frontend KPI_SENSOR_MAP) to the
        # real sensor_name above so the tile resolves its live value. `v` is the
        # pre-power placeholder shown before the pipeline is running.
        kpis=[
            {"k": "Bioreactor temp", "v": "37.0", "u": "°C"},
            {"k": "Bioreactor pH", "v": "7.15", "u": "pH"},
            {"k": "Dissolved O₂", "v": "39", "u": "%"},
            {"k": "Cleanroom ΔP", "v": "12", "u": "Pa"},
            {"k": "Tablet weight", "v": "100", "u": "mg"},
            {"k": "Autoclave temp", "v": "83", "u": "°C"},
        ],
        default_hz=25,
    ),
    "energy": IndustryTemplate(
        id="energy",
        name="Energy",
        accent="#6cbf3c",
        icon="energy",
        twin_type="grid",
        tag="Turbines · grid load · substations",
        lede=(
            "Grid and generation health — turbine output, frequency "
            "stability, transformer load and demand across substations."
        ),
        # Real simulator sensor names for the electric_power industry
        # (curated id remains ``energy`` on persisted records).
        sensors=[
            "gt_1_power_output",
            "gen_1_frequency",
            "transformer_1_load",
            "gen_1_voltage",
            "st_1_power_output",
            "gen_1_bearing_temp",
        ],
        kpis=[
            {"k": "Grid output", "v": "842", "u": "MW"},
            {"k": "Frequency", "v": "50.01", "u": "Hz"},
            {"k": "Turbine RPM", "v": "3,000", "u": ""},
            {"k": "Xfmr load", "v": "78", "u": "%", "s": "warn"},
            {"k": "Faults", "v": "0", "u": ""},
        ],
        default_hz=60,
    ),
    "automotive": IndustryTemplate(
        id="automotive",
        name="Automotive",
        accent="#5b7be8",
        icon="auto",
        twin_type="auto",
        tag="Body shop · paint · powertrain test",
        lede=(
            "Connected assembly and test — weld quality, paint-booth "
            "climate, torque verification and end-of-line diagnostics per "
            "vehicle."
        ),
        sensors=[
            "robot_1_weld_current",
            "torque_wrench_1_torque",
            "paint_booth_humidity",
            "paint_booth_temp",
            "assembly_line_speed",
            "agv_1_battery_level",
        ],
        kpis=[
            {"k": "Vehicles/hr", "v": "58", "u": ""},
            {"k": "First-pass yield", "v": "94.2", "u": "%"},
            {"k": "Weld current", "v": "11.4", "u": "kA"},
            {"k": "Booth humidity", "v": "61", "u": "%", "s": "warn"},
            {"k": "Rework flags", "v": "1", "u": ""},
        ],
        default_hz=50,
    ),
}


# Ported verbatim from MODES in the original design reference.
_MODES: dict[str, dict] = {
    "realtime": {
        "id": "realtime",
        "name": "Real-time",
        "to": "Lakebase + LakeTS",
        "desc": (
            "Spark Structured Streaming → Lakebase, served as time-series "
            "(hot) with CDF sync to Delta for cold history."
        ),
        "uses_kafka": True,
        "uses_lakebase": True,
        "has_metrics": True,
    },
    "microbatch": {
        "id": "microbatch",
        "name": "Micro-batch",
        "to": "Delta table",
        "desc": ("Spark Structured Streaming in micro-batches → a Delta table in Unity Catalog."),
        "uses_kafka": True,
        "uses_lakebase": False,
        "has_metrics": True,
    },
    "zerobus": {
        "id": "zerobus",
        "name": "Zerobus",
        "to": "Delta (direct)",
        "desc": "Zerobus SDK writes straight to a Delta table — no Spark streaming job.",
        "uses_kafka": False,
        "uses_lakebase": False,
        "has_metrics": False,
    },
}


def get_template(industry: str) -> IndustryTemplate | None:
    """Return the curated template for ``industry``, or None if unknown."""
    return _TEMPLATES.get(industry)


def mode_config(mode: str) -> dict:
    """Return the streaming-mode metadata for ``mode`` (realtime/microbatch/zerobus)."""
    return dict(_MODES[mode])
