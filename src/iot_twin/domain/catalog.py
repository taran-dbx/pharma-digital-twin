"""Maintainable four-industry domain catalog.

Stable site/asset/component/sensor IDs, simulator aliases, KPI bindings,
alert-rule definitions, and lightweight maintenance fixtures. Sensor
``name`` values match the iot_simulator catalog (energy uses the
``electric_power`` simulator industry while record ``industry`` stays
``energy``).
"""

from __future__ import annotations

from dataclasses import dataclass

CURATED_DOMAIN_INDUSTRIES: tuple[str, ...] = (
    "manufacturing",
    "pharmaceutical",
    "energy",
    "automotive",
)

_SIMULATOR_ALIASES: dict[str, str] = {
    "energy": "electric_power",
}

_RECORD_INDUSTRY: dict[str, str] = {
    "electric_power": "energy",
    "energy": "energy",
}


@dataclass(frozen=True)
class SensorNode:
    sensor_id: str
    name: str
    site_id: str
    asset_id: str
    component_id: str
    cadence_hz: float = 1.0


@dataclass(frozen=True)
class ComponentNode:
    component_id: str
    name: str
    sensors: tuple[SensorNode, ...]


@dataclass(frozen=True)
class AssetNode:
    asset_id: str
    site_id: str
    name: str
    components: tuple[ComponentNode, ...]


@dataclass(frozen=True)
class SiteNode:
    site_id: str
    name: str
    assets: tuple[AssetNode, ...]


@dataclass(frozen=True)
class KpiBinding:
    kpi_id: str
    label: str
    sensor_name: str
    unit: str


@dataclass(frozen=True)
class AlertRule:
    rule_id: str
    sensor_id: str
    operator: str
    threshold: float
    severity: str
    fault_code: str
    duration_s: float = 0.0
    hysteresis: float = 0.0
    correlate_sensor_ids: tuple[str, ...] = ()
    correlate_operator: str = "gt"
    correlate_threshold: float | None = None
    action: str = ""


@dataclass(frozen=True)
class MaintenanceEvent:
    event_id: str
    asset_id: str
    component_id: str
    kind: str
    started_at: str
    completed_at: str | None
    notes: str


@dataclass(frozen=True)
class IndustryDomain:
    industry_id: str
    simulator_industry: str
    sites: tuple[SiteNode, ...]
    kpi_bindings: tuple[KpiBinding, ...]
    alert_rules: tuple[AlertRule, ...]
    maintenance_history: tuple[MaintenanceEvent, ...]


def to_simulator_industry(industry: str) -> str:
    """Map a curated industry id to the iot_simulator IndustryType value."""
    return _SIMULATOR_ALIASES.get(industry, industry)


def to_record_industry(industry: str) -> str:
    """Map a simulator or curated id onto the persisted record industry."""
    return _RECORD_INDUSTRY.get(industry, industry)


def map_simulator_industries(industries: list[str]) -> list[str]:
    """Map a list of curated ids to simulator industry names."""
    return [to_simulator_industry(item.strip()) for item in industries if item.strip()]


def _sns(
    prefix: str,
    name: str,
    site_id: str,
    asset_id: str,
    component_id: str,
    hz: float = 1.0,
) -> SensorNode:
    return SensorNode(
        sensor_id=f"{prefix}-sns-{name}",
        name=name,
        site_id=site_id,
        asset_id=asset_id,
        component_id=component_id,
        cadence_hz=hz,
    )


def _manufacturing() -> IndustryDomain:
    site = "mfg-site-detroit"
    cnc = "mfg-asset-cnc-1"
    assy = "mfg-asset-assy-1"
    press = "mfg-asset-press-1"
    compressor = "mfg-asset-compressor-1"
    spindle = "mfg-comp-spindle"
    return IndustryDomain(
        industry_id="manufacturing",
        simulator_industry="manufacturing",
        sites=(
            SiteNode(
                site_id=site,
                name="Detroit Plant",
                assets=(
                    AssetNode(
                        asset_id=cnc,
                        site_id=site,
                        name="CNC-1",
                        components=(
                            ComponentNode(
                                component_id=spindle,
                                name="Spindle 3",
                                sensors=(
                                    _sns("mfg", "cnc_1_spindle_speed", site, cnc, spindle, 10.0),
                                    _sns("mfg", "cnc_1_spindle_temp", site, cnc, spindle, 5.0),
                                    _sns("mfg", "cnc_1_spindle_power", site, cnc, spindle, 10.0),
                                    _sns("mfg", "cnc_1_vibration", site, cnc, spindle, 20.0),
                                    _sns("mfg", "cnc_1_coolant_flow", site, cnc, spindle, 2.0),
                                    _sns("mfg", "cnc_1_coolant_temp", site, cnc, spindle, 2.0),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=assy,
                        site_id=site,
                        name="Assembly line 1",
                        components=(
                            ComponentNode(
                                component_id="mfg-comp-line-drive",
                                name="Line drive",
                                sensors=(
                                    _sns(
                                        "mfg",
                                        "assy_line_1_speed",
                                        site,
                                        assy,
                                        "mfg-comp-line-drive",
                                        5.0,
                                    ),
                                    _sns(
                                        "mfg",
                                        "assy_line_1_motor_current",
                                        site,
                                        assy,
                                        "mfg-comp-line-drive",
                                        5.0,
                                    ),
                                    _sns(
                                        "mfg",
                                        "assy_line_1_position",
                                        site,
                                        assy,
                                        "mfg-comp-line-drive",
                                        5.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=press,
                        site_id=site,
                        name="Hydraulic press 1",
                        components=(
                            ComponentNode(
                                component_id="mfg-comp-hydraulics",
                                name="Hydraulics",
                                sensors=(
                                    _sns(
                                        "mfg",
                                        "press_1_pressure",
                                        site,
                                        press,
                                        "mfg-comp-hydraulics",
                                        2.0,
                                    ),
                                    _sns(
                                        "mfg",
                                        "press_1_hydraulic_oil_temp",
                                        site,
                                        press,
                                        "mfg-comp-hydraulics",
                                        1.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=compressor,
                        site_id=site,
                        name="Air compressor 1",
                        components=(
                            ComponentNode(
                                component_id="mfg-comp-air-end",
                                name="Air end",
                                sensors=(
                                    _sns(
                                        "mfg",
                                        "compressor_1_pressure",
                                        site,
                                        compressor,
                                        "mfg-comp-air-end",
                                        2.0,
                                    ),
                                    _sns(
                                        "mfg",
                                        "compressor_1_motor_temp",
                                        site,
                                        compressor,
                                        "mfg-comp-air-end",
                                        1.0,
                                    ),
                                    _sns(
                                        "mfg",
                                        "compressor_1_flow",
                                        site,
                                        compressor,
                                        "mfg-comp-air-end",
                                        2.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        kpi_bindings=(
            KpiBinding("mfg-kpi-temp", "Spindle 3 temperature", "cnc_1_spindle_temp", "°C"),
            KpiBinding("mfg-kpi-vibration", "Spindle 3 vibration", "cnc_1_vibration", "mm/s"),
            KpiBinding("mfg-kpi-spindle-speed", "Spindle 3 speed", "cnc_1_spindle_speed", "RPM"),
            KpiBinding("mfg-kpi-line-speed", "Pack line speed", "assy_line_1_speed", "m/min"),
        ),
        alert_rules=(
            AlertRule(
                "mfg-alert-spindle-temp",
                "mfg-sns-cnc_1_spindle_temp",
                "gt",
                60.0,
                "crit",
                "M-SPINDLE-OVERTEMP",
                duration_s=2.0,
                hysteresis=5.0,
                action="Inspection required",
            ),
            AlertRule(
                "mfg-alert-vibration",
                "mfg-sns-cnc_1_vibration",
                "gt",
                8.0,
                "warn",
                "M-SPINDLE-VIBE",
            ),
            AlertRule(
                "mfg-alert-press", "mfg-sns-press_1_pressure", "lt", 120.0, "warn", "M-PRESS-LOW"
            ),
        ),
        maintenance_history=(
            MaintenanceEvent(
                "mfg-mnt-001",
                cnc,
                spindle,
                "inspection",
                "2026-09-01T08:00:00Z",
                "2026-09-01T12:00:00Z",
                "Spindle bearing inspection",
            ),
            MaintenanceEvent(
                "mfg-mnt-002",
                press,
                "mfg-comp-hydraulics",
                "planned",
                "2026-09-12T06:00:00Z",
                "2026-09-12T10:00:00Z",
                "Hydraulic oil change",
            ),
        ),
    )


def _pharmaceutical() -> IndustryDomain:
    site = "pharma-site-boston"
    reactor = "pharma-asset-bioreactor-1"
    cleanroom = "pharma-asset-cleanroom-1"
    tablet = "pharma-asset-tablet-line"
    autoclave = "pharma-asset-autoclave-1"
    vessel = "pharma-comp-vessel"
    return IndustryDomain(
        industry_id="pharmaceutical",
        simulator_industry="pharmaceutical",
        sites=(
            SiteNode(
                site_id=site,
                name="Boston Fill/Finish",
                assets=(
                    AssetNode(
                        asset_id=reactor,
                        site_id=site,
                        name="Bioreactor 1",
                        components=(
                            ComponentNode(
                                component_id=vessel,
                                name="Vessel",
                                sensors=(
                                    _sns("pharma", "bioreactor_1_temp", site, reactor, vessel, 1.0),
                                    _sns("pharma", "bioreactor_1_ph", site, reactor, vessel, 0.5),
                                    _sns(
                                        "pharma",
                                        "bioreactor_1_dissolved_oxygen",
                                        site,
                                        reactor,
                                        vessel,
                                        1.0,
                                    ),
                                    _sns(
                                        "pharma",
                                        "bioreactor_1_pressure",
                                        site,
                                        reactor,
                                        vessel,
                                        1.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=cleanroom,
                        site_id=site,
                        name="Cleanroom 1",
                        components=(
                            ComponentNode(
                                component_id="pharma-comp-hvac",
                                name="HVAC",
                                sensors=(
                                    _sns(
                                        "pharma",
                                        "cleanroom_1_pressure",
                                        site,
                                        cleanroom,
                                        "pharma-comp-hvac",
                                        1.0,
                                    ),
                                    _sns(
                                        "pharma",
                                        "cleanroom_1_temp",
                                        site,
                                        cleanroom,
                                        "pharma-comp-hvac",
                                        0.5,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=tablet,
                        site_id=site,
                        name="Tablet line",
                        components=(
                            ComponentNode(
                                component_id="pharma-comp-press",
                                name="Tablet press",
                                sensors=(
                                    _sns(
                                        "pharma",
                                        "tablet_weight",
                                        site,
                                        tablet,
                                        "pharma-comp-press",
                                        5.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=autoclave,
                        site_id=site,
                        name="Autoclave 1",
                        components=(
                            ComponentNode(
                                component_id="pharma-comp-chamber",
                                name="Chamber",
                                sensors=(
                                    _sns(
                                        "pharma",
                                        "autoclave_chamber_temp",
                                        site,
                                        autoclave,
                                        "pharma-comp-chamber",
                                        1.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        kpi_bindings=(
            KpiBinding("pharma-kpi-temp", "Bioreactor temp", "bioreactor_1_temp", "°C"),
            KpiBinding("pharma-kpi-ph", "Bioreactor pH", "bioreactor_1_ph", "pH"),
            KpiBinding("pharma-kpi-do", "Dissolved O₂", "bioreactor_1_dissolved_oxygen", "%"),
            KpiBinding("pharma-kpi-dp", "Cleanroom ΔP", "cleanroom_1_pressure", "Pa"),
            KpiBinding("pharma-kpi-weight", "Tablet weight", "tablet_weight", "mg"),
            KpiBinding("pharma-kpi-auto", "Autoclave temp", "autoclave_chamber_temp", "°C"),
        ),
        alert_rules=(
            AlertRule(
                "pharma-alert-temp",
                "pharma-sns-bioreactor_1_temp",
                "gt",
                38.5,
                "crit",
                "P-BRX-TEMP",
            ),
            AlertRule(
                "pharma-alert-ph", "pharma-sns-bioreactor_1_ph", "lt", 6.8, "warn", "P-BRX-PH"
            ),
            AlertRule(
                "pharma-alert-dp", "pharma-sns-cleanroom_1_pressure", "lt", 8.0, "crit", "P-CR-DP"
            ),
        ),
        maintenance_history=(
            MaintenanceEvent(
                "pharma-mnt-001",
                reactor,
                vessel,
                "planned",
                "2026-08-20T00:00:00Z",
                "2026-08-20T18:00:00Z",
                "CIP / SIP cycle",
            ),
        ),
    )


def _energy() -> IndustryDomain:
    site = "energy-site-north-grid"
    gt = "energy-asset-gt-1"
    st = "energy-asset-st-1"
    gen = "energy-asset-gen-1"
    xfmr = "energy-asset-transformer-1"
    turbine = "energy-comp-turbine"
    return IndustryDomain(
        industry_id="energy",
        simulator_industry="electric_power",
        sites=(
            SiteNode(
                site_id=site,
                name="North Grid Plant",
                assets=(
                    AssetNode(
                        asset_id=gt,
                        site_id=site,
                        name="Gas turbine 1",
                        components=(
                            ComponentNode(
                                component_id=turbine,
                                name="Turbine",
                                sensors=(
                                    _sns("energy", "gt_1_speed", site, gt, turbine, 10.0),
                                    _sns("energy", "gt_1_power_output", site, gt, turbine, 5.0),
                                    _sns("energy", "gt_1_exhaust_temp", site, gt, turbine, 5.0),
                                    _sns("energy", "gt_1_vibration", site, gt, turbine, 20.0),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=st,
                        site_id=site,
                        name="Steam turbine 1",
                        components=(
                            ComponentNode(
                                component_id="energy-comp-steam",
                                name="Steam path",
                                sensors=(
                                    _sns(
                                        "energy",
                                        "st_1_power_output",
                                        site,
                                        st,
                                        "energy-comp-steam",
                                        5.0,
                                    ),
                                    _sns(
                                        "energy",
                                        "st_1_steam_pressure",
                                        site,
                                        st,
                                        "energy-comp-steam",
                                        2.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=gen,
                        site_id=site,
                        name="Generator 1",
                        components=(
                            ComponentNode(
                                component_id="energy-comp-stator",
                                name="Stator",
                                sensors=(
                                    _sns(
                                        "energy",
                                        "gen_1_voltage",
                                        site,
                                        gen,
                                        "energy-comp-stator",
                                        10.0,
                                    ),
                                    _sns(
                                        "energy",
                                        "gen_1_frequency",
                                        site,
                                        gen,
                                        "energy-comp-stator",
                                        10.0,
                                    ),
                                    _sns(
                                        "energy",
                                        "gen_1_bearing_temp",
                                        site,
                                        gen,
                                        "energy-comp-stator",
                                        1.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=xfmr,
                        site_id=site,
                        name="Transformer 1",
                        components=(
                            ComponentNode(
                                component_id="energy-comp-winding",
                                name="Winding",
                                sensors=(
                                    _sns(
                                        "energy",
                                        "transformer_1_load",
                                        site,
                                        xfmr,
                                        "energy-comp-winding",
                                        1.0,
                                    ),
                                    _sns(
                                        "energy",
                                        "transformer_1_oil_temp",
                                        site,
                                        xfmr,
                                        "energy-comp-winding",
                                        0.5,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        kpi_bindings=(
            KpiBinding("energy-kpi-output", "Grid output", "gt_1_power_output", "MW"),
            KpiBinding("energy-kpi-freq", "Frequency", "gen_1_frequency", "Hz"),
            KpiBinding("energy-kpi-rpm", "Turbine RPM", "gt_1_speed", ""),
            KpiBinding("energy-kpi-xfmr", "Xfmr load", "transformer_1_load", "%"),
            KpiBinding("energy-kpi-faults", "Faults", "gt_1_vibration", ""),
        ),
        alert_rules=(
            AlertRule(
                "energy-alert-freq", "energy-sns-gen_1_frequency", "lt", 49.8, "crit", "E-FREQ-LOW"
            ),
            AlertRule(
                "energy-alert-gt-temp",
                "energy-sns-gt_1_exhaust_temp",
                "gt",
                620.0,
                "warn",
                "E-GT-EXHAUST",
            ),
            AlertRule(
                "energy-alert-xfmr",
                "energy-sns-transformer_1_load",
                "gt",
                90.0,
                "warn",
                "E-XFMR-LOAD",
            ),
        ),
        maintenance_history=(
            MaintenanceEvent(
                "energy-mnt-001",
                gt,
                turbine,
                "inspection",
                "2026-07-15T04:00:00Z",
                "2026-07-15T16:00:00Z",
                "Hot-gas-path borescope",
            ),
        ),
    )


def _automotive() -> IndustryDomain:
    site = "auto-site-stuttgart"
    robot = "auto-asset-robot-1"
    booth = "auto-asset-paint-booth"
    torque = "auto-asset-torque-1"
    line = "auto-asset-assy-line"
    agv = "auto-asset-agv-1"
    weld = "auto-comp-weld-gun"
    return IndustryDomain(
        industry_id="automotive",
        simulator_industry="automotive",
        sites=(
            SiteNode(
                site_id=site,
                name="Stuttgart Body Shop",
                assets=(
                    AssetNode(
                        asset_id=robot,
                        site_id=site,
                        name="Weld robot 1",
                        components=(
                            ComponentNode(
                                component_id=weld,
                                name="Weld gun",
                                sensors=(
                                    _sns("auto", "robot_1_weld_current", site, robot, weld, 20.0),
                                    _sns("auto", "robot_1_weld_voltage", site, robot, weld, 20.0),
                                    _sns("auto", "robot_1_motor_temp", site, robot, weld, 1.0),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=booth,
                        site_id=site,
                        name="Paint booth",
                        components=(
                            ComponentNode(
                                component_id="auto-comp-booth-hvac",
                                name="Booth HVAC",
                                sensors=(
                                    _sns(
                                        "auto",
                                        "paint_booth_temp",
                                        site,
                                        booth,
                                        "auto-comp-booth-hvac",
                                        1.0,
                                    ),
                                    _sns(
                                        "auto",
                                        "paint_booth_humidity",
                                        site,
                                        booth,
                                        "auto-comp-booth-hvac",
                                        1.0,
                                    ),
                                    _sns(
                                        "auto",
                                        "paint_booth_airflow",
                                        site,
                                        booth,
                                        "auto-comp-booth-hvac",
                                        1.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=torque,
                        site_id=site,
                        name="Torque station 7",
                        components=(
                            ComponentNode(
                                component_id="auto-comp-wrench",
                                name="Torque wrench",
                                sensors=(
                                    _sns(
                                        "auto",
                                        "torque_wrench_1_torque",
                                        site,
                                        torque,
                                        "auto-comp-wrench",
                                        10.0,
                                    ),
                                    _sns(
                                        "auto",
                                        "torque_wrench_1_angle",
                                        site,
                                        torque,
                                        "auto-comp-wrench",
                                        10.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=line,
                        site_id=site,
                        name="Assembly line",
                        components=(
                            ComponentNode(
                                component_id="auto-comp-conveyor",
                                name="Conveyor",
                                sensors=(
                                    _sns(
                                        "auto",
                                        "assembly_line_speed",
                                        site,
                                        line,
                                        "auto-comp-conveyor",
                                        5.0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    AssetNode(
                        asset_id=agv,
                        site_id=site,
                        name="AGV 1",
                        components=(
                            ComponentNode(
                                component_id="auto-comp-battery",
                                name="Battery",
                                sensors=(
                                    _sns(
                                        "auto",
                                        "agv_1_battery_level",
                                        site,
                                        agv,
                                        "auto-comp-battery",
                                        0.2,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        kpi_bindings=(
            KpiBinding("auto-kpi-vph", "Vehicles/hr", "assembly_line_speed", ""),
            KpiBinding("auto-kpi-fpy", "First-pass yield", "torque_wrench_1_torque", "%"),
            KpiBinding("auto-kpi-weld", "Weld current", "robot_1_weld_current", "kA"),
            KpiBinding("auto-kpi-rh", "Booth humidity", "paint_booth_humidity", "%"),
            KpiBinding("auto-kpi-rework", "Rework flags", "agv_1_battery_level", ""),
        ),
        alert_rules=(
            AlertRule(
                "auto-alert-weld",
                "auto-sns-robot_1_weld_current",
                "gt",
                14.0,
                "crit",
                "A-WELD-HIGH",
            ),
            AlertRule(
                "auto-alert-rh", "auto-sns-paint_booth_humidity", "gt", 70.0, "warn", "A-BOOTH-RH"
            ),
            AlertRule(
                "auto-alert-torque",
                "auto-sns-torque_wrench_1_torque",
                "lt",
                80.0,
                "warn",
                "A-TORQUE-LOW",
            ),
        ),
        maintenance_history=(
            MaintenanceEvent(
                "auto-mnt-001",
                robot,
                weld,
                "unplanned",
                "2026-09-04T13:10:00Z",
                "2026-09-04T15:40:00Z",
                "Tip dresser replacement",
            ),
        ),
    )


_DOMAINS: dict[str, IndustryDomain] = {
    "manufacturing": _manufacturing(),
    "pharmaceutical": _pharmaceutical(),
    "energy": _energy(),
    "automotive": _automotive(),
}


def get_domain(industry: str) -> IndustryDomain:
    """Return the domain catalog for a curated industry."""
    try:
        return _DOMAINS[industry]
    except KeyError:
        raise KeyError(industry) from None


def list_sites(industry: str) -> tuple[SiteNode, ...]:
    return get_domain(industry).sites


def list_assets(industry: str) -> tuple[AssetNode, ...]:
    assets: list[AssetNode] = []
    for site in get_domain(industry).sites:
        assets.extend(site.assets)
    return tuple(assets)


def list_sensors(industry: str) -> tuple[SensorNode, ...]:
    sensors: list[SensorNode] = []
    for asset in list_assets(industry):
        for component in asset.components:
            sensors.extend(component.sensors)
    return tuple(sensors)


def list_kpi_bindings(industry: str) -> tuple[KpiBinding, ...]:
    return get_domain(industry).kpi_bindings


def list_alert_rules(industry: str) -> tuple[AlertRule, ...]:
    return get_domain(industry).alert_rules


def list_maintenance_history(industry: str) -> tuple[MaintenanceEvent, ...]:
    return get_domain(industry).maintenance_history


def sensor_by_name(industry: str, name: str) -> SensorNode | None:
    """Return the catalog sensor for ``name``, or None if unknown."""
    for sensor in list_sensors(industry):
        if sensor.name == name:
            return sensor
    return None


def assets_payload(industry: str) -> dict:
    """JSON-ready hierarchy for the read-only assets API."""
    domain = get_domain(industry)
    return {
        "industry": domain.industry_id,
        "simulator_industry": domain.simulator_industry,
        "record_industry": to_record_industry(domain.industry_id),
        "sites": [
            {
                "site_id": site.site_id,
                "name": site.name,
                "assets": [
                    {
                        "asset_id": asset.asset_id,
                        "name": asset.name,
                        "components": [
                            {
                                "component_id": component.component_id,
                                "name": component.name,
                                "sensors": [
                                    {
                                        "sensor_id": sensor.sensor_id,
                                        "name": sensor.name,
                                        "cadence_hz": sensor.cadence_hz,
                                    }
                                    for sensor in component.sensors
                                ],
                            }
                            for component in asset.components
                        ],
                    }
                    for asset in site.assets
                ],
            }
            for site in domain.sites
        ],
    }


def reference_payload(industry: str) -> dict:
    """JSON-ready KPI, alert, and maintenance fixtures."""
    domain = get_domain(industry)
    return {
        "industry": domain.industry_id,
        "kpis": [
            {
                "kpi_id": kpi.kpi_id,
                "label": kpi.label,
                "sensor_name": kpi.sensor_name,
                "unit": kpi.unit,
            }
            for kpi in domain.kpi_bindings
        ],
        "alert_rules": [
            {
                "rule_id": rule.rule_id,
                "sensor_id": rule.sensor_id,
                "operator": rule.operator,
                "threshold": rule.threshold,
                "severity": rule.severity,
                "fault_code": rule.fault_code,
                "hysteresis": rule.hysteresis,
                "action": rule.action,
            }
            for rule in domain.alert_rules
        ],
        "maintenance_history": [
            {
                "event_id": event.event_id,
                "asset_id": event.asset_id,
                "component_id": event.component_id,
                "kind": event.kind,
                "started_at": event.started_at,
                "completed_at": event.completed_at,
                "notes": event.notes,
            }
            for event in domain.maintenance_history
        ],
    }
