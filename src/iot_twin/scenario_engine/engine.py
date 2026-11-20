"""Deterministic scenario wrapper for generated sensor records."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from iot_twin.domain.catalog import (
    get_domain,
    list_alert_rules,
    sensor_by_name,
    to_record_industry,
)
from iot_twin.scenario_engine.cadence import CadenceClock

PHASES: tuple[str, ...] = ("normal", "degraded", "failed", "recovering")
DEFAULT_PHASE_SECONDS = 60.0

# Correlated multipliers applied to catalog sensor names by phase.
# Values > 1 raise the reading; values < 1 suppress it (e.g. power sag).
_PHASE_EFFECTS: dict[str, dict[str, dict[str, float]]] = {
    "manufacturing": {
        "degraded": {
            "cnc_1_spindle_temp": 1.08,
            "cnc_1_vibration": 1.4,
            "cnc_1_spindle_power": 0.92,
        },
        "failed": {
            "cnc_1_spindle_temp": 1.3,
            "cnc_1_vibration": 2.2,
            "cnc_1_spindle_power": 0.65,
        },
        "recovering": {
            "cnc_1_spindle_temp": 1.12,
            "cnc_1_vibration": 1.2,
            "cnc_1_spindle_power": 0.88,
        },
    },
    "energy": {
        "degraded": {
            "gt_1_exhaust_temp": 1.08,
            "gt_1_vibration": 1.3,
            "gt_1_power_output": 0.9,
        },
        "failed": {
            "gt_1_exhaust_temp": 1.25,
            "gt_1_vibration": 2.0,
            "gt_1_power_output": 0.55,
        },
        "recovering": {
            "gt_1_exhaust_temp": 1.1,
            "gt_1_vibration": 1.15,
            "gt_1_power_output": 0.85,
        },
    },
    "pharmaceutical": {
        "degraded": {"bioreactor_1_temp": 1.03, "bioreactor_1_ph": 0.97},
        "failed": {"bioreactor_1_temp": 1.12, "bioreactor_1_ph": 0.9},
        "recovering": {"bioreactor_1_temp": 1.04, "bioreactor_1_ph": 0.98},
    },
    "automotive": {
        "degraded": {"robot_1_weld_current": 1.08, "paint_booth_humidity": 1.1},
        "failed": {"robot_1_weld_current": 1.25, "paint_booth_humidity": 1.2},
        "recovering": {"robot_1_weld_current": 1.05, "paint_booth_humidity": 1.05},
    },
}


def _get(record: Any, key: str, default: Any = None) -> Any:
    if isinstance(record, dict):
        return record.get(key, default)
    return getattr(record, key, default)


def _set(record: Any, key: str, value: Any) -> None:
    if isinstance(record, dict):
        record[key] = value
    else:
        setattr(record, key, value)


class ScenarioEngine:
    """Wraps generated records with seeded phases and catalog metadata."""

    def __init__(
        self,
        industry: str,
        seed: int = 0,
        scenario_id: str = "default",
        phase_seconds: float = DEFAULT_PHASE_SECONDS,
    ) -> None:
        self.domain = get_domain(industry)
        self.industry = industry
        self.seed = int(seed)
        self.scenario_id = scenario_id
        self.phase_seconds = phase_seconds
        self._alerts = {rule.sensor_id: rule for rule in list_alert_rules(industry)}
        self.clock = CadenceClock(industry)
        self.emitted_count = 0
        self.dropped_count = 0

    def cadence_metrics(self) -> dict[str, int]:
        """Emitted vs cadence-dropped record counts for this engine instance."""
        return {"emitted": self.emitted_count, "dropped": self.dropped_count}

    def phase_at(self, timestamp: float) -> str:
        """Return the deterministic phase for ``timestamp``."""
        offset = (self.seed % 17) * 13
        slot = int((timestamp + offset) / self.phase_seconds) % len(PHASES)
        return PHASES[slot]

    def wrap_records(self, records: Sequence[Any] | Iterable[Any]) -> list[Any]:
        """Attach metadata and apply correlated phase effects in place.

        Records that are not due on the catalog ``CadenceClock`` are dropped
        so emitted ticks respect per-sensor cadence.
        """
        wrapped: list[Any] = []
        for record in records:
            name = str(_get(record, "sensor_name", "") or "")
            timestamp = float(_get(record, "timestamp", 0.0) or 0.0)
            if not self.clock.due(name, timestamp):
                self.dropped_count += 1
                continue
            self.emitted_count += 1
            wrapped.append(self._wrap_one(record))
        return wrapped

    def _wrap_one(self, record: Any) -> Any:
        name = str(_get(record, "sensor_name", "") or "")
        timestamp = float(_get(record, "timestamp", 0.0) or 0.0)
        phase = self.phase_at(timestamp)
        sensor = sensor_by_name(self.industry, name)

        metadata = dict(_get(record, "metadata") or {})
        if sensor is not None:
            metadata["site_id"] = sensor.site_id
            metadata["asset_id"] = sensor.asset_id
            metadata["component_id"] = sensor.component_id
        else:
            site = self.domain.sites[0]
            metadata["site_id"] = site.site_id
            metadata["asset_id"] = site.assets[0].asset_id
            metadata["component_id"] = site.assets[0].components[0].component_id
        metadata["scenario_id"] = self.scenario_id
        metadata["phase"] = phase

        value = float(_get(record, "value", 0.0) or 0.0)
        effects = _PHASE_EFFECTS.get(self.industry, {}).get(phase, {})
        if name in effects:
            value = value * effects[name]
            _set(record, "value", value)

        fault_code = ""
        fault_active = bool(_get(record, "fault_active", False))
        if phase != "normal" and sensor is not None:
            rule = self._alerts.get(sensor.sensor_id)
            if rule is not None:
                fault_code = rule.fault_code
            elif effects:
                fault_code = f"{self.industry[:1].upper()}-SCENARIO-{phase.upper()}"
            if name in effects:
                fault_active = True
        metadata["fault_code"] = fault_code
        _set(record, "fault_active", fault_active)
        _set(record, "metadata", metadata)
        _set(record, "industry", to_record_industry(self.industry))
        return record
