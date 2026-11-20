"""Repository-owned scenario wrapper around iot_simulator records.

This package does not unpack or mutate the bundled simulator wheel. It
wraps already-generated records with deterministic phases, catalog
metadata, and per-sensor cadence helpers for notebook templates.
"""

from iot_twin.scenario_engine.cadence import CadenceClock, cadence_due
from iot_twin.scenario_engine.engine import ScenarioEngine

__all__ = [
    "CadenceClock",
    "ScenarioEngine",
    "cadence_due",
]
