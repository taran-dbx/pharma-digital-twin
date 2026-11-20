"""Shared four-industry domain catalog (sites, assets, sensors, KPIs)."""

from iot_twin.domain.catalog import (
    CURATED_DOMAIN_INDUSTRIES,
    get_domain,
    list_alert_rules,
    list_assets,
    list_kpi_bindings,
    list_maintenance_history,
    list_sensors,
    list_sites,
    map_simulator_industries,
    to_record_industry,
    to_simulator_industry,
)

__all__ = [
    "CURATED_DOMAIN_INDUSTRIES",
    "get_domain",
    "list_alert_rules",
    "list_assets",
    "list_kpi_bindings",
    "list_maintenance_history",
    "list_sensors",
    "list_sites",
    "map_simulator_industries",
    "to_record_industry",
    "to_simulator_industry",
]
