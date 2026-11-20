"""Tests for the shared four-industry domain catalog [qa].

The catalog is the maintainable source of truth for stable site/asset/
component/sensor IDs, simulator aliases, KPI bindings, alert rules, and
lightweight maintenance fixtures.
"""

from __future__ import annotations

import pytest

from iot_twin.domain.catalog import (
    CURATED_DOMAIN_INDUSTRIES,
    get_domain,
    list_alert_rules,
    list_assets,
    list_kpi_bindings,
    list_maintenance_history,
    list_sensors,
    list_sites,
    to_record_industry,
    to_simulator_industry,
)


class TestCuratedDomainCatalog:
    def test_four_industries_are_registered_c7301_smoke(self) -> None:
        """[C7301][smoke] Catalog covers manufacturing, pharmaceutical, energy, automotive."""
        assert CURATED_DOMAIN_INDUSTRIES == (
            "manufacturing",
            "pharmaceutical",
            "energy",
            "automotive",
        )
        for industry in CURATED_DOMAIN_INDUSTRIES:
            assert get_domain(industry).industry_id == industry

    def test_unknown_industry_raises_c7301_defect(self) -> None:
        """[C7301][defect] Unknown industry ids fail closed."""
        with pytest.raises(KeyError):
            get_domain("banking")
        with pytest.raises(KeyError):
            get_domain("")

    def test_energy_maps_to_electric_power_but_records_stay_energy_c7302_regression(
        self,
    ) -> None:
        """[C7302][regression] Simulator alias is electric_power; records stay energy."""
        domain = get_domain("energy")
        assert domain.simulator_industry == "electric_power"
        assert to_simulator_industry("energy") == "electric_power"
        assert to_simulator_industry("manufacturing") == "manufacturing"
        assert to_record_industry("energy") == "energy"
        assert to_record_industry("electric_power") == "energy"
        assert to_record_industry("pharmaceutical") == "pharmaceutical"

    def test_stable_hierarchy_ids_are_unique_and_linked_c7303_regression(self) -> None:
        """[C7303][regression] Site/asset/component/sensor IDs are stable and unique."""
        for industry in CURATED_DOMAIN_INDUSTRIES:
            sites = list_sites(industry)
            assert sites, industry
            site_ids = [site.site_id for site in sites]
            assert len(site_ids) == len(set(site_ids))
            assets = list_assets(industry)
            assert assets, industry
            asset_ids = [asset.asset_id for asset in assets]
            assert len(asset_ids) == len(set(asset_ids))
            sensors = list_sensors(industry)
            assert sensors, industry
            sensor_ids = [sensor.sensor_id for sensor in sensors]
            assert len(sensor_ids) == len(set(sensor_ids))
            site_id_set = set(site_ids)
            asset_by_id = {asset.asset_id: asset for asset in assets}
            for asset in assets:
                assert asset.site_id in site_id_set
                assert asset.components
            for sensor in sensors:
                asset = asset_by_id[sensor.asset_id]
                component_ids = {component.component_id for component in asset.components}
                assert sensor.component_id in component_ids
                assert sensor.site_id == asset.site_id
                assert sensor.name
                assert " " not in sensor.name

    def test_known_manufacturing_ids_are_stable_c7303_smoke(self) -> None:
        """[C7303][smoke] Manufacturing IDs do not churn across refactors."""
        sensors = {sensor.sensor_id: sensor for sensor in list_sensors("manufacturing")}
        assert "mfg-sns-cnc_1_spindle_temp" in sensors
        spindle = sensors["mfg-sns-cnc_1_spindle_temp"]
        assert spindle.name == "cnc_1_spindle_temp"
        assert spindle.site_id == "mfg-site-detroit"
        assert spindle.asset_id == "mfg-asset-cnc-1"
        assert spindle.component_id == "mfg-comp-spindle"

    def test_energy_sensors_use_electric_power_names_c7302_smoke(self) -> None:
        """[C7302][smoke] Energy sensors bind to electric_power simulator names."""
        names = {sensor.name for sensor in list_sensors("energy")}
        assert "gt_1_power_output" in names
        assert "gen_1_frequency" in names
        assert all(not name.startswith("Turbine") for name in names)

    def test_kpi_bindings_point_at_catalog_sensors_c7304_regression(self) -> None:
        """[C7304][regression] KPI bindings resolve to catalog sensor names."""
        for industry in CURATED_DOMAIN_INDUSTRIES:
            sensor_names = {sensor.name for sensor in list_sensors(industry)}
            bindings = list_kpi_bindings(industry)
            assert bindings, industry
            for binding in bindings:
                assert binding.sensor_name in sensor_names
                assert binding.kpi_id
                assert binding.label

    def test_alert_rules_carry_fault_codes_c7305_regression(self) -> None:
        """[C7305][regression] Alert rules bind sensors to stable fault codes."""
        for industry in CURATED_DOMAIN_INDUSTRIES:
            sensor_ids = {sensor.sensor_id for sensor in list_sensors(industry)}
            rules = list_alert_rules(industry)
            assert rules, industry
            for rule in rules:
                assert rule.sensor_id in sensor_ids
                assert rule.fault_code
                assert rule.operator in {"gt", "lt", "gte", "lte"}
                assert rule.severity in {"info", "warn", "crit"}

    def test_maintenance_history_is_lightweight_fixture_data_c7306_smoke(self) -> None:
        """[C7306][smoke] Maintenance fixtures hang off catalog assets."""
        for industry in CURATED_DOMAIN_INDUSTRIES:
            asset_ids = {asset.asset_id for asset in list_assets(industry)}
            history = list_maintenance_history(industry)
            assert history, industry
            assert len(history) <= 8
            for event in history:
                assert event.asset_id in asset_ids
                assert event.event_id
                assert event.started_at
                assert event.kind in {"planned", "unplanned", "inspection"}
