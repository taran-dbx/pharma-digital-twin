"""Medical-device recast of the manufacturing industry [qa].

The internal id stays ``manufacturing``; the catalog, lot clock and alert
metadata tell Trinity Medical's orthopaedic-implant machining story.
"""

from __future__ import annotations

import importlib
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from iot_twin.domain import lot_context
from iot_twin.domain.catalog import (
    list_alert_rules,
    list_assets,
    list_sensors,
    list_sites,
    reference_payload,
    rule_metadata,
)
from iot_twin.models.sensor_schema import SENSOR_FIELD_MAP
from iot_twin.scenario_engine import ScenarioEngine
from iot_twin.services.alerts.alert_engine import AlertEngine


def _rule(rule_id: str):
    return next(r for r in list_alert_rules("manufacturing") if r.rule_id == rule_id)


class TestMedtechCatalog:
    def test_site_and_line_read_as_trinity_medical_c7802_smoke(self) -> None:
        """[C7802][smoke] Site and assets name the implant line; IDs stay stable."""
        (site,) = list_sites("manufacturing")
        assert site.site_id == "mfg-site-detroit"
        assert site.name == "Trinity Medical · Warsaw, IN"
        names = {a.asset_id: a.name for a in list_assets("manufacturing")}
        assert names == {
            "mfg-asset-cnc-1": "CNC-1 5-axis mill",
            "mfg-asset-cmm-1": "CMM-1 metrology lab",
            "mfg-asset-assy-1": "Sterile pack line",
            "mfg-asset-press-1": "Clean & passivation line",
            "mfg-asset-compressor-1": "Clean dry air compressor",
        }
        cnc = next(a for a in list_assets("manufacturing") if a.asset_id == "mfg-asset-cnc-1")
        assert cnc.components[0].name == "Spindle 3"

    def test_cmm_lab_uses_existing_simulator_sensors_c7802_regression(self) -> None:
        """[C7802][regression] The CMM asset reuses simulator sensors; no new schema columns."""
        cmm = {s.name for s in list_sensors("manufacturing") if s.asset_id == "mfg-asset-cmm-1"}
        assert cmm == {"hvac_zone_1_temp", "hvac_zone_1_humidity"}
        columns = {spec.column for spec in SENSOR_FIELD_MAP}
        assert "lot_id" not in columns and "part_serial" not in columns

    def test_spindle_limit_is_a_validated_process_parameter_c7803_smoke(self) -> None:
        """[C7803][smoke] The 60 °C rule carries OQ/PQ references and quality wording."""
        rule = _rule("mfg-alert-spindle-temp")
        assert rule.threshold == 60.0
        assert rule.fault_code == "VPP-SPINDLE3-TEMP"
        meta = rule_metadata(rule)
        assert meta["parameter_type"] == "validated_process_parameter"
        assert meta["oq_ref"] == "OQ-TM-CNC1-014"
        assert meta["pq_ref"] == "PQ-TM-HS12-022"
        assert "quarantine" in rule.action.lower()
        assert "NCR" in rule.action

    def test_fault_codes_use_device_quality_language_c7803_regression(self) -> None:
        """[C7803][regression] No generic M-* fault codes remain on the implant line."""
        codes = {r.fault_code for r in list_alert_rules("manufacturing")}
        assert codes == {"VPP-SPINDLE3-TEMP", "SPC-SPINDLE3-VIBE", "CLN-PASSIVATION-PRESS-LOW"}

    def test_reference_payload_exposes_rule_metadata_c7803_regression(self) -> None:
        """[C7803][regression] The read-only reference API ships validation refs to the UI."""
        rules = {r["rule_id"]: r for r in reference_payload("manufacturing")["alert_rules"]}
        assert rules["mfg-alert-spindle-temp"]["metadata"]["oq_ref"] == "OQ-TM-CNC1-014"
        energy = reference_payload("energy")["alert_rules"]
        assert all(r["metadata"] == {} for r in energy)


def test_default_industries_lead_with_medical_devices_c7801_regression(monkeypatch) -> None:
    """[C7801][regression] Without INDUSTRIES set, the med-device industry is offered first."""
    from iot_twin.core import config

    monkeypatch.delenv("INDUSTRIES", raising=False)
    try:
        reloaded = importlib.reload(config)
        assert reloaded.INDUSTRIES.split(",") == ["manufacturing", "pharmaceutical"]
    finally:
        importlib.reload(config)


class TestLotClock:
    def test_one_lot_per_scenario_cycle_c7810_smoke(self) -> None:
        """[C7810][smoke] Lots roll every 300 s and parts every 20 s."""
        base = 1_800_000_000 - (1_800_000_000 % lot_context.LOT_CYCLE_S)
        ctx = lot_context.lot_context_at(base)
        assert ctx["part_serial"] == f"{ctx['lot_id']}-01"
        assert ctx["dhr_ref"] == f"DHR-{ctx['lot_id']}"
        assert ctx["part_number"] == "HS-TI64-12"
        assert ctx["udi_di"] == "00850012345671"
        assert lot_context.part_index_at(base + 299) == lot_context.PARTS_PER_LOT == 15
        assert lot_context.lot_id_at(base + 300) != ctx["lot_id"]

    def test_udi_di_has_a_valid_gtin_check_digit_c7810_regression(self) -> None:
        """[C7810][regression] The fictitious UDI-DI is still a well-formed GTIN-14."""
        digits = lot_context.UDI_DI
        assert len(digits) == 14
        body = digits[:-1]
        total = sum(int(c) * (3 if i % 2 == 0 else 1) for i, c in enumerate(reversed(body)))
        assert int(digits[-1]) == (10 - total % 10) % 10

    def test_excursion_window_names_the_parts_at_risk_c7811_smoke(self) -> None:
        """[C7811][smoke] 'Quarantine these 3 parts, release the rest.'"""
        base = 1_800_000_000 - (1_800_000_000 % lot_context.LOT_CYCLE_S)
        # Spindle 3 is over its limit from t=225 s to t=275 s of the cycle.
        disposition = lot_context.lot_disposition(base + 225, base + 275)
        lot = lot_context.lot_id_at(base)
        assert disposition["lot_ids"] == [lot]
        assert disposition["parts_at_risk"] == [f"{lot}-12", f"{lot}-13", f"{lot}-14"]
        assert disposition["parts_released"] == 12
        assert disposition["dhr_refs"] == [f"DHR-{lot}"]

    def test_window_spanning_two_lots_splits_by_lot_c7811_regression(self) -> None:
        """[C7811][regression] An excursion across a lot boundary touches both DHRs."""
        base = 1_800_000_000 - (1_800_000_000 % lot_context.LOT_CYCLE_S)
        disposition = lot_context.lot_disposition(base + 290, base + 305)
        assert len(disposition["lot_ids"]) == 2
        assert len(disposition["parts_at_risk"]) == 2
        assert disposition["parts_released"] == 28

    def test_lot_fields_ignore_unrelated_metadata_c7811_defect(self) -> None:
        """[C7811][defect] Only lot keys are lifted from reading metadata."""
        assert lot_context.lot_fields(None) == {}
        assert lot_context.lot_fields({"phase": "overheat", "lot_id": "L1", "udi_di": ""}) == {
            "lot_id": "L1"
        }


class TestLotTagging:
    def test_scenario_engine_tags_mill_readings_with_lot_context_c7812_smoke(self) -> None:
        """[C7812][smoke] cnc_1_* readings carry lot/part/UDI/DHR context in metadata."""
        engine = ScenarioEngine("manufacturing")
        ts = 1_800_000_245.0
        mill, pack = engine.wrap_records(
            [
                {"sensor_name": "cnc_1_spindle_temp", "timestamp": ts, "value": 50.0},
                {"sensor_name": "assy_line_1_speed", "timestamp": ts, "value": 12.0},
            ]
        )
        assert mill["metadata"]["lot_id"] == lot_context.lot_id_at(ts)
        assert mill["metadata"]["part_serial"] == lot_context.lot_context_at(ts)["part_serial"]
        assert mill["metadata"]["udi_di"] == lot_context.UDI_DI
        assert "lot_id" not in pack["metadata"]

    def test_other_industries_get_no_lot_context_c7812_defect(self) -> None:
        """[C7812][defect] Lot tagging is specific to the implant cell."""
        engine = ScenarioEngine("energy")
        (record,) = engine.wrap_records(
            [{"sensor_name": "gt_1_power_output", "timestamp": 1.0, "value": 1.0}]
        )
        assert "lot_id" not in record["metadata"]

    @pytest.mark.asyncio
    async def test_alert_opens_with_lot_and_validation_context_c7813_smoke(self) -> None:
        """[C7813][smoke] The opened alert names the lot, part and validated limit."""
        at = datetime(2026, 11, 20, 10, 3, 45, tzinfo=UTC)
        engine = AlertEngine(now_fn=lambda: at)
        rule = replace(_rule("mfg-alert-spindle-temp"), duration_s=0.0)
        reading = {
            "sensor_name": "cnc_1_spindle_temp",
            "value": 66.0,
            "metadata": {"lot_id": "TM-HS12-0042", "part_serial": "TM-HS12-0042-12"},
        }
        (opened,) = await engine.evaluate([reading], [rule], industry="manufacturing")
        meta = opened.alert.metadata
        assert meta["lot_id"] == "TM-HS12-0042"
        assert meta["part_serial"] == "TM-HS12-0042-12"
        assert meta["oq_ref"] == "OQ-TM-CNC1-014"
        assert meta["validated_limit"] == "60 °C"
        assert opened.alert.to_dict()["metadata"]["pq_ref"] == "PQ-TM-HS12-022"

    @pytest.mark.asyncio
    async def test_alert_derives_lot_from_the_clock_without_reading_context_c7813_regression(
        self,
    ) -> None:
        """[C7813][regression] Readings without lot metadata still get the cell's lot."""
        at = datetime(2026, 11, 20, 10, 3, 45, tzinfo=UTC)
        engine = AlertEngine(now_fn=lambda: at)
        rule = replace(_rule("mfg-alert-spindle-temp"), duration_s=0.0)
        (opened,) = await engine.evaluate(
            [{"sensor_name": "cnc_1_spindle_temp", "value": 66.0}],
            [rule],
            industry="manufacturing",
        )
        assert opened.alert.metadata["lot_id"] == lot_context.lot_id_at(at)
        assert opened.alert.metadata["dhr_ref"] == lot_context.dhr_ref(lot_context.lot_id_at(at))
