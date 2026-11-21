"""Medical-device recast of the manufacturing industry [qa].

The internal id stays ``manufacturing``; the catalog, lot clock and alert
metadata tell Trinity Medical's orthopaedic-implant machining story.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

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


def test_default_industries_lead_with_medical_devices_c7801_regression() -> None:
    """[C7801][regression] Without INDUSTRIES set, the med-device industry is offered first."""
    # A fresh interpreter reads the import-time default; reloading ``iot_twin.core.config``
    # in-process would replace classes other tests hold (e.g. ``NotOwnedError``).
    env = {k: v for k, v in os.environ.items() if k != "INDUSTRIES"}
    src = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    out = subprocess.run(
        [sys.executable, "-c", "from iot_twin.core import config; print(config.INDUSTRIES)"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip().split(",") == ["manufacturing", "pharmaceutical"]


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


class TestReviewFixes:
    def test_lot_ids_carry_the_date_and_never_wrap_c7815_regression(self) -> None:
        """[C7815][regression] Lot ids are <prefix>-<YYMMDD>-<cycle of the UTC day>."""
        day = datetime(2027, 1, 15, tzinfo=UTC).timestamp()
        assert lot_context.lot_id_at(day) == "TM-HS12-270115-000"
        assert lot_context.lot_id_at(day + 86_399) == "TM-HS12-270115-287"
        assert lot_context.lot_id_at(day + 86_400) == "TM-HS12-270116-000"
        # 34.7 days apart used to collide with a 4-digit counter.
        assert lot_context.lot_id_at(day) != lot_context.lot_id_at(day + 10_000 * 300)

    def test_projected_rft_is_the_single_parts_based_definition_c7816_smoke(self) -> None:
        """[C7816][smoke] (parts started - parts at risk) / parts started."""
        base = 1_800_000_000 - (1_800_000_000 % lot_context.LOT_CYCLE_S)
        full = lot_context.projected_rft(base + 225, base + 275, base + 299)
        assert full == {"pct": 80.0, "good": 12, "parts_started": 15, "parts_at_risk": 3}
        mid = lot_context.projected_rft(base + 225, base + 275, base + 275)
        assert mid is not None and mid["parts_started"] == 14
        assert (
            lot_context.lot_disposition(base + 225, base + 275, base + 299)["projected_rft"] == full
        )
        assert "(parts started - parts at risk) / parts started" in (
            lot_context.PROJECTED_RFT_DEFINITION
        )

    def test_excursion_starts_at_first_over_limit_reading_c7817_regression(self) -> None:
        """[C7817][regression] Containment starts at the first over-limit reading, else opened - duration."""
        from iot_twin.services.alerts.alert_engine import AlertRecord
        from iot_twin.services.alerts.alert_responses import excursion_start

        opened = datetime(2026, 11, 20, 10, 4, 1, tzinfo=UTC)

        def alert(metadata: dict) -> AlertRecord:
            return AlertRecord(
                id="a",
                fingerprint="f",
                industry="manufacturing",
                pipeline_id=None,
                rule_id="mfg-alert-spindle-temp",
                sensor_id="mfg-sns-cnc_1_spindle_temp",
                sensor_ids=[],
                severity="crit",
                status="open",
                fault_code=None,
                value=None,
                threshold=60.0,
                opened_at=opened,
                updated_at=opened,
                metadata=metadata,
            )

        stamped = alert({"excursion_started_at": "2026-11-20T10:03:58.500000+00:00"})
        assert excursion_start(stamped, opened).isoformat() == "2026-11-20T10:03:58.500000+00:00"
        assert (opened - excursion_start(alert({}), opened)).total_seconds() == 2.0

    @pytest.mark.asyncio
    async def test_alert_engine_stamps_the_first_violation_c7817_smoke(self) -> None:
        """[C7817][smoke] The engine records when the first over-limit reading arrived."""
        clock = {"t": datetime(2026, 11, 20, 10, 3, 58, tzinfo=UTC)}
        engine = AlertEngine(now_fn=lambda: clock["t"])
        rule = _rule("mfg-alert-spindle-temp")
        reading = [{"sensor_name": "cnc_1_spindle_temp", "value": 61.0}]
        assert await engine.evaluate(reading, [rule], industry="manufacturing") == []
        clock["t"] = datetime(2026, 11, 20, 10, 4, 0, 500000, tzinfo=UTC)
        (opened,) = await engine.evaluate(reading, [rule], industry="manufacturing")
        assert opened.alert.metadata["excursion_started_at"] == "2026-11-20T10:03:58+00:00"


class TestExcursionStampBounds:
    def _alert(self, metadata: dict):
        from iot_twin.services.alerts.alert_engine import AlertRecord

        opened = datetime(2026, 11, 20, 10, 4, 1, tzinfo=UTC)
        return AlertRecord(
            id="a",
            fingerprint="f",
            industry="manufacturing",
            pipeline_id=None,
            rule_id="mfg-alert-spindle-temp",
            sensor_id="mfg-sns-cnc_1_spindle_temp",
            sensor_ids=[],
            severity="crit",
            status="open",
            fault_code=None,
            value=None,
            threshold=60.0,
            opened_at=opened,
            updated_at=opened,
            metadata=metadata,
        )

    @pytest.mark.parametrize(
        "stamp",
        ["2026-11-18T09:00:00+00:00", "2026-11-20T10:04:05+00:00", "2026-11-20T10:01:00+00:00"],
    )
    def test_stamps_outside_the_episode_are_ignored_c7829_defect(self, stamp: str) -> None:
        """[C7829][defect] A stale (or future) stamp falls back to opened_at - duration_s."""
        from iot_twin.services.alerts.alert_responses import excursion_start

        alert = self._alert({"excursion_started_at": stamp})
        assert excursion_start(alert, alert.opened_at).isoformat() == "2026-11-20T10:03:59+00:00"

    def test_stamp_inside_the_lookback_is_kept_c7829_regression(self) -> None:
        """[C7829][regression] Up to duration_s + 120 s before opening is trusted."""
        from iot_twin.services.alerts.alert_responses import excursion_start

        alert = self._alert({"excursion_started_at": "2026-11-20T10:02:00+00:00"})
        assert excursion_start(alert, alert.opened_at).isoformat() == "2026-11-20T10:02:00+00:00"

    def test_stamp_prefers_the_readings_device_time_c7829_smoke(self) -> None:
        """[C7829][smoke] Device time of the first violating reading, else min(seen, opened - duration)."""
        from iot_twin.services.alerts.alert_engine import excursion_started_at, reading_time

        opened = datetime(2026, 11, 20, 10, 4, 1, tzinfo=UTC)
        first = reading_time({"timestamp": opened.timestamp() - 2.4})
        assert first is not None
        assert excursion_started_at(first, opened, opened, 2.0) == first
        # Lost pending state (first seen == opened): never stamp the opening instant.
        assert (opened - excursion_started_at(None, opened, opened, 2.0)).total_seconds() == 2.0
        # Skewed device clock: not trusted.
        skewed = reading_time({"timestamp": opened.timestamp() + 3600})
        assert (opened - excursion_started_at(skewed, opened, opened, 2.0)).total_seconds() == 2.0
        assert reading_time({"event_time": int(opened.timestamp() * 1_000_000)}) == opened
        assert reading_time({"timestamp": opened.timestamp() * 1000}) == opened
        assert reading_time({}) is None
