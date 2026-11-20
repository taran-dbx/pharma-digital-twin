"""Realtime telemetry lives in the Lakehouse Sync shadow history table.

Verified read-only on a test workspace (2026-11-18): ``manufacturing_realtime``
has 0 rows; the RTM stream writes Lakebase and Lakehouse Sync mirrors it into
``lb__shadow_manufacturing_sensor_readings_history`` (sensor columns + CDC
``_pg_*`` / ``_timestamp`` / ``_sort_by`` / ``id`` / ``source``).  Micro-batch and
zerobus write ``manufacturing_{mode}`` directly.
"""

from __future__ import annotations

import json

import pytest

from iot_twin.services import genie_service as gs
from iot_twin.services.cdf_reconciliation_service import delta_table_fqn
from iot_twin.services.genie import genie_space_service as gss

SHADOW = "cat.sch.lb__shadow_manufacturing_sensor_readings_history"


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")


def _space(mode: str) -> dict:
    return json.loads(gss.build_serialized_space("manufacturing", mode))


class TestTelemetryTableMapping:
    def test_realtime_uses_shadow_history_table(self) -> None:
        assert gss.telemetry_table("manufacturing", "realtime") == SHADOW
        assert delta_table_fqn("manufacturing") == SHADOW

    @pytest.mark.parametrize("mode", ["microbatch", "zerobus"])
    def test_other_modes_use_mode_table(self, mode: str) -> None:
        assert gss.telemetry_table("manufacturing", mode) == f"cat.sch.manufacturing_{mode}"

    def test_realtime_expected_tables(self) -> None:
        tables = gss.expected_tables("manufacturing", "realtime")
        assert SHADOW in tables
        assert "cat.sch.manufacturing_realtime" not in tables
        assert "cat.sch.manufacturing_metrics" in tables
        assert len(tables) == 7

    def test_realtime_space_and_allowlist_agree(self) -> None:
        raw = gss.build_serialized_space("manufacturing", "realtime")
        assert gs.parse_space_tables(raw) == list(gss.expected_tables("manufacturing", "realtime"))


class TestRealtimeContent:
    def test_column_configs_on_shadow_table(self) -> None:
        tables = {t["identifier"]: t for t in _space("realtime")["data_sources"]["tables"]}
        configs = {c["column_name"]: c for c in tables[SHADOW]["column_configs"]}
        for col in (
            "sensor_name",
            "value",
            "fault_active",
            "min_value",
            "max_value",
            "nominal_value",
            "event_time",
        ):
            assert col in configs
        for cdc in ("_pg_lsn", "_pg_xid", "_sort_by", "_timestamp"):
            assert configs[cdc].get("exclude") is True
        assert "_pg_change_type" in configs
        names = list(configs)
        assert names == sorted(names)

    def test_mode_tables_have_no_cdc_column_configs(self) -> None:
        tables = {t["identifier"]: t for t in _space("zerobus")["data_sources"]["tables"]}
        names = {
            c["column_name"] for c in tables["cat.sch.manufacturing_zerobus"]["column_configs"]
        }
        assert not any(n.startswith("_pg_") for n in names)

    def test_instructions_reference_shadow_and_insert_filter(self) -> None:
        body = "".join(_space("realtime")["instructions"]["text_instructions"][0]["content"])
        assert SHADOW in body
        assert "manufacturing_realtime" not in body.replace(
            "lb__shadow_manufacturing_sensor_readings_history", ""
        )
        assert "_pg_change_type IN ('insert', 'update_postimage')" in body

    def test_examples_query_shadow_with_insert_filter(self) -> None:
        examples = _space("realtime")["instructions"]["example_question_sqls"]
        telemetry_sql = [e["sql"][0] for e in examples if SHADOW in e["sql"][0]]
        assert len(telemetry_sql) >= 4
        assert all(
            "_pg_change_type IN ('insert', 'update_postimage')" in sql for sql in telemetry_sql
        )

    def test_snippets_qualified_with_shadow_name(self) -> None:
        snippets = _space("realtime")["instructions"]["sql_snippets"]
        short = "lb__shadow_manufacturing_sensor_readings_history."
        assert all(short in s["sql"][0] for s in snippets["measures"])

    def test_zerobus_examples_have_no_cdc_filter(self) -> None:
        sql = " ".join(
            e["sql"][0] for e in _space("zerobus")["instructions"]["example_question_sqls"]
        )
        assert "_pg_change_type" not in sql
