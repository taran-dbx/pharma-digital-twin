"""Tests for the SSOT sensor field-map (``models/sensor_schema.py``).

The field-map is the single source of truth that generates every schema
artifact (PG ChronoTable DDL, Delta DDL, Kafka ``from_json`` parse schema,
INSERT column list, zerobus record keys).  These tests assert that all 11
canonical ``SensorRecord`` fields survive into every generated artifact and
that the artifacts are mutually consistent (no drift), which is the bug this
module exists to prevent.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from iot_twin.models import sensor_schema as ss

# ---------------------------------------------------------------------------
# Canonical field expectations — mirror the ``SensorRecord`` in the
# ``iot_data_simulator`` wheel (11 fields).
# ---------------------------------------------------------------------------
CANONICAL_RECORD_FIELDS = (
    "sensor_name",
    "industry",
    "value",
    "unit",
    "sensor_type",
    "timestamp",
    "min_value",
    "max_value",
    "nominal_value",
    "fault_active",
    "metadata",
)

# The four columns that today's ``SENSOR_READINGS_DDL`` (9 cols) and the
# micro-batch Kafka parse schema silently drop.
MISSING_FOUR_COLUMNS = ("min_value", "max_value", "nominal_value", "metadata")


class TestFieldMap:
    """The field-map covers exactly the 11 canonical SensorRecord fields."""

    def test_map_has_eleven_entries(self) -> None:
        assert len(ss.SENSOR_FIELD_MAP) == 11

    def test_map_covers_all_canonical_record_fields(self) -> None:
        record_fields = tuple(spec.record_field for spec in ss.SENSOR_FIELD_MAP)
        assert record_fields == CANONICAL_RECORD_FIELDS

    def test_timestamp_maps_to_event_time_timestamptz(self) -> None:
        spec = ss.field_by_record("timestamp")
        assert spec.column == "event_time"
        assert spec.pg_type == "TIMESTAMPTZ"
        assert spec.transform == "epoch_to_timestamptz"

    def test_metadata_is_jsonb_in_pg(self) -> None:
        spec = ss.field_by_record("metadata")
        assert spec.column == "metadata"
        assert spec.pg_type == "JSONB"

    def test_numeric_fields_are_double(self) -> None:
        for name in ("value", "min_value", "max_value", "nominal_value"):
            spec = ss.field_by_record(name)
            assert spec.pg_type == "DOUBLE PRECISION"
            assert spec.delta_type == "DOUBLE"

    def test_fault_active_is_boolean(self) -> None:
        spec = ss.field_by_record("fault_active")
        assert spec.pg_type == "BOOLEAN"
        assert spec.delta_type == "BOOLEAN"


class TestPgChronotableDdl:
    """Postgres ChronoTable DDL is generated from the field-map."""

    def test_ddl_substitutes_table_name(self) -> None:
        ddl = ss.pg_chronotable_ddl("sensor_readings")
        assert "CREATE TABLE IF NOT EXISTS sensor_readings" in ddl

    def test_ddl_has_app_added_columns(self) -> None:
        ddl = ss.pg_chronotable_ddl("t")
        # PK uses a standalone sequence, NOT BIGSERIAL, so LakeTS's
        # create_chronotable can drop the renamed original cleanly.
        assert "BIGSERIAL" not in ddl
        assert "id BIGINT PRIMARY KEY DEFAULT nextval('t_id_seq')" in ddl
        assert "source TEXT DEFAULT 'sss'" in ddl

    def test_ddl_creates_standalone_sequence(self) -> None:
        ddl = ss.pg_chronotable_ddl("sensor_readings")
        assert "CREATE SEQUENCE IF NOT EXISTS sensor_readings_id_seq;" in ddl
        # Sequence is created before the table that references it.
        assert ddl.index("CREATE SEQUENCE") < ddl.index("CREATE TABLE")

    def test_ddl_contains_all_eleven_fields(self) -> None:
        ddl = ss.pg_chronotable_ddl("t")
        for spec in ss.SENSOR_FIELD_MAP:
            assert spec.column in ddl, f"PG DDL missing column {spec.column}"

    def test_ddl_contains_the_missing_four_columns(self) -> None:
        ddl = ss.pg_chronotable_ddl("t")
        for col in MISSING_FOUR_COLUMNS:
            assert col in ddl, f"PG DDL still drops {col}"

    def test_ddl_uses_event_time_timestamptz(self) -> None:
        ddl = ss.pg_chronotable_ddl("t")
        assert "event_time TIMESTAMPTZ" in ddl
        assert "timestamp DOUBLE" not in ddl  # epoch is transformed, not stored raw

    def test_metadata_default_is_format_safe(self) -> None:
        # The DDL is consumed via ``.format(table_name=...)``; literal ``{}``
        # would break that, so the JSONB default must not contain braces.
        ddl = ss.pg_chronotable_ddl("{table_name}")
        assert "{}" not in ddl.replace("{table_name}", "")
        # Round-trips through str.format without raising.
        assert "sensor_readings" in ddl.format(table_name="sensor_readings")


class TestDeltaTableDdl:
    """Delta target-table DDL is generated from the field-map."""

    def test_ddl_uses_fqn(self) -> None:
        ddl = ss.delta_table_ddl("iot_pharma.raw.iot_sensor_data")
        assert "iot_pharma.raw.iot_sensor_data" in ddl
        assert "USING DELTA" in ddl

    def test_ddl_contains_all_eleven_columns(self) -> None:
        ddl = ss.delta_table_ddl("c.s.t")
        for spec in ss.SENSOR_FIELD_MAP:
            assert spec.column in ddl, f"Delta DDL missing column {spec.column}"

    def test_ddl_contains_the_missing_four_columns(self) -> None:
        ddl = ss.delta_table_ddl("c.s.t")
        for col in MISSING_FOUR_COLUMNS:
            assert col in ddl, f"Delta DDL still drops {col}"

    def test_event_time_is_timestamp(self) -> None:
        ddl = ss.delta_table_ddl("c.s.t")
        assert "event_time TIMESTAMP" in ddl


class TestKafkaParseSchema:
    """The Spark ``from_json`` parse schema is generated from the field-map."""

    def test_schema_contains_all_eleven_record_fields(self) -> None:
        schema = ss.kafka_parse_schema()
        for field in CANONICAL_RECORD_FIELDS:
            assert f'"{field}"' in schema, f"Kafka schema missing {field}"

    def test_schema_contains_the_missing_four_fields(self) -> None:
        schema = ss.kafka_parse_schema()
        for field in MISSING_FOUR_COLUMNS:
            assert f'"{field}"' in schema, f"Kafka schema still drops {field}"

    def test_timestamp_parsed_as_double_epoch(self) -> None:
        # Raw Kafka JSON carries epoch seconds; the transform to TIMESTAMPTZ
        # happens at insert time, so the parse schema keeps ``timestamp`` DOUBLE.
        line = next(ln for ln in ss.kafka_parse_schema().splitlines() if '"timestamp"' in ln)
        assert "DoubleType" in line

    def test_metadata_parsed_as_map(self) -> None:
        line = next(ln for ln in ss.kafka_parse_schema().splitlines() if '"metadata"' in ln)
        assert "MapType" in line

    def test_add_lines_match_record_field_order(self) -> None:
        names = [name for name, _ in ss.kafka_schema_fields()]
        assert names == list(CANONICAL_RECORD_FIELDS)


class TestInsertColumns:
    """The PG INSERT column list is generated from the field-map."""

    def test_insert_contains_all_columns_plus_source(self) -> None:
        cols = ss.insert_columns()
        for spec in ss.SENSOR_FIELD_MAP:
            assert spec.column in cols, f"INSERT list missing {spec.column}"
        assert "source" in cols
        assert "id" not in cols  # BIGSERIAL is auto-assigned

    def test_insert_contains_the_missing_four_columns(self) -> None:
        cols = ss.insert_columns()
        for col in MISSING_FOUR_COLUMNS:
            assert col in cols, f"INSERT list still drops {col}"


class TestZerobusRecordKeys:
    """The zerobus record dict keys are generated from the field-map."""

    def test_keys_are_the_eleven_record_fields(self) -> None:
        assert ss.zerobus_record_keys() == CANONICAL_RECORD_FIELDS

    def test_keys_contain_the_missing_four_fields(self) -> None:
        keys = ss.zerobus_record_keys()
        for field in MISSING_FOUR_COLUMNS:
            assert field in keys


class TestMutualConsistency:
    """No drift: every artifact represents the same 11 canonical fields."""

    def test_pg_and_delta_cover_the_same_columns(self) -> None:
        expected = {spec.column for spec in ss.SENSOR_FIELD_MAP}
        pg_ddl = ss.pg_chronotable_ddl("t")
        delta_ddl = ss.delta_table_ddl("c.s.t")
        for col in expected:
            assert col in pg_ddl
            assert col in delta_ddl

    def test_insert_columns_match_pg_data_columns(self) -> None:
        # INSERT list == mapped columns + source (id excluded).
        expected = tuple(spec.column for spec in ss.SENSOR_FIELD_MAP) + ("source",)
        assert ss.insert_columns() == expected

    def test_kafka_and_zerobus_cover_the_same_record_fields(self) -> None:
        kafka_fields = {name for name, _ in ss.kafka_schema_fields()}
        zerobus_fields = set(ss.zerobus_record_keys())
        assert kafka_fields == zerobus_fields == set(CANONICAL_RECORD_FIELDS)

    def test_every_record_field_reaches_a_pg_column(self) -> None:
        pg_ddl = ss.pg_chronotable_ddl("t")
        insert_cols = ss.insert_columns()
        for spec in ss.SENSOR_FIELD_MAP:
            assert spec.column in pg_ddl
            assert spec.column in insert_cols


class TestSqlDdlDerivedFromSsot:
    """``iot_twin.models.sql_ddl.SENSOR_READINGS_DDL`` is derived from the SSOT."""

    def test_sensor_readings_ddl_matches_ssot(self) -> None:
        from iot_twin.models import sql_ddl

        assert ss.pg_chronotable_ddl("{table_name}") == sql_ddl.SENSOR_READINGS_DDL

    def test_sensor_readings_ddl_now_has_the_missing_four_columns(self) -> None:
        from iot_twin.models import sql_ddl

        for col in MISSING_FOUR_COLUMNS:
            assert col in sql_ddl.SENSOR_READINGS_DDL

    def test_sensor_readings_ddl_still_formats(self) -> None:
        from iot_twin.models import sql_ddl

        ddl = sql_ddl.SENSOR_READINGS_DDL.format(table_name="sensor_readings")
        assert "CREATE TABLE IF NOT EXISTS sensor_readings" in ddl


# ---------------------------------------------------------------------------
# TS notebook consistency — see module docstring "chosen approach".
#
# The consumer notebook is generated client-side in TypeScript
# (``client/src/lib/notebook-template.ts``); there is no TS test runner and no
# backend hook in the notebook-generation path, so backend injection of the
# parse schema would be a high-risk architectural change.  The lower-risk
# option (chosen) is this Python guard: parse the notebook's ``from_json``
# StructType and assert its fields match the SSOT.  If the notebook drifts
# from the field-map, this test fails.
# ---------------------------------------------------------------------------
NOTEBOOK_TS = (
    Path(__file__).resolve().parent.parent / "client" / "src" / "lib" / "notebook-template.ts"
)


def _notebook_schema_fields() -> list[tuple[str, str]]:
    """Extract ``.add("name", Type())`` pairs from the notebook's parse schema."""
    text = NOTEBOOK_TS.read_text()
    # Isolate the ``schema = ( StructType() ... )`` block.
    block = re.search(r"schema = \(\s*StructType\(\)(.*?)\n\)", text, re.DOTALL)
    assert block is not None, "could not locate from_json StructType in notebook"
    pairs = re.findall(r'\.add\("([^"]+)",\s*([A-Za-z]+)', block.group(1))
    return [(name, typ) for name, typ in pairs]


@pytest.mark.skipif(not NOTEBOOK_TS.exists(), reason="notebook template not present")
class TestNotebookMatchesSsot:
    """The TS notebook's from_json schema must match the SSOT (no drift)."""

    def test_notebook_field_names_match_ssot(self) -> None:
        notebook_fields = [name for name, _ in _notebook_schema_fields()]
        ssot_fields = [name for name, _ in ss.kafka_schema_fields()]
        assert notebook_fields == ssot_fields

    def test_notebook_has_the_missing_four_fields(self) -> None:
        notebook_fields = {name for name, _ in _notebook_schema_fields()}
        for field in MISSING_FOUR_COLUMNS:
            assert field in notebook_fields, f"notebook still drops {field}"

    def test_notebook_field_types_match_ssot(self) -> None:
        notebook = dict(_notebook_schema_fields())
        for name, spark_type in ss.kafka_schema_fields():
            # ``spark_type`` is e.g. ``StringType()``/``MapType(...)``; compare
            # on the leading type constructor name.
            expected_ctor = spark_type.split("(")[0]
            assert notebook[name] == expected_ctor, (
                f"notebook field {name} is {notebook[name]}, expected {expected_ctor}"
            )
