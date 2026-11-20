"""Single source of truth (SSOT) for the sensor schema contract.

Every schema artifact in the pipeline is derived from **one** declarative
field-map so nothing can drift.  The canonical record is the ``SensorRecord``
shipped in the ``iot_data_simulator`` wheel, which carries **11 fields**::

    sensor_name, industry, value, unit, sensor_type, timestamp,
    min_value, max_value, nominal_value, fault_active, metadata

Historically the streaming writer inserted ``min_value/max_value/
nominal_value/metadata`` while ``models/sql_ddl.py:SENSOR_READINGS_DDL`` (9
cols) and the micro-batch Kafka parse schema both dropped them, causing silent
data loss.  Generating all artifacts from this map reconciles them.

Generated artifacts (see §1.9 of the design spec):

1. :func:`pg_chronotable_ddl` — the real-time Lakebase/Postgres ChronoTable DDL.
2. :func:`insert_columns` — the Postgres INSERT column list.
3. :func:`kafka_parse_schema` — the Spark ``from_json`` parse schema (real-time
   *and* micro-batch share it — schema is industry-invariant).
4. :func:`delta_table_ddl` — the zerobus/micro-batch target Delta table DDL.
5. :func:`zerobus_record_keys` — the zerobus record dict keys.

The schema is **industry-invariant**: industry only changes row *values*
(``sensor_name``, ``value``, ``unit``, ``sensor_type``), never the columns.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "FieldSpec",
    "SENSOR_FIELD_MAP",
    "SOURCE_COLUMN",
    "ID_COLUMN_NAME",
    "field_by_record",
    "field_by_column",
    "pg_id_sequence_name",
    "pg_chronotable_ddl",
    "delta_table_ddl",
    "kafka_parse_schema",
    "kafka_schema_fields",
    "insert_columns",
    "zerobus_record_keys",
]


@dataclass(frozen=True)
class FieldSpec:
    """One ``SensorRecord`` field mapped to its persisted representations.

    Attributes:
        record_field: The ``SensorRecord`` attribute / raw-JSON key.  Used by
            the Kafka ``from_json`` parse schema and the zerobus record dict.
        column: The persisted column name.  Used by the PG DDL, Delta DDL and
            INSERT list.  May differ from ``record_field`` (e.g. epoch
            ``timestamp`` → ``event_time``).
        pg_type: Postgres column type.
        delta_type: Spark/Delta column type for the target Delta table.
        kafka_type: Spark type constructor used in the ``from_json`` parse
            schema for the *raw* JSON payload (e.g. epoch ``timestamp`` is
            parsed as ``DoubleType()`` and transformed later).
        pg_not_null: Emit ``NOT NULL`` on the PG column.
        pg_default: Optional PG column default clause (e.g. ``jsonb_build_object()``).
        transform: Name of the source→column transform, if any.
    """

    record_field: str
    column: str
    pg_type: str
    delta_type: str
    kafka_type: str
    pg_not_null: bool = False
    pg_default: str | None = None
    transform: str | None = None


# ---------------------------------------------------------------------------
# THE field-map — the single source of truth.  Order is the canonical
# ``SensorRecord`` field order.
# ---------------------------------------------------------------------------
SENSOR_FIELD_MAP: tuple[FieldSpec, ...] = (
    FieldSpec("sensor_name", "sensor_name", "TEXT", "STRING", "StringType()", pg_not_null=True),
    FieldSpec("industry", "industry", "TEXT", "STRING", "StringType()", pg_not_null=True),
    FieldSpec("value", "value", "DOUBLE PRECISION", "DOUBLE", "DoubleType()", pg_not_null=True),
    FieldSpec("unit", "unit", "TEXT", "STRING", "StringType()"),
    FieldSpec("sensor_type", "sensor_type", "TEXT", "STRING", "StringType()"),
    # Epoch seconds (float) in the raw JSON → TIMESTAMPTZ column on insert.
    FieldSpec(
        "timestamp",
        "event_time",
        "TIMESTAMPTZ",
        "TIMESTAMP",
        "DoubleType()",
        pg_not_null=True,
        transform="epoch_to_timestamptz",
    ),
    FieldSpec("min_value", "min_value", "DOUBLE PRECISION", "DOUBLE", "DoubleType()"),
    FieldSpec("max_value", "max_value", "DOUBLE PRECISION", "DOUBLE", "DoubleType()"),
    FieldSpec("nominal_value", "nominal_value", "DOUBLE PRECISION", "DOUBLE", "DoubleType()"),
    FieldSpec(
        "fault_active", "fault_active", "BOOLEAN", "BOOLEAN", "BooleanType()", pg_default="FALSE"
    ),
    # PG JSONB; Delta/Kafka use MAP<STRING,STRING> (VARIANT deferred per the
    # Phase-0 probe — the STRING/MAP fallback in §1.9).
    FieldSpec(
        "metadata",
        "metadata",
        "JSONB",
        "MAP<STRING, STRING>",
        "MapType(StringType(), StringType())",
        pg_default="jsonb_build_object()",
    ),
)

# App-added Postgres columns (not part of the canonical SensorRecord).
#
# The primary key uses an *independently-created* sequence (via nextval) rather
# than BIGSERIAL.  BIGSERIAL implicitly OWNs its sequence, and that ownership
# dependency blocks ``lakets.create_chronotable``'s internal DROP of the
# renamed original table ("cannot drop table ... other objects depend on it").
# A standalone sequence has no such dependency, so LakeTS can rename/drop the
# original cleanly.
ID_COLUMN_NAME = "id"
SOURCE_COLUMN_NAME = "source"
SOURCE_COLUMN = f"{SOURCE_COLUMN_NAME} TEXT DEFAULT 'sss'"


def pg_id_sequence_name(table: str) -> str:
    """Return the standalone id-sequence name for a ChronoTable ``table``."""
    return f"{table}_id_seq"


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------
def field_by_record(record_field: str) -> FieldSpec:
    """Return the :class:`FieldSpec` for a ``SensorRecord`` field name."""
    for spec in SENSOR_FIELD_MAP:
        if spec.record_field == record_field:
            return spec
    raise KeyError(f"unknown record field: {record_field!r}")


def field_by_column(column: str) -> FieldSpec:
    """Return the :class:`FieldSpec` for a persisted column name."""
    for spec in SENSOR_FIELD_MAP:
        if spec.column == column:
            return spec
    raise KeyError(f"unknown column: {column!r}")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def _pg_column_clause(spec: FieldSpec) -> str:
    clause = f"{spec.column} {spec.pg_type}"
    if spec.pg_not_null:
        clause += " NOT NULL"
    if spec.pg_default is not None:
        clause += f" DEFAULT {spec.pg_default}"
    return clause


def pg_chronotable_ddl(table: str) -> str:
    """Return the Postgres ChronoTable DDL for ``table``.

    Emits a standalone ``CREATE SEQUENCE`` followed by the ``CREATE TABLE``
    whose ``id`` PK defaults to ``nextval('<table>_id_seq')`` — deliberately
    *not* ``BIGSERIAL``, so the sequence is not owned by the column and
    ``lakets.create_chronotable`` can drop the renamed original cleanly.

    ``table`` may be a literal name (``"sensor_readings"``) or a ``str.format``
    placeholder (``"{table_name}"``); the generated DDL contains no other
    braces, so it round-trips through ``str.format`` safely.
    """
    seq = pg_id_sequence_name(table)
    id_column = f"{ID_COLUMN_NAME} BIGINT PRIMARY KEY DEFAULT nextval('{seq}')"
    columns = [f"    {id_column}"]
    columns += [f"    {_pg_column_clause(spec)}" for spec in SENSOR_FIELD_MAP]
    columns.append(f"    {SOURCE_COLUMN}")
    body = ",\n".join(columns)
    return f"CREATE SEQUENCE IF NOT EXISTS {seq};\nCREATE TABLE IF NOT EXISTS {table} (\n{body}\n)"


def delta_table_ddl(fqn: str) -> str:
    """Return the target Delta table DDL for fully-qualified name ``fqn``."""
    columns = [f"    {spec.column} {spec.delta_type}" for spec in SENSOR_FIELD_MAP]
    body = ",\n".join(columns)
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n{body}\n) USING DELTA"


def kafka_schema_fields() -> list[tuple[str, str]]:
    """Return ``(record_field, spark_type)`` pairs for the ``from_json`` schema."""
    return [(spec.record_field, spec.kafka_type) for spec in SENSOR_FIELD_MAP]


def kafka_parse_schema() -> str:
    """Return the Spark ``StructType`` source for the ``from_json`` parse schema.

    Uses the raw-JSON *record* field names (epoch ``timestamp``, ``metadata``
    map, …); the epoch→``TIMESTAMPTZ`` transform is applied at insert time.
    """
    lines = ["StructType()"]
    lines += [f'    .add("{field}", {spark_type})' for field, spark_type in kafka_schema_fields()]
    return "\n".join(lines)


def insert_columns() -> tuple[str, ...]:
    """Return the Postgres INSERT column list (``id`` excluded — it is serial)."""
    return tuple(spec.column for spec in SENSOR_FIELD_MAP) + (SOURCE_COLUMN_NAME,)


def zerobus_record_keys() -> tuple[str, ...]:
    """Return the zerobus record dict keys (the 11 canonical record fields)."""
    return tuple(spec.record_field for spec in SENSOR_FIELD_MAP)
