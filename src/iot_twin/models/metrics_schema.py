"""SSOT for the streaming-metrics external Delta tables (spec §1.11).

Streaming metrics for the real-time, micro-batch, and zerobus modes are
captured by a ``StreamingQueryListener`` (real-time/micro-batch) or the
zerobus ingest path and flushed — buffered, async — to **per-mode UC EXTERNAL
Delta tables** via delta-rs (pure Python, so no Spark job contends with the
streaming query).

Each mode gets its OWN table — ``streaming_metrics_realtime``,
``streaming_metrics_microbatch``, ``streaming_metrics_zerobus`` — all living in
the ``metrics`` schema of the per-industry catalog. Separating by table (rather
than a shared table tagged with a ``mode`` column) keeps each mode's metrics
independently queryable/droppable and avoids any write contention across
concurrently-running modes.

delta-rs cannot write VARIANT, so the raw progress payload is stored as a
``STRING`` column (``progress``) and read back with ``parse_json(progress):field``
via DBSQL.  The remaining columns are the typed values extracted from each
``onQueryProgress`` event, plus ``query_id`` / ``query_name`` / ``run_id``.

This module is the single source of truth for these tables' column set and DDL
so the provisioning engine (which creates the tables) and the Phase-4 metrics
writer (which appends to them) can never drift.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "MetricsFieldSpec",
    "METRICS_FIELD_MAP",
    "METRICS_SCHEMA",
    "METRICS_TABLE",
    "METRICS_MODE_COLUMN",
    "ALL_METRIC_MODES",
    "metrics_columns",
    "metrics_managed_columns",
    "metrics_managed_table_ddl",
    "metrics_external_table_ddl",
    "metrics_table_location",
    "metrics_table_fqn",
    "metrics_tables",
]

# Fixed schema + base table name for the per-industry, per-mode metrics tables.
METRICS_SCHEMA = "metrics"
METRICS_TABLE = "streaming_metrics"

# The three streaming-pipeline flavors, each with its own metrics table.
# Only the Spark structured-streaming modes emit onQueryProgress metrics, so only
# they get a streaming_metrics_<mode> table. Zerobus ingest is NOT a Spark stream
# (no query listener), so it has no streaming-metrics table.
ALL_METRIC_MODES: tuple[str, ...] = ("realtime", "microbatch")


@dataclass(frozen=True)
class MetricsFieldSpec:
    """One metrics column mapped to its Delta type."""

    column: str
    delta_type: str


# The canonical metrics column set (spec §1.11) — identical across all three
# per-mode tables.  ``progress`` is the raw JSON payload kept as STRING
# (delta-rs can't write VARIANT); the rest are the typed values extracted from
# ``StreamingQueryProgress`` — the ``*_p50/p99`` latency percentiles are
# Databricks-runtime-only fields.
METRICS_FIELD_MAP: tuple[MetricsFieldSpec, ...] = (
    MetricsFieldSpec("query_id", "STRING"),
    MetricsFieldSpec("query_name", "STRING"),
    MetricsFieldSpec("run_id", "STRING"),
    MetricsFieldSpec("batch_id", "BIGINT"),
    MetricsFieldSpec("timestamp", "TIMESTAMP"),
    MetricsFieldSpec("num_input_rows", "BIGINT"),
    MetricsFieldSpec("input_rps", "DOUBLE"),
    MetricsFieldSpec("processed_rps", "DOUBLE"),
    MetricsFieldSpec("trigger_ms", "DOUBLE"),
    MetricsFieldSpec("e2e_p50", "DOUBLE"),
    MetricsFieldSpec("e2e_p99", "DOUBLE"),
    MetricsFieldSpec("processing_p50", "DOUBLE"),
    MetricsFieldSpec("processing_p99", "DOUBLE"),
    MetricsFieldSpec("queuing_p50", "DOUBLE"),
    MetricsFieldSpec("queuing_p99", "DOUBLE"),
    # Raw StreamingQueryProgress JSON — parse_json(progress):field at read time.
    MetricsFieldSpec("progress", "STRING"),
)


def metrics_columns() -> tuple[str, ...]:
    """Return the metrics column names in canonical order."""
    return tuple(spec.column for spec in METRICS_FIELD_MAP)


# ---------------------------------------------------------------------------
# Managed, industry-prefixed metrics table (Task 3)
# ---------------------------------------------------------------------------
# The industry-first layout keeps a SINGLE ``{industry}_metrics`` MANAGED Delta
# table per industry in the shared ``{catalog}.{schema}``.  Unlike the per-mode
# EXTERNAL tables above, a managed table needs no ``LOCATION`` and carries a
# leading ``mode STRING`` column so a single table holds every mode's metrics,
# distinguished at query time by ``mode``.  A later task wires the notebook
# writer to this DDL; provisioning only creates the table.
METRICS_MODE_COLUMN = "mode"


def metrics_managed_columns() -> tuple[MetricsFieldSpec, ...]:
    """Return the managed-metrics column specs — ``mode`` first, then the map."""
    return (MetricsFieldSpec(METRICS_MODE_COLUMN, "STRING"),) + METRICS_FIELD_MAP


def metrics_managed_table_ddl(fqn: str) -> str:
    """Return the MANAGED ``{industry}_metrics`` Delta table DDL for ``fqn``.

    No ``LOCATION`` clause (managed storage), and a leading ``mode STRING``
    column so realtime/micro-batch metrics coexist in one table.
    """
    columns = ",\n".join(f"    {s.column} {s.delta_type}" for s in metrics_managed_columns())
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n{columns}\n) USING DELTA"


def _validate_mode(mode: str) -> str:
    """Return ``mode`` if it's one of :data:`ALL_METRIC_MODES`, else raise."""
    if mode not in ALL_METRIC_MODES:
        raise ValueError(f"unknown metrics mode {mode!r}; expected one of {ALL_METRIC_MODES}")
    return mode


def _table_name(mode: str) -> str:
    """Return the per-mode table name, e.g. ``streaming_metrics_realtime``."""
    return f"{METRICS_TABLE}_{_validate_mode(mode)}"


def metrics_table_fqn(catalog: str, mode: str) -> str:
    """Return the fully-qualified per-mode metrics table name for ``catalog``."""
    return f"{catalog}.{METRICS_SCHEMA}.{_table_name(mode)}"


def metrics_table_location(catalog: str, storage_base: str, mode: str) -> str:
    """Return the S3 location for the per-mode external metrics table.

    ``storage_base`` is the shared external-location root (e.g.
    ``s3://example-shared-bucket``).  Layout (spec §1.11)::

        <base>/iot-digital-twin/<catalog>/metrics/streaming_metrics_<mode>/
    """
    base = storage_base.rstrip("/")
    return f"{base}/iot-digital-twin/{catalog}/metrics/{_table_name(mode)}/"


def metrics_external_table_ddl(catalog: str, storage_base: str, mode: str) -> str:
    """Return the ``CREATE TABLE ... USING DELTA LOCATION`` DDL for one mode's
    external metrics table.

    The explicit ``LOCATION`` makes this an EXTERNAL table so delta-rs can write
    to it with UC temporary-table-credentials (requires ``EXTERNAL USE SCHEMA``
    + ``MODIFY`` on the metrics schema).
    """
    fqn = metrics_table_fqn(catalog, mode)
    location = metrics_table_location(catalog, storage_base, mode)
    columns = ",\n".join(f"    {s.column} {s.delta_type}" for s in METRICS_FIELD_MAP)
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n{columns}\n) USING DELTA\nLOCATION '{location}'"


def metrics_tables(catalog: str) -> dict[str, str]:
    """Return ``{mode: fqn}`` for all three per-mode metrics tables in ``catalog``."""
    return {mode: metrics_table_fqn(catalog, mode) for mode in ALL_METRIC_MODES}
