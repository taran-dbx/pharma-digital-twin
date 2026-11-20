"""Guard tests for the managed metrics table DDL and Zerobus writer contract.

Covers two layers:

1. Python model (``models/metrics_schema.metrics_managed_table_ddl``):
   - DDL is ``CREATE TABLE IF NOT EXISTS … USING DELTA`` with **no** ``LOCATION``
     (managed storage — no delta-rs / S3 credential vending required).
   - ``mode STRING`` is the first column (Task 3/4 one-schema layout).

2. TypeScript notebook-template source (``client/src/lib/notebook-template.ts``):
   - ``BufferedDeltaMetricsListener._metrics_flush_loop`` uses the Zerobus SDK
     (``ingest_record``) — NOT Spark ``.write…save(LOCATION)`` or delta-rs.
   - ``_metrics_record`` includes a ``mode`` key and epoch-µs int timestamp.
   - The zerobus producer (``data-producer-notebook.ts``) reads the app SP client
     secret from the UC secret ``app_sp_oauth`` via
     ``dbutils.secrets.get(catalog=…, schema=…, key="app_sp_oauth")``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from iot_twin.models import metrics_schema as ms

# ---------------------------------------------------------------------------
# TypeScript source guards
# ---------------------------------------------------------------------------
NOTEBOOK_TS = (
    Path(__file__).resolve().parent.parent / "client" / "src" / "lib" / "notebook-template.ts"
)
PRODUCER_TS = (
    Path(__file__).resolve().parent.parent / "client" / "src" / "lib" / "data-producer-notebook.ts"
)

_ts_skip = pytest.mark.skipif(
    not NOTEBOOK_TS.exists(),
    reason="notebook-template.ts not present (client/ gitignored)",
)
_producer_skip = pytest.mark.skipif(
    not PRODUCER_TS.exists(),
    reason="data-producer-notebook.ts not present (client/ gitignored)",
)


# ---------------------------------------------------------------------------
# 1. Python model: managed metrics table DDL
# ---------------------------------------------------------------------------
class TestManagedMetricsDdl:
    """``metrics_managed_table_ddl`` must produce a MANAGED table DDL."""

    def test_create_table_if_not_exists(self) -> None:
        ddl = ms.metrics_managed_table_ddl("cat.sch.pharma_metrics")
        assert "CREATE TABLE IF NOT EXISTS cat.sch.pharma_metrics" in ddl

    def test_no_location_clause(self) -> None:
        """Managed table — no LOCATION means no delta-rs credential vending."""
        ddl = ms.metrics_managed_table_ddl("cat.sch.pharma_metrics")
        assert "LOCATION" not in ddl

    def test_using_delta(self) -> None:
        ddl = ms.metrics_managed_table_ddl("cat.sch.pharma_metrics")
        assert "USING DELTA" in ddl

    def test_mode_string_is_first_column(self) -> None:
        """``mode STRING`` is the leading column in the managed table (Task 3/4)."""
        cols = ms.metrics_managed_columns()
        assert cols[0].column == ms.METRICS_MODE_COLUMN
        assert cols[0].delta_type == "STRING"
        # Confirmed in the DDL text too
        ddl = ms.metrics_managed_table_ddl("cat.sch.pharma_metrics")
        # mode STRING must appear before the first METRICS_FIELD_MAP column
        mode_pos = ddl.index("mode STRING")
        first_field_pos = ddl.index(ms.METRICS_FIELD_MAP[0].column)
        assert mode_pos < first_field_pos, "mode STRING must be the first column in the DDL"

    def test_all_metric_fields_present(self) -> None:
        ddl = ms.metrics_managed_table_ddl("cat.sch.pharma_metrics")
        for spec in ms.METRICS_FIELD_MAP:
            assert spec.column in ddl, f"DDL missing field {spec.column}"

    def test_industry_prefix_in_fqn(self) -> None:
        """FQN is ``{catalog}.{schema}.{industry}_metrics`` (one-schema layout)."""
        ddl = ms.metrics_managed_table_ddl(
            "example_eu_catalog.rtm_lakets_zb.pharmaceutical_metrics"
        )
        assert "pharmaceutical_metrics" in ddl


# ---------------------------------------------------------------------------
# 2. TypeScript guard: metrics writer uses Zerobus (no Spark .save / delta-rs)
# ---------------------------------------------------------------------------
def _metrics_block() -> str:
    """Region from the Zerobus-metrics header to addListener() call."""
    text = NOTEBOOK_TS.read_text()
    m = re.search(
        r"# Streaming metrics — buffered async Zerobus push.*?"
        r"spark\.streams\.addListener\(BufferedDeltaMetricsListener\(\)\)",
        text,
        re.DOTALL,
    )
    assert m is not None, (
        "could not locate Zerobus metrics listener block in notebook-template.ts — "
        "did the header comment change?"
    )
    return m.group(0)


@_ts_skip
class TestMetricsWriterUsesZerobus:
    def test_uses_zerobus_ingest_record(self) -> None:
        block = _metrics_block()
        assert "_zb_metrics_stream.ingest_record(rec)" in block

    def test_no_spark_save_to_location(self) -> None:
        """Must NOT use Spark .save(LOCATION) — that was the old delta-rs path."""
        block = _metrics_block()
        assert ".save(METRICS_TABLE_LOCATION)" not in block
        assert ".save(" not in block

    def test_no_saveastable(self) -> None:
        block = _metrics_block()
        assert ".saveAsTable(" not in block

    def test_no_create_dataframe_write(self) -> None:
        """spark.createDataFrame(…).write must not appear in the metrics listener."""
        block = _metrics_block()
        assert "spark.createDataFrame(" not in block

    def test_mode_column_in_record(self) -> None:
        """``_metrics_record`` must include a ``mode`` key (managed table layout)."""
        block = _metrics_block()
        assert '"mode": MODE' in block

    def test_timestamp_is_epoch_microseconds(self) -> None:
        """timestamp must be epoch-µs INT via .timestamp() * 1_000_000."""
        block = _metrics_block()
        # The value must use the epoch-µs int pattern, not a bare datetime object.
        assert "timestamp() * 1_000_000" in block
        # The datetime is used ONLY to get epoch seconds, not stored as a datetime.
        assert '"timestamp": int(datetime.now(timezone.utc).timestamp() * 1_000_000)' in block

    def test_no_progress_blob(self) -> None:
        """Raw progress blob must NOT be written (managed table; no delta-rs STRING
        constraint). The key ``"progress": progress_json`` must not appear."""
        block = _metrics_block()
        assert '"progress": progress_json' not in block

    def test_uc_secret_for_auth(self) -> None:
        """Metrics writer must use UC secret (catalog/schema) not a scope-based key."""
        block = _metrics_block()
        assert 'key="app_sp_oauth"' in block
        assert "catalog=_zb_uc_catalog" in block
        assert "schema=_zb_uc_schema" in block

    def test_buffers_via_queue_and_daemon_thread(self) -> None:
        block = _metrics_block()
        assert "_metrics_q.put" in block
        assert "daemon=True" in block

    def test_targets_metrics_table_variable(self) -> None:
        """Notebook must reference METRICS_TABLE (the managed table FQN)."""
        block = _metrics_block()
        assert "METRICS_TABLE" in block


# ---------------------------------------------------------------------------
# 3. TypeScript guard: zerobus producer reads UC secret for OAuth
# ---------------------------------------------------------------------------
def _zerobus_producer_section() -> str:
    """Isolate the zerobus producer section from data-producer-notebook.ts."""
    text = PRODUCER_TS.read_text()
    m = re.search(
        r"// ---------------------------------------------------------------------------\n// Zerobus producer.*?dbutils\.notebook\.exit",
        text,
        re.DOTALL,
    )
    assert m is not None, "could not locate zerobus producer section"
    return m.group(0)


@_producer_skip
class TestZerobusProducerUcSecret:
    def test_reads_app_sp_oauth_from_uc_secret(self) -> None:
        section = _zerobus_producer_section()
        # Primary path: dbutils.secrets.get(catalog=…, schema=…, key=…)
        assert 'key="app_sp_oauth"' in section
        assert "catalog=_uc_catalog" in section
        assert "schema=_uc_schema" in section

    def test_derives_catalog_schema_from_table_name(self) -> None:
        section = _zerobus_producer_section()
        assert "_m_parts = TABLE_NAME.split" in section
        assert "_uc_catalog = _m_parts[0]" in section
        assert "_uc_schema" in section

    def test_uses_client_id_from_config(self) -> None:
        """CLIENT_ID must come from injected config — not from a secret scope."""
        section = _zerobus_producer_section()
        assert "CLIENT_ID" in section
        # The stream must be created with CLIENT_ID (not with a scope-derived id)
        assert "CLIENT_ID, client_secret" in section

    def test_event_time_epoch_microseconds(self) -> None:
        """event_time must be int(rec.timestamp * 1_000_000)."""
        section = _zerobus_producer_section()
        assert "int(rec.timestamp * 1_000_000)" in section

    def test_no_scope_based_secret(self) -> None:
        """Legacy scope-based secret reading must be gone."""
        section = _zerobus_producer_section()
        assert 'key="zerobus-client-secret"' not in section
        assert 'key="zerobus-client-id"' not in section
