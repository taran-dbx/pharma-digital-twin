"""Tests for the per-mode streaming-metrics external Delta SSOT (spec §1.11)."""

from __future__ import annotations

import pytest

from iot_twin.models import metrics_schema as ms


class TestMetricsColumns:
    def test_progress_is_string_not_variant(self) -> None:
        by_col = {s.column: s.delta_type for s in ms.METRICS_FIELD_MAP}
        # delta-rs cannot write VARIANT → progress must be STRING (§1.11).
        assert by_col["progress"] == "STRING"

    def test_has_all_typed_latency_percentiles(self) -> None:
        cols = set(ms.metrics_columns())
        for c in (
            "batch_id",
            "timestamp",
            "num_input_rows",
            "input_rps",
            "processed_rps",
            "trigger_ms",
            "e2e_p50",
            "e2e_p99",
            "processing_p50",
            "processing_p99",
            "queuing_p50",
            "queuing_p99",
            "progress",
        ):
            assert c in cols, f"missing metrics column {c}"

    def test_mode_tagging_columns_present(self) -> None:
        cols = set(ms.metrics_columns())
        assert {"query_id", "query_name", "run_id"} <= cols

    def test_no_mode_column_separation_is_by_table(self) -> None:
        # Separation across modes is by TABLE, not by a `mode` column.
        assert "mode" not in set(ms.metrics_columns())


class TestAllMetricModes:
    def test_streaming_modes_only(self) -> None:
        # Only Spark streaming modes emit query metrics — zerobus is not a stream.
        assert ms.ALL_METRIC_MODES == ("realtime", "microbatch")


class TestMetricsDdl:
    @pytest.mark.parametrize("mode", ["realtime", "microbatch"])
    def test_fqn_and_location_per_mode(self, mode: str) -> None:
        assert ms.metrics_table_fqn("iot_spiketest", mode) == (
            f"iot_spiketest.metrics.streaming_metrics_{mode}"
        )
        loc = ms.metrics_table_location("iot_spiketest", "s3://example-shared-bucket", mode)
        assert loc == (
            "s3://example-shared-bucket/iot-digital-twin/iot_spiketest/metrics/"
            f"streaming_metrics_{mode}/"
        )

    def test_ddl_is_external_with_location(self) -> None:
        ddl = ms.metrics_external_table_ddl(
            "iot_spiketest", "s3://example-shared-bucket", "realtime"
        )
        assert "CREATE TABLE IF NOT EXISTS iot_spiketest.metrics.streaming_metrics_realtime" in ddl
        assert "USING DELTA" in ddl
        assert "LOCATION 's3://example-shared-bucket/iot-digital-twin/" in ddl
        assert "streaming_metrics_realtime/" in ddl
        assert "progress STRING" in ddl

    def test_location_trims_trailing_slash_on_base(self) -> None:
        loc = ms.metrics_table_location("iot_x", "s3://bucket/", "microbatch")
        assert loc == "s3://bucket/iot-digital-twin/iot_x/metrics/streaming_metrics_microbatch/"

    def test_unknown_mode_raises(self) -> None:
        with pytest.raises(ValueError):
            ms.metrics_table_fqn("iot_x", "batch")
        with pytest.raises(ValueError):
            ms.metrics_table_location("iot_x", "s3://bucket", "batch")
        with pytest.raises(ValueError):
            ms.metrics_external_table_ddl("iot_x", "s3://bucket", "batch")

    def test_metrics_tables_returns_streaming_mode_fqns(self) -> None:
        # Only Spark streaming modes get a metrics table — zerobus is not a stream.
        tables = ms.metrics_tables("iot_spiketest")
        assert tables == {
            "realtime": "iot_spiketest.metrics.streaming_metrics_realtime",
            "microbatch": "iot_spiketest.metrics.streaming_metrics_microbatch",
        }
        assert "zerobus" not in tables
