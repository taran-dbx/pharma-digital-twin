"""Tests for iot_twin.services.lakets.lakets_service — LakeTS bootstrap + version probe."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services.lakets import lakets_service


# ---------------------------------------------------------------------------
# get_installed_version
# ---------------------------------------------------------------------------
class TestGetInstalledVersion:
    """Queries `lakets._version` and returns the latest installed version."""

    @pytest.mark.asyncio
    async def test_returns_none_when_lakets_missing(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(
            side_effect=Exception('relation "lakets._version" does not exist')
        )
        result = await lakets_service.get_installed_version(pool)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_table_empty(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=None)
        result = await lakets_service.get_installed_version(pool)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_version_string(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value="0.1.0")
        result = await lakets_service.get_installed_version(pool)
        assert result == "0.1.0"

    @pytest.mark.asyncio
    async def test_queries_latest_row_by_installed_at(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value="0.1.0")
        await lakets_service.get_installed_version(pool)
        sql_arg = pool.fetchval.await_args.args[0]
        assert "lakets._version" in sql_arg
        assert "ORDER BY installed_at DESC" in sql_arg
        assert "LIMIT 1" in sql_arg


# ---------------------------------------------------------------------------
# resolve_lakets_source
# ---------------------------------------------------------------------------
class TestResolveLakeTsSource:
    """Resolves where to read lakets.sql from — local path or git clone."""

    def test_uses_explicit_path_arg(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS v0.1.0\n")
        resolved = lakets_service.resolve_lakets_source(explicit_path=str(sql_file))
        assert resolved == sql_file

    def test_raises_when_explicit_path_missing(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.sql"
        with pytest.raises(FileNotFoundError, match="LakeTS SQL not found"):
            lakets_service.resolve_lakets_source(explicit_path=str(missing))

    def test_uses_env_sql_path_when_no_arg(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS v0.1.0\n")
        monkeypatch.setenv("LAKETS_SQL_PATH", str(sql_file))
        monkeypatch.delenv("LAKETS_GIT_URL", raising=False)
        resolved = lakets_service.resolve_lakets_source()
        assert resolved == sql_file

    def test_clones_git_url_when_no_local_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("LAKETS_SQL_PATH", raising=False)
        monkeypatch.setenv("LAKETS_GIT_URL", "https://example.com/fake/LakeTS.git")

        def fake_clone(url: str, dest: Path) -> None:
            (dest / "dist").mkdir(parents=True)
            (dest / "dist" / "lakets.sql").write_text("-- cloned\n")

        with patch.object(lakets_service, "_git_clone", side_effect=fake_clone) as mock_clone:
            resolved = lakets_service.resolve_lakets_source()
            assert resolved.name == "lakets.sql"
            assert resolved.read_text() == "-- cloned\n"
            mock_clone.assert_called_once()

    def test_falls_back_to_packaged_sql(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LAKETS_SQL_PATH", raising=False)
        monkeypatch.delenv("LAKETS_GIT_URL", raising=False)
        resolved = lakets_service.resolve_lakets_source()
        assert resolved == lakets_service.PACKAGED_LAKETS_SQL
        assert resolved.is_file()

    def test_raises_when_no_source_configured(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("LAKETS_SQL_PATH", raising=False)
        monkeypatch.delenv("LAKETS_GIT_URL", raising=False)
        monkeypatch.setattr(lakets_service, "PACKAGED_LAKETS_SQL", tmp_path / "missing.sql")
        with pytest.raises(RuntimeError, match="LakeTS source not configured"):
            lakets_service.resolve_lakets_source()


# ---------------------------------------------------------------------------
# install_lakets
# ---------------------------------------------------------------------------
class TestInstallLakeTs:
    """Executes lakets.sql and returns the installed version."""

    @pytest.mark.asyncio
    async def test_executes_sql_against_pool(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- dummy LakeTS SQL\nCREATE SCHEMA IF NOT EXISTS lakets;\n")
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)
        pool.fetchval = AsyncMock(return_value="0.1.0")

        version = await lakets_service.install_lakets(pool, sql_file)

        assert version == "0.1.0"
        pool.execute.assert_awaited_once()
        executed_sql = pool.execute.await_args.args[0]
        assert "lakets" in executed_sql

    @pytest.mark.asyncio
    async def test_returns_version_from_registry(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)
        pool.fetchval = AsyncMock(return_value="0.1.0")

        version = await lakets_service.install_lakets(pool, sql_file)
        assert version == "0.1.0"

    @pytest.mark.asyncio
    async def test_idempotent_second_call_skips_exec_when_already_installed(
        self, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)
        pool.fetchval = AsyncMock(return_value="0.1.0")

        version = await lakets_service.install_lakets(pool, sql_file, skip_if_installed=True)

        assert version == "0.1.0"
        pool.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_raises_when_sql_file_missing(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.sql"
        pool = MagicMock()
        with pytest.raises(FileNotFoundError):
            await lakets_service.install_lakets(pool, missing)

    @pytest.mark.asyncio
    async def test_propagates_pool_execute_errors(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")
        pool = MagicMock()
        pool.execute = AsyncMock(side_effect=RuntimeError("permission denied"))
        pool.fetchval = AsyncMock(return_value=None)

        with pytest.raises(RuntimeError, match="permission denied"):
            await lakets_service.install_lakets(pool, sql_file)


# ---------------------------------------------------------------------------
# create_chronotable
# ---------------------------------------------------------------------------
class TestCreateChronotable:
    """Wraps lakets.create_chronotable; idempotent via _chronotable_registry probe."""

    @pytest.mark.asyncio
    async def test_creates_when_not_registered(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 42])  # registry probe, create return
        pool.execute = AsyncMock(return_value=None)

        already = await lakets_service.create_chronotable(
            pool,
            table="sensor_readings",
            time_col="event_time",
            chunk="15 minutes",
        )

        assert already is False
        call_sql = pool.fetchval.await_args_list[1].args[0]
        assert "lakets.create_chronotable" in call_sql

    @pytest.mark.asyncio
    async def test_skips_when_already_registered(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=7)  # registry probe returns existing id

        already = await lakets_service.create_chronotable(
            pool,
            table="sensor_readings",
            time_col="event_time",
            chunk="15 minutes",
        )

        assert already is True
        pool.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passes_time_column_and_chunk_to_sql(self) -> None:
        import datetime as _dt

        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 1])

        await lakets_service.create_chronotable(
            pool,
            table="sensor_readings",
            time_col="event_time",
            chunk="15 minutes",
        )

        create_call = pool.fetchval.await_args_list[1]
        assert create_call.args[1] == "sensor_readings"
        assert create_call.args[2] == "event_time"
        assert create_call.args[3] == _dt.timedelta(minutes=15)


# ---------------------------------------------------------------------------
# apply_tier_policy
# ---------------------------------------------------------------------------
class TestApplyTierPolicy:
    """Applies tiered-retention policy idempotently."""

    @pytest.mark.asyncio
    async def test_applies_when_no_existing_policy(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 99])  # probe, apply
        pool.execute = AsyncMock(return_value=None)

        already = await lakets_service.apply_tier_policy(
            pool,
            table="sensor_readings",
            hot="1 hour",
            drop="7 days",
        )

        assert already is False
        apply_sql = pool.fetchval.await_args_list[1].args[0]
        assert "lakets.add_tiered_retention_policy" in apply_sql

    @pytest.mark.asyncio
    async def test_skips_when_policy_exists(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=17)

        already = await lakets_service.apply_tier_policy(
            pool,
            table="sensor_readings",
            hot="1 hour",
            drop="7 days",
        )

        assert already is True
        pool.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passes_intervals_as_params(self) -> None:
        import datetime as _dt

        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 1])

        await lakets_service.apply_tier_policy(
            pool,
            table="sensor_readings",
            hot="1 hour",
            drop="7 days",
        )

        apply_call = pool.fetchval.await_args_list[1]
        assert apply_call.args[1] == "sensor_readings"
        assert apply_call.args[2] == _dt.timedelta(hours=1)
        assert apply_call.args[3] == _dt.timedelta(days=7)

    @pytest.mark.asyncio
    async def test_rejects_hot_gte_drop(self) -> None:
        pool = MagicMock()
        with pytest.raises(ValueError, match="hot .* must be less than drop"):
            await lakets_service.apply_tier_policy(
                pool,
                table="sensor_readings",
                hot="7 days",
                drop="1 hour",
            )
        with pytest.raises(ValueError, match="hot .* must be less than drop"):
            await lakets_service.apply_tier_policy(
                pool,
                table="sensor_readings",
                hot="1 day",
                drop="1 day",
            )


# ---------------------------------------------------------------------------
# check_lakehouse_sync_readiness
# ---------------------------------------------------------------------------
class TestCheckLakehouseSyncReadiness:
    """Reports whether the database is ready to enable Lakehouse Sync."""

    @pytest.mark.asyncio
    async def test_all_green(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(
            side_effect=[
                "databricks_postgres",  # current_database
                True,  # wal2delta available
                "0.1.0",  # wal2delta installed version
                True,  # lakets.enable_sync present
            ]
        )
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["database_compatible"] is True
        assert result["wal2delta_available"] is True
        assert result["wal2delta_installed"] is True
        assert result["lakets_sync_functions_present"] is True
        assert result["ready_to_enable"] is True
        assert result.get("reason") in (None, "")

    @pytest.mark.asyncio
    async def test_database_not_compatible(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(
            side_effect=[
                "custom_db",
                True,
                "0.1.0",
                True,
            ]
        )
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["database_compatible"] is False
        assert result["current_database"] == "custom_db"
        assert result["ready_to_enable"] is False
        assert "databricks_postgres" in result["reason"]

    @pytest.mark.asyncio
    async def test_wal2delta_does_not_gate_readiness(self) -> None:
        # wal2delta is managed by the platform — absence doesn't block sync.
        pool = MagicMock()
        pool.fetchval = AsyncMock(
            side_effect=[
                "databricks_postgres",
                True,
                None,
                True,
            ]
        )
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["wal2delta_available"] is True
        assert result["wal2delta_installed"] is False
        assert result["ready_to_enable"] is True  # <-- key behaviour change
        assert result.get("reason") in (None, "")

    @pytest.mark.asyncio
    async def test_wal2delta_not_available_is_still_ready(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(
            side_effect=[
                "databricks_postgres",
                False,
                None,
                True,
            ]
        )
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["wal2delta_available"] is False
        assert result["wal2delta_installed"] is False
        assert result["ready_to_enable"] is True

    @pytest.mark.asyncio
    async def test_lakets_sync_missing(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(
            side_effect=[
                "databricks_postgres",
                True,
                "0.1.0",
                False,
            ]
        )
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["lakets_sync_functions_present"] is False
        assert result["ready_to_enable"] is False
        assert "LakeTS" in result["reason"]

    @pytest.mark.asyncio
    async def test_pool_error_returns_failed_status(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=RuntimeError("connection timeout"))
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["ready_to_enable"] is False
        assert "connection timeout" in result.get("error", "")


# ---------------------------------------------------------------------------
# run_retention
# ---------------------------------------------------------------------------
class TestRunRetention:
    """Discovers tables with retention policies and invokes lakets.execute_retention."""

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_policies(self) -> None:
        pool = MagicMock()
        pool.fetch = AsyncMock(return_value=[])

        result = await lakets_service.run_retention(pool)
        assert result == {}

    @pytest.mark.asyncio
    async def test_invokes_execute_retention_for_each_table(self) -> None:
        pool = MagicMock()
        pool.fetch = AsyncMock(
            return_value=[
                {"schema_name": "public", "table_name": "sensor_readings"},
                {"schema_name": "public", "table_name": "sliding_aggregates"},
            ]
        )
        pool.fetchval = AsyncMock(side_effect=[5, 0])

        result = await lakets_service.run_retention(pool)
        assert result == {
            "public.sensor_readings": 5,
            "public.sliding_aggregates": 0,
        }
        # One fetchval per table
        assert pool.fetchval.await_count == 2

    @pytest.mark.asyncio
    async def test_per_table_failure_does_not_abort_rest(self) -> None:
        pool = MagicMock()
        pool.fetch = AsyncMock(
            return_value=[
                {"schema_name": "public", "table_name": "a"},
                {"schema_name": "public", "table_name": "b"},
            ]
        )
        pool.fetchval = AsyncMock(side_effect=[RuntimeError("locked"), 3])

        result = await lakets_service.run_retention(pool)
        assert result["public.a"] == "error: locked"
        assert result["public.b"] == 3


class TestLaketsDashboard:
    """Unit contract for the consolidated LakeTS observability service."""

    @pytest.mark.asyncio
    async def test_rollup_series_values_are_json_serializable_c7226_regression(
        self,
    ) -> None:
        """[C7226][regression] Preserve sensor rollup values in the dashboard map."""
        bucket = dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.UTC)
        refreshed_at = bucket + dt.timedelta(minutes=1)
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            return_value={
                "name": "sensor_1m",
                "rollup_table": "_rollup_sensor_1m",
                "bucket": "00:01:00",
                "watermark": bucket,
                "last_refreshed_at": refreshed_at,
                "refresh_age_seconds": Decimal("4.25"),
                "pending_invalidations": 2,
            }
        )
        pool.fetch = AsyncMock(
            return_value=[
                {
                    "bucket": bucket,
                    "sensor_name": "temperature",
                    "sample_count": Decimal("60"),
                    "avg_value": Decimal("21.75"),
                    "min_value": Decimal("20.5"),
                    "max_value": Decimal("23.0"),
                    "stddev_value": Decimal("0.625"),
                }
            ]
        )

        section = await lakets_service._rollup_dashboard(pool)

        assert section["value"]["series"] == [
            {
                "bucket": bucket.isoformat(),
                "sensor_name": "temperature",
                "sample_count": 60,
                "avg_value": 21.75,
                "min_value": 20.5,
                "max_value": 23.0,
                "stddev_value": 0.625,
            }
        ]
        json.dumps(section)

    @pytest.mark.asyncio
    async def test_unavailable_probes_return_null_values_with_reasons_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Never invent dashboard values after probe failures."""
        pool = MagicMock()
        unavailable = RuntimeError("observability catalog unavailable")
        pool.fetch = AsyncMock(side_effect=unavailable)
        pool.fetchrow = AsyncMock(side_effect=unavailable)
        pool.fetchval = AsyncMock(side_effect=unavailable)

        result = await lakets_service.get_dashboard(pool)

        expected_sections = {
            "partitions",
            "current_partition",
            "tiering",
            "rollup",
            "query_comparison",
            "data_quality",
            "lvc",
            "cdf",
        }
        assert set(result) == expected_sections
        for name in expected_sections - {"query_comparison"}:
            assert result[name]["value"] is None, name
            assert result[name]["reason"].endswith("telemetry unavailable"), name
            assert "observability catalog unavailable" not in result[name]["reason"], name
        comparison = result["query_comparison"]
        assert comparison["value"]["raw_rows"] is None
        assert comparison["value"]["rollup_rows"] is None
        assert comparison["reason"] == (
            "Raw aggregate query unavailable; Rollup aggregate query unavailable"
        )

    @pytest.mark.asyncio
    async def test_partition_phases_and_json_serialization_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Serialize partition phases and numeric metadata."""
        start = dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.UTC)
        end = start + dt.timedelta(minutes=15)
        pool = MagicMock()
        pool.fetch = AsyncMock(
            return_value=[
                {
                    "name": "public.sensor_readings_20260901_080000",
                    "range_start": start,
                    "range_end": end,
                    "phase": "current",
                    "status": "active",
                    "recorded_row_count": 12,
                    "recorded_size_bytes": 4096,
                    "active_count": 1,
                    "tiered_count": 0,
                    "dropped_count": 0,
                    "total_count": 600,
                    "is_current": True,
                    "live_row_count": 13,
                    "live_size_bytes": 8192,
                    "age_seconds": Decimal("30.5"),
                }
            ]
        )

        partitions, current = await lakets_service._partition_dashboard(pool)

        assert partitions["value"]["timeline"][0]["phase"] == "current"
        assert partitions["value"]["timeline"][0]["range_start"] == start.isoformat()
        assert current["value"]["phase"] == "current"
        assert current["value"]["age_seconds"] == 30.5
        assert partitions["value"]["counts"]["total"] == 600
        assert "LIMIT 500" in lakets_service._PARTITIONS_DASHBOARD_SQL
        assert "PARTITION BY phase" in lakets_service._PARTITIONS_DASHBOARD_SQL
        json.dumps({"partitions": partitions, "current_partition": current})

    @pytest.mark.asyncio
    async def test_no_policy_preserves_tier_counts_c7225_regression(self) -> None:
        """[C7225][regression] Preserve tier counts without claiming eligibility."""
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            return_value={
                "active": 4,
                "age_eligible": 0,
                "durable_eligible": 0,
                "tiered": 2,
                "dropped": 1,
                "policy_type": None,
                "policy": None,
                "enabled": None,
                "last_run_at": None,
                "committed_lsn": None,
            }
        )

        section = await lakets_service._tiering_dashboard(pool)

        assert section["value"]["active"] == 4
        assert section["value"]["tiered"] == 2
        assert section["value"]["dropped"] == 1
        assert section["value"]["eligible"] is None
        assert "No tiering policy configured" in section["reason"]
        tier_sql = lakets_service._TIERING_DASHBOARD_SQL
        assert "'tiering', 'tiered_retention'" in tier_sql
        assert "p.config->>'after'" in tier_sql
        assert "p.config->>'tier_after'" in tier_sql
        assert "enabled DESC NULLS LAST" in tier_sql

    @pytest.mark.asyncio
    async def test_data_quality_reports_exact_missing_buckets_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Report missing buckets without inventing samples."""
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            return_value={
                "sensor_count": 2,
                "expected_bucket_count": 60,
                "observed_bucket_count": 57,
                "missing_bucket_count": 3,
                "missing_bucket_percentage": Decimal("5.00"),
            }
        )

        section = await lakets_service._data_quality_dashboard(pool)

        assert section["value"]["missing_bucket_count"] == 3
        assert section["value"]["gapfilled_count"] is None
        assert section["value"]["missing_sample_percentage"] is None
        assert "generate_series" in lakets_service._DATA_QUALITY_DASHBOARD_SQL

    @pytest.mark.asyncio
    async def test_query_comparison_keeps_independent_failures_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Compare equivalent aggregates without hiding failures."""
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            side_effect=[
                RuntimeError("raw unavailable"),
                {"aggregate_rows": 12, "source_rows": 120},
            ]
        )

        section = await lakets_service._query_comparison_dashboard(pool)

        assert section["value"]["raw_rows"] is None
        assert section["value"]["rollup_rows"] == 12
        assert section["value"]["raw_error"] == "Raw aggregate query unavailable"
        assert section["value"]["source_event_rows"] is None
        assert section["value"]["persisted_rollup_rows"] == 12
        assert section["value"]["event_to_rollup_row_reduction_percentage"] is None
        assert "GROUP BY bucket, sensor_name" in (lakets_service._RAW_AGGREGATE_COUNT_SQL)
        assert "sample_count" in lakets_service._ROLLUP_AGGREGATE_COUNT_SQL

    @pytest.mark.asyncio
    async def test_query_comparison_runs_probes_concurrently_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Time raw and rollup probes in parallel."""
        both_started = asyncio.Event()
        started = 0

        async def fetchrow(sql: str) -> dict[str, int]:
            nonlocal started
            started += 1
            if started == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=0.2)
            if sql == lakets_service._RAW_AGGREGATE_COUNT_SQL:
                return {"aggregate_rows": 10, "source_rows": 100}
            return {"aggregate_rows": 10, "source_rows": 100}

        pool = MagicMock()
        pool.fetchrow = AsyncMock(side_effect=fetchrow)

        section = await lakets_service._query_comparison_dashboard(pool)

        assert pool.fetchrow.await_count == 2
        assert section["value"]["source_event_rows"] == 100
        assert section["value"]["persisted_rollup_rows"] == 10
        assert section["value"]["event_to_rollup_row_reduction_percentage"] == 90

    @pytest.mark.asyncio
    async def test_lvc_event_and_update_lag_are_json_safe_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Expose cache event lag and update age."""
        event_time = dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.UTC)
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            side_effect=[
                {
                    "enabled": True,
                    "cache_table_name": "_lvc_sensor_readings",
                    "key_columns": ["sensor_name"],
                    "value_columns": ["event_time", "value"],
                },
                {
                    "cached_series": 8,
                    "source_event_time": event_time,
                    "cache_event_time": event_time - dt.timedelta(seconds=2),
                    "cache_updated_at": event_time + dt.timedelta(seconds=1),
                    "source_to_cache_event_lag_seconds": Decimal("2.0"),
                    "cache_update_age_seconds": Decimal("4.5"),
                },
            ]
        )

        section = await lakets_service._lvc_dashboard(pool)

        assert section["value"]["source_event_time"] == event_time.isoformat()
        assert section["value"]["source_to_cache_event_lag_seconds"] == 2.0
        assert section["value"]["cache_update_age_seconds"] == 4.5

    @pytest.mark.asyncio
    async def test_cdf_serializes_lsn_and_shadow_lag_c7225_regression(
        self,
    ) -> None:
        """[C7225][regression] Serialize CDF positions and event lag."""

        class PgLsn:
            def __str__(self) -> str:
                return "0/16B6C50"

        hot_latest = dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.UTC)
        pool = MagicMock()
        pool.fetchrow = AsyncMock(
            return_value={
                "hot_latest": hot_latest,
                "sync_enabled": True,
                "shadow_table_name": "_shadow_sensor_readings",
                "committed_lsn": PgLsn(),
                "wal_lag_bytes": Decimal("128"),
            }
        )
        pool.fetchval = AsyncMock(return_value=hot_latest - dt.timedelta(seconds=3))

        section = await lakets_service._cdf_dashboard(pool)

        assert "committed_lsn" not in section["value"]
        assert section["value"]["wal_lag_bytes"] == 128
        assert section["value"]["hot_to_shadow_event_lag_seconds"] == 3.0


@pytest.mark.asyncio
async def test_unique_shadow_migration_is_atomic_and_idempotent_c7238():
    """Cutover precedes unlocked history copies [C7238][defect][regression]."""
    pool = MagicMock()
    target = "_shadow_manufacturing_sensor_readings"
    pool.fetchrow = AsyncMock(
        side_effect=[
            {"shadow_table_name": "_shadow_sensor_readings", "sync_enabled": True},
            {"backfill_complete": False, "index_valid": False},
            {"backfill_complete": False, "index_valid": True},
            {"shadow_table_name": target, "sync_enabled": True},
            {"backfill_complete": True, "index_valid": True},
        ]
    )
    pool.fetchval = AsyncMock(side_effect=[True, True, True])
    pool.execute = AsyncMock(return_value="DO")

    migrated = await lakets_service.reconcile_chronotable_shadow(pool, shadow_table=target)

    assert migrated["status"] == "migrated"
    sql_calls = [call.args[0] for call in pool.execute.await_args_list]
    cutover_index = next(index for index, sql in enumerate(sql_calls) if "DO $reconcile$" in sql)
    legacy_copy_index = next(
        index
        for index, sql in enumerate(sql_calls)
        if "FROM lakets_cdf._shadow_sensor_readings AS source_rows" in sql
    )
    public_copy_index = next(
        index
        for index, sql in enumerate(sql_calls)
        if "FROM public.sensor_readings AS source_rows" in sql
    )
    marker_index = next(
        index for index, sql in enumerate(sql_calls) if "lakets-cdf-backfill-complete:v1" in sql
    )
    cutover_sql = sql_calls[cutover_index]
    legacy_copy_sql = sql_calls[legacy_copy_index]
    assert cutover_index < legacy_copy_index < public_copy_index < marker_index
    assert "INSERT INTO" not in cutover_sql
    assert "max(id)" not in cutover_sql
    assert "LOCK TABLE public.sensor_readings" in cutover_sql
    assert "CREATE OR REPLACE TRIGGER trg_lakets_sync" in cutover_sql
    assert "source IS DISTINCT FROM 'validation'" in legacy_copy_sql
    assert "ON CONFLICT (id) DO NOTHING" in legacy_copy_sql
    assert "LOCK TABLE" not in legacy_copy_sql
    assert any(
        "DROP INDEX CONCURRENTLY IF EXISTS "
        "lakets_cdf._shadow_manufacturing_sensor_readings_id_uidx" in sql
        for sql in sql_calls
    )
    assert any(
        "CREATE UNIQUE INDEX CONCURRENTLY _shadow_manufacturing_sensor_readings_id_uidx" in sql
        for sql in sql_calls
    )
    assert "DROP TABLE" not in "\n".join(sql_calls)
    assert migrated["copy_sources"] == [
        "lakets_cdf._shadow_sensor_readings",
        "public.sensor_readings",
    ]


@pytest.mark.asyncio
async def test_completed_shadow_fast_path_has_no_writes_c7254():
    """Durably completed migrations perform no writes [C7254][smoke][regression]."""
    pool = MagicMock()
    target = "_shadow_manufacturing_sensor_readings"
    pool.fetchrow = AsyncMock(
        side_effect=[
            {"shadow_table_name": target, "sync_enabled": True},
            {"backfill_complete": True, "index_valid": True},
        ]
    )
    pool.fetchval = AsyncMock()
    pool.execute = AsyncMock()

    existing = await lakets_service.reconcile_chronotable_shadow(pool, shadow_table=target)

    assert existing["status"] == "already_exists"
    assert existing["copy_sources"] == []
    pool.fetchval.assert_not_awaited()
    pool.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_interrupted_backfill_retries_after_cutover_c7255():
    """Missing marker forces idempotent crash recovery [C7255][defect][regression]."""
    target = "_shadow_manufacturing_sensor_readings"
    first_pool = MagicMock()
    first_pool.fetchrow = AsyncMock(
        side_effect=[
            {"shadow_table_name": "_shadow_sensor_readings", "sync_enabled": True},
            {"backfill_complete": False, "index_valid": True},
            {"shadow_table_name": target, "sync_enabled": True},
        ]
    )
    first_pool.fetchval = AsyncMock(side_effect=[True, True, True])
    first_sql: list[str] = []

    async def fail_during_legacy_copy(sql):
        first_sql.append(sql)
        if "FROM lakets_cdf._shadow_sensor_readings AS source_rows" in sql:
            raise RuntimeError("connection lost")
        return "OK"

    first_pool.execute = AsyncMock(side_effect=fail_during_legacy_copy)
    interrupted = await lakets_service.reconcile_chronotable_shadow(first_pool, shadow_table=target)

    assert interrupted["status"] == "failed"
    assert any("DO $reconcile$" in sql for sql in first_sql)
    assert not any("lakets-cdf-backfill-complete:v1" in sql for sql in first_sql)

    retry_pool = MagicMock()
    retry_pool.fetchrow = AsyncMock(
        side_effect=[
            {"shadow_table_name": target, "sync_enabled": True},
            {"backfill_complete": False, "index_valid": True},
            {"backfill_complete": False, "index_valid": True},
            {"shadow_table_name": target, "sync_enabled": True},
            {"backfill_complete": True, "index_valid": True},
        ]
    )
    retry_pool.fetchval = AsyncMock(side_effect=[True, True, True])
    retry_pool.execute = AsyncMock(return_value="OK")

    recovered = await lakets_service.reconcile_chronotable_shadow(retry_pool, shadow_table=target)

    assert recovered["status"] == "migrated"
    retry_sql = "\n".join(call.args[0] for call in retry_pool.execute.await_args_list)
    assert "FROM lakets_cdf._shadow_sensor_readings AS source_rows" in retry_sql
    assert "FROM public.sensor_readings AS source_rows" in retry_sql
    assert "lakets-cdf-backfill-complete:v1" in retry_sql


@pytest.mark.asyncio
async def test_new_shadow_without_legacy_copies_public_source_c7250():
    """Fresh branches reconcile without a legacy shadow [C7250][smoke][regression]."""
    pool = MagicMock()
    target = "_shadow_energy_sensor_readings"
    pool.fetchrow = AsyncMock(
        side_effect=[
            {"shadow_table_name": None, "sync_enabled": False},
            {"backfill_complete": False, "index_valid": False},
            {"backfill_complete": False, "index_valid": True},
            {"shadow_table_name": target, "sync_enabled": True},
            {"backfill_complete": True, "index_valid": True},
        ]
    )
    pool.fetchval = AsyncMock(side_effect=[True, False])
    pool.execute = AsyncMock(return_value="OK")

    migrated = await lakets_service.reconcile_chronotable_shadow(pool, shadow_table=target)

    assert migrated["status"] == "migrated"
    assert migrated["copy_sources"] == ["public.sensor_readings"]
    all_sql = "\n".join(call.args[0] for call in pool.execute.await_args_list)
    assert "FROM public.sensor_readings AS source_rows" in all_sql
    assert "FROM lakets_cdf._shadow_sensor_readings" not in all_sql
