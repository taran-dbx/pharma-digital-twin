"""Tests for iot_twin.services.lakets.lakets_service.create_rollup (RollUp creation).

The asyncpg pool is mocked with an ``AsyncMock``; we assert that
``create_rollup`` calls ``lakets.create_rollup`` with the mapped arguments and
is idempotent against ``lakets._rollup_registry``.
"""

from __future__ import annotations

import datetime as _dt
from unittest.mock import AsyncMock

import pytest

from iot_twin.services.lakets import lakets_service


def _pool(*, existing: bool = False) -> AsyncMock:
    """Mocked asyncpg pool.

    ``fetchrow`` probes the registry; ``fetchval`` invokes
    ``lakets.create_rollup`` when the definition does not exist.
    """
    pool = AsyncMock()
    pool.fetchrow.return_value = (
        {
            "id": 1,
            "query_text": (
                "SELECT lakets.time_bucket('1 hour'::interval, event_time) AS bucket, "
                "avg(value) AS avg_value FROM public.sensor_readings GROUP BY bucket"
            ),
        }
        if existing
        else None
    )
    pool.fetchval.return_value = 7
    return pool


class TestCreateRollup:
    @pytest.mark.asyncio
    async def test_groups_by_dimensions_and_supports_stddev_samp_c7223_regression(
        self,
    ) -> None:
        """[C7223][regression] Group a RollUp by bucket plus dimensions."""
        pool = _pool(existing=False)

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_1m",
            bucket="1 minute",
            group_columns=["sensor_name", "sensor_type"],
            aggregates=[
                {"fn": "avg", "column": "value", "as": "avg_value"},
                {
                    "fn": "stddev_samp",
                    "column": "value",
                    "as": "stddev_value",
                },
            ],
        )

        assert result["status"] == "created"
        query = pool.fetchval.await_args_list[0].args[2]
        normalized_query = " ".join(query.lower().split())
        assert (
            "select lakets.time_bucket('1 minute'::interval, event_time) as bucket, "
            "sensor_name, sensor_type, avg(value) as avg_value, "
            "stddev_samp(value) as stddev_value"
        ) in normalized_query
        assert "group by bucket, sensor_name, sensor_type" in normalized_query

    @pytest.mark.asyncio
    async def test_creates_rollup_with_mapped_args(self) -> None:
        pool = _pool(existing=False)

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[
                {"fn": "avg", "column": "value"},
                {"fn": "max", "column": "value"},
            ],
        )

        assert result == {
            "resource": "LakeTS rollup sensor_hourly",
            "status": "created",
        }

        create_call = pool.fetchval.await_args_list[0]
        args = create_call.args
        # SELECT lakets.create_rollup($1=name, $2=query, $3=bucket,
        #                             $4=source_table, $5=source_schema)
        assert "lakets.create_rollup" in args[0]
        assert args[1] == "sensor_hourly"  # p_name
        query = args[2]  # p_query
        assert "lakets.time_bucket" in query
        assert "avg(value)" in query
        assert "max(value)" in query
        assert "from public.sensor_readings" in query.lower()
        assert "group by" in query.lower()
        # bucket interval mapped to a timedelta (asyncpg INTERVAL on the wire)
        assert args[3] == _dt.timedelta(hours=1)
        assert args[4] == "sensor_readings"  # p_source_table
        assert args[5] == "public"  # p_source_schema

    @pytest.mark.asyncio
    async def test_idempotent_when_already_exists(self) -> None:
        pool = _pool(existing=True)

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[{"fn": "avg", "column": "value"}],
        )

        assert result == {
            "resource": "LakeTS rollup sensor_hourly",
            "status": "already_exists",
        }
        pool.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_accepts_raw_sql_aggregate_spec(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="15 minutes",
            aggregates="avg(value) AS avg_value, count(*) AS n",
        )

        query = pool.fetchval.await_args_list[0].args[2]
        assert "avg(value) AS avg_value, count(*) AS n" in query
        assert "lakets.time_bucket" in query

    @pytest.mark.asyncio
    async def test_custom_schema(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="readings",
            name="r_roll",
            bucket="1 day",
            aggregates=[{"fn": "sum", "column": "delta"}],
            schema="iot",
        )

        create_args = pool.fetchval.await_args_list[0].args
        assert create_args[5] == "iot"  # p_source_schema
        assert "from iot.readings" in create_args[2].lower()

    @pytest.mark.asyncio
    async def test_never_raises_returns_failed(self) -> None:
        pool = AsyncMock()
        pool.fetchrow.return_value = None
        pool.fetchval.side_effect = RuntimeError("boom")

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[{"fn": "avg", "column": "value"}],
        )

        assert result["status"] == "failed"
        assert "boom" in result["error"]

    @pytest.mark.asyncio
    async def test_named_aggregate_uses_alias(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[{"fn": "avg", "column": "value", "as": "avg_v"}],
        )

        query = pool.fetchval.await_args_list[0].args[2]
        assert "avg(value) AS avg_v" in query

    @pytest.mark.asyncio
    async def test_existing_definition_mismatch_returns_failed(self) -> None:
        pool = AsyncMock()
        pool.fetchrow.return_value = {
            "id": 5,
            "query_text": (
                "SELECT lakets.time_bucket('1 minute'::interval, event_time) AS bucket, "
                "avg(value) AS avg_value FROM public.sensor_readings GROUP BY bucket"
            ),
        }

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_1m",
            bucket="1 minute",
            group_columns=["sensor_name"],
            aggregates=[
                {"fn": "avg", "column": "value", "as": "avg_value"},
                {"fn": "stddev_samp", "column": "value", "as": "stddev_value"},
            ],
        )

        assert result["status"] == "failed"
        assert "definition mismatch" in result["error"]
        assert "drop or migrate it explicitly" in result["error"]
        pool.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_list_aggregate_is_rejected(self) -> None:
        pool = AsyncMock()

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="unsafe",
            bucket="1 minute",
            aggregates=["avg(value); DROP TABLE sensor_readings"],
        )

        assert result["status"] == "failed"
        assert "unsupported aggregate function" in result["error"]
        pool.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_named_count_uses_count_star(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_1m",
            bucket="1 minute",
            aggregates=[{"fn": "count", "column": "value", "as": "sample_count"}],
        )

        query = pool.fetchval.await_args.args[2]
        assert "count(*) AS sample_count" in query


class TestRollupInvalidation:
    @pytest.mark.asyncio
    async def test_existing_triggers_are_refreshed_for_new_chunks(self) -> None:
        pool = AsyncMock()
        pool.fetchval.side_effect = [True, None]

        result = await lakets_service.enable_rollup_invalidation(pool, "sensor_1m")

        assert result["status"] == "already_exists"
        assert pool.fetchval.await_count == 2
        assert "lakets.enable_rollup_invalidation" in (pool.fetchval.await_args_list[1].args[0])


class TestEnableLvc:
    @pytest.mark.asyncio
    async def test_existing_definition_mismatch_returns_failed(self) -> None:
        pool = AsyncMock()
        pool.fetchrow.return_value = {
            "id": 3,
            "enabled": True,
            "key_columns": ["sensor_name"],
            "value_columns": ["value"],
        }

        result = await lakets_service.enable_lvc(
            pool,
            table="sensor_readings",
            key_columns=["sensor_name"],
            value_columns=["event_time", "value"],
        )

        assert result["status"] == "failed"
        assert "definition mismatch" in result["error"]
        pool.fetchval.assert_not_awaited()
