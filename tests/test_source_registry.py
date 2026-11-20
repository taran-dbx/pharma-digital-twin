"""Tests for iot_twin.services.source_registry — key derivation, claim, release, check."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services.source_registry import (
    check_source_available,
    claim_source,
    derive_source_key,
    release_source,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(fetchrow_return=None) -> MagicMock:
    db = MagicMock()
    db.fetchrow = AsyncMock(return_value=fetchrow_return)
    db.execute = AsyncMock(return_value=None)
    return db


# ---------------------------------------------------------------------------
# Task 2: derive_source_key
# ---------------------------------------------------------------------------


class TestDeriveSourceKey:
    """Verify normalized key derivation for all 6 source types."""

    def test_kafka_single_server(self) -> None:
        cfg = {"bootstrap_servers": "broker1:9092", "topic": "sensors"}
        key = derive_source_key("kafka", cfg)
        assert key == "broker1:9092::sensors"

    def test_kafka_servers_sorted_alphabetically(self) -> None:
        cfg = {"bootstrap_servers": "z-broker:9092, a-broker:9092", "topic": "iot"}
        key = derive_source_key("kafka", cfg)
        assert key.startswith("a-broker:9092,z-broker:9092::")

    def test_kafka_case_insensitive(self) -> None:
        cfg = {"bootstrap_servers": "Broker1:9092", "topic": "SENSORS"}
        key = derive_source_key("kafka", cfg)
        assert key == key.lower()

    def test_kafka_missing_topic_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("kafka", {"bootstrap_servers": "b:9092"})

    def test_kafka_missing_bootstrap_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("kafka", {"topic": "t"})

    def test_kinesis_basic(self) -> None:
        cfg = {"region": "us-east-1", "stream_name": "iot-stream"}
        key = derive_source_key("kinesis", cfg)
        assert key == "us-east-1::iot-stream"

    def test_kinesis_lowercased(self) -> None:
        cfg = {"region": "US-EAST-1", "stream_name": "IOT-STREAM"}
        key = derive_source_key("kinesis", cfg)
        assert key == "us-east-1::iot-stream"

    def test_kinesis_missing_region_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("kinesis", {"stream_name": "s"})

    def test_eventhub_basic(self) -> None:
        cfg = {
            "namespace": "my-ns",
            "eventhub_name": "hub1",
            "consumer_group": "$Default",
        }
        key = derive_source_key("eventhub", cfg)
        assert key == "my-ns::hub1::$default"

    def test_eventhub_missing_consumer_group_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("eventhub", {"namespace": "ns", "eventhub_name": "h"})

    def test_rabbitmq_basic(self) -> None:
        cfg = {"host": "rabbit.local", "port": 5672, "queue_name": "jobs"}
        key = derive_source_key("rabbitmq", cfg)
        assert key == "rabbit.local:5672::jobs"

    def test_rabbitmq_missing_queue_name_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("rabbitmq", {"host": "h", "port": 5672})

    def test_pubsub_basic(self) -> None:
        cfg = {"project_id": "my-project", "subscription_id": "sub-1"}
        key = derive_source_key("pubsub", cfg)
        assert key == "my-project::sub-1"

    def test_pubsub_slash_variant(self) -> None:
        cfg = {"project_id": "my-project", "subscription_id": "sub-1"}
        key = derive_source_key("pub/sub", cfg)
        assert key == "my-project::sub-1"

    def test_pubsub_missing_subscription_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("pubsub", {"project_id": "p"})

    def test_zerobus_basic(self) -> None:
        cfg = {"catalog": "main", "schema": "raw", "table_name": "events"}
        key = derive_source_key("zerobus", cfg)
        assert key == "main::raw::events"

    def test_zerobus_missing_table_name_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            derive_source_key("zerobus", {"catalog": "c", "schema": "s"})

    def test_unknown_source_type_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown source_type"):
            derive_source_key("mqtt", {"topic": "t"})


# ---------------------------------------------------------------------------
# Task 3: check_source_available
# ---------------------------------------------------------------------------


class TestCheckSourceAvailable:
    """Verify check_source_available returns correct availability dicts."""

    @pytest.mark.asyncio
    async def test_returns_available_true_when_no_row(self) -> None:
        db = _make_db(fetchrow_return=None)
        result = await check_source_available(
            db, "kinesis", {"region": "us-east-1", "stream_name": "s"}
        )
        assert result == {"available": True}

    @pytest.mark.asyncio
    async def test_returns_available_false_with_pipeline_info(self) -> None:
        row = {"pipeline_id": "pipe-abc-123", "claimed_by": "user@example.com"}
        db = _make_db(fetchrow_return=row)
        result = await check_source_available(
            db, "kinesis", {"region": "us-east-1", "stream_name": "s"}
        )
        assert result["available"] is False
        assert result["claimed_by_pipeline"] == "pipe-abc-123"
        assert result["claimed_by"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_passes_correct_source_type_and_key_to_db(self) -> None:
        db = _make_db(fetchrow_return=None)
        await check_source_available(
            db, "kinesis", {"region": "eu-west-1", "stream_name": "my-stream"}
        )
        db.fetchrow.assert_awaited_once()
        call_args = db.fetchrow.call_args[0]
        assert call_args[1] == "kinesis"
        assert call_args[2] == "eu-west-1::my-stream"

    @pytest.mark.asyncio
    async def test_raises_value_error_for_unknown_source_type(self) -> None:
        db = _make_db()
        with pytest.raises(ValueError):
            await check_source_available(db, "mqtt", {"topic": "t"})

    @pytest.mark.asyncio
    async def test_raises_key_error_for_missing_config_field(self) -> None:
        db = _make_db()
        with pytest.raises(KeyError):
            await check_source_available(db, "kinesis", {"region": "us-east-1"})


# ---------------------------------------------------------------------------
# Task 3: claim_source
# ---------------------------------------------------------------------------


class TestClaimSource:
    """Verify claim_source inserts or returns conflict info."""

    @pytest.mark.asyncio
    async def test_claims_successfully_when_source_is_free(self) -> None:
        db = _make_db(fetchrow_return=None)
        result = await claim_source(
            db,
            pipeline_id="pipe-1",
            source_type="kinesis",
            source_config={"region": "us-east-1", "stream_name": "s"},
            user_email="user@example.com",
        )
        assert result == {"claimed": True}
        db.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_claimed_false_when_already_taken(self) -> None:
        row = {"pipeline_id": "pipe-other"}
        db = _make_db(fetchrow_return=row)
        result = await claim_source(
            db,
            pipeline_id="pipe-new",
            source_type="kinesis",
            source_config={"region": "us-east-1", "stream_name": "s"},
            user_email="user@example.com",
        )
        assert result["claimed"] is False
        assert result["claimed_by_pipeline"] == "pipe-other"
        db.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_inserts_with_partition_count(self) -> None:
        db = _make_db(fetchrow_return=None)
        await claim_source(
            db,
            pipeline_id="pipe-1",
            source_type="kafka",
            source_config={"bootstrap_servers": "b:9092", "topic": "t"},
            user_email="user@example.com",
            partition_count=4,
        )
        db.execute.assert_awaited_once()
        call_args = db.execute.call_args[0]
        # partition_count is the 4th parameter (index 4)
        assert call_args[4] == 4

    @pytest.mark.asyncio
    async def test_passes_lowercased_source_type_to_db(self) -> None:
        db = _make_db(fetchrow_return=None)
        await claim_source(
            db,
            pipeline_id="pipe-1",
            source_type="Kafka",
            source_config={"bootstrap_servers": "b:9092", "topic": "t"},
            user_email="user@example.com",
        )
        execute_call = db.execute.call_args[0]
        assert execute_call[2] == "kafka"


# ---------------------------------------------------------------------------
# Task 3: release_source
# ---------------------------------------------------------------------------


class TestReleaseSource:
    """Verify release_source issues correct UPDATE statement."""

    @pytest.mark.asyncio
    async def test_calls_db_execute_with_pipeline_id(self) -> None:
        db = _make_db()
        await release_source(db, "pipe-uuid-999")
        db.execute.assert_awaited_once()
        call_args = db.execute.call_args[0]
        assert call_args[1] == "pipe-uuid-999"

    @pytest.mark.asyncio
    async def test_update_sets_status_to_released(self) -> None:
        db = _make_db()
        await release_source(db, "pipe-uuid-999")
        sql: str = db.execute.call_args[0][0]
        assert "released" in sql.lower()
        assert "UPDATE source_bindings" in sql

    @pytest.mark.asyncio
    async def test_filters_only_active_bindings(self) -> None:
        db = _make_db()
        await release_source(db, "pipe-uuid-999")
        sql: str = db.execute.call_args[0][0]
        assert "status = 'active'" in sql
