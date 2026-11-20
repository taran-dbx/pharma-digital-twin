"""Tests for iot_twin.services.pipelines.pipeline_service — row conversion and resource tracking."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services.pipelines.pipeline_service import pipeline_row_to_dict, track_resource


class TestPipelineRowToDict:
    """Verify pipeline_row_to_dict conversion logic."""

    def test_returns_empty_dict_for_none(self) -> None:
        assert pipeline_row_to_dict(None) == {}

    def test_converts_dict_like_row(self) -> None:
        row = {"name": "test-pipeline", "status": "active"}
        result = pipeline_row_to_dict(row)
        assert result["name"] == "test-pipeline"
        assert result["status"] == "active"

    def test_stringifies_id_field(self) -> None:
        row = {"id": 42, "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["id"] == "42"

    def test_stringifies_created_at_field(self) -> None:
        from datetime import datetime

        now = datetime(2025, 6, 15, 12, 0, 0)
        row = {"created_at": now, "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert isinstance(result["created_at"], str)

    def test_stringifies_updated_at_field(self) -> None:
        from datetime import datetime

        now = datetime(2025, 6, 15, 12, 0, 0)
        row = {"updated_at": now, "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert isinstance(result["updated_at"], str)

    def test_parses_sensor_config_json_string(self) -> None:
        cfg = {"sensors": ["temp", "pressure"]}
        row = {"sensor_config": json.dumps(cfg), "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["sensor_config"] == cfg

    def test_parses_source_config_json_string(self) -> None:
        cfg = {"type": "kafka", "topic": "sensors"}
        row = {"source_config": json.dumps(cfg), "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["source_config"] == cfg

    def test_parses_sink_config_json_string(self) -> None:
        cfg = {"type": "delta", "path": "/data"}
        row = {"sink_config": json.dumps(cfg), "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["sink_config"] == cfg

    def test_parses_resources_json_string(self) -> None:
        res = {"cluster_id": "abc-123"}
        row = {"resources": json.dumps(res), "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["resources"] == res

    def test_leaves_non_string_json_field_unchanged(self) -> None:
        """If a JSON field is already a dict (not a string), leave it alone."""
        cfg = {"key": "val"}
        row = {"sensor_config": cfg, "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["sensor_config"] == cfg

    def test_leaves_invalid_json_string_as_is(self) -> None:
        row = {"sensor_config": "not-valid-json{", "name": "p1"}
        result = pipeline_row_to_dict(row)
        assert result["sensor_config"] == "not-valid-json{"

    def test_preserves_other_fields(self) -> None:
        row = {"name": "p1", "industry": "pharma", "status": "running"}
        result = pipeline_row_to_dict(row)
        assert result["industry"] == "pharma"
        assert result["status"] == "running"


class TestTrackResource:
    """Verify track_resource calls db.execute correctly."""

    @pytest.mark.asyncio
    async def test_calls_db_execute_with_correct_args(self) -> None:
        db = MagicMock()
        db.execute = AsyncMock()

        await track_resource(db, True, "pipe-uuid-123", "cluster_id", "cls-abc")

        db.execute.assert_awaited_once()
        call_args = db.execute.call_args
        sql = call_args[0][0]
        assert "UPDATE pipelines" in sql
        assert call_args[0][1] == "pipe-uuid-123"
        assert json.loads(call_args[0][2]) == {"cluster_id": "cls-abc"}

    @pytest.mark.asyncio
    async def test_noop_when_pipeline_id_is_none(self) -> None:
        db = MagicMock()
        db.execute = AsyncMock()

        await track_resource(db, True, None, "key", "val")
        db.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_when_db_not_available(self) -> None:
        db = MagicMock()
        db.execute = AsyncMock()

        await track_resource(db, False, "pipe-uuid", "key", "val")
        db.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_when_db_is_none(self) -> None:
        await track_resource(None, True, "pipe-uuid", "key", "val")
        # Should not raise

    @pytest.mark.asyncio
    async def test_swallows_db_exception(self) -> None:
        db = MagicMock()
        db.execute = AsyncMock(side_effect=RuntimeError("connection lost"))

        # Should not raise
        await track_resource(db, True, "pipe-uuid", "key", "val")
