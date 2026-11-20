"""[C7263] Lakebase pool/query timeouts keep dashboard probes bounded."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.core.state import AppState
from iot_twin.services.pipelines import pipeline_db

ROOT = Path(__file__).parents[1]


def test_lakebase_pool_uses_bounded_timeouts_c7263() -> None:
    """[C7263][defect][regression] asyncpg pools must not hang forever."""
    backend = (ROOT / "src" / "iot_twin" / "core" / "lakebase.py").read_text(encoding="utf-8")
    pipeline_db = (
        ROOT / "src" / "iot_twin" / "services" / "pipelines" / "pipeline_db.py"
    ).read_text(encoding="utf-8")
    lakets = (ROOT / "src" / "iot_twin" / "routes" / "lakets.py").read_text(encoding="utf-8")

    assert "command_timeout=" in backend
    assert "timeout=" in backend
    assert "LAKEBASE_CONNECT_TIMEOUT_SECONDS" in backend or "command_timeout=" in backend
    assert "wait_for" in pipeline_db or "timeout" in pipeline_db.lower()
    assert "_LAKEBASE_PROBE_TIMEOUT_SECONDS" in lakets
    assert "asyncio.wait_for" in lakets


@pytest.mark.asyncio
async def test_failed_handle_initialization_closes_resources_c7263(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[C7263][defect][regression] Timeout cleanup closes partial pools."""

    class SlowHandle:
        closed = False

        async def initialize(self) -> None:
            await asyncio.sleep(60)

        async def close(self) -> None:
            self.closed = True

    handle = SlowHandle()
    monkeypatch.setattr(pipeline_db, "_LAKEBASE_INIT_TIMEOUT_SECONDS", 0.01)

    initialized = await pipeline_db._initialize_handle(handle, description="test handle")

    assert initialized is False
    assert handle.closed is True


@pytest.mark.asyncio
async def test_pipeline_pool_initialization_is_single_flight_c7263() -> None:
    """[C7263][defect][regression] Concurrent probes share one pool."""
    state = AppState()
    handle = MagicMock()

    async def initialize_once(*_args, **_kwargs) -> bool:
        await asyncio.sleep(0.01)
        return True

    with (
        patch(
            "iot_twin.services.pipelines.pipeline_db._load_pipeline_sink_config",
            AsyncMock(
                return_value={
                    "lb_project_id": "project",
                    "lb_branch_id": "branch",
                    "lb_database": "database",
                }
            ),
        ),
        patch(
            "iot_twin.services.pipelines.pipeline_db._resolve_endpoint_host_bounded",
            AsyncMock(return_value="host"),
        ),
        patch(
            "iot_twin.services.pipelines.pipeline_db.PipelineLakebaseHandle",
            MagicMock(return_value=handle),
        ) as handle_factory,
        patch(
            "iot_twin.services.pipelines.pipeline_db._initialize_handle",
            AsyncMock(side_effect=initialize_once),
        ) as initialize,
    ):
        first, second = await asyncio.gather(
            pipeline_db.get_pipeline_lakebase(state, "pipeline-1"),
            pipeline_db.get_pipeline_lakebase(state, "pipeline-1"),
        )

    assert first is handle
    assert second is handle
    handle_factory.assert_called_once()
    initialize.assert_awaited_once()
