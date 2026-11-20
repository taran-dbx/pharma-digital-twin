"""Centralised mutable application state.

All former module-level globals live here.  ``get_app_state()`` is a FastAPI
dependency that retrieves the singleton from ``request.app.state.app_state``.
"""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import field
from typing import TYPE_CHECKING, Any

from fastapi import Request, WebSocket

if TYPE_CHECKING:
    from iot_twin.core.lakebase import LakebaseConnectionManager
    from iot_twin.models.source_metrics import SourceMetrics
    from iot_twin.services.pipelines.pipeline_db import PipelineLakebaseHandle


def _default_sources() -> dict[str, SourceMetrics]:
    from iot_twin.models.source_metrics import create_default_sources

    return create_default_sources()


@dataclasses.dataclass
class AppState:
    """Holds every piece of mutable state the application needs."""

    # Database
    db: LakebaseConnectionManager | None = None
    db_available: bool = False
    db_error: str | None = None

    # App-backend metadata DB (additive; separate iot-app-backend project).
    # Backs pipeline_setup / pipeline_deployment / provisioned_resource.
    app_backend_db: LakebaseConnectionManager | None = None
    app_backend_available: bool = False
    app_backend_error: str | None = None

    # WebSocket clients
    connected_clients: list[WebSocket] = field(default_factory=list)

    # SSE clients (each is an asyncio.Queue that the SSE endpoint drains)
    sse_clients: list[asyncio.Queue] = field(default_factory=list)

    # In-memory alert state machine (persists to app-backend Lakebase when up)
    alert_engine: Any = None
    # Operator decisions per alert episode (services/alerts/alert_responses.py)
    alert_response_store: Any = None
    production_control: Any = None

    # Genie conversation ownership — process-local, fail-closed after restart
    genie_conversations: dict[str, dict[str, str]] = field(default_factory=dict)
    genie_sdk_semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(4))

    # Background tasks
    simulator_task: asyncio.Task | None = None
    refresh_task: asyncio.Task | None = None
    retention_task: asyncio.Task | None = None
    industry_runtime_refresh_task: asyncio.Task | None = None
    industry_runtime_refresh_tasks: set[asyncio.Task] = field(default_factory=set)
    alert_snapshot_synchronizer: Any = None
    alert_snapshot_sync_task: asyncio.Task | None = None

    # Shutdown coordination
    shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Source metrics registry
    sources: dict[str, SourceMetrics] = field(default_factory=_default_sources)

    # Per-pipeline producer state
    producer_states: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Per-pipeline Lakebase connection pools (resolved from source_config)
    pipeline_dbs: dict[str, PipelineLakebaseHandle] = field(default_factory=dict)
    pipeline_db_locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    # Stale-while-revalidate snapshot for the industry gallery. The live Jobs
    # and freshness sweep is intentionally kept off the request critical path.
    industry_runtime_cache: dict[str, Any] | None = None
    industry_runtime_cached_at: float = 0.0
    industry_runtime_cache_generation: int = 0
    industry_runtime_refresh_generation: int = -1

    def invalidate_industry_runtime_cache(self) -> None:
        """Mark runtime state stale after a pipeline mutation."""
        self.industry_runtime_cache_generation += 1
        self.industry_runtime_cached_at = 0.0

    # Runtime-mutable config (set via /api/simulator/restart)
    industries: str = ""
    update_rate_hz: float = 2.0


def get_app_state(request: Request) -> AppState:
    """FastAPI dependency — retrieve the shared ``AppState``."""
    return request.app.state.app_state
