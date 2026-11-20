"""Bounded background copy of Lakebase/in-memory alerts into UC.

Lifecycle events mark the snapshot dirty. A periodic loop (default 60s)
writes the current alert set to ``intelligence_alert_snapshot``. Empty
background copies never DELETE the last-known-good table; admin full sync
may clear it explicitly.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from iot_twin.core.workspace_config import get_genie_space_id, get_uc_catalog, get_uc_schema
from iot_twin.services import intelligence_catalog_service as ics
from iot_twin.services.alerts.alert_engine import engine_for

logger = logging.getLogger("iot_digital_twin.services.alert_snapshot_sync")

DEFAULT_INTERVAL_S = 60.0
INTERVAL_ENV = "ALERT_SNAPSHOT_SYNC_INTERVAL_S"


def resolve_interval_s() -> float:
    raw = os.getenv(INTERVAL_ENV, "").strip()
    if not raw:
        return DEFAULT_INTERVAL_S
    return float(raw)


def missing_sync_config() -> str | None:
    missing: list[str] = []
    if not get_uc_catalog():
        missing.append("UC_CATALOG")
    if not get_uc_schema():
        missing.append("UC_SCHEMA")
    if not os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "").strip():
        missing.append("DATABRICKS_SQL_WAREHOUSE_ID")
    if not get_genie_space_id():
        missing.append("GENIE_SPACE_ID")
    if not missing:
        return None
    return "Missing " + ", ".join(missing)


def mark_snapshot_dirty(state: Any) -> None:
    syncer = getattr(state, "alert_snapshot_synchronizer", None)
    if syncer is not None:
        syncer.mark_dirty()


async def provision_reference_tables(**kwargs: Any) -> dict[str, Any]:
    return await ics.provision_reference_tables(**kwargs)


async def sync_alert_snapshot(
    *,
    alerts: Iterable[dict[str, Any]] | None = None,
    allow_empty: bool = False,
    warehouse_id: str | None = None,
    ws: Any | None = None,
) -> dict[str, Any]:
    """Copy alert rows into the UC snapshot table."""
    rows = ics._alert_snapshot_rows(alerts or ())
    if not rows and not allow_empty:
        logger.info("Skipping empty alert snapshot overwrite")
        return {"rows": 0, "skipped": True}
    count = await ics._replace_table(
        ics._quoted_table("intelligence_alert_snapshot"),
        ics._warehouse(ws, warehouse_id),
        rows,
        "alert_id, industry, rule_id, sensor_id, status, severity, fault_code, value, threshold, opened_at, updated_at",
    )
    return {"rows": count, "skipped": False}


class AlertSnapshotSynchronizer:
    """Copies alert state into UC on a bounded interval while dirty."""

    def __init__(self, state: Any, interval_s: float | None = None) -> None:
        self.state = state
        self.interval_s = float(interval_s) if interval_s is not None else resolve_interval_s()
        self.dirty = False
        self._dirty_generation = 0
        self.last_sync_at: str | None = None
        self.last_error: str | None = None
        self.retrying = False
        reason = missing_sync_config()
        self.enabled = reason is None
        if reason:
            self.last_error = reason

    def mark_dirty(self) -> None:
        self.dirty = True
        self._dirty_generation += 1

    async def run_once(self) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        generation = self._dirty_generation
        engine = engine_for(self.state)
        db = None
        if getattr(self.state, "app_backend_available", False):
            db = getattr(self.state, "app_backend_db", None)
        alerts = await engine.list_alerts(db=db)
        result = await sync_alert_snapshot(
            alerts=[item.to_dict() for item in alerts],
            allow_empty=False,
        )
        if generation == self._dirty_generation:
            self.dirty = False
        self.last_sync_at = datetime.now(UTC).isoformat()
        self.last_error = None
        self.retrying = False
        return result

    def mark_clean_if_generation(self, generation: int) -> None:
        if generation == self._dirty_generation:
            self.dirty = False

    async def run_full_sync(self, *, alerts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        generation = self._dirty_generation
        rows = alerts
        if rows is None:
            engine = engine_for(self.state)
            db = None
            if getattr(self.state, "app_backend_available", False):
                db = getattr(self.state, "app_backend_db", None)
            listed = await engine.list_alerts(db=db)
            rows = [item.to_dict() for item in listed]
        provision = await provision_reference_tables()
        sync = await ics.sync_reference_data(alerts=rows)
        self.mark_clean_if_generation(generation)
        self.last_sync_at = datetime.now(UTC).isoformat()
        self.last_error = None
        self.retrying = False
        return {"provision": provision, "sync": sync}

    async def run_loop(self) -> None:
        if not self.enabled:
            return
        backoff = max(0.05, min(float(self.interval_s), 1.0))
        max_backoff = 30.0
        while not self.state.shutdown_event.is_set():
            try:
                await provision_reference_tables()
                self.retrying = False
                self.last_error = None
                break
            except Exception as exc:
                self.last_error = str(exc)
                self.retrying = True
                logger.exception("Alert snapshot table provision failed")
                try:
                    await asyncio.wait_for(
                        self.state.shutdown_event.wait(),
                        timeout=backoff,
                    )
                    return
                except TimeoutError:
                    backoff = min(backoff * 2, max_backoff)
                    continue
        else:
            return
        self.mark_dirty()
        while not self.state.shutdown_event.is_set():
            if self.dirty:
                try:
                    await self.run_once()
                except Exception as exc:
                    self.last_error = str(exc)
                    self.retrying = True
                    logger.exception("Alert snapshot sync failed")
            try:
                await asyncio.wait_for(
                    self.state.shutdown_event.wait(),
                    timeout=self.interval_s,
                )
                return
            except TimeoutError:
                continue


def start_alert_snapshot_sync(state: Any) -> asyncio.Task | None:
    """Attach a synchronizer to ``state`` and start the loop when configured."""
    syncer = AlertSnapshotSynchronizer(state)
    state.alert_snapshot_synchronizer = syncer
    if not syncer.enabled:
        logger.warning("Alert snapshot sync disabled: %s", syncer.last_error)
        return None
    task = asyncio.create_task(syncer.run_loop(), name="alert-snapshot-sync")
    state.alert_snapshot_sync_task = task
    return task
