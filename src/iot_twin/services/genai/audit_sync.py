"""Bounded background copy of the AI action-plan audit into UC.

Mirrors ``genai_action_plan_audit`` (Lakebase) into
``{UC_CATALOG}.{UC_SCHEMA}.intelligence_genai_audit`` so Genie can answer
"which rows did the AI use for alert X?" — same pattern as
:mod:`iot_twin.services.alerts.alert_snapshot_sync`: a new plan marks the
mirror dirty, a periodic loop rewrites the recent rows, and an empty copy
never deletes the last-known-good table.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

from iot_twin.services import intelligence_catalog_service as ics
from iot_twin.services.alerts.alert_snapshot_sync import missing_sync_config, resolve_interval_s
from iot_twin.services.genai.audit_store import AuditRecord, audit_store_for
from iot_twin.services.uc_ident import sql_fqn

logger = logging.getLogger("iot_digital_twin.services.genai_audit_sync")

TABLE = "intelligence_genai_audit"
MAX_ROWS = 200
COLUMNS = (
    "plan_id, alert_id, industry, requested_by, requested_at, endpoint, model, model_version, "
    "prompt_template_version, prompt_hash, grounding_hash, grounding_check, grounding_reasons, "
    "status, citation_ids, grounding_rows, plan_summary, plan, risk_score, risk_model, "
    "risk_model_version, latency_ms, total_tokens, applied_by, applied_at"
)


def _risk(record: AuditRecord) -> dict[str, Any]:
    row = next((r for r in record.grounding_rows if r.get("source") == "risk_score"), None)
    return dict((row or {}).get("data") or {})


def _int(value: Any) -> str:
    return "NULL" if value is None else str(int(value))


def _json(value: Any) -> str:
    return ics._sql_str(json.dumps(value, sort_keys=True, default=str))


def audit_row_sql(record: AuditRecord) -> str:
    """One ``VALUES`` tuple for the UC mirror."""
    plan = record.plan or {}
    summary = (plan.get("summary") or {}).get("text") if isinstance(plan, dict) else None
    risk = _risk(record)
    values = [
        ics._sql_str(record.id),
        ics._sql_str(record.alert_id),
        ics._sql_str(record.industry),
        ics._sql_str(record.requested_by),
        ics._sql_str(record.requested_at.isoformat()),
        ics._sql_str(record.endpoint),
        ics._sql_str(record.model),
        ics._sql_str(record.model_version),
        ics._sql_str(record.prompt_template_version),
        ics._sql_str(record.prompt_hash),
        ics._sql_str(record.grounding_hash),
        ics._sql_str(record.grounding_check),
        _json(record.grounding_reasons),
        ics._sql_str(record.status),
        _json([row.get("id") for row in record.grounding_rows]),
        _json(record.grounding_rows),
        ics._sql_str(summary),
        _json(plan) if plan else "NULL",
        ics._sql_num(risk.get("risk_score")),
        ics._sql_str(risk.get("model_name")),
        ics._sql_str(risk.get("model_version")),
        _int(record.latency_ms),
        _int(record.total_tokens),
        ics._sql_str(record.applied_by),
        ics._sql_str(record.applied_at.isoformat() if record.applied_at else None),
    ]
    return f"({', '.join(values)})"


async def provision_audit_table(*, warehouse_id: str | None = None, ws: Any | None = None) -> str:
    catalog, schema = ics._catalog(), ics._schema()
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {sql_fqn(catalog, schema, TABLE)} (\n"
        f"{ics._TABLE_DDL[TABLE].strip()}\n) USING DELTA"
    )
    await ics.execute_dbsql(ddl, ics._warehouse(ws, warehouse_id))
    return f"{catalog}.{schema}.{TABLE}"


async def sync_genai_audit(
    records: list[AuditRecord],
    *,
    warehouse_id: str | None = None,
    ws: Any | None = None,
) -> dict[str, Any]:
    """Rewrite the UC mirror with ``records`` (newest first, capped); skip when empty."""
    rows = [audit_row_sql(r) for r in records[:MAX_ROWS]]
    if not rows:
        logger.info("Skipping empty genai audit mirror overwrite")
        return {"rows": 0, "skipped": True}
    count = await ics._replace_table(
        ics._quoted_table(TABLE), ics._warehouse(ws, warehouse_id), rows, COLUMNS
    )
    return {"rows": count, "skipped": False}


def mark_audit_dirty(state: Any) -> None:
    syncer = getattr(state, "genai_audit_synchronizer", None)
    if syncer is not None:
        syncer.mark_dirty()


class GenaiAuditSynchronizer:
    """Copies the audit into UC on a bounded interval while dirty."""

    def __init__(self, state: Any, interval_s: float | None = None) -> None:
        self.state = state
        self.interval_s = float(interval_s) if interval_s is not None else resolve_interval_s()
        self.dirty = False
        self._generation = 0
        self.last_sync_at: str | None = None
        self.last_error: str | None = missing_sync_config()
        self.enabled = self.last_error is None

    def mark_dirty(self) -> None:
        self.dirty = True
        self._generation += 1

    def _db(self) -> Any | None:
        if getattr(self.state, "app_backend_available", False):
            return getattr(self.state, "app_backend_db", None)
        return None

    async def run_once(self) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        generation = self._generation
        records = await audit_store_for(self.state).list_recent(db=self._db(), limit=MAX_ROWS)
        result = await sync_genai_audit(records)
        if generation == self._generation:
            self.dirty = False
        self.last_sync_at = datetime.now(UTC).isoformat()
        self.last_error = None
        return result

    async def _wait(self, seconds: float) -> bool:
        """Sleep up to ``seconds``; True when shutdown was requested."""
        try:
            await asyncio.wait_for(self.state.shutdown_event.wait(), timeout=seconds)
            return True
        except TimeoutError:
            return False

    async def run_loop(self) -> None:
        if not self.enabled:
            return
        backoff = max(0.05, min(self.interval_s, 1.0))
        while not self.state.shutdown_event.is_set():
            try:
                await provision_audit_table()
                break
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("genai audit mirror provision failed")
                if await self._wait(backoff):
                    return
                backoff = min(backoff * 2, 30.0)
        self.mark_dirty()
        while not self.state.shutdown_event.is_set():
            if self.dirty:
                try:
                    await self.run_once()
                except Exception as exc:
                    self.last_error = str(exc)
                    logger.exception("genai audit mirror sync failed")
            if await self._wait(self.interval_s):
                return


def start_genai_audit_sync(state: Any) -> asyncio.Task | None:
    """Attach a synchronizer to ``state`` and start the loop when configured."""
    syncer = GenaiAuditSynchronizer(state)
    state.genai_audit_synchronizer = syncer
    if not syncer.enabled:
        logger.warning("GenAI audit mirror disabled: %s", syncer.last_error)
        return None
    task = asyncio.create_task(syncer.run_loop(), name="genai-audit-sync")
    state.genai_audit_sync_task = task
    return task
