"""Append-only audit of AI action plans (Lakebase ``genai_action_plan_audit``).

One row per generation attempt — successful, rejected by the grounding check,
or failed at the endpoint — so every answer the app ever showed can be traced
to the prompt and the exact rows it was given. The only later write is
``applied_by`` / ``applied_at`` / ``applied_response_id``, set once when a
person signs a decision through the response workflow citing the plan.

With Lakebase bound the store persists or refuses (``AuditStoreUnavailable``);
process memory is only used when no Lakebase is configured.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

_JSON_COLUMNS = ("grounding_rows", "response", "grounding_reasons")
_COLUMNS = (
    "id",
    "alert_id",
    "industry",
    "requested_by",
    "requested_at",
    "endpoint",
    "model",
    "model_version",
    "prompt_template_version",
    "prompt_hash",
    "grounding_hash",
    "grounding_rows",
    "response",
    "grounding_check",
    "grounding_reasons",
    "status",
    "error",
    "latency_ms",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "applied_by",
    "applied_at",
    "applied_response_id",
)
_INSERT_COLUMNS = _COLUMNS[:21]


def _placeholder(index: int, column: str) -> str:
    if column in ("id", "alert_id"):
        return f"${index}::uuid"
    if column in _JSON_COLUMNS:
        return f"${index}::jsonb"
    return f"${index}"


_INSERT_SQL = (
    f"INSERT INTO genai_action_plan_audit ({', '.join(_INSERT_COLUMNS)}) VALUES ("
    + ", ".join(_placeholder(i, c) for i, c in enumerate(_INSERT_COLUMNS, start=1))
    + ") ON CONFLICT (id) DO NOTHING"
)

_LIST_SQL = """
SELECT * FROM genai_action_plan_audit
WHERE alert_id = $1::uuid ORDER BY requested_at DESC LIMIT $2
"""

_RECENT_SQL = "SELECT * FROM genai_action_plan_audit ORDER BY requested_at DESC LIMIT $1"

_GET_SQL = "SELECT * FROM genai_action_plan_audit WHERE id = $1::uuid"

_APPLY_SQL = """
UPDATE genai_action_plan_audit
SET applied_by = $2, applied_at = $3, applied_response_id = $4::uuid
WHERE id = $1::uuid AND applied_at IS NULL
RETURNING *
"""


@dataclass(frozen=True)
class AuditRecord:
    id: str
    alert_id: str
    industry: str | None
    requested_by: str
    requested_at: datetime
    endpoint: str
    model: str | None
    model_version: str | None
    prompt_template_version: str
    prompt_hash: str
    grounding_hash: str
    grounding_rows: list[dict[str, Any]]
    response: dict[str, Any]
    grounding_check: str
    grounding_reasons: list[str]
    status: str
    error: str | None = None
    latency_ms: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    applied_by: str | None = None
    applied_at: datetime | None = None
    applied_response_id: str | None = None

    @property
    def plan(self) -> dict[str, Any] | None:
        plan = self.response.get("plan")
        return plan if isinstance(plan, dict) else None

    def summary(self) -> dict[str, Any]:
        """Listing view: everything except the grounding rows and raw answer."""
        plan = self.plan
        return {
            "plan_id": self.id,
            "alert_id": self.alert_id,
            "industry": self.industry,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat(),
            "endpoint": self.endpoint,
            "model": self.model,
            "model_version": self.model_version,
            "prompt_template_version": self.prompt_template_version,
            "prompt_hash": self.prompt_hash,
            "grounding_hash": self.grounding_hash,
            "grounding_check": self.grounding_check,
            "grounding_reasons": list(self.grounding_reasons),
            "status": self.status,
            "error": self.error,
            "latency_ms": self.latency_ms,
            "usage": {
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.total_tokens,
            },
            "plan": plan,
            "citation_ids": [row.get("id") for row in self.grounding_rows],
            "applied_by": self.applied_by,
            "applied_at": self.applied_at.isoformat() if self.applied_at else None,
            "applied_response_id": self.applied_response_id,
            "advisory": True,
        }

    def trace(self) -> dict[str, Any]:
        """Full grounding trace: the summary plus the exact rows and the raw answer."""
        return {
            **self.summary(),
            "grounding_rows": [dict(row) for row in self.grounding_rows],
            "response": dict(self.response),
        }

    def insert_args(self) -> tuple[Any, ...]:
        values = []
        for column in _INSERT_COLUMNS:
            value = getattr(self, column)
            values.append(json.dumps(value, default=str) if column in _JSON_COLUMNS else value)
        return tuple(values)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _row_to_record(row: Any) -> AuditRecord:
    data = {column: row[column] for column in _COLUMNS}
    for column in _JSON_COLUMNS:
        if isinstance(data[column], str):
            data[column] = json.loads(data[column] or "null")
    return AuditRecord(
        **{
            **data,
            "id": str(data["id"]),
            "alert_id": str(data["alert_id"]),
            "requested_at": _as_utc(data["requested_at"]),
            "applied_at": _as_utc(data["applied_at"]),
            "applied_response_id": (
                str(data["applied_response_id"]) if data["applied_response_id"] else None
            ),
            "grounding_rows": list(data["grounding_rows"] or []),
            "response": dict(data["response"] or {}),
            "grounding_reasons": list(data["grounding_reasons"] or []),
        }
    )


class AuditStoreUnavailable(RuntimeError):
    """Lakebase is bound but the audit could not be read or written (→ 503)."""


class GenaiAuditStore:
    """Lakebase-backed action-plan audit.

    With a database, every read and write goes to Lakebase or raises
    :class:`AuditStoreUnavailable` — an AI answer is never shown without its
    audit row, and an audit row never silently lands in memory (same rule as
    signed decisions). Process memory is used only when no Lakebase is bound
    (local runs and tests).
    """

    def __init__(self) -> None:
        self._memory: dict[str, AuditRecord] = {}
        self._lock = asyncio.Lock()

    async def record(self, entry: AuditRecord, db: Any | None = None) -> AuditRecord:
        if db is not None:
            try:
                await db.execute(_INSERT_SQL, *entry.insert_args())
            except Exception as exc:
                logger.exception("genai audit persist failed")
                raise AuditStoreUnavailable(f"action plan audit not persisted: {exc}") from exc
            return entry
        async with self._lock:
            self._memory = {**self._memory, entry.id: entry}
        return entry

    async def get(self, plan_id: str, db: Any | None = None) -> AuditRecord | None:
        if db is not None:
            try:
                row = await db.fetchrow(_GET_SQL, plan_id)
            except Exception as exc:
                raise AuditStoreUnavailable(f"action plan audit read failed: {exc}") from exc
            return _row_to_record(row) if row is not None else None
        async with self._lock:
            return self._memory.get(plan_id)

    async def list_for_alert(
        self, alert_id: str, db: Any | None = None, limit: int = 20
    ) -> list[AuditRecord]:
        if db is not None:
            try:
                rows = await db.fetch(_LIST_SQL, alert_id, limit)
            except Exception as exc:
                raise AuditStoreUnavailable(f"action plan audit list failed: {exc}") from exc
            return [_row_to_record(r) for r in rows]
        async with self._lock:
            memory = [r for r in self._memory.values() if r.alert_id == alert_id]
        return sorted(memory, key=lambda r: r.requested_at, reverse=True)[:limit]

    async def list_recent(self, db: Any | None = None, limit: int = 500) -> list[AuditRecord]:
        if db is not None:
            try:
                rows = await db.fetch(_RECENT_SQL, limit)
            except Exception as exc:
                raise AuditStoreUnavailable(f"action plan audit list failed: {exc}") from exc
            return [_row_to_record(r) for r in rows]
        async with self._lock:
            memory = list(self._memory.values())
        return sorted(memory, key=lambda r: r.requested_at, reverse=True)[:limit]

    async def mark_applied(
        self,
        plan_id: str,
        *,
        applied_by: str,
        response_id: str,
        db: Any | None = None,
        now: datetime | None = None,
    ) -> AuditRecord | None:
        """Record that a person applied the plan; first application wins (None after)."""
        at = _as_utc(now or datetime.now(UTC))
        if db is not None:
            try:
                row = await db.fetchrow(_APPLY_SQL, plan_id, applied_by, at, response_id)
            except Exception as exc:
                raise AuditStoreUnavailable(f"action plan apply not recorded: {exc}") from exc
            return _row_to_record(row) if row is not None else None
        async with self._lock:
            current = self._memory.get(plan_id)
            if current is None or current.applied_at is not None:
                return None
            updated = replace(
                current, applied_by=applied_by, applied_at=at, applied_response_id=response_id
            )
            self._memory = {**self._memory, plan_id: updated}
            return updated


def audit_store_for(state: Any) -> GenaiAuditStore:
    store = getattr(state, "genai_audit_store", None)
    if store is None:
        store = GenaiAuditStore()
        state.genai_audit_store = store
    return store
