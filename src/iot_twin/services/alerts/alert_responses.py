"""Signed operator decisions recorded against one alert episode.

The implant cell's quality workflow: put the affected lot on QA hold
(``quarantine_lot``), open a nonconformance report (``open_ncr``), send the
parts at risk to CMM (``request_cmm_inspection``, which also stops the cell —
that actuation lives in ``iot_twin.services.production_control``) and request
Spindle 3 maintenance. ``hold_batch`` / ``send_to_inspect`` are the CNC-era
names, still accepted so older clients and rows keep working.

Every decision is a Part 11-style signed record: who (``recorded_by``), when
(``created_at`` / ``signed_at``), why (``reason`` + optional ``comment``) and
what the signature means (``signature_meaning``). Decisions belong to an
*episode* — the alert row is reused across re-opens, so ``episode_opened_at``
keeps each excursion's decisions separate.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from iot_twin.domain.catalog import list_assets, list_maintenance_history, list_sensors
from iot_twin.domain.lot_context import LOT_METADATA_KEYS, lot_disposition
from iot_twin.services.alerts.alert_engine import AlertRecord

logger = logging.getLogger(__name__)

ALERT_RESPONSE_EVENT = "alert_response"

# action -> cell station the decision applies to
RESPONSE_STATIONS: dict[str, str] = {
    "quarantine_lot": "PACK",
    "open_ncr": "INSPECT",
    "request_cmm_inspection": "INSPECT",
    "request_maintenance": "MILL",
    # Legacy CNC-cell decisions, kept for backward compatibility.
    "hold_batch": "PACK",
    "send_to_inspect": "INSPECT",
}
LEGACY_ACTIONS: frozenset[str] = frozenset({"hold_batch", "send_to_inspect"})

# 21 CFR 11.50: a signed record shows the meaning of the signature.
SIGNATURE_MEANINGS: tuple[str, ...] = ("authored", "reviewed", "approved", "responsible")
MAX_REASON_CHARS = 280
MAX_COMMENT_CHARS = 1000
NCR_DISPOSITION_PENDING = "Pending MRB disposition"

_VALIDATION_KEYS = ("parameter", "validated_limit", "oq_ref", "pq_ref", "process_spec")


class ResponseValidationError(ValueError):
    """The request body cannot become a signed decision."""


@dataclass(frozen=True)
class ResponseAudit:
    reason: str
    signature_meaning: str
    comment: str | None = None


def parse_audit(body: Mapping[str, Any]) -> ResponseAudit:
    """Validate the signing fields of a decision request."""
    reason = body.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ResponseValidationError("reason is required to sign the decision")
    reason = reason.strip()
    if len(reason) > MAX_REASON_CHARS:
        raise ResponseValidationError(f"reason must be at most {MAX_REASON_CHARS} characters")
    meaning = body.get("signature_meaning")
    if meaning not in SIGNATURE_MEANINGS:
        raise ResponseValidationError(
            f"signature_meaning must be one of: {', '.join(SIGNATURE_MEANINGS)}"
        )
    comment = body.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise ResponseValidationError("comment must be text")
    comment = (comment or "").strip() or None
    if comment is not None and len(comment) > MAX_COMMENT_CHARS:
        raise ResponseValidationError(f"comment must be at most {MAX_COMMENT_CHARS} characters")
    return ResponseAudit(reason=reason, signature_meaning=str(meaning), comment=comment)


@dataclass(frozen=True)
class AlertResponse:
    id: str
    alert_id: str
    episode_opened_at: datetime
    action: str
    recorded_by: str
    created_at: datetime
    reference: dict[str, Any] = field(default_factory=dict)
    reason: str | None = None
    comment: str | None = None
    signature_meaning: str | None = None
    signed_at: datetime | None = None

    def signature(self) -> str | None:
        """Printed manifestation of the signature: name, meaning, date/time."""
        if not self.signature_meaning:
            return None
        signed = (self.signed_at or self.created_at).isoformat()
        return f"{self.recorded_by} · {self.signature_meaning} · {signed}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "alert_id": self.alert_id,
            "episode_opened_at": self.episode_opened_at.isoformat(),
            "action": self.action,
            "station": RESPONSE_STATIONS.get(self.action),
            "recorded_by": self.recorded_by,
            "reference": dict(self.reference),
            "created_at": self.created_at.isoformat(),
            "reason": self.reason,
            "comment": self.comment,
            "signature_meaning": self.signature_meaning,
            "signed_at": self.signed_at.isoformat() if self.signed_at else None,
            "signature": self.signature(),
        }


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _maintenance_reference(alert: AlertRecord) -> dict[str, Any]:
    industry = alert.industry or ""
    try:
        sensors = list_sensors(industry)
        assets = list_assets(industry)
        history = list_maintenance_history(industry)
    except KeyError:
        return {}
    sensor = next((s for s in sensors if alert.sensor_id in (s.sensor_id, s.name)), None)
    if sensor is None:
        return {}
    asset = next((a for a in assets if a.asset_id == sensor.asset_id), None)
    component = None
    if asset is not None:
        component = next(
            (c for c in asset.components if c.component_id == sensor.component_id), None
        )
    services = sorted(
        (e for e in history if e.component_id == sensor.component_id),
        key=lambda e: e.started_at,
        reverse=True,
    )
    reference: dict[str, Any] = {
        "asset": asset.name if asset else sensor.asset_id,
        "component": component.name if component else sensor.component_id,
    }
    if services:
        last = services[0]
        reference["last_service"] = {
            "event_id": last.event_id,
            "kind": last.kind,
            "notes": last.notes,
            "started_at": last.started_at,
        }
    return reference


def _excursion(alert: AlertRecord, now: datetime) -> tuple[datetime, datetime]:
    start = _as_utc(alert.opened_at or now)
    end = _as_utc(alert.cleared_at) if alert.cleared_at else now
    return start, max(start, end)


def _lot_reference(alert: AlertRecord, now: datetime) -> dict[str, Any]:
    """The excursion window and which parts of which lots it touched."""
    start, end = _excursion(alert, now)
    reference: dict[str, Any] = {
        "excursion": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "open": alert.cleared_at is None,
        },
        **lot_disposition(start, end),
    }
    lot = {key: alert.metadata[key] for key in LOT_METADATA_KEYS if alert.metadata.get(key)}
    if lot:
        reference["triggering_part"] = lot
    return reference


def ncr_number(alert: AlertRecord) -> str:
    """Stable NCR id for an episode, e.g. ``NCR-20261120-3F2A``."""
    opened = _as_utc(alert.opened_at or datetime.now(UTC))
    return f"NCR-{opened:%Y%m%d}-{alert.id.replace('-', '')[:4].upper()}"


def _ncr_reference(alert: AlertRecord, now: datetime) -> dict[str, Any]:
    validation = {key: alert.metadata[key] for key in _VALIDATION_KEYS if alert.metadata.get(key)}
    return {
        "ncr_id": ncr_number(alert),
        "fault_code": alert.fault_code,
        "observed_peak": alert.value,
        "limit": alert.threshold,
        "validation": validation,
        "disposition": NCR_DISPOSITION_PENDING,
        "capa": "Evaluate for CAPA if the excursion recurs",
        **_lot_reference(alert, now),
    }


def response_reference(
    action: str, alert: AlertRecord, now: datetime | None = None
) -> dict[str, Any]:
    """Facts attached to a decision, derived from the alert, the lot clock and the catalog."""
    at = _as_utc(now or datetime.now(UTC))
    reference: dict[str, Any] = {"station": RESPONSE_STATIONS[action]}
    if action in {"send_to_inspect", "request_cmm_inspection"} and alert.opened_at is not None:
        reference["parts_since"] = _as_utc(alert.opened_at).isoformat()
    if action == "quarantine_lot":
        reference.update({"hold": "QA hold", **_lot_reference(alert, at)})
    elif action == "open_ncr":
        reference.update(_ncr_reference(alert, at))
    elif action == "request_cmm_inspection":
        reference["parts_to_measure"] = _lot_reference(alert, at)["parts_at_risk"]
    elif action == "request_maintenance":
        reference.update(_maintenance_reference(alert))
    return reference


def _field(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def _row_to_response(row: Any) -> AlertResponse:
    reference = row["reference"]
    if isinstance(reference, str):
        reference = json.loads(reference or "{}")
    reference = dict(reference or {})
    # Rows written while the audit columns were unavailable keep them here.
    stashed = reference.pop("audit", None) or {}
    signed_at = _field(row, "signed_at")
    if signed_at is None and stashed.get("signed_at"):
        signed_at = datetime.fromisoformat(str(stashed["signed_at"]))
    return AlertResponse(
        id=str(row["id"]),
        alert_id=str(row["alert_id"]),
        episode_opened_at=_as_utc(row["episode_opened_at"]),
        action=str(row["action"]),
        recorded_by=str(row["recorded_by"]),
        reference=reference,
        created_at=_as_utc(row["created_at"]),
        reason=_field(row, "reason") or stashed.get("reason"),
        comment=_field(row, "comment") or stashed.get("comment"),
        signature_meaning=_field(row, "signature_meaning") or stashed.get("signature_meaning"),
        signed_at=_as_utc(signed_at) if signed_at else None,
    )


def _missing_audit_columns(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "undefinedcolumn" in type(exc).__name__.lower() or (
        "column" in msg and "does not exist" in msg
    )


_INSERT_SIGNED = """
INSERT INTO alert_response
    (id, alert_id, episode_opened_at, action, recorded_by, reference,
     reason, comment, signature_meaning, signed_at)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)
ON CONFLICT (alert_id, episode_opened_at, action) DO NOTHING
RETURNING *
"""

_INSERT_LEGACY = """
INSERT INTO alert_response
    (id, alert_id, episode_opened_at, action, recorded_by, reference)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb)
ON CONFLICT (alert_id, episode_opened_at, action) DO NOTHING
RETURNING *
"""


class AlertResponseStore:
    """Lakebase-backed decision log with an in-memory fallback."""

    def __init__(self) -> None:
        self._memory: dict[tuple[str, str], dict[str, AlertResponse]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _episode(alert: AlertRecord) -> datetime:
        return _as_utc(alert.opened_at or datetime.now(UTC))

    async def list_for(self, alert: AlertRecord, db: Any | None = None) -> list[AlertResponse]:
        episode = self._episode(alert)
        if db is not None:
            try:
                rows = await db.fetch(
                    """
                    SELECT * FROM alert_response
                    WHERE alert_id = $1::uuid AND episode_opened_at = $2
                    ORDER BY created_at
                    """,
                    alert.id,
                    episode,
                )
                return [_row_to_response(row) for row in rows]
            except Exception:
                logger.exception("alert response list from Lakebase failed; using memory")
        async with self._lock:
            found = self._memory.get((alert.id, episode.isoformat()), {})
            return sorted(found.values(), key=lambda r: r.created_at)

    async def _insert(
        self,
        db: Any,
        response: AlertResponse,
    ) -> Any:
        base = (
            response.id,
            response.alert_id,
            response.episode_opened_at,
            response.action,
            response.recorded_by,
        )
        try:
            return await db.fetchrow(
                _INSERT_SIGNED,
                *base,
                json.dumps(response.reference),
                response.reason,
                response.comment,
                response.signature_meaning,
                response.signed_at,
            )
        except Exception as exc:
            if not _missing_audit_columns(exc):
                raise
            logger.warning(
                "alert_response has no audit columns yet (table not upgraded); "
                "storing the signature in reference.audit"
            )
            audit = {
                "reason": response.reason,
                "comment": response.comment,
                "signature_meaning": response.signature_meaning,
                "signed_at": response.signed_at.isoformat() if response.signed_at else None,
            }
            return await db.fetchrow(
                _INSERT_LEGACY, *base, json.dumps({**response.reference, "audit": audit})
            )

    async def record(
        self,
        alert: AlertRecord,
        action: str,
        recorded_by: str,
        db: Any | None = None,
        *,
        audit: ResponseAudit,
        now: datetime | None = None,
    ) -> tuple[AlertResponse, bool]:
        """Record a signed ``action`` once per episode; returns (response, newly_recorded)."""
        if action not in RESPONSE_STATIONS:
            raise ValueError(f"unknown response action: {action}")
        at = _as_utc(now or datetime.now(UTC))
        episode = self._episode(alert)
        candidate = AlertResponse(
            id=str(uuid.uuid4()),
            alert_id=alert.id,
            episode_opened_at=episode,
            action=action,
            recorded_by=recorded_by,
            reference=response_reference(action, alert, at),
            created_at=at,
            reason=audit.reason,
            comment=audit.comment,
            signature_meaning=audit.signature_meaning,
            signed_at=at,
        )
        if db is not None:
            try:
                row = await self._insert(db, candidate)
                if row is not None:
                    return _row_to_response(row), True
                existing = await db.fetchrow(
                    """
                    SELECT * FROM alert_response
                    WHERE alert_id = $1::uuid AND episode_opened_at = $2 AND action = $3
                    """,
                    alert.id,
                    episode,
                    action,
                )
                if existing is not None:
                    return _row_to_response(existing), False
            except Exception:
                logger.exception("alert response persist failed; recording in memory")
        async with self._lock:
            bucket = self._memory.setdefault((alert.id, episode.isoformat()), {})
            if action in bucket:
                return bucket[action], False
            bucket[action] = candidate
            return candidate, True


def response_store_for(state: Any) -> AlertResponseStore:
    store = getattr(state, "alert_response_store", None)
    if store is None:
        store = AlertResponseStore()
        state.alert_response_store = store
    return store


def response_event(alert: AlertRecord, response: AlertResponse) -> dict[str, Any]:
    return {
        "type": ALERT_RESPONSE_EVENT,
        "industry": alert.industry,
        "alert_id": alert.id,
        "response": response.to_dict(),
    }
