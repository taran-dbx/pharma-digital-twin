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

Durability: when the app backend is configured, a signed decision is either
persisted in ``alert_response`` or refused (``ResponseStoreUnavailable`` → 503);
it is never kept only in memory. If the table has not been upgraded (the app
identity does not own it), the signature fields go to ``reference.audit`` and a
decision whose action the old CHECK rejects is stored under its legacy name
(``quarantine_lot`` → ``hold_batch``, ``request_cmm_inspection`` →
``send_to_inspect``) with the real action in ``reference.audit.action``.
``open_ncr`` has no legacy equivalent and is refused until the upgrade runs.
A legacy-named row already recorded for the episode — written before the
upgrade, or a genuine ``hold_batch`` / ``send_to_inspect`` from an older client
— counts as that decision: recording ``quarantine_lot`` again returns it with
``created`` false (its action reads ``quarantine_lot`` when the row carries
``reference.audit.action``, otherwise the legacy name).
The in-memory store is used only when no app backend is configured at all.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from iot_twin.domain.catalog import (
    list_alert_rules,
    list_assets,
    list_maintenance_history,
    list_sensors,
)
from iot_twin.domain.lot_context import LOT_METADATA_KEYS, lot_disposition
from iot_twin.services.alerts.alert_engine import (
    DETECT_LOOKBACK_S,
    EXCURSION_STARTED_KEY,
    AlertRecord,
)

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
# Storage name for a new action on a table whose CHECK predates it.
LEGACY_STORAGE_ACTION: dict[str, str] = {
    "quarantine_lot": "hold_batch",
    "request_cmm_inspection": "send_to_inspect",
}

# 21 CFR 11.50: a signed record shows the meaning of the signature.
SIGNATURE_MEANINGS: tuple[str, ...] = ("authored", "reviewed", "approved", "responsible")
# Recorded for legacy actions posted by older clients without signing fields.
LEGACY_UNSIGNED = "legacy-unsigned"
LEGACY_UNSIGNED_REASON = "Recorded by a legacy client without a signature"
MAX_REASON_CHARS = 280
MAX_COMMENT_CHARS = 1000
NCR_DISPOSITION_PENDING = "Pending MRB disposition"

_VALIDATION_KEYS = ("parameter", "validated_limit", "oq_ref", "pq_ref", "process_spec")


class ResponseValidationError(ValueError):
    """The request body cannot become a signed decision."""


class ResponseStoreUnavailable(RuntimeError):
    """The signed decision could not be persisted; nothing was recorded."""


@dataclass(frozen=True)
class ResponseAudit:
    reason: str
    signature_meaning: str
    comment: str | None = None


def parse_audit(body: Mapping[str, Any], action: str | None = None) -> ResponseAudit:
    """Validate the signing fields of a decision request.

    Legacy actions posted without any signing field (older clients) are
    accepted and marked ``legacy-unsigned``; every other request must sign.
    """
    if (
        action in LEGACY_ACTIONS
        and body.get("reason") is None
        and body.get("signature_meaning") is None
    ):
        return ResponseAudit(reason=LEGACY_UNSIGNED_REASON, signature_meaning=LEGACY_UNSIGNED)
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
        if not self.signature_meaning or self.signature_meaning == LEGACY_UNSIGNED:
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


def _rule_duration_s(alert: AlertRecord) -> float:
    try:
        rules = list_alert_rules(alert.industry or "")
    except KeyError:
        return 0.0
    rule = next((r for r in rules if r.rule_id == alert.rule_id), None)
    return float(rule.duration_s) if rule else 0.0


def excursion_start(alert: AlertRecord, now: datetime) -> datetime:
    """First over-limit reading when known, else ``opened_at - duration_s``.

    The stamp is trusted only inside ``[opened - (duration_s + DETECT_LOOKBACK_S),
    opened]``; anything else (for example a stamp left over from an earlier
    episode of the reused alert row) falls back to ``opened_at - duration_s``.
    Containment errs inclusive: the window never starts after the alert opened.
    """
    opened = _as_utc(alert.opened_at or now)
    duration = _rule_duration_s(alert)
    fallback = opened - timedelta(seconds=duration)
    raw = alert.metadata.get(EXCURSION_STARTED_KEY)
    if not raw:
        return fallback
    try:
        stamped = _as_utc(datetime.fromisoformat(str(raw)))
    except ValueError:
        logger.warning("alert %s has an unreadable %s", alert.id, EXCURSION_STARTED_KEY)
        return fallback
    earliest = opened - timedelta(seconds=duration + DETECT_LOOKBACK_S)
    if earliest <= stamped <= opened:
        return stamped
    logger.warning(
        "alert %s %s=%s is outside its episode; using opened_at - duration_s",
        alert.id,
        EXCURSION_STARTED_KEY,
        raw,
    )
    return fallback


def _excursion(alert: AlertRecord, now: datetime) -> tuple[datetime, datetime]:
    start = excursion_start(alert, now)
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
        **lot_disposition(start, end, now),
    }
    lot = {key: alert.metadata[key] for key in LOT_METADATA_KEYS if alert.metadata.get(key)}
    if lot:
        reference["triggering_part"] = lot
    return reference


def ncr_number(alert: AlertRecord) -> str:
    """Stable NCR id per (alert, episode), e.g. ``NCR-20261120-104512-3F2A``.

    The alert row is reused for every excursion (~288 a day), so the episode's
    opening second is part of the id.
    """
    opened = _as_utc(alert.opened_at or datetime.now(UTC))
    return f"NCR-{opened:%Y%m%d-%H%M%S}-{alert.id.replace('-', '')[:4].upper()}"


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
    # Rows written to an un-upgraded table keep the signature (and, when the
    # action was stored under its legacy name, the real action) here.
    stashed = reference.pop("audit", None) or {}
    signed_at = _field(row, "signed_at")
    if signed_at is None and stashed.get("signed_at"):
        signed_at = datetime.fromisoformat(str(stashed["signed_at"]))
    return AlertResponse(
        id=str(row["id"]),
        alert_id=str(row["alert_id"]),
        episode_opened_at=_as_utc(row["episode_opened_at"]),
        action=str(stashed.get("action") or row["action"]),
        recorded_by=str(row["recorded_by"]),
        reference=reference,
        created_at=_as_utc(row["created_at"]),
        reason=_field(row, "reason") or stashed.get("reason"),
        comment=_field(row, "comment") or stashed.get("comment"),
        signature_meaning=_field(row, "signature_meaning") or stashed.get("signature_meaning"),
        signed_at=_as_utc(signed_at) if signed_at else None,
    )


def _sqlstate(exc: Exception) -> str:
    return str(getattr(exc, "sqlstate", "") or "")


def _missing_audit_columns(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        _sqlstate(exc) == "42703"
        or "undefinedcolumn" in type(exc).__name__.lower()
        or ("column" in msg and "does not exist" in msg)
    )


def _action_check_violation(exc: Exception) -> bool:
    return (
        _sqlstate(exc) == "23514"
        or "checkviolation" in type(exc).__name__.lower()
        or "alert_response_action_check" in str(exc)
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

_SELECT_EXISTING = """
SELECT * FROM alert_response
WHERE alert_id = $1::uuid AND episode_opened_at = $2 AND action = $3
"""


def _audit_stash(response: AlertResponse, *, signature: bool, action: bool) -> dict[str, Any]:
    stash: dict[str, Any] = {}
    if action:
        stash["action"] = response.action
    if signature:
        stash.update(
            {
                "reason": response.reason,
                "comment": response.comment,
                "signature_meaning": response.signature_meaning,
                "signed_at": response.signed_at.isoformat() if response.signed_at else None,
            }
        )
    return stash


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
            except Exception as exc:
                # Memory never holds decisions when a backend is configured.
                raise ResponseStoreUnavailable(f"decision list failed: {exc}") from exc
            return [_row_to_response(row) for row in rows]
        async with self._lock:
            found = self._memory.get((alert.id, episode.isoformat()), {})
            return sorted(found.values(), key=lambda r: r.created_at)

    @staticmethod
    async def _insert_once(
        db: Any, response: AlertResponse, *, stored_action: str, audit_columns: bool
    ) -> Any:
        reference = dict(response.reference)
        stash = _audit_stash(
            response,
            signature=not audit_columns,
            action=stored_action != response.action,
        )
        if stash:
            reference["audit"] = stash
        base = (
            response.id,
            response.alert_id,
            response.episode_opened_at,
            stored_action,
            response.recorded_by,
            json.dumps(reference),
        )
        if not audit_columns:
            return await db.fetchrow(_INSERT_LEGACY, *base)
        return await db.fetchrow(
            _INSERT_SIGNED,
            *base,
            response.reason,
            response.comment,
            response.signature_meaning,
            response.signed_at,
        )

    async def _insert(self, db: Any, response: AlertResponse) -> tuple[Any, str]:
        """Persist ``response``; returns (row or None on conflict, stored action).

        Adapts to an un-upgraded table (see the module docstring) and raises
        ``ResponseStoreUnavailable`` when the decision cannot be stored.
        """
        stored_action = response.action
        audit_columns = True
        for _attempt in range(4):
            try:
                row = await self._insert_once(
                    db, response, stored_action=stored_action, audit_columns=audit_columns
                )
                return row, stored_action
            except Exception as exc:
                if audit_columns and _missing_audit_columns(exc):
                    logger.warning(
                        "alert_response has no audit columns (table not upgraded); "
                        "storing the signature in reference.audit"
                    )
                    audit_columns = False
                    continue
                legacy = LEGACY_STORAGE_ACTION.get(response.action)
                if _action_check_violation(exc) and stored_action == response.action and legacy:
                    logger.warning(
                        "alert_response CHECK predates %s (table not upgraded); "
                        "storing it as %s with the real action in reference.audit",
                        response.action,
                        legacy,
                    )
                    stored_action = legacy
                    continue
                if _action_check_violation(exc):
                    raise ResponseStoreUnavailable(
                        "alert_response table not upgraded: "
                        f"{response.action} cannot be recorded until its owner runs the upgrade"
                    ) from exc
                raise ResponseStoreUnavailable(f"signed decision not persisted: {exc}") from exc
        raise ResponseStoreUnavailable("signed decision not persisted")

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
        """Record a signed ``action`` once per episode; returns (response, newly_recorded).

        With a database the decision is persisted or ``ResponseStoreUnavailable``
        is raised; it never silently lands in memory.
        """
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
            legacy = LEGACY_STORAGE_ACTION.get(action)
            if legacy is not None:
                # A QA hold / CMM request stored under its legacy name before the table
                # upgrade (or a genuine legacy decision) is the same decision for this
                # episode: return it rather than writing a second row.
                try:
                    prior = await db.fetchrow(_SELECT_EXISTING, alert.id, episode, legacy)
                except Exception as exc:
                    raise ResponseStoreUnavailable(f"decision lookup failed: {exc}") from exc
                if prior is not None:
                    return _row_to_response(prior), False
            row, stored_action = await self._insert(db, candidate)
            if row is not None:
                return _row_to_response(row), True
            try:
                existing = await db.fetchrow(_SELECT_EXISTING, alert.id, episode, stored_action)
            except Exception as exc:
                raise ResponseStoreUnavailable(f"decision lookup failed: {exc}") from exc
            if existing is None:
                raise ResponseStoreUnavailable("decision conflicted but could not be read back")
            return _row_to_response(existing), False
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
