"""Deterministic alert state machine with optional Lakebase persistence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from iot_twin.domain.catalog import (
    AlertRule,
    list_alert_rules,
    sensor_by_name,
    to_record_industry,
)
from iot_twin.services.alerts.alert_sse import (
    EVENT_ACKNOWLEDGED,
    EVENT_CLEARED,
    EVENT_OPENED,
    EVENT_UPDATED,
    build_lifecycle_payload,
    enqueue_alert_events,
)

logger = logging.getLogger("iot_digital_twin.services.alert_engine")

ACTIVE_STATUSES = ("open", "acknowledged")

_DB_EVENT_TYPES = {
    EVENT_OPENED: "opened",
    EVENT_UPDATED: "updated",
    EVENT_ACKNOWLEDGED: "acknowledged",
    EVENT_CLEARED: "cleared",
}


def fingerprint_for(
    rule_id: str,
    sensor_ids: Iterable[str],
    pipeline_id: str | None,
) -> str:
    """Stable SHA-256 identity for a rule × sensors × pipeline tuple."""
    sensors = ",".join(sorted({str(item) for item in sensor_ids if item}))
    raw = f"{rule_id}|{sensors}|{pipeline_id or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def event_fingerprint(alert_fp: str, event_type: str, stamp: str) -> str:
    raw = f"{alert_fp}|{event_type}|{stamp}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class AlertRecord:
    id: str
    fingerprint: str
    industry: str | None
    pipeline_id: str | None
    rule_id: str
    sensor_id: str
    sensor_ids: list[str]
    severity: str
    status: str
    fault_code: str | None
    value: float | None
    threshold: float | None
    opened_at: datetime
    updated_at: datetime
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None
    cleared_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    version: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "fingerprint": self.fingerprint,
            "industry": self.industry,
            "pipeline_id": self.pipeline_id,
            "rule_id": self.rule_id,
            "sensor_id": self.sensor_id,
            "sensor_ids": list(self.sensor_ids),
            "severity": self.severity,
            "status": self.status,
            "fault_code": self.fault_code,
            "value": self.value,
            "threshold": self.threshold,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "acknowledged_at": (self.acknowledged_at.isoformat() if self.acknowledged_at else None),
            "acknowledged_by": self.acknowledged_by,
            "cleared_at": self.cleared_at.isoformat() if self.cleared_at else None,
            "metadata": dict(self.metadata),
            "version": int(self.version or 0),
        }


@dataclass
class AlertTransition:
    event_type: str
    alert: AlertRecord
    extra: dict[str, Any] = field(default_factory=dict)


def engine_for(state: Any) -> AlertEngine:
    """Return the process-wide engine stored on ``AppState``."""
    engine = getattr(state, "alert_engine", None)
    if engine is None:
        engine = AlertEngine()
        state.alert_engine = engine
    return engine


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_violation(operator: str, value: float, threshold: float) -> bool:
    if operator == "gt":
        return value > threshold
    if operator == "gte":
        return value >= threshold
    if operator == "lt":
        return value < threshold
    if operator == "lte":
        return value <= threshold
    return False


def _in_clear_band(operator: str, value: float, threshold: float, hysteresis: float) -> bool:
    band = float(hysteresis or 0.0)
    if operator == "gt" or operator == "gte":
        return value <= threshold - band
    if operator == "lt" or operator == "lte":
        return value >= threshold + band
    return False


def _catalog_industry(industry: str | None) -> str | None:
    if not industry:
        return None
    return to_record_industry(str(industry))


def _rules_for(industry: str | None) -> tuple[AlertRule, ...]:
    mapped = _catalog_industry(industry)
    if not mapped:
        return ()
    try:
        return list_alert_rules(mapped)
    except KeyError:
        return ()


def _index_readings(
    readings: Sequence[Mapping[str, Any]] | None, industry: str | None
) -> dict[str, Mapping[str, Any]]:
    index: dict[str, Mapping[str, Any]] = {}
    if not readings:
        return index
    for raw in readings:
        if not isinstance(raw, Mapping):
            continue
        for key in (raw.get("sensor_id"), raw.get("sensor_name")):
            if key:
                index[str(key)] = raw
        name = raw.get("sensor_name")
        if industry and name:
            try:
                node = sensor_by_name(industry, str(name))
            except KeyError:
                node = None
            if node is not None:
                index[node.sensor_id] = raw
                index[node.name] = raw
    return index


def _configured_sensor_ids(rule: AlertRule) -> list[str]:
    ids = [rule.sensor_id, *list(rule.correlate_sensor_ids)]
    return [str(item) for item in ids if item]


def _rule_conditions(rule: AlertRule) -> list[tuple[str, str, float]]:
    conditions = [(rule.sensor_id, rule.operator, float(rule.threshold))]
    for extra_id in rule.correlate_sensor_ids:
        operator = rule.correlate_operator or rule.operator
        threshold = (
            float(rule.correlate_threshold)
            if rule.correlate_threshold is not None
            else float(rule.threshold)
        )
        conditions.append((str(extra_id), operator, threshold))
    return conditions


def _any_required_recovered(
    rule: AlertRule, index: Mapping[str, Mapping[str, Any]]
) -> tuple[bool, float | None]:
    """True when every configured condition is readable and any one has recovered."""
    hysteresis = float(getattr(rule, "hysteresis", 0.0) or 0.0)
    primary_value: float | None = None
    recovered = False
    for sensor_id, operator, threshold in _rule_conditions(rule):
        reading = index.get(sensor_id)
        if reading is None:
            return False, primary_value
        value = _to_float(reading.get("value"))
        if value is None:
            return False, primary_value
        if sensor_id == rule.sensor_id:
            primary_value = value
        if _in_clear_band(operator, value, threshold, hysteresis):
            recovered = True
    return recovered, primary_value


def _rule_state(
    rule: AlertRule, index: Mapping[str, Mapping[str, Any]]
) -> tuple[bool, float | None, list[str]]:
    primary = index.get(rule.sensor_id)
    if primary is None:
        return False, None, []
    value = _to_float(primary.get("value"))
    if value is None:
        return False, None, []
    if not _is_violation(rule.operator, value, float(rule.threshold)):
        return False, value, [rule.sensor_id]
    sensor_ids = [rule.sensor_id]
    for extra_id in rule.correlate_sensor_ids:
        extra = index.get(extra_id)
        if extra is None:
            return False, value, sensor_ids
        extra_value = _to_float(extra.get("value"))
        operator = rule.correlate_operator or rule.operator
        threshold = (
            float(rule.correlate_threshold)
            if rule.correlate_threshold is not None
            else float(rule.threshold)
        )
        if extra_value is None or not _is_violation(operator, extra_value, threshold):
            return False, value, sensor_ids
        sensor_ids.append(extra_id)
    return True, value, sensor_ids


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _row_to_alert(row: Any) -> AlertRecord:
    data = dict(row)
    sensor_ids = data.get("sensor_ids") or []
    if isinstance(sensor_ids, str):
        sensor_ids = [sensor_ids]
    metadata = data.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    return AlertRecord(
        id=str(data["id"]),
        fingerprint=data["fingerprint"],
        industry=data.get("industry"),
        pipeline_id=data.get("pipeline_id"),
        rule_id=data["rule_id"],
        sensor_id=data.get("sensor_id") or "",
        sensor_ids=list(sensor_ids),
        severity=data["severity"],
        status=data["status"],
        fault_code=data.get("fault_code"),
        value=_to_float(data.get("value")),
        threshold=_to_float(data.get("threshold")),
        opened_at=_as_datetime(data.get("opened_at")) or _utcnow(),
        updated_at=_as_datetime(data.get("updated_at")) or _utcnow(),
        acknowledged_at=_as_datetime(data.get("acknowledged_at")),
        acknowledged_by=data.get("acknowledged_by"),
        cleared_at=_as_datetime(data.get("cleared_at")),
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
        version=int(data.get("version") or 0),
    )


class AlertEngine:
    """In-memory state machine with optional append-only Lakebase writes."""

    def __init__(self, now_fn=None) -> None:
        self._now = now_fn or _utcnow
        self._by_fingerprint: dict[str, AlertRecord] = {}
        self._by_id: dict[str, AlertRecord] = {}
        self._pending: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

    def get_alert_sync(self, alert_id: str) -> AlertRecord | None:
        return self._by_id.get(alert_id)

    def _store(self, alert: AlertRecord) -> AlertRecord:
        self._by_fingerprint[alert.fingerprint] = alert
        self._by_id[alert.id] = alert
        return alert

    def _now_dt(self) -> datetime:
        instant = self._now()
        if instant.tzinfo is None:
            return instant.replace(tzinfo=UTC)
        return instant

    async def list_alerts(
        self,
        *,
        status: str | None = None,
        industry: str | None = None,
        severity: str | None = None,
        db: Any | None = None,
        limit: int | None = 200,
        offset: int = 0,
    ) -> list[AlertRecord]:
        async with self._lock:
            return await self._list_alerts(
                status=status,
                industry=industry,
                severity=severity,
                db=db,
                limit=limit,
                offset=offset,
            )

    async def _list_alerts(
        self,
        *,
        status: str | None,
        industry: str | None,
        severity: str | None,
        db: Any | None,
        limit: int | None = 200,
        offset: int = 0,
    ) -> list[AlertRecord]:
        if db is not None:
            try:
                rows = await db.fetch(
                    """
                    SELECT * FROM alert
                    WHERE (
                            ($1::text IS NULL AND status IN ('open', 'acknowledged'))
                            OR status = $1
                          )
                      AND ($2::text IS NULL OR industry = $2)
                      AND ($3::text IS NULL OR severity = $3)
                    ORDER BY updated_at DESC
                    LIMIT $4 OFFSET $5
                    """,
                    status,
                    industry,
                    severity,
                    limit if limit is not None else 200,
                    offset or 0,
                )
                alerts = [_row_to_alert(row) for row in rows]
                for alert in alerts:
                    self._store(alert)
                return alerts
            except Exception:
                logger.exception("alert list from Lakebase failed; using memory")
        alerts = list(self._by_id.values())
        if status:
            alerts = [item for item in alerts if item.status == status]
        else:
            alerts = [item for item in alerts if item.status in ACTIVE_STATUSES]
        if industry:
            alerts = [item for item in alerts if item.industry == industry]
        if severity:
            alerts = [item for item in alerts if item.severity == severity]
        alerts.sort(key=lambda item: item.updated_at, reverse=True)
        start = max(0, int(offset or 0))
        cap = 200 if limit is None else max(1, min(int(limit), 200))
        return alerts[start : start + cap]

    async def get_alert(self, alert_id: str, db: Any | None = None) -> AlertRecord | None:
        async with self._lock:
            return await self._get_alert(alert_id, db)

    async def _get_alert(self, alert_id: str, db: Any | None) -> AlertRecord | None:
        found = self._by_id.get(alert_id)
        if found is not None:
            return found
        if db is None or not alert_id:
            return None
        try:
            row = await db.fetchrow("SELECT * FROM alert WHERE id = $1::uuid", alert_id)
        except Exception:
            logger.exception("alert detail from Lakebase failed")
            return None
        if row is None:
            return None
        return self._store(_row_to_alert(row))

    async def _lookup_fingerprint(self, fingerprint: str, db: Any | None) -> AlertRecord | None:
        existing = self._by_fingerprint.get(fingerprint)
        if existing is not None:
            return existing
        if db is None or not fingerprint:
            return None
        try:
            row = await db.fetchrow(
                "SELECT * FROM alert WHERE fingerprint = $1",
                fingerprint,
            )
        except Exception:
            logger.exception("alert fingerprint lookup failed")
            return None
        if row is None:
            return None
        return self._store(_row_to_alert(row))

    async def evaluate(
        self,
        readings: Sequence[Mapping[str, Any]] | None,
        rules: Sequence[AlertRule],
        *,
        industry: str | None = None,
        pipeline_id: str | None = None,
        db: Any | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> list[AlertTransition]:
        async with self._lock:
            if not readings:
                return []
            index = _index_readings(readings, industry)
            transitions: list[AlertTransition] = []
            for rule in rules:
                transition = await self._evaluate_rule(
                    rule,
                    index,
                    industry=industry,
                    pipeline_id=pipeline_id,
                    db=db,
                    extra=dict(extra or {}),
                )
                if transition is not None:
                    transitions.append(transition)
            return transitions

    async def ingest_threshold_alerts(
        self,
        alerts: Sequence[Mapping[str, Any]] | None,
        *,
        db: Any | None = None,
    ) -> list[AlertTransition]:
        async with self._lock:
            transitions: list[AlertTransition] = []
            bounded = list(alerts or [])[:200]
            for raw in bounded:
                if not isinstance(raw, Mapping):
                    continue
                transition = await self._ingest_one(raw, db=db)
                if transition is not None:
                    transitions.append(transition)
            return transitions

    async def acknowledge(
        self,
        alert_id: str,
        user: str,
        db: Any | None = None,
    ) -> AlertTransition | None:
        async with self._lock:
            if not alert_id:
                return None
            alert = await self._get_alert(alert_id, db)
            if alert is None:
                return None
            if alert.status == "acknowledged":
                return AlertTransition(event_type=EVENT_ACKNOWLEDGED, alert=alert)
            if alert.status != "open":
                return None
            now = self._now_dt()
            alert = replace(
                alert,
                status="acknowledged",
                acknowledged_at=now,
                acknowledged_by=user,
                updated_at=now,
            )
            self._store(alert)
            transition = AlertTransition(event_type=EVENT_ACKNOWLEDGED, alert=alert)
            applied = await self._persist_transition(transition, db)
            if not applied:
                return None
            return transition

    async def _evaluate_rule(
        self,
        rule: AlertRule,
        index: Mapping[str, Mapping[str, Any]],
        *,
        industry: str | None,
        pipeline_id: str | None,
        db: Any | None,
        extra: dict[str, Any],
        force_open: bool = False,
        forced_value: float | None = None,
        forced_sensor_ids: list[str] | None = None,
    ) -> AlertTransition | None:
        violated, value, sensor_ids = _rule_state(rule, index)
        if force_open:
            violated = True
            value = forced_value if forced_value is not None else value
            sensor_ids = forced_sensor_ids or sensor_ids or [rule.sensor_id]
        fp = fingerprint_for(rule.rule_id, _configured_sensor_ids(rule), pipeline_id)
        existing = await self._lookup_fingerprint(fp, db)
        now = self._now_dt()

        if violated:
            duration = float(getattr(rule, "duration_s", 0.0) or 0.0)
            if existing is None or existing.status == "cleared":
                started = self._pending.get(fp, now)
                self._pending[fp] = started
                if (now - started).total_seconds() < duration:
                    return None
                self._pending.pop(fp, None)
                alert = AlertRecord(
                    id=str(uuid.uuid4()),
                    fingerprint=fp,
                    industry=industry,
                    pipeline_id=pipeline_id,
                    rule_id=rule.rule_id,
                    sensor_id=rule.sensor_id,
                    sensor_ids=list(sensor_ids or [rule.sensor_id]),
                    severity=rule.severity,
                    status="open",
                    fault_code=rule.fault_code,
                    value=value,
                    threshold=float(rule.threshold),
                    opened_at=now,
                    updated_at=now,
                )
                if existing is not None:
                    alert = replace(alert, id=existing.id)
                self._store(alert)
                transition = AlertTransition(event_type=EVENT_OPENED, alert=alert, extra=extra)
                applied = await self._persist_transition(transition, db)
                if not applied:
                    return None
                return transition
            if existing.status in {"open", "acknowledged"}:
                if existing.value == value and existing.severity == rule.severity:
                    return None
                alert = replace(
                    existing,
                    value=value,
                    severity=rule.severity,
                    updated_at=now,
                    threshold=float(rule.threshold),
                    sensor_ids=list(sensor_ids or existing.sensor_ids),
                )
                self._store(alert)
                transition = AlertTransition(event_type=EVENT_UPDATED, alert=alert, extra=extra)
                applied = await self._persist_transition(transition, db)
                if not applied:
                    return None
                return transition
            return None

        # Realtime batches arrive per Kafka partition, so most carry no reading
        # for this sensor; only an in-limit reading restarts the duration timer.
        if value is not None:
            self._pending.pop(fp, None)
        if existing is None or existing.status == "cleared":
            return None
        recovered, clear_value = _any_required_recovered(rule, index)
        if not recovered or clear_value is None:
            return None
        alert = replace(
            existing,
            status="cleared",
            value=clear_value,
            updated_at=now,
            cleared_at=now,
        )
        self._store(alert)
        transition = AlertTransition(event_type=EVENT_CLEARED, alert=alert, extra=extra)
        applied = await self._persist_transition(transition, db)
        if not applied:
            return None
        return transition

    async def _ingest_one(
        self, raw: Mapping[str, Any], *, db: Any | None
    ) -> AlertTransition | None:
        value = _to_float(raw.get("value"))
        tmax = _to_float(raw.get("threshold_max"))
        tmin = _to_float(raw.get("threshold_min"))
        sensor_id = str(raw.get("sensor_id") or "")
        pipeline_id = raw.get("pipeline_id")
        extra = {
            "sensor_type": raw.get("sensor_type"),
            "threshold_min": raw.get("threshold_min"),
            "threshold_max": raw.get("threshold_max"),
            "timestamp": raw.get("timestamp"),
        }
        if tmax is not None:
            operator = "gt"
            threshold = tmax
            violating = value is None or value > tmax
        elif tmin is not None:
            operator = "lt"
            threshold = tmin
            violating = value is None or value < tmin
        else:
            operator = "gt"
            threshold = 0.0
            violating = True
        rule = AlertRule(
            rule_id=f"sss:{pipeline_id or ''}:{sensor_id}",
            sensor_id=sensor_id,
            operator=operator,
            threshold=threshold,
            severity="crit",
            fault_code="SSS-THRESHOLD",
        )
        reading = {
            "sensor_id": sensor_id,
            "value": value if value is not None else (threshold + 1 if violating else threshold),
            "industry": raw.get("industry"),
        }
        return await self._evaluate_rule(
            rule,
            {sensor_id: reading},
            industry=raw.get("industry"),
            pipeline_id=str(pipeline_id) if pipeline_id is not None else None,
            db=db,
            extra=extra,
            force_open=violating,
            forced_value=value,
            forced_sensor_ids=[sensor_id],
        )

    async def _persist_transition(self, transition: AlertTransition, db: Any | None) -> bool:
        if db is None:
            return True
        alert = transition.alert
        event_fp = event_fingerprint(
            alert.fingerprint,
            transition.event_type,
            alert.updated_at.isoformat(),
        )
        payload = build_lifecycle_payload(transition.event_type, alert, transition.extra)
        db_event = _DB_EVENT_TYPES.get(transition.event_type, "updated")
        try:
            row = await db.fetchrow(
                """
                WITH upserted AS (
                    INSERT INTO alert (
                        id, fingerprint, industry, pipeline_id, rule_id, sensor_id,
                        sensor_ids, severity, status, fault_code, value, threshold,
                        opened_at, updated_at, acknowledged_at, acknowledged_by,
                        cleared_at, metadata, version
                    ) VALUES (
                        $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18::jsonb, $19
                    )
                    ON CONFLICT (fingerprint) DO UPDATE SET
                        opened_at = CASE
                            WHEN alert.status = 'cleared' THEN EXCLUDED.opened_at
                            ELSE alert.opened_at
                        END,
                        status = EXCLUDED.status,
                        severity = EXCLUDED.severity,
                        value = EXCLUDED.value,
                        threshold = EXCLUDED.threshold,
                        sensor_ids = EXCLUDED.sensor_ids,
                        updated_at = EXCLUDED.updated_at,
                        acknowledged_at = EXCLUDED.acknowledged_at,
                        acknowledged_by = EXCLUDED.acknowledged_by,
                        cleared_at = EXCLUDED.cleared_at,
                        pipeline_id = EXCLUDED.pipeline_id,
                        industry = EXCLUDED.industry,
                        version = alert.version + 1
                    WHERE
                        EXCLUDED.status = 'cleared'
                        OR alert.status = 'cleared'
                        OR (
                            alert.status IS DISTINCT FROM 'cleared'
                            AND NOT (
                                alert.status = 'acknowledged'
                                AND EXCLUDED.status = 'open'
                            )
                            AND EXCLUDED.version >= alert.version
                        )
                    RETURNING id, status, version, acknowledged_at, acknowledged_by,
                              cleared_at, updated_at
                ),
                ins_event AS (
                    INSERT INTO alert_event (alert_id, event_type, fingerprint, payload)
                    SELECT id, $20, $21, $22::jsonb FROM upserted
                    ON CONFLICT (fingerprint) DO NOTHING
                ),
                canonical AS (
                    SELECT id, status, version, acknowledged_at, acknowledged_by,
                           cleared_at, updated_at, TRUE AS applied
                    FROM upserted
                    UNION ALL
                    SELECT id, status, version, acknowledged_at, acknowledged_by,
                           cleared_at, updated_at, FALSE AS applied
                    FROM alert
                    WHERE fingerprint = $2
                      AND NOT EXISTS (SELECT 1 FROM upserted)
                )
                SELECT id, status, version, acknowledged_at, acknowledged_by,
                       cleared_at, updated_at, applied
                FROM canonical
                """,
                alert.id,
                alert.fingerprint,
                alert.industry,
                alert.pipeline_id,
                alert.rule_id,
                alert.sensor_id,
                list(alert.sensor_ids),
                alert.severity,
                alert.status,
                alert.fault_code,
                alert.value,
                alert.threshold,
                alert.opened_at,
                alert.updated_at,
                alert.acknowledged_at,
                alert.acknowledged_by,
                alert.cleared_at,
                json.dumps(alert.metadata),
                int(alert.version or 0),
                db_event,
                event_fp,
                json.dumps(payload, default=str),
            )
            if row is None:
                return True
            persisted_id = str(row["id"]) if row.get("id") is not None else alert.id
            applied = bool(row.get("applied", True))
            canonical_status = str(row.get("status") or alert.status)
            canonical = replace(
                alert,
                id=persisted_id,
                status=canonical_status,
                version=int(row.get("version") or alert.version or 0),
                acknowledged_at=_as_datetime(row.get("acknowledged_at")) or alert.acknowledged_at,
                acknowledged_by=row.get("acknowledged_by", alert.acknowledged_by),
                cleared_at=_as_datetime(row.get("cleared_at")) or alert.cleared_at,
                updated_at=_as_datetime(row.get("updated_at")) or alert.updated_at,
            )
            if persisted_id != alert.id:
                self._by_id.pop(alert.id, None)
            transition.alert = canonical
            self._store(canonical)
            if not applied and canonical_status != alert.status:
                return False
            return applied or transition.event_type not in {EVENT_OPENED, EVENT_UPDATED}
        except Exception:
            logger.exception("alert persist failed; continuing in memory")
            return True


async def evaluate_and_emit(
    state: Any,
    readings: Sequence[Mapping[str, Any]] | None,
    *,
    industry: str | None = None,
    pipeline_id: str | None = None,
) -> list[AlertTransition]:
    """Evaluate catalog rules and fan lifecycle events to SSE clients."""
    if not readings:
        return []
    engine = engine_for(state)
    resolved_industry = industry
    if not resolved_industry:
        first = readings[0] if readings else {}
        if isinstance(first, Mapping):
            resolved_industry = first.get("industry")
    resolved_industry = _catalog_industry(str(resolved_industry) if resolved_industry else None)
    rules = _rules_for(resolved_industry)
    db = None
    if getattr(state, "app_backend_available", False):
        db = getattr(state, "app_backend_db", None)
    transitions = await engine.evaluate(
        readings,
        rules,
        industry=str(resolved_industry) if resolved_industry else None,
        pipeline_id=pipeline_id,
        db=db,
    )
    payloads = [
        build_lifecycle_payload(item.event_type, item.alert, item.extra) for item in transitions
    ]
    enqueue_alert_events(state, payloads)
    if transitions:
        syncer = getattr(state, "alert_snapshot_synchronizer", None)
        if syncer is not None:
            syncer.mark_dirty()
    return transitions
