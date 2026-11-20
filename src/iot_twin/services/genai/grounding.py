"""The grounding set for an AI action plan: the exact rows the model may use.

Every item carries a stable citation id (``source:key``) so each claim in a
plan can be traced back to the row it came from:

* ``alert:<uuid>``                     — the alert row (Lakebase ``alert``)
* ``alert_event:<uuid>``               — its lifecycle history (``alert_event``)
* ``alert_response:<uuid>``            — signed decisions already recorded
* ``readings:<query-hash>#<n>``        — 10 s buckets of the excursion window (DBSQL)
* ``lot:<lot_id>``                     — lots and part serials at risk (lot clock)
* ``alert_rule:<rule_id>``             — the validated limit and OQ/PQ refs (catalog)
* ``maintenance:<event_id>``           — maintenance history of the asset/component
* ``risk_score:<alert_id>``            — model risk score, when the alert carries one

Reads are best-effort: a source that is unavailable is reported in ``gaps``
instead of failing the request, so the plan is honest about what it did not see.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from iot_twin.domain.catalog import (
    AlertRule,
    list_alert_rules,
    list_maintenance_history,
    list_sensors,
)
from iot_twin.domain.lot_context import LOT_METADATA_KEYS, lot_disposition, parts_in_window
from iot_twin.services.alerts.alert_engine import AlertRecord

logger = logging.getLogger(__name__)

RISK_RULE_ID = "medtech-model-spindle-risk"
RISK_METADATA_KEYS: tuple[str, ...] = ("risk_score", "model_name", "model_version", "top_features")
MAX_EVENTS = 50
MAX_TOP_FEATURES = 10
MAX_TEXT_CHARS = 500
WINDOW_BEFORE_S = 120
WINDOW_AFTER_S = 60
MAX_WINDOW_S = 1800
BUCKET_S = 10
MAX_READING_BUCKETS = 120

_EVENT_PAYLOAD_KEYS = ("status", "value", "threshold", "severity", "acknowledged_by", "cleared_at")

_EVENTS_SQL = """
SELECT id, event_type, created_at, payload
FROM alert_event
WHERE alert_id = $1::uuid AND created_at >= $2
ORDER BY created_at
LIMIT $3
"""

ReadingsRunner = Callable[[str], Awaitable[list[dict[str, Any]]]]


@dataclass(frozen=True)
class GroundingItem:
    """One cited row. ``id`` is the citation id the model must quote."""

    id: str
    source: str
    key: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "source": self.source, "key": self.key, "data": dict(self.data)}


@dataclass(frozen=True)
class GroundingSet:
    alert_id: str
    items: tuple[GroundingItem, ...]
    gaps: tuple[str, ...] = ()
    window: dict[str, str] = field(default_factory=dict)

    @property
    def ids(self) -> frozenset[str]:
        return frozenset(item.id for item in self.items)

    def by_source(self, source: str) -> list[GroundingItem]:
        return [item for item in self.items if item.source == source]

    def rows(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.items]

    def digest(self) -> str:
        """SHA-256 over the canonical rows — identical grounding, identical digest."""
        raw = json.dumps(self.rows(), sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    return value


def _clip(value: Any) -> Any:
    if isinstance(value, str) and len(value) > MAX_TEXT_CHARS:
        return value[:MAX_TEXT_CHARS] + "…"
    return value


def _json_safe(value: Any, depth: int = 0) -> Any:
    """Coerce to JSON primitives (bounded depth/size) so rows store and render safely."""
    if depth > 4:
        return None
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, str):
        return _clip(value)
    if isinstance(value, Mapping):
        return {str(k)[:80]: _json_safe(v, depth + 1) for k, v in list(value.items())[:25]}
    if isinstance(value, list | tuple):
        return [_json_safe(v, depth + 1) for v in list(value)[: MAX_TOP_FEATURES * 5]]
    return _clip(str(value))


def excursion_window(alert: AlertRecord, now: datetime) -> tuple[datetime, datetime]:
    """``[opened, cleared-or-now]`` for the episode, never inverted."""
    start = _as_utc(alert.opened_at or now)
    end = _as_utc(alert.cleared_at) if alert.cleared_at else _as_utc(now)
    return start, max(start, end)


def _alert_item(alert: AlertRecord) -> GroundingItem:
    lot = {key: alert.metadata[key] for key in LOT_METADATA_KEYS if alert.metadata.get(key)}
    data = {
        "rule_id": alert.rule_id,
        "sensor_id": alert.sensor_id,
        "severity": alert.severity,
        "status": alert.status,
        "fault_code": alert.fault_code,
        "value": alert.value,
        "threshold": alert.threshold,
        "opened_at": _iso(alert.opened_at),
        "acknowledged_at": _iso(alert.acknowledged_at),
        "acknowledged_by": alert.acknowledged_by,
        "cleared_at": _iso(alert.cleared_at),
        "triggering_part": lot or None,
    }
    return GroundingItem(f"alert:{alert.id}", "alert", alert.id, _json_safe(data))


def _rule_for(alert: AlertRecord) -> AlertRule | None:
    try:
        rules = list_alert_rules(alert.industry or "")
    except KeyError:
        return None
    return next((rule for rule in rules if rule.rule_id == alert.rule_id), None)


def _rule_item(rule: AlertRule) -> GroundingItem:
    data = {
        "sensor_id": rule.sensor_id,
        "operator": rule.operator,
        "threshold": rule.threshold,
        "severity": rule.severity,
        "fault_code": rule.fault_code,
        "duration_s": rule.duration_s,
        "hysteresis": rule.hysteresis,
        "required_action": rule.action or None,
        **dict(rule.metadata),
    }
    return GroundingItem(f"alert_rule:{rule.rule_id}", "alert_rule", rule.rule_id, data)


def _maintenance_items(alert: AlertRecord) -> list[GroundingItem]:
    try:
        sensors = list_sensors(alert.industry or "")
        history = list_maintenance_history(alert.industry or "")
    except KeyError:
        return []
    sensor = next((s for s in sensors if alert.sensor_id in (s.sensor_id, s.name)), None)
    if sensor is None:
        return []
    events = sorted(
        (e for e in history if e.asset_id == sensor.asset_id),
        key=lambda e: e.started_at,
        reverse=True,
    )
    return [
        GroundingItem(
            f"maintenance:{event.event_id}",
            "maintenance",
            event.event_id,
            {
                "asset_id": event.asset_id,
                "component_id": event.component_id,
                "same_component": event.component_id == sensor.component_id,
                "kind": event.kind,
                "started_at": event.started_at,
                "completed_at": event.completed_at,
                "notes": _clip(event.notes),
            },
        )
        for event in events
    ]


def _lot_items(alert: AlertRecord, start: datetime, end: datetime) -> list[GroundingItem]:
    if (alert.industry or "") != "manufacturing" or not _lot_tracked(alert):
        return []
    disposition = lot_disposition(start, end)
    parts = parts_in_window(start, end)
    items = []
    for lot_id, dhr in zip(disposition["lot_ids"], disposition["dhr_refs"], strict=True):
        serials = [p["serial"] for p in parts if p["lot_id"] == lot_id]
        items.append(
            GroundingItem(
                f"lot:{lot_id}",
                "lot",
                lot_id,
                {
                    "lot_id": lot_id,
                    "dhr_ref": dhr,
                    "parts_at_risk": serials,
                    "parts_at_risk_count": len(serials),
                    "parts_per_lot": disposition["parts_per_lot"],
                    "parts_releasable": disposition["parts_per_lot"] - len(serials),
                    "part_number": disposition["part_number"],
                    "udi_di": disposition["udi_di"],
                    "work_order": disposition["work_order"],
                },
            )
        )
    return items


def _lot_tracked(alert: AlertRecord) -> bool:
    sensor = alert.sensor_id.rsplit("-sns-", 1)[-1]
    return sensor.startswith("cnc_1_") or bool(alert.metadata.get("lot_id"))


def _top_features(raw: Any) -> Any:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return _clip(raw)
    if isinstance(raw, Mapping):
        raw = [{"feature": k, "contribution": v} for k, v in raw.items()]
    if isinstance(raw, list | tuple):
        return _json_safe(list(raw)[:MAX_TOP_FEATURES])
    return None


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def risk_fields(metadata: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The model-score keys from alert metadata, read defensively (absent → None)."""
    if not metadata:
        return None
    score = _to_float(metadata.get("risk_score"))
    if score is None:
        return None
    return {
        "risk_score": score,
        "model_name": _clip(str(metadata["model_name"])) if metadata.get("model_name") else None,
        "model_version": (
            _clip(str(metadata["model_version"])) if metadata.get("model_version") else None
        ),
        "top_features": _top_features(metadata.get("top_features")),
    }


def _risk_item(alert: AlertRecord) -> GroundingItem | None:
    fields = risk_fields(alert.metadata)
    if fields is None:
        return None
    data = {"alert_id": alert.id, "rule_id": alert.rule_id, **fields}
    return GroundingItem(f"risk_score:{alert.id}", "risk_score", alert.id, data)


def _related_risk_alert(
    alert: AlertRecord, others: list[AlertRecord], start: datetime, end: datetime
) -> AlertRecord | None:
    """The model-scored risk alert for the same industry that overlaps this episode."""
    candidates = [
        other
        for other in others
        if other.id != alert.id
        and other.rule_id == RISK_RULE_ID
        and other.industry == alert.industry
        and risk_fields(other.metadata) is not None
        and _as_utc(other.opened_at) <= end + timedelta(seconds=WINDOW_AFTER_S)
        and (other.cleared_at is None or _as_utc(other.cleared_at) >= start)
    ]
    return max(candidates, key=lambda a: _as_utc(a.opened_at), default=None)


async def _event_items(
    alert: AlertRecord, db: Any | None
) -> tuple[list[GroundingItem], str | None]:
    if db is None:
        return [], "alert_event history unavailable (no Lakebase connection)"
    since = _as_utc(alert.opened_at) - timedelta(seconds=5)
    try:
        rows = await db.fetch(_EVENTS_SQL, alert.id, since, MAX_EVENTS)
    except Exception as exc:  # noqa: BLE001 - grounding is best effort
        logger.warning("action plan: alert_event read failed: %s", exc)
        return [], "alert_event history read failed"
    items = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload or "{}")
        payload = dict(payload or {})
        data = {
            "event_type": row["event_type"],
            "created_at": _iso(row["created_at"]),
            **{key: payload.get(key) for key in _EVENT_PAYLOAD_KEYS if key in payload},
        }
        event_id = str(row["id"])
        items.append(
            GroundingItem(f"alert_event:{event_id}", "alert_event", event_id, _json_safe(data))
        )
    return items, None


def _response_items(responses: list[Any]) -> list[GroundingItem]:
    items = []
    for response in responses:
        data = response.to_dict()
        data = {
            key: data.get(key)
            for key in (
                "action",
                "station",
                "recorded_by",
                "created_at",
                "reason",
                "signature_meaning",
                "reference",
            )
        }
        items.append(
            GroundingItem(
                f"alert_response:{response.id}", "alert_response", response.id, _json_safe(data)
            )
        )
    return items


def readings_sql(
    table_sql: str, sensor_name: str, start: datetime, end: datetime, row_filter: str | None
) -> str:
    """10-second buckets of one sensor across the excursion window."""
    name = sensor_name.replace("'", "''")
    extra = f"AND {row_filter} " if row_filter else ""
    lo, hi = int(start.timestamp()), int(end.timestamp())
    return (
        f"SELECT timestamp_seconds(CAST(FLOOR(unix_timestamp(event_time) / {BUCKET_S}) "
        f"* {BUCKET_S} AS BIGINT)) AS bucket_start, "
        "ROUND(MIN(value), 2) AS min_value, ROUND(MAX(value), 2) AS max_value, "
        "ROUND(AVG(value), 2) AS avg_value, COUNT(*) AS n "
        f"FROM {table_sql} WHERE sensor_name = '{name}' {extra}"
        f"AND event_time BETWEEN timestamp_seconds({lo}) AND timestamp_seconds({hi}) "
        f"GROUP BY 1 ORDER BY 1 LIMIT {MAX_READING_BUCKETS}"
    )


def reading_window(alert: AlertRecord, now: datetime) -> tuple[datetime, datetime]:
    start, end = excursion_window(alert, now)
    lo = start - timedelta(seconds=WINDOW_BEFORE_S)
    hi = min(end + timedelta(seconds=WINDOW_AFTER_S), lo + timedelta(seconds=MAX_WINDOW_S))
    return lo, hi


def sensor_name_for(alert: AlertRecord) -> str | None:
    """The catalog sensor name behind ``alert.sensor_id`` (None when unknown)."""
    try:
        sensors = list_sensors(alert.industry or "")
    except KeyError:
        return None
    sensor = next((s for s in sensors if alert.sensor_id in (s.sensor_id, s.name)), None)
    return sensor.name if sensor else None


async def _reading_items(
    sql: str | None, runner: ReadingsRunner | None
) -> tuple[list[GroundingItem], str | None]:
    if sql is None or runner is None:
        return [], "telemetry readings unavailable (no pipeline mode or SQL warehouse)"
    try:
        rows = await runner(sql)
    except Exception as exc:  # noqa: BLE001 - grounding is best effort
        logger.warning("action plan: readings window query failed: %s", exc)
        return [], "telemetry readings query failed"
    query_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:12]
    items = [
        GroundingItem(
            f"readings:{query_hash}#{index}",
            "readings",
            f"{query_hash}#{index}",
            _json_safe(dict(row)),
        )
        for index, row in enumerate(rows[:MAX_READING_BUCKETS])
    ]
    return items, None if items else "no telemetry readings in the excursion window"


@dataclass(frozen=True)
class GroundingSources:
    """Where the grounding comes from; every field is optional and injectable."""

    db: Any | None = None
    responses: list[Any] = field(default_factory=list)
    other_alerts: list[AlertRecord] = field(default_factory=list)
    readings_sql: str | None = None
    readings_runner: ReadingsRunner | None = None


async def assemble_grounding(
    alert: AlertRecord, sources: GroundingSources, now: datetime | None = None
) -> GroundingSet:
    """Collect every row the model may cite, in a stable order."""
    at = _as_utc(now or datetime.now(UTC))
    start, end = excursion_window(alert, at)
    items: list[GroundingItem] = [_alert_item(alert)]
    gaps: list[str] = []

    events, gap = await _event_items(alert, sources.db)
    items.extend(events)
    gaps.extend([gap] if gap else [])
    items.extend(_response_items(sources.responses))

    readings, gap = await _reading_items(sources.readings_sql, sources.readings_runner)
    items.extend(readings)
    gaps.extend([gap] if gap else [])

    items.extend(_lot_items(alert, start, end))
    rule = _rule_for(alert)
    if rule is not None:
        items.append(_rule_item(rule))
    else:
        gaps.append(f"no catalog rule for {alert.rule_id}")
    items.extend(_maintenance_items(alert))

    risk = _risk_item(alert)
    related = _related_risk_alert(alert, sources.other_alerts, start, end)
    if risk is None and related is not None:
        risk = _risk_item(related)
    if risk is not None:
        items.append(risk)
    else:
        gaps.append("no model risk score on this alert")

    return GroundingSet(
        alert_id=alert.id,
        items=tuple(items),
        gaps=tuple(gaps),
        window={"start": start.isoformat(), "end": end.isoformat()},
    )
