"""SSE helpers for alert lifecycle fan-out."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import to_record_industry

EVENT_OPENED = "alert_opened"
EVENT_UPDATED = "alert_updated"
EVENT_ACKNOWLEDGED = "alert_acknowledged"
EVENT_CLEARED = "alert_cleared"

LIFECYCLE_EVENT_TYPES: tuple[str, str, str, str] = (
    EVENT_OPENED,
    EVENT_UPDATED,
    EVENT_ACKNOWLEDGED,
    EVENT_CLEARED,
)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def build_lifecycle_payload(
    event_type: str,
    alert: Any,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the exhaustive snapshot for one lifecycle event."""
    payload: dict[str, Any] = {
        "type": event_type,
        "alert_id": getattr(alert, "id", None),
        "fingerprint": getattr(alert, "fingerprint", None),
        "industry": getattr(alert, "industry", None),
        "pipeline_id": getattr(alert, "pipeline_id", None),
        "rule_id": getattr(alert, "rule_id", None),
        "sensor_id": getattr(alert, "sensor_id", None),
        "sensor_ids": list(getattr(alert, "sensor_ids", ()) or ()),
        "severity": getattr(alert, "severity", None),
        "status": getattr(alert, "status", None),
        "fault_code": getattr(alert, "fault_code", None),
        "value": getattr(alert, "value", None),
        "threshold": getattr(alert, "threshold", None),
        "opened_at": _iso(getattr(alert, "opened_at", None)),
        "updated_at": _iso(getattr(alert, "updated_at", None)),
        "acknowledged_at": _iso(getattr(alert, "acknowledged_at", None)),
        "acknowledged_by": getattr(alert, "acknowledged_by", None),
        "cleared_at": _iso(getattr(alert, "cleared_at", None)),
    }
    if extra:
        reserved = {"type", "alert_id", "fingerprint", "status", "severity"}
        for key, value in extra.items():
            if key in reserved and payload.get(key) not in (None, "", []):
                continue
            payload[key] = value
    return payload


def _queue_industry(queue: asyncio.Queue) -> str | None:
    raw = getattr(queue, "filter_industry", None)
    if not raw:
        return None
    return to_record_industry(str(raw))


def enqueue_alert_events(state: AppState, payloads: Iterable[Mapping[str, Any]]) -> None:
    """Fan lifecycle payloads out to every registered SSE client queue.

    Alert lifecycle events are filtered by ``queue.filter_industry`` when set.
    Queues without a filter (including raw unit-test queues) still receive all
    events so existing tests stay compatible.
    """
    items = list(payloads)
    if not items or not state.sse_clients:
        return
    for payload in items:
        is_lifecycle = str(payload.get("type") or "") in LIFECYCLE_EVENT_TYPES
        event_industry = to_record_industry(str(payload.get("industry") or "")) or None
        for queue in state.sse_clients[:]:
            wanted = _queue_industry(queue)
            if is_lifecycle and wanted and event_industry != wanted:
                continue
            try:
                queue.put_nowait(dict(payload))
            except asyncio.QueueFull:
                continue
            except Exception:
                continue
