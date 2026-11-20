"""Operator decisions recorded against one alert episode.

A decision (hold the batch at PACK, send parts to INSPECT, request
maintenance) is a record of what the operator chose. Sending parts to
inspection also stops the cell — that actuation lives in
``iot_twin.services.production_control``. Decisions belong to an *episode* — the alert
row is reused across re-opens, so ``episode_opened_at`` keeps each heat event's
decisions separate.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from iot_twin.domain.catalog import list_assets, list_maintenance_history, list_sensors
from iot_twin.services.alerts.alert_engine import AlertRecord

logger = logging.getLogger(__name__)

ALERT_RESPONSE_EVENT = "alert_response"

# action -> cell station the decision applies to
RESPONSE_STATIONS: dict[str, str] = {
    "hold_batch": "PACK",
    "send_to_inspect": "INSPECT",
    "request_maintenance": "MILL",
}


@dataclass(frozen=True)
class AlertResponse:
    id: str
    alert_id: str
    episode_opened_at: datetime
    action: str
    recorded_by: str
    created_at: datetime
    reference: dict[str, Any] = field(default_factory=dict)

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


def response_reference(action: str, alert: AlertRecord) -> dict[str, Any]:
    """Facts attached to a decision, derived from the alert and the catalog."""
    reference: dict[str, Any] = {"station": RESPONSE_STATIONS[action]}
    if action == "send_to_inspect" and alert.opened_at is not None:
        reference["parts_since"] = _as_utc(alert.opened_at).isoformat()
    if action == "request_maintenance":
        reference.update(_maintenance_reference(alert))
    return reference


def _row_to_response(row: Any) -> AlertResponse:
    reference = row["reference"]
    if isinstance(reference, str):
        reference = json.loads(reference or "{}")
    return AlertResponse(
        id=str(row["id"]),
        alert_id=str(row["alert_id"]),
        episode_opened_at=_as_utc(row["episode_opened_at"]),
        action=str(row["action"]),
        recorded_by=str(row["recorded_by"]),
        reference=dict(reference or {}),
        created_at=_as_utc(row["created_at"]),
    )


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

    async def record(
        self,
        alert: AlertRecord,
        action: str,
        recorded_by: str,
        db: Any | None = None,
    ) -> tuple[AlertResponse, bool]:
        """Record ``action`` once per episode; returns (response, newly_recorded)."""
        if action not in RESPONSE_STATIONS:
            raise ValueError(f"unknown response action: {action}")
        episode = self._episode(alert)
        reference = response_reference(action, alert)
        if db is not None:
            try:
                row = await db.fetchrow(
                    """
                    INSERT INTO alert_response
                        (id, alert_id, episode_opened_at, action, recorded_by, reference)
                    VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb)
                    ON CONFLICT (alert_id, episode_opened_at, action) DO NOTHING
                    RETURNING *
                    """,
                    str(uuid.uuid4()),
                    alert.id,
                    episode,
                    action,
                    recorded_by,
                    json.dumps(reference),
                )
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
            response = AlertResponse(
                id=str(uuid.uuid4()),
                alert_id=alert.id,
                episode_opened_at=episode,
                action=action,
                recorded_by=recorded_by,
                reference=reference,
                created_at=datetime.now(UTC),
            )
            bucket[action] = response
            return response, True


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
