"""Alert episodes for an industry, rebuilt from the append-only event log.

The ``alert`` row is reused each time a rule re-opens, so history comes from
``alert_event``: events are grouped by the episode's ``opened_at`` (carried in
every lifecycle payload) into one episode with its peak reading, the
operator's decisions and the production stops it caused.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from iot_twin.services.alerts.alert_engine import AlertEngine
from iot_twin.services.alerts.alert_responses import RESPONSE_STATIONS

logger = logging.getLogger(__name__)

_EPISODES_SQL = """
SELECT e.alert_id,
       e.payload->>'opened_at' AS episode,
       MIN(e.created_at) FILTER (WHERE e.event_type = 'opened') AS opened_seen,
       MIN(e.created_at) FILTER (WHERE e.event_type = 'acknowledged') AS acknowledged_at,
       MAX(e.payload->>'acknowledged_by') FILTER (WHERE e.event_type = 'acknowledged')
           AS acknowledged_by,
       MIN(e.created_at) FILTER (WHERE e.event_type = 'cleared') AS cleared_at,
       MAX((e.payload->>'value')::double precision)
           FILTER (WHERE e.event_type <> 'cleared') AS peak_value,
       MAX(e.created_at) AS last_event_at
FROM alert_event e
JOIN alert a ON a.id = e.alert_id
WHERE a.industry = $1 AND e.created_at >= $2
GROUP BY e.alert_id, e.payload->>'opened_at'
"""

_ALERTS_SQL = """
SELECT id, rule_id, sensor_id, severity, fault_code, threshold
FROM alert WHERE id = ANY($1::uuid[])
"""

_DECISIONS_SQL = """
SELECT alert_id, episode_opened_at, action, recorded_by, created_at
FROM alert_response WHERE alert_id = ANY($1::uuid[]) AND created_at >= $2
ORDER BY created_at
"""


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _iso(value: datetime | None) -> str | None:
    return _utc(value).isoformat() if value else None


def _episode_key(alert_id: str, opened_at: datetime) -> tuple[str, str]:
    return str(alert_id), _utc(opened_at).isoformat()


def _status(episode: dict[str, Any]) -> str:
    if episode["cleared_at"]:
        return "cleared"
    if episode["acknowledged_at"]:
        return "acknowledged"
    return "open"


async def _from_lakebase(db: Any, industry: str, since: datetime) -> list[dict[str, Any]]:
    rows = await db.fetch(_EPISODES_SQL, industry, since)
    alert_ids = sorted({str(r["alert_id"]) for r in rows})
    if not alert_ids:
        return []
    alerts = {str(r["id"]): r for r in await db.fetch(_ALERTS_SQL, alert_ids)}
    decisions: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in await db.fetch(_DECISIONS_SQL, alert_ids, since):
        decisions.setdefault(_episode_key(r["alert_id"], r["episode_opened_at"]), []).append(
            {
                "action": r["action"],
                "station": RESPONSE_STATIONS.get(r["action"]),
                "recorded_by": r["recorded_by"],
                "created_at": _iso(r["created_at"]),
            }
        )
    episodes = []
    for r in rows:
        if not r["episode"]:
            continue
        opened_at = datetime.fromisoformat(r["episode"])
        alert = alerts.get(str(r["alert_id"]))
        episode = {
            "alert_id": str(r["alert_id"]),
            "episode_opened_at": _iso(opened_at),
            "rule_id": alert["rule_id"] if alert else None,
            "sensor_id": alert["sensor_id"] if alert else None,
            "severity": alert["severity"] if alert else None,
            "fault_code": alert["fault_code"] if alert else None,
            "threshold": alert["threshold"] if alert else None,
            "peak_value": r["peak_value"],
            "acknowledged_at": _iso(r["acknowledged_at"]),
            "acknowledged_by": r["acknowledged_by"],
            "cleared_at": _iso(r["cleared_at"]),
            "last_event_at": _iso(r["last_event_at"]),
            "decisions": decisions.get(_episode_key(r["alert_id"], opened_at), []),
        }
        episode["status"] = _status(episode)
        episodes.append(episode)
    return episodes


async def _from_memory(engine: AlertEngine, industry: str, since: datetime) -> list[dict[str, Any]]:
    episodes = []
    for alert in await engine.list_alerts(industry=industry, db=None, limit=200):
        if alert.opened_at is None or _utc(alert.updated_at or alert.opened_at) < since:
            continue
        episode = {
            "alert_id": alert.id,
            "episode_opened_at": _iso(alert.opened_at),
            "rule_id": alert.rule_id,
            "sensor_id": alert.sensor_id,
            "severity": alert.severity,
            "fault_code": alert.fault_code,
            "threshold": alert.threshold,
            "peak_value": alert.value,
            "acknowledged_at": _iso(alert.acknowledged_at),
            "acknowledged_by": alert.acknowledged_by,
            "cleared_at": _iso(alert.cleared_at),
            "last_event_at": _iso(alert.updated_at),
            "decisions": [],
        }
        episode["status"] = _status(episode)
        episodes.append(episode)
    return episodes


async def alert_episodes(
    engine: AlertEngine, industry: str, since: datetime, db: Any | None = None
) -> list[dict[str, Any]]:
    """Episodes touching the window, newest first."""
    episodes: list[dict[str, Any]] | None = None
    if db is not None:
        try:
            episodes = await _from_lakebase(db, industry, since)
        except Exception:
            logger.exception("alert history from Lakebase failed; using memory")
    if episodes is None:
        episodes = await _from_memory(engine, industry, since)
    return sorted(episodes, key=lambda e: e["episode_opened_at"], reverse=True)
