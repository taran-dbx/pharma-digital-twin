"""Excursion episodes for one pipeline, read from the app backend.

An *episode* is one open → cleared run of an alert row (the row is reused on
re-open, so episodes are keyed by ``alert_id`` + the ``opened_at`` carried in
every lifecycle payload, exactly as ``alert_history`` does). Every query is
scoped to the pipeline's alert keys and parameterised; nothing is formatted
into SQL.

Definitions (see docs/value-model.md):

* **onset** — the event time of the first over-limit reading (``onset_at`` on
  the ``opened`` event, written by the alert engine); when that is unknown,
  ``opened_at - rule.duration_s``. Never later than ``opened_at``.
* **containment window** — onset → ``cleared_at``, or the ``request_cmm_inspection``
  decision if earlier (it stops the mill, so no more parts are cut); the window end
  while still open.
* **release deadline** — the end of the lot cycle the onset part belongs to:
  the moment that lot moves to CMM and on to clean / pack.
* **caught in time** — a containment decision (``quarantine_lot`` or
  ``request_cmm_inspection``, or their legacy names) was signed no later than
  the release deadline, so the first out-of-limit part never left the cell
  unheld.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from iot_twin.domain.catalog import list_alert_rules
from iot_twin.domain.lot_context import LOT_CYCLE_S, PARTS_PER_LOT, lot_disposition, lot_id_at

logger = logging.getLogger(__name__)

CONTAINMENT_ACTIONS: tuple[str, ...] = (
    "quarantine_lot",
    "request_cmm_inspection",
    "hold_batch",
    "send_to_inspect",
)
QUARANTINE_ACTIONS: tuple[str, ...] = ("quarantine_lot", "hold_batch")
CMM_ACTIONS: tuple[str, ...] = ("request_cmm_inspection", "send_to_inspect")
NCR_ACTIONS: tuple[str, ...] = ("open_ncr",)
MAINTENANCE_ACTIONS: tuple[str, ...] = ("request_maintenance",)

INDUSTRY_PIPELINE_PREFIX = "industry:"
MAX_EPISODES = 500
MAX_WINDOW_HOURS = 365 * 24
DEFAULT_WINDOW = "30d"
_WINDOW_RE = re.compile(r"^(\d{1,4})([hd])$")
_PIPELINE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:_\-]{0,127}$")


class ScopeError(ValueError):
    """A window or pipeline id that cannot scope a query."""


def parse_window(raw: str | None) -> timedelta:
    """``"24h"`` / ``"7d"`` → timedelta; 1 hour to 365 days."""
    text = (raw or DEFAULT_WINDOW).strip().lower()
    match = _WINDOW_RE.match(text)
    if not match:
        raise ScopeError("window must look like 24h or 30d")
    amount = int(match.group(1))
    hours = amount * (24 if match.group(2) == "d" else 1)
    if hours < 1 or hours > MAX_WINDOW_HOURS:
        raise ScopeError("window must be between 1h and 365d")
    return timedelta(hours=hours)


def validate_pipeline_id(raw: str | None) -> str:
    text = (raw or "").strip()
    if not text:
        raise ScopeError("pipeline_id is required")
    if not _PIPELINE_ID_RE.match(text):
        raise ScopeError("pipeline_id has an invalid format")
    return text


_EPISODES_SQL = """
WITH scoped AS (
    SELECT id, rule_id, severity, fault_code, threshold
    FROM alert
    WHERE pipeline_id = ANY($1::text[]) AND industry = $2
),
events AS (
    SELECT e.alert_id, e.event_type, e.created_at, e.payload,
           (e.payload->>'opened_at')::timestamptz AS episode_opened_at
    FROM alert_event e
    JOIN scoped s ON s.id = e.alert_id
    WHERE e.payload->>'opened_at' IS NOT NULL
),
episodes AS (
    SELECT alert_id, episode_opened_at,
           MIN(payload->>'onset_at') FILTER (WHERE event_type = 'opened') AS onset_raw,
           MIN(payload->>'onset_source') FILTER (WHERE event_type = 'opened') AS onset_source,
           MIN(created_at) FILTER (WHERE event_type = 'acknowledged') AS acknowledged_at,
           MIN(created_at) FILTER (WHERE event_type = 'cleared') AS cleared_at,
           MAX((payload->>'value')::double precision)
               FILTER (WHERE event_type <> 'cleared') AS peak_value
    FROM events
    WHERE episode_opened_at >= $3 AND episode_opened_at < $4
    GROUP BY alert_id, episode_opened_at
),
decisions AS (
    SELECT r.alert_id, r.episode_opened_at,
           MIN(r.created_at) AS first_decision_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($5::text[])) AS contained_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($6::text[])) AS quarantined_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($7::text[])) AS cmm_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($8::text[])) AS ncr_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($9::text[])) AS maintenance_at
    FROM alert_response r
    JOIN scoped s ON s.id = r.alert_id
    GROUP BY r.alert_id, r.episode_opened_at
)
SELECT ep.alert_id, ep.episode_opened_at, ep.onset_raw, ep.onset_source,
       ep.acknowledged_at, ep.cleared_at, ep.peak_value,
       s.rule_id, s.severity, s.fault_code, s.threshold,
       d.first_decision_at, d.contained_at, d.quarantined_at, d.cmm_at, d.ncr_at,
       d.maintenance_at
FROM episodes ep
JOIN scoped s ON s.id = ep.alert_id
LEFT JOIN decisions d
    ON d.alert_id = ep.alert_id AND d.episode_opened_at = ep.episode_opened_at
ORDER BY ep.episode_opened_at DESC
LIMIT $10
"""

_STOPS_SQL = """
SELECT COUNT(*) AS stops
FROM production_event p
JOIN alert a ON a.id = p.alert_id
WHERE a.pipeline_id = ANY($1::text[]) AND a.industry = $2
  AND p.state = 'stopped' AND p.created_at >= $3 AND p.created_at < $4
"""


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _opt_utc(value: Any) -> datetime | None:
    return _utc(value) if isinstance(value, datetime) else None


def _parse_iso(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return _utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    except ValueError:
        return None


def _seconds(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return max(0.0, (later - earlier).total_seconds())


def rule_duration_s(industry: str, rule_id: str | None) -> float:
    """The rule's debounce from the catalog; 0 when the rule is unknown."""
    if not rule_id:
        return 0.0
    try:
        rules = list_alert_rules(industry)
    except KeyError:
        return 0.0
    for rule in rules:
        if rule.rule_id == rule_id:
            return float(getattr(rule, "duration_s", 0.0) or 0.0)
    return 0.0


def release_deadline(onset: datetime) -> datetime:
    """End of the lot cycle containing ``onset``: when that lot moves on."""
    cycle_end = (math.floor(onset.timestamp() / LOT_CYCLE_S) + 1) * LOT_CYCLE_S
    return datetime.fromtimestamp(cycle_end, tz=UTC)


@dataclass(frozen=True)
class Episode:
    alert_id: str
    opened_at: datetime
    onset_at: datetime
    onset_source: str
    rule_id: str | None = None
    severity: str | None = None
    fault_code: str | None = None
    threshold: float | None = None
    peak_value: float | None = None
    acknowledged_at: datetime | None = None
    cleared_at: datetime | None = None
    first_decision_at: datetime | None = None
    contained_at: datetime | None = None
    quarantined_at: datetime | None = None
    cmm_at: datetime | None = None
    ncr_at: datetime | None = None
    maintenance_at: datetime | None = None
    window_end: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def deadline(self) -> datetime:
        return release_deadline(self.onset_at)

    @property
    def caught_in_time(self) -> bool:
        return self.contained_at is not None and self.contained_at <= self.deadline

    @property
    def lot_id(self) -> str:
        return lot_id_at(self.onset_at)

    @property
    def containment_end(self) -> datetime:
        """Clear, or the CMM request that stops the mill if earlier; else the window end."""
        ends = [t for t in (self.cleared_at, self.cmm_at) if t is not None]
        end = min(ends) if ends else self.window_end
        return max(end, self.opened_at)

    @property
    def disposition(self) -> dict[str, Any]:
        return lot_disposition(self.onset_at, self.containment_end)

    @property
    def time_to_detect_s(self) -> float:
        return _seconds(self.opened_at, self.onset_at) or 0.0

    @property
    def time_to_acknowledge_s(self) -> float | None:
        stamps = [t for t in (self.acknowledged_at, self.first_decision_at) if t is not None]
        return _seconds(min(stamps), self.opened_at) if stamps else None

    @property
    def time_to_disposition_s(self) -> float | None:
        return _seconds(self.contained_at, self.opened_at)

    @property
    def time_to_restore_s(self) -> float | None:
        return _seconds(self.cleared_at, self.opened_at)


def episode_from_row(row: Mapping[str, Any], industry: str, window_end: datetime) -> Episode:
    """Build an episode, resolving onset as documented in the module docstring."""
    opened_at = _utc(row["episode_opened_at"])
    onset = _parse_iso(row.get("onset_raw"))
    source = str(row.get("onset_source") or "first_reading")
    if onset is None:
        onset = opened_at - timedelta(seconds=rule_duration_s(industry, row.get("rule_id")))
        source = "rule_duration"
    threshold = row.get("threshold")
    peak = row.get("peak_value")
    return Episode(
        alert_id=str(row["alert_id"]),
        opened_at=opened_at,
        onset_at=min(onset, opened_at),
        onset_source=source,
        rule_id=row.get("rule_id"),
        severity=row.get("severity"),
        fault_code=row.get("fault_code"),
        threshold=float(threshold) if threshold is not None else None,
        peak_value=float(peak) if peak is not None else None,
        acknowledged_at=_opt_utc(row.get("acknowledged_at")),
        cleared_at=_opt_utc(row.get("cleared_at")),
        first_decision_at=_opt_utc(row.get("first_decision_at")),
        contained_at=_opt_utc(row.get("contained_at")),
        quarantined_at=_opt_utc(row.get("quarantined_at")),
        cmm_at=_opt_utc(row.get("cmm_at")),
        ncr_at=_opt_utc(row.get("ncr_at")),
        maintenance_at=_opt_utc(row.get("maintenance_at")),
        window_end=window_end,
    )


@dataclass(frozen=True)
class EpisodeWindow:
    """Episodes of one pipeline over ``[start, end)`` plus the cell stops they caused."""

    pipeline_keys: tuple[str, ...]
    industry: str
    start: datetime
    end: datetime
    episodes: tuple[Episode, ...]
    production_stops: int
    available: bool

    def meta(self) -> dict[str, Any]:
        return {
            "industry": self.industry,
            "pipeline_keys": list(self.pipeline_keys),
            "window_start": self.start.isoformat(),
            "window_end": self.end.isoformat(),
            "data_available": self.available,
            "parts_per_lot": PARTS_PER_LOT,
        }


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row.items()) if hasattr(row, "items") else dict(row)


async def fetch_episode_window(
    db: Any | None,
    pipeline_keys: Sequence[str],
    industry: str,
    start: datetime,
    end: datetime,
) -> EpisodeWindow:
    """Episodes opened in ``[start, end)`` for the pipeline's alert keys.

    Without an app backend (local dev, tests) the window is empty and marked
    ``available=False`` so the UI can show its empty state rather than zeros.
    """
    keys = tuple(dict.fromkeys(pipeline_keys))
    if not keys:
        raise ScopeError("pipeline_id is required")
    if db is None:
        return EpisodeWindow(keys, industry, start, end, (), 0, False)
    rows = await db.fetch(
        _EPISODES_SQL,
        list(keys),
        industry,
        start,
        end,
        list(CONTAINMENT_ACTIONS),
        list(QUARANTINE_ACTIONS),
        list(CMM_ACTIONS),
        list(NCR_ACTIONS),
        list(MAINTENANCE_ACTIONS),
        MAX_EPISODES,
    )
    stops_row = await db.fetchrow(_STOPS_SQL, list(keys), industry, start, end)
    episodes = tuple(episode_from_row(_row_dict(r), industry, end) for r in rows)
    stops = int(_row_dict(stops_row).get("stops") or 0) if stops_row is not None else 0
    return EpisodeWindow(keys, industry, start, end, episodes, stops, True)


def pipeline_keys_for(pipeline_id: str, industry: str, realtime_bound: bool) -> tuple[str, ...]:
    """Alert ``pipeline_id`` values that belong to this dashboard's pipeline.

    Industry-first realtime consumers stamp alerts with ``industry:<industry>``
    rather than the deployment id, so a bound realtime deployment also reads
    that alias. Every query still filters on these keys.
    """
    alias = f"{INDUSTRY_PIPELINE_PREFIX}{industry}"
    if pipeline_id == alias or not realtime_bound:
        return (pipeline_id,)
    return (pipeline_id, alias)
