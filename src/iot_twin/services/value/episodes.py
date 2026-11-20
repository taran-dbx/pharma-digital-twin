"""Excursion episodes for one pipeline, read from the app backend.

An *episode* is one open → cleared run of an alert row (the row is reused on
re-open, so episodes are keyed by ``alert_id`` + the ``opened_at`` carried in
every lifecycle payload, exactly as ``alert_history`` does). Every query is
scoped to the pipeline and parameterised; nothing is formatted into SQL.

Totals (counts, caught in time, lots, parts, time statistics) are computed in
SQL over the **whole** window. Only the recent-events table and the
distribution samples are limited, and the response says so.

Definitions (see docs/value-model.md):

* **onset / containment window**: the canonical ``alert_responses`` window
  (``excursion_start`` → ``cleared_at`` or now), the one the signed quarantine
  and NCR records use. Recent events call it directly; the SQL totals mirror
  it, and a parity test keeps the two equal.
* **release deadline**: ``max(end of the onset part's lot cycle,
  opened_at + caught_in_time_grace_s)``.
* **caught in time**: a containment decision (``quarantine_lot`` or
  ``request_cmm_inspection``, or their legacy names) signed no later than the
  release deadline.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import Any

from iot_twin.domain.catalog import list_alert_rules
from iot_twin.domain.lot_context import (
    LOT_CYCLE_S,
    PART_CYCLE_S,
    PARTS_PER_LOT,
    lot_disposition,
    lot_id_at,
)
from iot_twin.services.alerts.alert_engine import (
    DETECT_LOOKBACK_S,
    EXCURSION_STARTED_KEY,
    AlertRecord,
)
from iot_twin.services.alerts.alert_responses import _excursion as canonical_excursion
from iot_twin.services.value.config import Definitions, definitions

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
RECENT_EVENTS = 20
MAX_SAMPLES = 200
MAX_WINDOW_HOURS = 365 * 24
DEFAULT_WINDOW = "30d"
_WINDOW_RE = re.compile(r"^(\d{1,4})([hd])$")
_PIPELINE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:_\-]{0,127}$")


class ScopeError(ValueError):
    """A window or pipeline id that cannot scope a query."""

    def __init__(self, field_name: str, message: str) -> None:
        super().__init__(message)
        self.field = field_name


def parse_window(raw: str | None) -> timedelta:
    """``"24h"`` / ``"7d"`` → timedelta; 1 hour to 365 days."""
    text = (raw or DEFAULT_WINDOW).strip().lower()
    match = _WINDOW_RE.match(text)
    if not match:
        raise ScopeError("window", "window must look like 24h or 30d")
    amount = int(match.group(1))
    hours = amount * (24 if match.group(2) == "d" else 1)
    if hours < 1 or hours > MAX_WINDOW_HOURS:
        raise ScopeError("window", "window must be between 1h and 365d")
    return timedelta(hours=hours)


def validate_pipeline_id(raw: str | None) -> str:
    text = (raw or "").strip()
    if not text:
        raise ScopeError("pipeline_id", "pipeline_id is required")
    if not _PIPELINE_ID_RE.match(text):
        raise ScopeError("pipeline_id", "pipeline_id has an invalid format")
    return text


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _opt_utc(value: Any) -> datetime | None:
    return _utc(value) if isinstance(value, datetime) else None


@dataclass(frozen=True)
class PipelineScope:
    """The alert rows one dashboard reads.

    Industry-first realtime consumers stamp alerts with ``industry:<industry>``
    rather than the deployment id, so a bound realtime deployment also reads
    that alias, but only for episodes opened since the deployment was created.
    """

    pipeline_id: str
    industry: str
    alias: str | None = None
    alias_since: datetime | None = None

    def keys(self) -> list[str]:
        return [self.pipeline_id, *([self.alias] if self.alias else [])]


def pipeline_scope(
    pipeline_id: str, industry: str, realtime_since: datetime | None
) -> PipelineScope:
    alias = f"{INDUSTRY_PIPELINE_PREFIX}{industry}"
    if pipeline_id == alias or realtime_since is None:
        return PipelineScope(pipeline_id, industry)
    return PipelineScope(pipeline_id, industry, alias, _utc(realtime_since))


# Per-episode rows with onset, window end, deadline and caught-in-time; mirrors
# alert_responses.excursion_start / _excursion. Parameters: $1 pipeline_id,
# $2 alias, $3 alias_since, $4 industry, $5 window start, $6 window end,
# $7 rule ids, $8 rule durations (s), $9 onset lookback (s), $10 grace (s),
# $11 open-excursion cap (s), $12 lot cycle (s), $13-$17 containment /
# quarantine / CMM / NCR / maintenance actions.
_RESOLVED_CTE = """
WITH scoped AS (
    SELECT id, rule_id, severity, fault_code, threshold, pipeline_id,
           opened_at AS row_opened_at,
           metadata->>'excursion_started_at' AS row_onset
    FROM alert
    WHERE industry = $4
      AND (pipeline_id = $1 OR ($2::text IS NOT NULL AND pipeline_id = $2))
),
durations AS (
    SELECT * FROM unnest($7::text[], $8::float8[]) AS d(rule_id, duration_s)
),
events AS (
    SELECT e.alert_id, e.event_type, e.created_at, e.payload,
           (e.payload->>'opened_at')::timestamptz AS opened
    FROM alert_event e
    JOIN scoped s ON s.id = e.alert_id
    WHERE e.payload->>'opened_at' IS NOT NULL
),
grouped AS (
    SELECT alert_id, opened,
           MIN(payload->>'excursion_started_at') FILTER (WHERE event_type = 'opened')
               AS event_onset,
           MIN(created_at) FILTER (WHERE event_type = 'acknowledged') AS acknowledged_at,
           MIN(created_at) FILTER (WHERE event_type = 'cleared') AS cleared_at,
           MAX((payload->>'value')::double precision)
               FILTER (WHERE event_type <> 'cleared') AS peak_value
    FROM events
    WHERE opened >= $5 AND opened < $6
    GROUP BY alert_id, opened
),
decisions AS (
    SELECT r.alert_id, r.episode_opened_at,
           MIN(r.created_at) AS first_decision_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($13::text[])) AS contained_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($14::text[])) AS quarantined_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($15::text[])) AS cmm_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($16::text[])) AS ncr_at,
           MIN(r.created_at) FILTER (WHERE r.action = ANY($17::text[])) AS maintenance_at
    FROM alert_response r
    JOIN scoped s ON s.id = r.alert_id
    GROUP BY r.alert_id, r.episode_opened_at
),
base AS (
    SELECT g.alert_id, g.opened, g.acknowledged_at, g.cleared_at, g.peak_value,
           s.rule_id, s.severity, s.fault_code, s.threshold,
           COALESCE(du.duration_s, 0) AS duration_s,
           -- The opened event's copy. Events written before the copy existed
           -- fall back to the row, whose metadata is the current episode's
           -- (the engine replaces it on reopen), so only that episode uses it.
           COALESCE(g.event_onset,
                    CASE WHEN s.row_opened_at = g.opened THEN s.row_onset END) AS onset_raw,
           d.first_decision_at, d.contained_at, d.quarantined_at, d.cmm_at, d.ncr_at,
           d.maintenance_at
    FROM grouped g
    JOIN scoped s ON s.id = g.alert_id
    LEFT JOIN durations du ON du.rule_id = s.rule_id
    LEFT JOIN decisions d ON d.alert_id = g.alert_id AND d.episode_opened_at = g.opened
    WHERE s.pipeline_id = $1 OR g.opened >= $3
),
stamped AS (
    SELECT b.*,
           CASE WHEN b.onset_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}'
                THEN b.onset_raw::timestamptz END AS onset_ts
    FROM base b
),
onsets AS (
    SELECT st.*,
           COALESCE(st.onset_ts >= st.opened - make_interval(secs => st.duration_s + $9)
                    AND st.onset_ts <= st.opened, FALSE) AS onset_trusted
    FROM stamped st
),
resolved AS (
    SELECT o.*,
           CASE WHEN o.onset_trusted THEN o.onset_ts
                ELSE o.opened - make_interval(secs => o.duration_s) END AS onset,
           CASE WHEN o.onset_trusted THEN 'first_reading' ELSE 'rule_duration' END
               AS onset_source,
           COALESCE(o.cleared_at, LEAST($6, o.opened + make_interval(secs => $11)))
               AS raw_end
    FROM onsets o
),
windowed AS (
    SELECT r.*,
           GREATEST(r.onset, r.raw_end) AS window_end,
           to_timestamp((floor(extract(epoch FROM r.onset) / $12) + 1) * $12) AS lot_end,
           EXTRACT(epoch FROM r.opened - r.onset)::float8 AS ttd_s,
           EXTRACT(epoch FROM LEAST(r.acknowledged_at, r.first_decision_at) - r.opened)::float8
               AS tta_s,
           EXTRACT(epoch FROM r.contained_at - r.opened)::float8 AS ttdisp_s,
           EXTRACT(epoch FROM r.cleared_at - r.opened)::float8 AS restore_s
    FROM resolved r
),
episodes AS (
    SELECT w.*,
           GREATEST(w.lot_end, w.opened + make_interval(secs => $10)) AS deadline,
           (w.lot_end < w.opened) AS deadline_before_alert,
           COALESCE(w.contained_at
                    <= GREATEST(w.lot_end, w.opened + make_interval(secs => $10)), FALSE)
               AS caught
    FROM windowed w
)
"""


def _stats(prefix: str, column: str) -> str:
    value = f"GREATEST({column}, 0)"
    return f"""
       COUNT({column}) AS {prefix}_count,
       AVG({value}) AS {prefix}_mean,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY {value}) AS {prefix}_p50,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY {value}) AS {prefix}_p90,
       MAX({value}) AS {prefix}_max"""


_TOTALS_SQL = (
    _RESOLVED_CTE
    + """
SELECT COUNT(*) AS episodes,
       COUNT(*) FILTER (WHERE cleared_at IS NULL) AS open_episodes,
       COUNT(*) FILTER (WHERE caught) AS caught_in_time,
       COUNT(*) FILTER (WHERE deadline_before_alert) AS deadline_before_alert,
       COUNT(*) FILTER (WHERE onset_source = 'first_reading') AS onset_from_reading,
       COUNT(ncr_at) AS ncrs_opened,
       COUNT(cmm_at) AS cmm_inspections,
       COUNT(quarantined_at) AS quarantine_events,
       COUNT(maintenance_at) AS maintenance_requests,"""
    + ",".join(
        _stats(prefix, column)
        for prefix, column in (
            ("ttd", "ttd_s"),
            ("tta", "tta_s"),
            ("ttdisp", "ttdisp_s"),
            ("restore", "restore_s"),
        )
    )
    + """,
       (SELECT COUNT(*)
          FROM production_event p
          JOIN episodes ep ON ep.alert_id = p.alert_id
           AND p.created_at >= ep.opened AND p.created_at <= ep.window_end
         WHERE p.state = 'stopped') AS production_stops
FROM episodes
"""
)

# Lots and parts are deduped across episodes by part slot, so a lot touched by
# two excursions counts once. Part slot = floor(epoch / PART_CYCLE_S), the same
# arithmetic as lot_context.parts_in_window. $18 part cycle (s), $19 parts/lot.
_PARTS_SQL = (
    _RESOLVED_CTE
    + """
, parts AS (
    SELECT DISTINCT ep.caught, (ep.quarantined_at IS NOT NULL) AS held, gs.slot,
           floor(gs.slot * $18 / $12)::bigint AS lot
    FROM episodes ep,
         generate_series(floor(extract(epoch FROM ep.onset) / $18)::bigint,
                         floor(extract(epoch FROM ep.window_end) / $18)::bigint) AS gs(slot)
),
lots AS (
    SELECT lot,
           LEAST($19::int, GREATEST(0, floor((extract(epoch FROM $6::timestamptz)
                                              - lot * $12) / $18)::int + 1)) AS started
    FROM (SELECT DISTINCT lot FROM parts) l
)
SELECT (SELECT COUNT(DISTINCT slot) FROM parts) AS parts_at_risk,
       (SELECT COUNT(*) FROM lots) AS lots_touched,
       (SELECT COALESCE(SUM(started), 0) FROM lots) AS parts_started,
       (SELECT COUNT(DISTINCT lot) FROM parts WHERE caught) AS lots_protected,
       (SELECT COUNT(DISTINCT lot) FROM parts WHERE held) AS lots_quarantined,
       (SELECT COUNT(DISTINCT slot) FROM parts WHERE held) AS parts_quarantined
"""
)

_RECENT_SQL = (
    _RESOLVED_CTE
    + """
SELECT alert_id, opened, onset_raw, onset_source, acknowledged_at, cleared_at, peak_value,
       rule_id, severity, fault_code, threshold, first_decision_at, contained_at,
       quarantined_at, cmm_at, ncr_at, maintenance_at, onset, window_end, deadline, caught
FROM episodes
ORDER BY opened DESC
LIMIT $18
"""
)

_SAMPLES_SQL = (
    _RESOLVED_CTE
    + """
SELECT ttd_s, tta_s FROM episodes ORDER BY opened DESC LIMIT $18
"""
)


def _seconds(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return max(0.0, (later - earlier).total_seconds())


def rule_durations(industry: str) -> tuple[list[str], list[float]]:
    """Catalog debounce per rule id, the source ``alert_responses`` uses."""
    try:
        rules = list_alert_rules(industry)
    except KeyError:
        return [], []
    return [r.rule_id for r in rules], [float(r.duration_s or 0.0) for r in rules]


def lot_end(onset: datetime) -> datetime:
    """End of the lot cycle containing ``onset``: when that lot moves on."""
    cycle_end = (math.floor(onset.timestamp() / LOT_CYCLE_S) + 1) * LOT_CYCLE_S
    return datetime.fromtimestamp(cycle_end, tz=UTC)


def release_deadline(onset: datetime, opened_at: datetime, grace_s: float) -> datetime:
    """``max(lot_end(onset), opened_at + grace)``: the containment deadline."""
    return max(lot_end(onset), opened_at + timedelta(seconds=grace_s))


@dataclass(frozen=True)
class Episode:
    """One excursion, windowed by the canonical ``alert_responses`` function."""

    alert_id: str
    industry: str
    opened_at: datetime
    onset_raw: str | None = None
    rule_id: str = ""
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
    settings: Definitions = field(default_factory=definitions)

    def as_alert(self) -> AlertRecord:
        """The episode as the alert record the canonical window function takes."""
        metadata = {EXCURSION_STARTED_KEY: self.onset_raw} if self.onset_raw else {}
        return AlertRecord(
            id=self.alert_id,
            fingerprint="",
            industry=self.industry,
            pipeline_id=None,
            rule_id=self.rule_id,
            sensor_id="",
            sensor_ids=[],
            severity=self.severity or "info",
            status="cleared" if self.cleared_at else "open",
            fault_code=self.fault_code,
            value=self.peak_value,
            threshold=self.threshold,
            opened_at=self.opened_at,
            updated_at=self.opened_at,
            cleared_at=self.cleared_at,
            metadata=metadata,
        )

    @property
    def now(self) -> datetime:
        """Window end, capped for excursions left open (``max_open_excursion_s``)."""
        cap = self.opened_at + timedelta(seconds=self.settings.max_open_excursion_s)
        return min(self.window_end, cap)

    @cached_property
    def window(self) -> tuple[datetime, datetime]:
        return canonical_excursion(self.as_alert(), self.now)

    @property
    def onset_at(self) -> datetime:
        return self.window[0]

    @property
    def onset_source(self) -> str:
        """``first_reading`` when the canonical window used the stamp itself."""
        if not self.onset_raw:
            return "rule_duration"
        try:
            stamped = _utc(datetime.fromisoformat(self.onset_raw))
        except ValueError:
            return "rule_duration"
        return "first_reading" if stamped == self.onset_at else "rule_duration"

    @cached_property
    def disposition(self) -> dict[str, Any]:
        start, end = self.window
        return lot_disposition(start, end, self.now)

    @property
    def deadline(self) -> datetime:
        return release_deadline(self.onset_at, self.opened_at, self.settings.caught_in_time_grace_s)

    @property
    def deadline_before_alert(self) -> bool:
        return lot_end(self.onset_at) < self.opened_at

    @property
    def caught_in_time(self) -> bool:
        return self.contained_at is not None and self.contained_at <= self.deadline

    @property
    def lot_id(self) -> str:
        return lot_id_at(self.onset_at)

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
    threshold = row.get("threshold")
    peak = row.get("peak_value")
    raw = row.get("onset_raw")
    return Episode(
        alert_id=str(row["alert_id"]),
        industry=industry,
        opened_at=_utc(row["opened"]),
        onset_raw=str(raw) if raw else None,
        rule_id=str(row.get("rule_id") or ""),
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


def _empty_stats() -> dict[str, Any]:
    return {"count": 0, "mean": None, "p50": None, "p90": None, "max": None}


def _stat_block(row: Mapping[str, Any], prefix: str) -> dict[str, Any]:
    def num(key: str) -> float | None:
        value = row.get(f"{prefix}_{key}")
        return round(float(value), 3) if value is not None else None

    return {
        "count": int(row.get(f"{prefix}_count") or 0),
        "mean": num("mean"),
        "p50": num("p50"),
        "p90": num("p90"),
        "max": num("max"),
    }


@dataclass(frozen=True)
class WindowTotals:
    """Full-window totals, computed in SQL (never sampled)."""

    episodes: int = 0
    open_episodes: int = 0
    caught_in_time: int = 0
    deadline_before_alert: int = 0
    onset_from_reading: int = 0
    ncrs_opened: int = 0
    cmm_inspections: int = 0
    quarantine_events: int = 0
    maintenance_requests: int = 0
    production_stops: int = 0
    lots_touched: int = 0
    lots_protected: int = 0
    lots_quarantined: int = 0
    parts_at_risk: int = 0
    parts_started: int = 0
    parts_quarantined: int = 0
    ttd: dict[str, Any] = field(default_factory=_empty_stats)
    tta: dict[str, Any] = field(default_factory=_empty_stats)
    ttdisp: dict[str, Any] = field(default_factory=_empty_stats)
    restore: dict[str, Any] = field(default_factory=_empty_stats)

    @property
    def parts_released(self) -> int:
        """Parts of the quarantined lots that were not at risk."""
        return max(0, self.lots_quarantined * PARTS_PER_LOT - self.parts_quarantined)


def totals_from_rows(totals: Mapping[str, Any], parts: Mapping[str, Any]) -> WindowTotals:
    def n(row: Mapping[str, Any], key: str) -> int:
        return int(row.get(key) or 0)

    at_risk = n(parts, "parts_at_risk")
    return WindowTotals(
        episodes=n(totals, "episodes"),
        open_episodes=n(totals, "open_episodes"),
        caught_in_time=n(totals, "caught_in_time"),
        deadline_before_alert=n(totals, "deadline_before_alert"),
        onset_from_reading=n(totals, "onset_from_reading"),
        ncrs_opened=n(totals, "ncrs_opened"),
        cmm_inspections=n(totals, "cmm_inspections"),
        quarantine_events=n(totals, "quarantine_events"),
        maintenance_requests=n(totals, "maintenance_requests"),
        production_stops=n(totals, "production_stops"),
        lots_touched=n(parts, "lots_touched"),
        lots_protected=n(parts, "lots_protected"),
        lots_quarantined=n(parts, "lots_quarantined"),
        parts_at_risk=at_risk,
        # as lot_context.projected_rft: never fewer parts started than at risk
        parts_started=max(n(parts, "parts_started"), at_risk),
        parts_quarantined=n(parts, "parts_quarantined"),
        ttd=_stat_block(totals, "ttd"),
        tta=_stat_block(totals, "tta"),
        ttdisp=_stat_block(totals, "ttdisp"),
        restore=_stat_block(totals, "restore"),
    )


@dataclass(frozen=True)
class EpisodeWindow:
    scope: PipelineScope
    start: datetime
    end: datetime
    totals: WindowTotals
    recent: tuple[Episode, ...]
    ttd_samples: tuple[float, ...]
    tta_samples: tuple[float, ...]
    available: bool

    @property
    def truncated(self) -> bool:
        """True when the recent table or the distributions show only part of the window."""
        return self.totals.episodes > min(RECENT_EVENTS, MAX_SAMPLES)

    def meta(self) -> dict[str, Any]:
        return {
            "industry": self.scope.industry,
            "pipeline_keys": self.scope.keys(),
            "alias_since": self.scope.alias_since.isoformat() if self.scope.alias_since else None,
            "window_start": self.start.isoformat(),
            "window_end": self.end.isoformat(),
            "data_available": self.available,
            "parts_per_lot": PARTS_PER_LOT,
            "episodes_total": self.totals.episodes,
            "truncated": self.truncated,
            "recent_shown": len(self.recent),
            "samples_shown": len(self.ttd_samples),
        }


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row.items()) if hasattr(row, "items") else dict(row)


def query_params(
    scope: PipelineScope, start: datetime, end: datetime, settings: Definitions
) -> list[Any]:
    """$1-$17 of ``_RESOLVED_CTE``."""
    ids, durations = rule_durations(scope.industry)
    return [
        scope.pipeline_id,
        scope.alias,
        scope.alias_since,
        scope.industry,
        start,
        end,
        ids,
        durations,
        DETECT_LOOKBACK_S,
        settings.caught_in_time_grace_s,
        settings.max_open_excursion_s,
        float(LOT_CYCLE_S),
        list(CONTAINMENT_ACTIONS),
        list(QUARANTINE_ACTIONS),
        list(CMM_ACTIONS),
        list(NCR_ACTIONS),
        list(MAINTENANCE_ACTIONS),
    ]


async def fetch_episode_window(
    db: Any | None,
    scope: PipelineScope,
    start: datetime,
    end: datetime,
    settings: Definitions | None = None,
) -> EpisodeWindow:
    """Full-window totals plus the most recent episodes and the latest samples.

    Without an app backend (local dev, tests) the window is empty and marked
    ``available=False`` so the UI can show its empty state rather than zeros.
    """
    if not scope.pipeline_id:
        raise ScopeError("pipeline_id", "pipeline_id is required")
    if db is None:
        return EpisodeWindow(scope, start, end, WindowTotals(), (), (), (), False)
    conf = settings or definitions()
    params = query_params(scope, start, end, conf)
    totals_row = await db.fetchrow(_TOTALS_SQL, *params)
    parts_row = await db.fetchrow(_PARTS_SQL, *params, float(PART_CYCLE_S), PARTS_PER_LOT)
    recent_rows = await db.fetch(_RECENT_SQL, *params, RECENT_EVENTS)
    sample_rows = [_row_dict(r) for r in await db.fetch(_SAMPLES_SQL, *params, MAX_SAMPLES)]
    totals = totals_from_rows(
        _row_dict(totals_row) if totals_row is not None else {},
        _row_dict(parts_row) if parts_row is not None else {},
    )
    recent = tuple(
        replace_settings(episode_from_row(_row_dict(r), scope.industry, end), conf)
        for r in recent_rows
    )
    return EpisodeWindow(
        scope,
        start,
        end,
        totals,
        recent,
        tuple(max(0.0, float(r["ttd_s"])) for r in sample_rows if r.get("ttd_s") is not None),
        tuple(max(0.0, float(r["tta_s"])) for r in sample_rows if r.get("tta_s") is not None),
        True,
    )


def replace_settings(episode: Episode, settings: Definitions) -> Episode:
    return episode if episode.settings == settings else replace(episode, settings=settings)
