"""Episode definitions (onset, deadline, caught-in-time), ops metrics and the actuals SQL [qa].

The SQL tests run the real queries against a throwaway local Postgres seeded
with fixture rows (skipped when no Postgres binaries are installed).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from iot_twin.domain.lot_context import (
    LOT_CYCLE_S,
    PARTS_PER_LOT,
    PROJECTED_RFT_LABEL,
)
from iot_twin.domain.lot_context import projected_rft as lot_projected_rft
from iot_twin.models import app_schema
from iot_twin.services.value.episodes import (
    Episode,
    ScopeError,
    episode_from_row,
    fetch_episode_window,
    parse_window,
    pipeline_keys_for,
    release_deadline,
    rule_duration_s,
    validate_pipeline_id,
)
from iot_twin.services.value.ops_metrics import ops_summary, percentile, projected_rft
from iot_twin.services.value.value_model import actuals, resolve_assumptions
from tests._local_pg import local_postgres

LOT_START = datetime.fromtimestamp(LOT_CYCLE_S * 6_000_000, tz=UTC)
SPINDLE_RULE = "mfg-alert-spindle-temp"


def _row(**kw):
    base = {
        "alert_id": "a1",
        "episode_opened_at": LOT_START + timedelta(seconds=30),
        "onset_raw": None,
        "rule_id": SPINDLE_RULE,
    }
    return {**base, **kw}


class TestScope:
    @pytest.mark.parametrize(
        ("raw", "hours"), [("24h", 24), ("7d", 168), (None, 720), ("365d", 8760)]
    )
    def test_window_parses_c7920_smoke(self, raw, hours) -> None:
        """[C7920][smoke] Windows are <n>h or <n>d, default 30d."""
        assert parse_window(raw) == timedelta(hours=hours)

    @pytest.mark.parametrize("raw", ["0h", "366d", "7w", "-1d", "1.5d", "abc"])
    def test_window_rejects_c7921_regression(self, raw) -> None:
        """[C7921][regression] Zero, too long or malformed windows are rejected."""
        with pytest.raises(ScopeError):
            parse_window(raw)

    @pytest.mark.parametrize("raw", ["", None, "a b", "x;drop", "x" * 200])
    def test_pipeline_id_rejects_c7922_regression(self, raw) -> None:
        """[C7922][regression] pipeline_id is required and restricted to a safe id charset."""
        with pytest.raises(ScopeError):
            validate_pipeline_id(raw)

    def test_realtime_alias_c7923_smoke(self) -> None:
        """[C7923][smoke] A bound realtime deployment also reads the industry alert alias."""
        pid = str(uuid.uuid4())
        assert pipeline_keys_for(pid, "manufacturing", True) == (pid, "industry:manufacturing")
        assert pipeline_keys_for(pid, "manufacturing", False) == (pid,)
        alias = "industry:manufacturing"
        assert pipeline_keys_for(alias, "manufacturing", True) == (alias,)


class TestDefinitions:
    def test_onset_from_first_reading_c7924_smoke(self) -> None:
        """[C7924][smoke] Onset is the first over-limit reading when the opened event has it."""
        opened = LOT_START + timedelta(seconds=30)
        onset = opened - timedelta(seconds=4)
        ep = episode_from_row(_row(onset_raw=onset.isoformat()), "manufacturing", opened)
        assert ep.onset_at == onset
        assert ep.onset_source == "first_reading"
        assert ep.time_to_detect_s == 4

    def test_onset_fallback_rule_duration_c7925_smoke(self) -> None:
        """[C7925][smoke] Unknown onset falls back to opened_at − rule.duration_s."""
        duration = rule_duration_s("manufacturing", SPINDLE_RULE)
        assert duration > 0
        ep = episode_from_row(_row(), "manufacturing", LOT_START + timedelta(minutes=5))
        assert ep.onset_source == "rule_duration"
        assert ep.time_to_detect_s == duration

    def test_onset_never_after_opened_c7926_regression(self) -> None:
        """[C7926][regression] A skewed reading clock cannot put onset after the alert opened."""
        opened = LOT_START + timedelta(seconds=30)
        ep = episode_from_row(
            _row(onset_raw=(opened + timedelta(seconds=9)).isoformat()), "manufacturing", opened
        )
        assert ep.onset_at == opened
        assert ep.time_to_detect_s == 0

    def test_unknown_rule_duration_is_zero_c7927_regression(self) -> None:
        """[C7927][regression] An unknown rule or industry contributes no debounce."""
        assert rule_duration_s("manufacturing", "nope") == 0
        assert rule_duration_s("no-such-industry", SPINDLE_RULE) == 0

    def test_release_deadline_is_lot_end_c7928_smoke(self) -> None:
        """[C7928][smoke] The release deadline is the end of the onset part's lot cycle."""
        assert release_deadline(LOT_START + timedelta(seconds=1)) == LOT_START + timedelta(
            seconds=LOT_CYCLE_S
        )
        assert release_deadline(LOT_START) == LOT_START + timedelta(seconds=LOT_CYCLE_S)

    def test_caught_in_time_boundaries_c7929_regression(self) -> None:
        """[C7929][regression] Contained at the deadline counts; one second late does not."""
        onset = LOT_START + timedelta(seconds=100)
        deadline = LOT_START + timedelta(seconds=LOT_CYCLE_S)

        def ep(contained):
            return Episode("a", onset, onset, "first_reading", contained_at=contained)

        assert ep(deadline).caught_in_time
        assert not ep(deadline + timedelta(seconds=1)).caught_in_time
        assert not ep(None).caught_in_time

    def test_tta_uses_first_of_ack_or_decision_c7930_regression(self) -> None:
        """[C7930][regression] A signed decision before the ack button counts as acknowledgement."""
        opened = LOT_START + timedelta(seconds=10)
        ep = Episode(
            "a",
            opened,
            opened,
            "first_reading",
            acknowledged_at=opened + timedelta(seconds=50),
            first_decision_at=opened + timedelta(seconds=20),
        )
        assert ep.time_to_acknowledge_s == 20


class TestOpsMetrics:
    def test_percentile_c7931_smoke(self) -> None:
        """[C7931][smoke] Linear-interpolated percentiles."""
        assert percentile([], 50) is None
        assert percentile([5], 90) == 5
        assert percentile([0, 10], 50) == 5
        assert percentile([1, 2, 3, 4], 90) == pytest.approx(3.7)

    def test_projected_rft_c7932_smoke(self) -> None:
        """[C7932][smoke] Window RFT sums the canonical per-excursion projected RFT."""
        onset = LOT_START + timedelta(seconds=1)
        end = onset + timedelta(seconds=LOT_CYCLE_S)
        ep = Episode(
            "a",
            onset,
            onset,
            "first_reading",
            cleared_at=onset + timedelta(seconds=30),
            window_end=end,
        )
        canonical = lot_projected_rft(onset, onset + timedelta(seconds=30), end)
        rft = projected_rft([ep])
        assert rft["label"] == PROJECTED_RFT_LABEL
        assert rft["parts_started"] == canonical["parts_started"] == PARTS_PER_LOT
        assert rft["parts_at_risk"] == canonical["parts_at_risk"]
        assert rft["pct"] == canonical["pct"]
        doubled = projected_rft([ep, ep])
        assert doubled["pct"] == rft["pct"]
        assert projected_rft([])["pct"] is None

    def test_ops_summary_counts_c7933_smoke(self) -> None:
        """[C7933][smoke] NCRs, CMM inspections, MTTR and TTD samples come from the episodes."""
        opened = LOT_START + timedelta(seconds=20)
        a = Episode(
            "a",
            opened,
            opened - timedelta(seconds=3),
            "first_reading",
            cleared_at=opened + timedelta(seconds=60),
            ncr_at=opened + timedelta(seconds=5),
            cmm_at=opened + timedelta(seconds=6),
            contained_at=opened + timedelta(seconds=6),
        )
        b = Episode("b", opened + timedelta(hours=1), opened + timedelta(hours=1), "rule_duration")
        out = ops_summary([a, b], production_stops=1)
        assert out["episodes"] == 2
        assert out["open_episodes"] == 1
        assert out["ncrs_opened"] == 1
        assert out["cmm_inspections"] == 1
        assert out["mttr"]["count"] == 1 and out["mttr"]["mean"] == 60
        assert out["time_to_detect"]["from_first_reading"] == 1
        assert out["time_to_detect"]["from_rule_duration"] == 1
        assert out["time_to_disposition"]["p50"] == 6
        assert out["recent_events"][0]["alert_id"] == "b"
        assert out["production_stops"] == 1

    def test_ops_summary_empty_c7934_smoke(self) -> None:
        """[C7934][smoke] No episodes → nulls, not zeros, for the time metrics."""
        out = ops_summary([])
        assert out["time_to_detect"]["p50"] is None
        assert out["mttr"]["mean"] is None
        assert out["recent_events"] == []


# ── actuals SQL against fixtures ───────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_server() -> Iterator:
    with local_postgres() as server:
        yield server


@pytest_asyncio.fixture()
async def pg(pg_server) -> AsyncIterator:
    asyncpg = pytest.importorskip("asyncpg")
    if pg_server is None:
        pytest.skip("no local Postgres binaries (initdb/pg_ctl)")
    name = f"t_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(
        host=pg_server.socket_dir, port=pg_server.port, user=pg_server.superuser
    )
    await admin.execute(f"CREATE DATABASE {name}")
    conn = await asyncpg.connect(
        host=pg_server.socket_dir, port=pg_server.port, user=pg_server.superuser, database=name
    )
    try:
        await app_schema.ensure_alert_table(conn)
        await app_schema.ensure_alert_event_table(conn)
        await app_schema.ensure_alert_response_table(conn)
        await app_schema.ensure_production_tables(conn)
        yield conn
    finally:
        await conn.close()
        await admin.execute(f"DROP DATABASE {name}")
        await admin.close()


async def _alert(conn, pipeline_id: str, industry: str = "manufacturing") -> str:
    alert_id = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO alert (id, fingerprint, industry, pipeline_id, rule_id, severity, threshold)"
        " VALUES ($1::uuid, $2, $3, $4, $5, 'crit', 60)",
        alert_id,
        alert_id,
        industry,
        pipeline_id,
        SPINDLE_RULE,
    )
    return alert_id


async def _event(conn, alert_id: str, kind: str, at: datetime, opened: datetime, **payload):
    body = {"opened_at": opened.isoformat(), "value": 70.0, **payload}
    await conn.execute(
        "INSERT INTO alert_event (alert_id, event_type, fingerprint, payload, created_at)"
        " VALUES ($1::uuid, $2, $3, $4::jsonb, $5)",
        alert_id,
        kind,
        uuid.uuid4().hex,
        json.dumps(body),
        at,
    )


async def _decision(conn, alert_id: str, opened: datetime, action: str, at: datetime):
    await conn.execute(
        "INSERT INTO alert_response (alert_id, episode_opened_at, action, recorded_by, created_at)"
        " VALUES ($1::uuid, $2, $3, 'qa@example.com', $4)",
        alert_id,
        opened,
        action,
        at,
    )


async def _seed(conn, pipeline_id: str) -> dict[str, datetime]:
    """Two episodes on one alert row (re-open), one late episode, and noise."""
    t0 = LOT_START + timedelta(seconds=40)
    a = await _alert(conn, pipeline_id)
    # episode 1: onset known, contained (quarantine + CMM + NCR) inside the lot, cleared
    await _event(
        conn, a, "opened", t0, t0, excursion_started_at=(t0 - timedelta(seconds=5)).isoformat()
    )
    await _event(conn, a, "acknowledged", t0 + timedelta(seconds=15), t0)
    await _event(conn, a, "cleared", t0 + timedelta(seconds=90), t0, value=55.0)
    await _decision(conn, a, t0, "quarantine_lot", t0 + timedelta(seconds=20))
    await _decision(conn, a, t0, "request_cmm_inspection", t0 + timedelta(seconds=25))
    await _decision(conn, a, t0, "open_ncr", t0 + timedelta(seconds=30))
    await conn.execute(
        "INSERT INTO production_event (industry, state, alert_id, changed_by, created_at)"
        " VALUES ('manufacturing', 'stopped', $1::uuid, 'qa', $2)",
        a,
        t0 + timedelta(seconds=25),
    )
    # episode 2 on the same row, no onset on the event (legacy) → rule-duration fallback, contained late
    t1 = t0 + timedelta(seconds=LOT_CYCLE_S * 3)
    await _event(conn, a, "opened", t1, t1)
    await _decision(conn, a, t1, "quarantine_lot", t1 + timedelta(seconds=LOT_CYCLE_S))
    # another pipeline and another industry: must never leak in
    other = await _alert(conn, "industry:manufacturing-other")
    await _event(conn, other, "opened", t0, t0)
    pharma = await _alert(conn, pipeline_id, industry="pharma")
    await _event(conn, pharma, "opened", t0, t0)
    return {"t0": t0, "t1": t1}


class TestActualsSql:
    @pytest.mark.asyncio
    async def test_episodes_and_actuals_from_fixtures_c7935_smoke(self, pg) -> None:
        """[C7935][smoke] The SQL groups episodes, joins decisions and prices the actuals."""
        pid = str(uuid.uuid4())
        times = await _seed(pg, pid)
        start = times["t0"] - timedelta(hours=1)
        end = times["t1"] + timedelta(hours=1)
        window = await fetch_episode_window(pg, (pid,), "manufacturing", start, end)
        assert window.available
        assert len(window.episodes) == 2
        assert window.production_stops == 1
        newest, oldest = window.episodes
        assert oldest.onset_source == "first_reading"
        assert oldest.time_to_detect_s == 5
        assert oldest.time_to_acknowledge_s == 15
        assert oldest.caught_in_time
        assert oldest.ncr_at is not None and oldest.cmm_at is not None
        assert oldest.time_to_restore_s == 90
        assert newest.onset_source == "rule_duration"
        assert not newest.caught_in_time

        a = resolve_assumptions({"cost_per_lot_lost": 40000, "inspection_cost": 1000})
        out = actuals(window.episodes, a)
        assert out["excursion_events_detected"] == 2
        assert out["caught_in_time"] == 1
        assert out["cost_avoided_to_date"] == 39000
        assert out["ncrs_opened"] == 1
        assert out["cmm_inspections"] == 1
        assert out["lots_quarantined"] >= 2  # the open episode spans to the window end

    @pytest.mark.asyncio
    async def test_current_episode_onset_from_row_metadata_c7939_regression(self, pg) -> None:
        """[C7939][regression] The live episode falls back to the alert row's stamped onset."""
        pid = str(uuid.uuid4())
        t0 = LOT_START + timedelta(seconds=60)
        alert_id = await _alert(pg, pid)
        await pg.execute(
            "UPDATE alert SET opened_at = $2, metadata = jsonb_build_object("
            "'excursion_started_at', $3::text) WHERE id = $1::uuid",
            alert_id,
            t0,
            (t0 - timedelta(seconds=7)).isoformat(),
        )
        await _event(pg, alert_id, "opened", t0, t0)
        window = await fetch_episode_window(
            pg, (pid,), "manufacturing", t0 - timedelta(hours=1), t0 + timedelta(hours=1)
        )
        (episode,) = window.episodes
        assert episode.onset_source == "first_reading"
        assert episode.time_to_detect_s == 7

    @pytest.mark.asyncio
    async def test_window_and_pipeline_filters_c7936_regression(self, pg) -> None:
        """[C7936][regression] Episodes outside the window or pipeline are excluded."""
        pid = str(uuid.uuid4())
        times = await _seed(pg, pid)
        only_second = await fetch_episode_window(
            pg,
            (pid,),
            "manufacturing",
            times["t1"] - timedelta(seconds=1),
            times["t1"] + timedelta(hours=1),
        )
        assert len(only_second.episodes) == 1
        nothing = await fetch_episode_window(
            pg,
            ("not-this-pipeline",),
            "manufacturing",
            times["t0"] - timedelta(hours=1),
            times["t1"] + timedelta(hours=1),
        )
        assert nothing.episodes == ()
        assert nothing.production_stops == 0

    @pytest.mark.asyncio
    async def test_no_backend_is_unavailable_c7937_smoke(self) -> None:
        """[C7937][smoke] Without an app backend the window is empty and flagged unavailable."""
        now = datetime.now(UTC)
        window = await fetch_episode_window(
            None, ("p",), "manufacturing", now - timedelta(days=1), now
        )
        assert not window.available
        assert window.episodes == ()


class TestContainmentWindow:
    def test_cmm_stop_ends_the_window_c7938_regression(self) -> None:
        """[C7938][regression] A CMM request stops the mill, so parts after it are not at risk."""
        onset = LOT_START + timedelta(seconds=1)
        open_ep = Episode("a", onset, onset, "first_reading", window_end=onset + timedelta(hours=1))
        stopped = Episode(
            "a",
            onset,
            onset,
            "first_reading",
            cmm_at=onset + timedelta(seconds=30),
            window_end=onset + timedelta(hours=1),
        )
        assert stopped.containment_end == onset + timedelta(seconds=30)
        assert len(stopped.disposition["parts_at_risk"]) < len(open_ep.disposition["parts_at_risk"])
        cleared_first = Episode(
            "a",
            onset,
            onset,
            "first_reading",
            cleared_at=onset + timedelta(seconds=10),
            cmm_at=onset + timedelta(seconds=30),
        )
        assert cleared_first.containment_end == onset + timedelta(seconds=10)
