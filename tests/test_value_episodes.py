"""Episode definitions, full-window SQL totals and canonical-window parity [qa].

The SQL tests run the real queries against a throwaway local Postgres seeded
with fixture rows (skipped when no Postgres binaries are installed).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from iot_twin.domain.catalog import list_alert_rules
from iot_twin.domain.lot_context import LOT_CYCLE_S, PARTS_PER_LOT, lot_disposition
from iot_twin.models import app_schema
from iot_twin.services.alerts.alert_engine import EXCURSION_STARTED_KEY, AlertEngine
from iot_twin.services.alerts.alert_responses import _excursion
from iot_twin.services.value import episodes as episodes_mod
from iot_twin.services.value.config import Definitions, definitions
from iot_twin.services.value.episodes import (
    MAX_SAMPLES,
    RECENT_EVENTS,
    Episode,
    PipelineScope,
    ScopeError,
    fetch_episode_window,
    lot_end,
    parse_window,
    pipeline_scope,
    release_deadline,
    rule_durations,
    validate_pipeline_id,
)
from iot_twin.services.value.ops_metrics import ops_summary, percentile
from tests._local_pg import local_postgres

LOT_START = datetime.fromtimestamp(LOT_CYCLE_S * 6_000_000, tz=UTC)
SPINDLE_RULE = "mfg-alert-spindle-temp"
GRACE = definitions().caught_in_time_grace_s


def _duration() -> float:
    ids, durations = rule_durations("manufacturing")
    return dict(zip(ids, durations, strict=True))[SPINDLE_RULE]


def _ep(opened: datetime, onset: datetime | None = None, **kw) -> Episode:
    return Episode(
        alert_id=kw.pop("alert_id", "a"),
        industry="manufacturing",
        opened_at=opened,
        onset_raw=onset.isoformat() if onset else None,
        rule_id=SPINDLE_RULE,
        window_end=kw.pop("window_end", opened + timedelta(hours=1)),
        **kw,
    )


class TestScope:
    @pytest.mark.parametrize(
        ("raw", "hours"), [("24h", 24), ("7d", 168), (None, 720), ("365d", 8760)]
    )
    def test_window_parses_c7920_smoke(self, raw, hours) -> None:
        """[C7920][smoke] Windows are <n>h or <n>d, default 30d."""
        assert parse_window(raw) == timedelta(hours=hours)

    @pytest.mark.parametrize("raw", ["0h", "366d", "7w", "-1d", "1.5d", "abc"])
    def test_window_rejects_with_field_c7921_regression(self, raw) -> None:
        """[C7921][regression] Bad windows raise ScopeError naming the window field."""
        with pytest.raises(ScopeError) as info:
            parse_window(raw)
        assert info.value.field == "window"

    @pytest.mark.parametrize("raw", ["", None, "a b", "x;drop", "x" * 200])
    def test_pipeline_id_rejects_c7922_regression(self, raw) -> None:
        """[C7922][regression] pipeline_id is required and restricted to a safe charset."""
        with pytest.raises(ScopeError) as info:
            validate_pipeline_id(raw)
        assert info.value.field == "pipeline_id"

    def test_realtime_alias_needs_a_deployment_time_c7923_smoke(self) -> None:
        """[C7923][smoke] Only a bound realtime deployment reads the alias, from its creation."""
        pid = str(uuid.uuid4())
        since = datetime(2026, 11, 1, tzinfo=UTC)
        scope = pipeline_scope(pid, "manufacturing", since)
        assert scope.keys() == [pid, "industry:manufacturing"]
        assert scope.alias_since == since
        assert pipeline_scope(pid, "manufacturing", None).keys() == [pid]
        alias = "industry:manufacturing"
        assert pipeline_scope(alias, "manufacturing", since).keys() == [alias]


class TestDefinitions:
    def test_canonical_window_is_used_c7924_smoke(self) -> None:
        """[C7924][smoke] Episodes window exactly as alert_responses._excursion does."""
        opened = LOT_START + timedelta(seconds=30)
        ep = _ep(opened, opened - timedelta(seconds=4), cleared_at=opened + timedelta(seconds=50))
        assert ep.window == _excursion(ep.as_alert(), ep.now)
        assert ep.onset_source == "first_reading"
        assert ep.time_to_detect_s == 4
        start, end = ep.window
        assert ep.disposition == lot_disposition(start, end, ep.now)

    @pytest.mark.parametrize("offset_s", [9, -10_000])
    def test_out_of_bounds_onset_falls_back_c7925_regression(self, offset_s) -> None:
        """[C7925][regression] A stamp after opening or past the lookback uses opened − duration."""
        opened = LOT_START + timedelta(seconds=30)
        ep = _ep(opened, opened + timedelta(seconds=offset_s))
        assert ep.onset_source == "rule_duration"
        assert ep.time_to_detect_s == _duration()

    def test_missing_onset_falls_back_c7926_regression(self) -> None:
        """[C7926][regression] No stamp → opened_at − rule.duration_s."""
        ep = _ep(LOT_START + timedelta(seconds=30))
        assert ep.onset_source == "rule_duration"
        assert ep.time_to_detect_s == _duration()

    def test_disposition_is_cached_c7927_regression(self) -> None:
        """[C7927][regression] The lot disposition is computed once per episode."""
        ep = _ep(LOT_START + timedelta(seconds=30))
        assert ep.disposition is ep.disposition

    def test_open_excursion_end_is_capped_c7928_regression(self) -> None:
        """[C7928][regression] An excursion left open counts at most max_open_excursion_s."""
        opened = LOT_START
        ep = _ep(
            opened,
            window_end=opened + timedelta(days=30),
            settings=Definitions(max_open_excursion_s=600),
        )
        assert ep.now == opened + timedelta(seconds=600)
        assert ep.window[1] == opened + timedelta(seconds=600)

    def test_deadline_has_grace_after_opening_c7929_regression(self) -> None:
        """[C7929][regression] An onset at the end of a lot still leaves the grace to contain."""
        onset = LOT_START + timedelta(seconds=LOT_CYCLE_S - 1)
        opened = onset + timedelta(seconds=3)
        assert lot_end(onset) < opened
        deadline = release_deadline(onset, opened, GRACE)
        assert deadline == opened + timedelta(seconds=GRACE)
        ep = _ep(opened, onset, contained_at=opened + timedelta(seconds=GRACE))
        assert ep.deadline_before_alert
        assert ep.caught_in_time
        late = _ep(opened, onset, contained_at=opened + timedelta(seconds=GRACE + 1))
        assert not late.caught_in_time

    def test_tta_uses_first_of_ack_or_decision_c7930_regression(self) -> None:
        """[C7930][regression] A signed decision before the ack button counts as acknowledgement."""
        opened = LOT_START + timedelta(seconds=10)
        ep = _ep(
            opened,
            acknowledged_at=opened + timedelta(seconds=50),
            first_decision_at=opened + timedelta(seconds=20),
        )
        assert ep.time_to_acknowledge_s == 20

    def test_percentile_matches_percentile_cont_c7931_smoke(self) -> None:
        """[C7931][smoke] Linear interpolation, the same as Postgres percentile_cont."""
        assert percentile([], 50) is None
        assert percentile([5], 90) == 5
        assert percentile([0, 10], 50) == 5
        assert percentile([1, 2, 3, 4], 90) == pytest.approx(3.7)


# ── SQL against fixtures ───────────────────────────────────────────────────


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


async def _stop(conn, alert_id: str, at: datetime) -> None:
    await conn.execute(
        "INSERT INTO production_event (industry, state, alert_id, changed_by, created_at)"
        " VALUES ('manufacturing', 'stopped', $1::uuid, 'qa', $2)",
        alert_id,
        at,
    )


async def _seed(conn, pipeline_id: str) -> dict[str, datetime]:
    """Two episodes on one alert row (re-open), one late; plus noise elsewhere."""
    t0 = LOT_START + timedelta(seconds=40)
    a = await _alert(conn, pipeline_id)
    onset = (t0 - timedelta(seconds=5)).isoformat()
    await _event(conn, a, "opened", t0, t0, **{EXCURSION_STARTED_KEY: onset})
    await _event(conn, a, "acknowledged", t0 + timedelta(seconds=15), t0)
    await _event(conn, a, "cleared", t0 + timedelta(seconds=90), t0, value=55.0)
    await _decision(conn, a, t0, "quarantine_lot", t0 + timedelta(seconds=20))
    await _decision(conn, a, t0, "request_cmm_inspection", t0 + timedelta(seconds=25))
    await _decision(conn, a, t0, "open_ncr", t0 + timedelta(seconds=30))
    await _stop(conn, a, t0 + timedelta(seconds=25))
    # episode 2: no onset on the event (legacy) and contained long after its lot moved
    t1 = t0 + timedelta(seconds=LOT_CYCLE_S * 3)
    await _event(conn, a, "opened", t1, t1)
    await _event(conn, a, "cleared", t1 + timedelta(seconds=60), t1)
    await _decision(conn, a, t1, "quarantine_lot", t1 + timedelta(seconds=LOT_CYCLE_S))
    # a stop on the same alert row outside both episodes: must not count
    await _stop(conn, a, t1 + timedelta(hours=2))
    other = await _alert(conn, "another-pipeline")
    await _event(conn, other, "opened", t0, t0)
    pharma = await _alert(conn, pipeline_id, industry="pharma")
    await _event(conn, pharma, "opened", t0, t0)
    return {"t0": t0, "t1": t1}


def _scope(pid: str) -> PipelineScope:
    return PipelineScope(pid, "manufacturing")


class TestSql:
    @pytest.mark.asyncio
    async def test_totals_from_fixtures_c7935_smoke(self, pg) -> None:
        """[C7935][smoke] SQL totals: episodes, caught, decisions, stops in-episode only."""
        pid = str(uuid.uuid4())
        times = await _seed(pg, pid)
        window = await fetch_episode_window(
            pg, _scope(pid), times["t0"] - timedelta(hours=1), times["t1"] + timedelta(hours=3)
        )
        t = window.totals
        assert window.available
        assert t.episodes == 2
        assert t.caught_in_time == 1
        assert t.ncrs_opened == 1 and t.cmm_inspections == 1 and t.quarantine_events == 2
        assert t.production_stops == 1
        assert t.onset_from_reading == 1
        assert t.ttd["max"] == pytest.approx(max(5, _duration()))
        assert t.restore["mean"] == pytest.approx(75)
        assert t.tta["count"] == 2
        assert window.meta()["truncated"] is False

    @pytest.mark.asyncio
    async def test_sql_matches_canonical_window_c7936_smoke(self, pg) -> None:
        """[C7936][smoke] Parity: SQL onset, end, deadline and caught equal the canonical episode."""
        pid = str(uuid.uuid4())
        times = await _seed(pg, pid)
        start, end = times["t0"] - timedelta(hours=1), times["t1"] + timedelta(hours=3)
        params = episodes_mod.query_params(_scope(pid), start, end, definitions())
        rows = await pg.fetch(episodes_mod._RECENT_SQL, *params, 50)
        window = await fetch_episode_window(pg, _scope(pid), start, end)
        by_open = {e.opened_at: e for e in window.recent}
        assert len(rows) == 2
        for row in rows:
            ep = by_open[row["opened"]]
            assert row["onset"] == ep.window[0]
            assert row["window_end"] == ep.window[1]
            assert row["deadline"] == ep.deadline
            assert row["caught"] == ep.caught_in_time
            assert row["onset_source"] == ep.onset_source

    @pytest.mark.asyncio
    async def test_parts_match_signed_disposition_c7937_regression(self, pg) -> None:
        """[C7937][regression] Parts held equal the canonical disposition (the signed part list)."""
        pid = str(uuid.uuid4())
        t0 = LOT_START + timedelta(seconds=40)
        a = await _alert(pg, pid)
        onset = t0 - timedelta(seconds=5)
        await _event(pg, a, "opened", t0, t0, **{EXCURSION_STARTED_KEY: onset.isoformat()})
        await _event(pg, a, "cleared", t0 + timedelta(seconds=50), t0)
        await _decision(pg, a, t0, "quarantine_lot", t0 + timedelta(seconds=10))
        window = await fetch_episode_window(
            pg, _scope(pid), t0 - timedelta(hours=1), t0 + timedelta(hours=1)
        )
        (ep,) = window.recent
        signed = ep.disposition
        assert window.totals.parts_quarantined == len(signed["parts_at_risk"])
        assert window.totals.parts_released == signed["parts_released"]
        assert window.totals.lots_quarantined == len(signed["lot_ids"])

    @pytest.mark.asyncio
    async def test_lots_deduped_across_episodes_c7938_regression(self, pg) -> None:
        """[C7938][regression] Two excursions in one lot count the lot and shared parts once."""
        pid = str(uuid.uuid4())
        t0 = LOT_START + timedelta(seconds=40)
        for offset in (0, 60):
            a = await _alert(pg, pid)
            opened = t0 + timedelta(seconds=offset)
            await _event(pg, a, "opened", opened, opened)
            await _event(pg, a, "cleared", opened + timedelta(seconds=100), opened)
        window = await fetch_episode_window(
            pg, _scope(pid), t0 - timedelta(hours=1), t0 + timedelta(hours=1)
        )
        t = window.totals
        assert t.episodes == 2
        assert t.lots_touched == 1
        assert t.parts_started == PARTS_PER_LOT
        summed = sum(len(e.disposition["parts_at_risk"]) for e in window.recent)
        assert t.parts_at_risk < summed
        rft = ops_summary(window)["projected_rft"]
        assert rft["pct"] == round(100 * (PARTS_PER_LOT - t.parts_at_risk) / PARTS_PER_LOT, 2)

    @pytest.mark.asyncio
    async def test_more_episodes_than_the_caps_c7939_regression(self, pg) -> None:
        """[C7939][regression] Totals use every episode; only samples and recent are limited."""
        pid = str(uuid.uuid4())
        count = MAX_SAMPLES + 50
        a = await _alert(pg, pid)
        t0 = LOT_START + timedelta(seconds=10)
        for i in range(count):
            opened = t0 + timedelta(seconds=LOT_CYCLE_S * i)
            await _event(pg, a, "opened", opened, opened)
            await _event(pg, a, "cleared", opened + timedelta(seconds=30), opened)
            await _decision(pg, a, opened, "quarantine_lot", opened + timedelta(seconds=5))
        window = await fetch_episode_window(
            pg, _scope(pid), t0 - timedelta(hours=1), t0 + timedelta(days=2)
        )
        assert window.totals.episodes == count
        assert window.totals.caught_in_time == count
        assert window.totals.lots_quarantined == count
        assert len(window.ttd_samples) == MAX_SAMPLES
        assert len(window.recent) == RECENT_EVENTS
        meta = window.meta()
        assert meta["truncated"] is True
        assert meta["episodes_total"] == count

    @pytest.mark.asyncio
    async def test_alias_clamped_to_deployment_c7940_regression(self, pg) -> None:
        """[C7940][regression] Alias alerts from before the deployment existed are excluded."""
        pid = str(uuid.uuid4())
        alias_alert = await _alert(pg, "industry:manufacturing")
        before = LOT_START + timedelta(seconds=10)
        after = before + timedelta(hours=2)
        for opened in (before, after):
            await _event(pg, alias_alert, "opened", opened, opened)
        scope = pipeline_scope(pid, "manufacturing", before + timedelta(hours=1))
        window = await fetch_episode_window(
            pg, scope, before - timedelta(hours=1), after + timedelta(hours=1)
        )
        assert window.totals.episodes == 1
        assert window.recent[0].opened_at == after

    @pytest.mark.asyncio
    async def test_window_and_pipeline_filters_c7941_regression(self, pg) -> None:
        """[C7941][regression] Episodes outside the window or pipeline are excluded."""
        pid = str(uuid.uuid4())
        times = await _seed(pg, pid)
        only_second = await fetch_episode_window(
            pg,
            _scope(pid),
            times["t1"] - timedelta(seconds=1),
            times["t1"] + timedelta(hours=1),
        )
        assert only_second.totals.episodes == 1
        nothing = await fetch_episode_window(
            pg,
            _scope("not-this-pipeline"),
            times["t0"] - timedelta(hours=1),
            times["t1"] + timedelta(hours=1),
        )
        assert nothing.totals.episodes == 0
        assert nothing.totals.production_stops == 0
        assert nothing.recent == ()

    @pytest.mark.asyncio
    async def test_reopen_keeps_each_episode_ttd_c7942_regression(self, pg) -> None:
        """[C7942][regression] Engine → Postgres: after a reopen each episode keeps its own onset."""

        class Clock:
            instant = LOT_START

            def __call__(self) -> datetime:
                return self.instant

        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        base = next(r for r in list_alert_rules("manufacturing") if r.rule_id == SPINDLE_RULE)
        rule = replace(base, duration_s=4.0)
        pid = str(uuid.uuid4())

        def reading(value: float, device: datetime | None = None) -> dict:
            out = {
                "sensor_id": base.sensor_id,
                "sensor_name": "cnc_1_spindle_temp",
                "value": value,
            }
            if device is not None:
                out["timestamp"] = device.timestamp()
            return out

        async def step(seconds: float, value: float, device: datetime | None = None) -> None:
            clock.instant = clock.instant + timedelta(seconds=seconds)
            await engine.evaluate(
                [reading(value, device)], [rule], industry="manufacturing", pipeline_id=pid, db=pg
            )

        # episode 1: device time 2 s before the engine first saw it
        await step(0, 90, LOT_START - timedelta(seconds=2))
        await step(4, 91)
        await step(30, 40)  # clears
        # episode 2: no device time → engine first-seen clock
        await step(60, 92)
        await step(4, 93)
        window = await fetch_episode_window(
            pg,
            PipelineScope(pid, "manufacturing"),
            LOT_START - timedelta(hours=1),
            LOT_START + timedelta(hours=1),
        )
        ttds = sorted(e.time_to_detect_s for e in window.recent)
        assert ttds == [pytest.approx(4), pytest.approx(6)]
        assert all(e.onset_source == "first_reading" for e in window.recent)
        assert window.totals.onset_from_reading == 2

    @pytest.mark.asyncio
    async def test_no_backend_is_unavailable_c7943_smoke(self) -> None:
        """[C7943][smoke] Without an app backend the window is empty and flagged unavailable."""
        now = datetime.now(UTC)
        window = await fetch_episode_window(None, _scope("p"), now - timedelta(days=1), now)
        assert not window.available
        assert window.totals.episodes == 0
