"""Stall-safe retention: CDF shadow pruning and hot-tier policies [pipeline-throughput].

The live tests install the real ``lakets.sql`` into a throwaway local Postgres and stand
in for Lakehouse Sync with a ``wal2delta.tables`` row whose ``committed_lsn`` the test
moves by hand (the real one is maintained by the managed writer).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from iot_twin.core.paths import LAKETS_SQL_PATH
from iot_twin.models.sensor_schema import pg_chronotable_ddl
from iot_twin.services.industry import industry_setup_service as iss
from iot_twin.services.lakets import (
    insert_triggers,
    lakets_deploy_service,
    lakets_service,
    retention,
)
from tests._local_pg import local_postgres

SHADOW = "_shadow_manufacturing_sensor_readings"
PRUNE = "SELECT lakets.prune_cdf_shadow($1, $2::text::interval, $3)"


# -- contract -----------------------------------------------------------------------------------
def test_prune_asset_is_fail_closed_and_bounded_c9401_smoke() -> None:
    """[C9401][smoke] Prune is gated on the committed LSN and deletes one bounded batch."""
    sql = " ".join(retention.prune_sql().split())
    assert "v_committed := lakets._cdf_committed_lsn(p_shadow_name);" in sql
    assert "IF v_committed IS NULL THEN" in sql
    assert "mark_lsn <= v_committed" in sql
    assert "age(xmin) > age($1)" in sql
    assert "LIMIT $3" in sql


def test_tiering_job_prunes_in_bounded_batches_c9402_regression() -> None:
    """[C9402][regression] The Tiering job calls prune per synced shadow, batch by batch."""
    src = lakets_deploy_service._TIERING_NOTEBOOK_SOURCE.decode("utf-8")
    assert "to_regprocedure('lakets.prune_cdf_shadow(text,interval,integer)')" in src
    assert "SELECT lakets.prune_cdf_shadow(%s, %s::interval, %s)" in src
    assert "SHADOW_PRUNE_MAX_BATCHES = 80" in src
    assert "if deleted == 0:" in src
    compile(src.replace("# MAGIC", "#").replace("dbutils.library.restartPython()", "pass"),
            "tiering", "exec")  # fmt: skip


@pytest.fixture(autouse=True)
def _retention_on(monkeypatch, request):
    """Tests exercise the enabled path unless they opt out with the 'retention_off' mark."""
    if request.node.get_closest_marker("retention_off") is None:
        monkeypatch.setenv(retention.RETENTION_ENABLED_ENV, "true")


@pytest.mark.retention_off
@pytest.mark.asyncio
async def test_retention_is_off_by_default_and_touches_nothing_c9412_regression(
    monkeypatch,
) -> None:
    """[C9412][regression] Without LAKETS_RETENTION_ENABLED nothing is installed or applied."""
    monkeypatch.delenv(retention.RETENTION_ENABLED_ENV, raising=False)
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.fetchval = AsyncMock()
    result = await retention.ensure_retention(conn)
    assert result["status"] == "disabled" and result["applied"] == []
    conn.execute.assert_not_awaited()
    conn.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_retention_never_raises_c9403_regression() -> None:
    """[C9403][regression] A failing policy call is reported, not raised."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value="OK")
    conn.fetchval = AsyncMock(side_effect=RuntimeError("permission denied"))
    result = await retention.ensure_retention(conn)
    assert result["status"] == "failed"
    assert "permission denied" in result["error"]


def test_hot_tier_window_defaults_and_env_c9404_smoke(monkeypatch) -> None:
    """[C9404][smoke] 1 h tier / 24 h drop by default; overridable per workspace."""
    monkeypatch.delenv(retention.TIER_AFTER_ENV, raising=False)
    monkeypatch.delenv(retention.DROP_AFTER_ENV, raising=False)
    assert retention.hot_tier_window() == ("1 hour", "24 hours")
    monkeypatch.setenv(retention.DROP_AFTER_ENV, "6 hours")
    assert retention.hot_tier_window() == ("1 hour", "6 hours")


# -- live (local Postgres) ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def pg_server() -> Iterator:
    with local_postgres() as server:
        yield server


async def _connect(pg_server, database: str):
    import asyncpg

    return await asyncpg.connect(
        host=pg_server.socket_dir, port=pg_server.port, user=pg_server.superuser, database=database
    )


@pytest_asyncio.fixture()
async def db(pg_server) -> AsyncIterator:
    pytest.importorskip("asyncpg")
    if pg_server is None:
        pytest.skip("no local Postgres binaries (initdb/pg_ctl)")
    name = f"t_{uuid.uuid4().hex[:12]}"
    admin = await _connect(pg_server, "postgres")
    await admin.execute(f"CREATE DATABASE {name}")
    conn = await _connect(pg_server, name)
    try:
        await lakets_service.install_lakets(conn, LAKETS_SQL_PATH)
        await conn.execute(pg_chronotable_ddl(iss.SENSOR_TABLE))
        await lakets_service.create_chronotable(
            conn, table=iss.SENSOR_TABLE, time_col=iss.TIME_COLUMN, chunk=iss.CHUNK_INTERVAL
        )
        chrono_id = await conn.fetchval(iss._CHRONOTABLE_ID_SQL, "public", iss.SENSOR_TABLE)
        await conn.fetchval(iss._ENSURE_PARTITIONS_SQL, chrono_id, 8)
        assert (await lakets_service.reconcile_chronotable_shadow(conn, shadow_table=SHADOW))[
            "status"
        ] == "migrated"
        assert (await insert_triggers.ensure_statement_insert_triggers(conn))["status"] == "ok"
        # Stand-in for the managed Lakehouse Sync bookkeeping.
        await conn.execute(
            "CREATE SCHEMA wal2delta; CREATE TABLE wal2delta.tables (table_oid oid, status text,"
            " committed_lsn pg_lsn, last_write_time timestamptz, status_detail text)"
        )
        await conn.execute(
            "INSERT INTO wal2delta.tables VALUES ($1::regclass::oid, 'STREAMING', '0/0', now(), NULL)",
            f"lakets_cdf.{SHADOW}",
        )
        result = await retention.ensure_retention(conn)
        assert result["status"] == "ok", result
        yield conn, pg_server, name
    finally:
        await conn.close()
        await admin.execute(f"DROP DATABASE {name} WITH (FORCE)")
        await admin.close()


_INSERT = """
INSERT INTO public.sensor_readings (sensor_name, industry, value, unit, sensor_type, event_time,
    min_value, max_value, nominal_value, fault_active, metadata, source)
SELECT 'sensor_' || (g % 5), 'manufacturing', g, 'C', 'temperature',
       now() + make_interval(secs => g / 1000.0), 0, 100, 50, false, '{}'::jsonb, $2
FROM generate_series(1, $1) AS g
"""


async def _commit_sync_to_now(conn) -> None:
    await conn.execute("UPDATE wal2delta.tables SET committed_lsn = pg_current_wal_lsn()")


async def _prune(conn) -> dict:
    # The fixture's event times are a few ms in the future; a negative keep takes the
    # event-time buffer out of these tests (it has its own test below).
    return json.loads(await conn.fetchval(PRUNE, SHADOW, "-1 hour", 100_000))


async def _shadow_sources(conn) -> dict:
    rows = await conn.fetch(f"SELECT source, count(*) n FROM lakets_cdf.{SHADOW} GROUP BY 1")
    return {r["source"]: r["n"] for r in rows}


class TestShadowPruneSafety:
    @pytest.mark.asyncio
    async def test_only_rows_behind_a_committed_mark_are_pruned_c9405_smoke(self, db) -> None:
        """[C9405][smoke] Rows committed before a mark the sync passed go; later rows stay."""
        conn, *_ = db
        await conn.execute(_INSERT, 300, "batch1")
        first = await _prune(conn)  # mark 1, but the sync has committed nothing yet
        assert first["deleted"] == 0 and first["status"] == "skipped"
        await _commit_sync_to_now(conn)  # sync passes mark 1
        await conn.execute(_INSERT, 200, "batch2")  # after mark 1: not provably durable
        second = await _prune(conn)
        assert second["status"] == "ok" and second["deleted"] == 300
        assert await _shadow_sources(conn) == {"batch2": 200}
        # The hot tier itself is untouched by shadow pruning.
        assert await conn.fetchval("SELECT count(*) FROM public.sensor_readings") == 500

    @pytest.mark.asyncio
    async def test_a_stalled_sync_is_never_overtaken_c9406_regression(self, db) -> None:
        """[C9406][regression] With committed_lsn frozen (a stall), new rows are never pruned."""
        conn, *_ = db
        await conn.execute(_INSERT, 100, "before")
        await _prune(conn)
        await _commit_sync_to_now(conn)
        await _prune(conn)  # prunes "before"
        # Stall: the writer stops; rows and marks keep coming, committed_lsn does not move.
        for _ in range(3):
            await conn.execute(_INSERT, 100, "during_stall")
            assert (await _prune(conn))["deleted"] == 0
        assert await _shadow_sources(conn) == {"during_stall": 300}

    @pytest.mark.asyncio
    async def test_not_streaming_fails_closed_c9407_regression(self, db) -> None:
        """[C9407][regression] SKIPPED/absent wal2delta status => nothing is deleted."""
        conn, *_ = db
        await conn.execute(_INSERT, 100, "x")
        await _prune(conn)
        await _commit_sync_to_now(conn)
        await conn.execute("UPDATE wal2delta.tables SET status = 'SKIPPED'")
        result = await _prune(conn)
        assert result == {"status": "skipped", "reason": "shadow not STREAMING", "deleted": 0}
        assert await _shadow_sources(conn) == {"x": 100}

    @pytest.mark.asyncio
    async def test_in_flight_transaction_is_not_pruned_c9408_regression(self, db) -> None:
        """[C9408][regression] A writer open when the mark is taken commits later: kept."""
        conn, pg_server, name = db
        writer = await _connect(pg_server, name)
        try:
            tx = writer.transaction()
            await tx.start()
            await writer.execute(_INSERT, 50, "in_flight")  # xid assigned, not committed
            await _prune(conn)  # mark: snapshot xmin <= the writer's xid
            await tx.commit()
            await _commit_sync_to_now(conn)  # the sync is past the mark...
            result = await _prune(conn)  # ...but the writer had not finished before it
            assert result["deleted"] == 0
            assert await _shadow_sources(conn) == {"in_flight": 50}
            await _prune(conn)
            await _commit_sync_to_now(conn)
            assert (await _prune(conn))["deleted"] == 50  # durable once a later mark is passed
        finally:
            await writer.close()

    @pytest.mark.asyncio
    async def test_keep_window_and_batch_bound_c9409_smoke(self, db) -> None:
        """[C9409][smoke] Rows younger than p_keep stay; one call deletes at most p_batch."""
        conn, *_ = db
        await conn.execute(_INSERT, 500, "old")
        await _prune(conn)
        await _commit_sync_to_now(conn)
        kept = json.loads(await conn.fetchval(PRUNE, SHADOW, "1 hour", 100_000))
        assert kept["deleted"] == 0  # durable, but inside the 1 h keep window
        bounded = json.loads(await conn.fetchval(PRUNE, SHADOW, "-1 hour", 120))
        assert bounded["deleted"] == 120


class TestHotTierPolicies:
    @pytest.mark.asyncio
    async def test_tiering_and_tiered_retention_installed_once_c9410_smoke(self, db) -> None:
        """[C9410][smoke] Both policies exist (with write tracking) and re-running is a no-op."""
        conn, *_ = db
        again = await retention.ensure_retention(conn)
        assert again["status"] == "ok" and again["applied"] == []
        rows = await conn.fetch(
            "SELECT policy_type, config FROM lakets._policy_registry ORDER BY policy_type"
        )
        policies = {r["policy_type"]: json.loads(r["config"]) for r in rows}
        assert policies["tiering"] == {"after": "01:00:00"}
        assert policies["tiered_retention"] == {"tier_after": "01:00:00", "drop_after": "24:00:00"}
        triggers = {
            r["tgname"]
            for r in await conn.fetch(
                "SELECT tgname FROM pg_trigger WHERE tgrelid = 'public.sensor_readings'::regclass"
            )
        }
        assert "trg_lakets_tier_lsn_insert" in triggers

    @pytest.mark.asyncio
    async def test_retention_defers_drops_while_the_sync_is_behind_c9411_regression(
        self, db
    ) -> None:
        """[C9411][regression] A chunk past drop_after is not dropped until it is durable."""
        conn, *_ = db
        await conn.execute(_INSERT, 100, "x")
        # Age every chunk past drop_after, with writes the sync has not committed yet.
        await conn.execute(
            "UPDATE lakets._chunk_metadata SET range_start = range_start - interval '3 days',"
            " range_end = range_end - interval '3 days', last_write_lsn = pg_current_wal_lsn()"
        )
        await conn.execute("UPDATE wal2delta.tables SET committed_lsn = '0/1'")
        dropped = await conn.fetchval("SELECT lakets.execute_retention('sensor_readings')")
        assert dropped == 0
        await _commit_sync_to_now(conn)
        dropped = await conn.fetchval("SELECT lakets.execute_retention('sensor_readings')")
        assert dropped > 0
