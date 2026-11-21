"""Set-based LakeTS INSERT triggers [pipeline-throughput].

Contract tests run everywhere. The live tests install the real ``lakets.sql``
into a throwaway local Postgres (skipped without Postgres binaries), build the
real ChronoTable, LVC and CDF shadow, then swap to statement-level triggers and
check that the shadow and the last-value cache stay exact.
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
from iot_twin.services.lakets import insert_triggers, lakets_service
from tests._local_pg import local_postgres

SHADOW = "_shadow_manufacturing_sensor_readings"
LVC_VALUES = ["event_time", "industry", "sensor_type", "unit", "value", "fault_active"]


def _compact(sql: str) -> str:
    """Collapse whitespace and join adjacent SQL string literals ('a ' 'b' -> 'a b')."""
    return " ".join(sql.split()).replace("' '", "")


class TestContract:
    def test_asset_defines_statement_triggers_only_for_insert_c9101_smoke(self) -> None:
        """[C9101][smoke] Inserts use transition tables; UPDATE/DELETE keep the row trigger."""
        sql = _compact(insert_triggers.insert_triggers_sql())
        assert "FOR EACH ROW EXECUTE FUNCTION lakets._sync_insert_stmt_fn" not in sql
        assert (
            "AFTER INSERT ON %I.%I REFERENCING NEW TABLE AS _lakets_new_rows "
            "FOR EACH STATEMENT EXECUTE FUNCTION lakets._sync_insert_stmt_fn()" in sql
        )
        assert (
            "AFTER INSERT ON %I.%I REFERENCING NEW TABLE AS _lakets_new_rows "
            "FOR EACH STATEMENT EXECUTE FUNCTION lakets._lvc_insert_stmt_fn()" in sql
        )
        assert (
            "AFTER UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION lakets._sync_trigger_fn()"
            in sql
        )
        assert "DROP TRIGGER IF EXISTS trg_lakets_lvc ON" in sql
        # One transaction, writers wait instead of racing the swap.
        assert "LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE" in sql
        assert "SET LOCAL lock_timeout" in sql
        # One LVC row per key per statement, newest by the ChronoTable time column.
        assert "SELECT DISTINCT ON (%4$s) %3$s, now() FROM _lakets_new_rows" in sql
        assert "ORDER BY %4$s, %5$I DESC" in sql

    def test_cutover_never_reinstalls_a_row_insert_sync_trigger_c9102_regression(self) -> None:
        """[C9102][regression] A re-run cutover must not make INSERT copy rows twice."""
        import inspect

        source = _compact(inspect.getsource(lakets_service.reconcile_chronotable_shadow))
        guard = source.index("tgname = 'trg_lakets_sync_insert'")
        update_only = source.index("AFTER UPDATE OR DELETE ON {schema}.{table}")
        legacy = source.index("AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table}")
        assert guard < update_only < legacy

    @pytest.mark.asyncio
    async def test_ensure_reports_ok_with_summary_c9103_smoke(self) -> None:
        """[C9103][smoke] Installs the functions, then swaps, and returns the summary."""
        conn = MagicMock()
        conn.execute = AsyncMock(return_value="CREATE FUNCTION")
        conn.fetchval = AsyncMock(
            return_value=json.dumps(
                {"table": "public.sensor_readings", "sync": True, "lvc": True,
                 "row_insert_triggers": []}
            )
        )  # fmt: skip
        result = await insert_triggers.ensure_statement_insert_triggers(conn)
        assert result["status"] == "ok"
        assert result["summary"]["sync"] is True
        assert "lakets.use_statement_insert_triggers" in conn.execute.await_args.args[0]
        assert conn.fetchval.await_args.args == (
            insert_triggers.SWAP_SQL,
            "sensor_readings",
            "public",
        )

    @pytest.mark.asyncio
    async def test_ensure_never_raises_c9104_regression(self) -> None:
        """[C9104][regression] A lock timeout is reported, not raised, so deploys continue."""
        conn = MagicMock()
        conn.execute = AsyncMock(return_value="OK")
        conn.fetchval = AsyncMock(
            side_effect=RuntimeError("canceling statement due to lock timeout")
        )
        result = await insert_triggers.ensure_statement_insert_triggers(conn)
        assert result["status"] == "failed"
        assert "lock timeout" in result["error"]

    @pytest.mark.asyncio
    async def test_leftover_row_triggers_are_a_failure_c9105_regression(self) -> None:
        """[C9105][regression] A per-row INSERT trigger left behind is not reported as ok."""
        conn = MagicMock()
        conn.execute = AsyncMock(return_value="OK")
        conn.fetchval = AsyncMock(return_value={"row_insert_triggers": ["trg_custom"]})
        result = await insert_triggers.ensure_statement_insert_triggers(conn)
        assert result["status"] == "failed"
        assert "trg_custom" in result["error"]


@pytest.fixture(scope="module")
def pg_server() -> Iterator:
    with local_postgres() as server:
        yield server


@pytest_asyncio.fixture()
async def lakets_db(pg_server) -> AsyncIterator:
    """A fresh database with LakeTS, the SSOT ChronoTable, the LVC and the CDF shadow."""
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
        await lakets_service.install_lakets(conn, LAKETS_SQL_PATH)
        await conn.execute(pg_chronotable_ddl(iss.SENSOR_TABLE))
        await lakets_service.create_chronotable(
            conn, table=iss.SENSOR_TABLE, time_col=iss.TIME_COLUMN, chunk=iss.CHUNK_INTERVAL
        )
        chrono_id = await conn.fetchval(iss._CHRONOTABLE_ID_SQL, "public", iss.SENSOR_TABLE)
        await conn.fetchval(iss._ENSURE_PARTITIONS_SQL, chrono_id, 8)
        lvc = await lakets_service.enable_lvc(
            conn, table=iss.SENSOR_TABLE, key_columns=["sensor_name"], value_columns=LVC_VALUES
        )
        assert lvc["status"] != "failed", lvc
        shadow = await lakets_service.reconcile_chronotable_shadow(conn, shadow_table=SHADOW)
        assert shadow["status"] == "migrated", shadow
        yield conn
    finally:
        await conn.close()
        await admin.execute(f"DROP DATABASE {name} WITH (FORCE)")
        await admin.close()


_INSERT = """
INSERT INTO public.sensor_readings
    (sensor_name, industry, value, unit, sensor_type, event_time, min_value, max_value,
     nominal_value, fault_active, metadata, source)
SELECT 'sensor_' || (g % $2), 'manufacturing', g::float8, 'C', 'temperature',
       now() + make_interval(secs => ((g * 7919) % $1) / 1000.0),
       0, 100, 50, false, '{}'::jsonb, 'sss'
FROM generate_series(1, $1) AS g
"""


async def _row_insert_triggers(conn) -> list[str]:
    rows = await conn.fetch(
        "SELECT tgname FROM pg_trigger WHERE tgrelid = 'public.sensor_readings'::regclass"
        " AND NOT tgisinternal AND (tgtype & 1) = 1 AND (tgtype & 4) = 4 ORDER BY tgname"
    )
    return [r["tgname"] for r in rows]


async def _assert_shadow_exact(conn) -> None:
    missing = await conn.fetchval(
        f"SELECT count(*) FROM (SELECT * FROM public.sensor_readings"
        f" EXCEPT ALL SELECT * FROM lakets_cdf.{SHADOW}) d"
    )
    extra = await conn.fetchval(
        f"SELECT count(*) FROM (SELECT * FROM lakets_cdf.{SHADOW}"
        f" EXCEPT ALL SELECT * FROM public.sensor_readings) d"
    )
    assert (missing, extra) == (0, 0)


async def _assert_lvc_is_newest(conn) -> None:
    wrong = await conn.fetchval(
        """
        SELECT count(*) FROM public._lvc_sensor_readings c
        JOIN LATERAL (
            SELECT value, event_time FROM public.sensor_readings s
            WHERE s.sensor_name = c.sensor_name ORDER BY event_time DESC LIMIT 1
        ) newest ON TRUE
        WHERE (c.value, c.event_time) IS DISTINCT FROM (newest.value, newest.event_time)
        """
    )
    assert wrong == 0


class TestLive:
    @pytest.mark.asyncio
    async def test_swap_keeps_shadow_and_lvc_exact_c9106_smoke(self, lakets_db) -> None:
        """[C9106][smoke] Before and after the swap every insert lands once in the shadow."""
        conn = lakets_db
        assert await _row_insert_triggers(conn) == ["trg_lakets_lvc", "trg_lakets_sync"]
        await conn.execute(_INSERT, 300, 7)
        await _assert_shadow_exact(conn)

        result = await insert_triggers.ensure_statement_insert_triggers(conn)

        assert result["status"] == "ok", result
        assert result["summary"] == {
            "table": "public.sensor_readings",
            "sync": True,
            "lvc": True,
            "row_insert_triggers": [],
        }
        assert await _row_insert_triggers(conn) == []
        # Out-of-order event times inside one statement: the cache keeps the newest.
        await conn.execute(_INSERT, 500, 9)
        assert await conn.fetchval(f"SELECT count(*) FROM lakets_cdf.{SHADOW}") == 800
        await _assert_shadow_exact(conn)
        await _assert_lvc_is_newest(conn)
        assert await conn.fetchval("SELECT count(*) FROM public._lvc_sensor_readings") == 9

    @pytest.mark.asyncio
    async def test_update_and_delete_still_sync_per_row_c9107_regression(self, lakets_db) -> None:
        """[C9107][regression] The swap leaves UPDATE/DELETE shadowing intact."""
        conn = lakets_db
        await insert_triggers.ensure_statement_insert_triggers(conn)
        await conn.execute(_INSERT, 50, 5)
        target = await conn.fetchval("SELECT min(id) FROM public.sensor_readings")
        await conn.execute("UPDATE public.sensor_readings SET value = -1 WHERE id = $1", target)
        await conn.execute(
            "DELETE FROM public.sensor_readings WHERE id = (SELECT max(id) FROM public.sensor_readings)"
        )
        await _assert_shadow_exact(conn)
        assert await conn.fetchval(f"SELECT count(*) FROM lakets_cdf.{SHADOW}") == 49

    @pytest.mark.asyncio
    async def test_swap_and_cutover_are_idempotent_c9108_regression(self, lakets_db) -> None:
        """[C9108][regression] Re-running the swap or the CDF cutover never double-copies."""
        conn = lakets_db
        await insert_triggers.ensure_statement_insert_triggers(conn)
        again = await insert_triggers.ensure_statement_insert_triggers(conn)
        assert again["status"] == "ok"
        # Force the cutover to run again (marker cleared), as an interrupted migration would.
        await conn.execute(f"COMMENT ON TABLE lakets_cdf.{SHADOW} IS NULL")
        rerun = await lakets_service.reconcile_chronotable_shadow(conn, shadow_table=SHADOW)
        assert rerun["status"] == "migrated", rerun
        assert await _row_insert_triggers(conn) == []
        await conn.execute(_INSERT, 200, 4)
        await _assert_shadow_exact(conn)
        await _assert_lvc_is_newest(conn)
