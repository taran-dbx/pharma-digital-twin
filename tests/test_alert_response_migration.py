"""alert_response upgrade to the medical-device decision workflow [qa].

Contract tests run everywhere. The live tests run the real migration against a
throwaway local Postgres (skipped when no Postgres binaries are installed):
an existing CNC-era table is upgraded in place, re-running is a no-op, and a
non-owner role can boot against an upgraded table.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from iot_twin.models import app_schema
from tests._local_pg import local_postgres

LEGACY_RESPONSE_DDL = """
CREATE TABLE IF NOT EXISTS alert_response (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL REFERENCES alert(id) ON DELETE CASCADE,
    episode_opened_at TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL
        CHECK (action IN ('hold_batch', 'send_to_inspect', 'request_maintenance')),
    recorded_by TEXT NOT NULL,
    reference JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (alert_id, episode_opened_at, action)
)
"""


def _compact(sql: str) -> str:
    return " ".join(sql.split())


class RecordingDb:
    def __init__(self, fail_with: Exception | None = None) -> None:
        self.statements: list[str] = []
        self.fail_with = fail_with

    async def execute(self, query: str, *args) -> None:
        self.statements.append(query)
        if self.fail_with is not None and query.strip().startswith("DO $$"):
            raise self.fail_with


class InsufficientPrivilegeError(Exception):
    pass


class TestMigrationContract:
    def test_ddl_accepts_new_and_legacy_actions_c7820_smoke(self) -> None:
        """[C7820][smoke] Fresh tables accept the med-device actions and the legacy ones."""
        ddl = _compact(app_schema.ALERT_RESPONSE_DDL)
        assert "CONSTRAINT alert_response_action_check CHECK (action IN (" in ddl
        for action in (
            "quarantine_lot",
            "open_ncr",
            "request_cmm_inspection",
            "request_maintenance",
            "hold_batch",
            "send_to_inspect",
        ):
            assert f"'{action}'" in ddl
        for column in ("reason TEXT", "comment TEXT", "signature_meaning TEXT", "signed_at"):
            assert column in ddl

    def test_upgrades_are_guarded_so_reruns_do_nothing_c7821_regression(self) -> None:
        """[C7821][regression] Each ALTER sits behind a check, avoiding the 'must be owner' pitfall."""
        audit = _compact(app_schema.ALERT_RESPONSE_AUDIT_MIGRATION)
        assert audit.count("IF NOT EXISTS ( SELECT 1 FROM information_schema.columns") == 4
        assert "table_schema = current_schema()" in audit
        check = _compact(app_schema.ALERT_RESPONSE_ACTION_MIGRATION)
        assert "pg_get_constraintdef" in check
        assert "IF current_def IS NULL OR" in check
        assert check.index("DROP CONSTRAINT IF EXISTS") < check.index("ADD CONSTRAINT")

    @pytest.mark.asyncio
    async def test_ensure_runs_create_then_the_guarded_upgrades_c7821_smoke(self) -> None:
        """[C7821][smoke] The creator issues the DDL then the three guarded upgrades."""
        db = RecordingDb()
        await app_schema.ensure_alert_response_table(db)
        assert db.statements == [
            app_schema.ALERT_RESPONSE_DDL,
            app_schema.ALERT_RESPONSE_AUDIT_MIGRATION,
            app_schema.ALERT_RESPONSE_ACTION_MIGRATION,
            app_schema.ALERT_RESPONSE_FK_MIGRATION,
        ]

    @pytest.mark.asyncio
    async def test_ownership_errors_do_not_block_boot_c7822_defect(self, caplog) -> None:
        """[C7822][defect] A table owned by another role logs a warning instead of failing boot."""
        db = RecordingDb(InsufficientPrivilegeError("must be owner of table alert_response"))
        await app_schema.ensure_alert_response_table(db)
        assert "owned by another role" in caplog.text

    @pytest.mark.asyncio
    async def test_other_errors_still_raise_c7822_regression(self) -> None:
        """[C7822][regression] Only ownership errors are tolerated."""
        db = RecordingDb(RuntimeError("syntax error at or near DO"))
        with pytest.raises(RuntimeError):
            await app_schema.ensure_alert_response_table(db)


@pytest.fixture(scope="module")
def pg_server() -> Iterator:
    with local_postgres() as server:
        yield server


@pytest_asyncio.fixture()
async def pg(pg_server) -> AsyncIterator:
    """A fresh database (and app role name) per test on the module's cluster."""
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
        yield conn, pg_server, name
    finally:
        await conn.close()
        await admin.execute(f"DROP DATABASE {name}")
        await admin.close()


async def _legacy_cell(conn) -> str:
    await conn.execute(app_schema.ALERT_DDL)
    alert_id = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO alert (id, fingerprint, rule_id, severity) VALUES ($1::uuid, 'fp', 'r', 'crit')",
        alert_id,
    )
    await conn.execute(LEGACY_RESPONSE_DDL)
    for action in ("hold_batch", "send_to_inspect", "request_maintenance"):
        await conn.execute(
            "INSERT INTO alert_response (alert_id, episode_opened_at, action, recorded_by) "
            "VALUES ($1::uuid, now(), $2, 'op@example.com')",
            alert_id,
            action,
        )
    return alert_id


async def _check_oid(conn) -> int:
    return await conn.fetchval(
        "SELECT oid FROM pg_constraint WHERE conrelid = 'alert_response'::regclass "
        "AND conname = 'alert_response_action_check'"
    )


@pytest.mark.asyncio
class TestMigrationLive:
    async def test_legacy_table_upgrades_without_data_loss_c7823_smoke(self, pg) -> None:
        """[C7823][smoke] A CNC-era table gains the new actions and audit columns in place."""
        conn, _server, _db = pg
        alert_id = await _legacy_cell(conn)
        await app_schema.ensure_alert_response_table(conn)

        rows = await conn.fetch("SELECT action FROM alert_response ORDER BY action")
        assert [r["action"] for r in rows] == [
            "hold_batch",
            "request_maintenance",
            "send_to_inspect",
        ]
        columns = {
            r["column_name"]
            for r in await conn.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'alert_response'"
            )
        }
        assert {"reason", "comment", "signature_meaning", "signed_at"} <= columns
        for action in ("quarantine_lot", "open_ncr", "request_cmm_inspection"):
            await conn.execute(
                "INSERT INTO alert_response (alert_id, episode_opened_at, action, recorded_by, "
                "reason, signature_meaning, signed_at) "
                "VALUES ($1::uuid, now(), $2, 'qa@example.com', 'excursion', 'approved', now())",
                alert_id,
                action,
            )
        with pytest.raises(Exception, match="alert_response_action_check"):
            await conn.execute(
                "INSERT INTO alert_response (alert_id, episode_opened_at, action, recorded_by) "
                "VALUES ($1::uuid, now(), 'release_everything', 'x')",
                alert_id,
            )

    async def test_rerun_is_a_no_op_c7824_regression(self, pg) -> None:
        """[C7824][regression] Running the upgrade again leaves the constraint untouched."""
        conn, _server, _db = pg
        await _legacy_cell(conn)
        await app_schema.ensure_alert_response_table(conn)
        first = await _check_oid(conn)
        count = await conn.fetchval("SELECT count(*) FROM alert_response")
        await app_schema.ensure_alert_response_table(conn)
        await app_schema.ensure_alert_response_table(conn)
        assert await _check_oid(conn) == first
        assert await conn.fetchval("SELECT count(*) FROM alert_response") == count

    async def test_fresh_table_needs_no_upgrade_c7824_smoke(self, pg) -> None:
        """[C7824][smoke] A table created by this build is already current."""
        conn, _server, _db = pg
        await conn.execute(app_schema.ALERT_DDL)
        await conn.execute(app_schema.ALERT_RESPONSE_DDL)
        created = await _check_oid(conn)
        await app_schema.ensure_alert_response_table(conn)
        assert await _check_oid(conn) == created

    async def test_non_owner_boots_against_upgraded_and_stale_tables_c7825_defect(self, pg) -> None:
        """[C7825][defect] The app identity need not own the table to start."""
        import asyncpg

        conn, server, database = pg
        await _legacy_cell(conn)
        role = f"app_{database}"
        await conn.execute(f"CREATE ROLE {role} LOGIN")
        await conn.execute(f"GRANT USAGE, CREATE ON SCHEMA public TO {role}")
        await conn.execute(f"GRANT ALL ON alert, alert_response TO {role}")
        app = await asyncpg.connect(
            host=server.socket_dir, port=server.port, user=role, database=database
        )
        try:
            # Stale table owned by someone else: warn and continue.
            await app_schema.ensure_alert_response_table(app)
            # The owner upgrades it; afterwards the app role's boot is a clean no-op.
            await app_schema.ensure_alert_response_table(conn)
            before = await _check_oid(conn)
            await app_schema.ensure_alert_response_table(app)
            assert await _check_oid(conn) == before
        finally:
            await app.close()


def _alert_record(alert_id: str):
    from datetime import UTC, datetime

    from iot_twin.services.alerts.alert_engine import AlertRecord

    opened = datetime(2026, 11, 20, 10, 3, 45, tzinfo=UTC)
    return AlertRecord(
        id=alert_id,
        fingerprint="fp",
        industry="manufacturing",
        pipeline_id=None,
        rule_id="mfg-alert-spindle-temp",
        sensor_id="mfg-sns-cnc_1_spindle_temp",
        sensor_ids=["mfg-sns-cnc_1_spindle_temp"],
        severity="crit",
        status="open",
        fault_code="VPP-SPINDLE3-TEMP",
        value=66.0,
        threshold=60.0,
        opened_at=opened,
        updated_at=opened,
        metadata={"oq_ref": "OQ-TM-CNC1-014"},
    )


@pytest.mark.asyncio
class TestSignedRecordsOnStaleTable:
    async def test_non_owner_records_signed_decisions_durably_c7826_defect(self, pg) -> None:
        """[C7826][defect] On an un-upgraded table, signed decisions persist or are refused."""
        import asyncpg

        from iot_twin.services.alerts.alert_responses import (
            AlertResponseStore,
            ResponseAudit,
            ResponseStoreUnavailable,
        )

        conn, server, database = pg
        alert_id = await _legacy_cell(conn)
        await conn.execute("DELETE FROM alert_response")
        role = f"app_{database}"
        await conn.execute(f"CREATE ROLE {role} LOGIN")
        await conn.execute(f"GRANT USAGE, CREATE ON SCHEMA public TO {role}")
        await conn.execute(f"GRANT ALL ON alert, alert_response TO {role}")
        app = await asyncpg.connect(
            host=server.socket_dir, port=server.port, user=role, database=database
        )
        try:
            await app_schema.ensure_alert_response_table(app)  # warns; cannot upgrade
            store = AlertResponseStore()
            alert = _alert_record(alert_id)
            audit = ResponseAudit(reason="excursion", signature_meaning="responsible")

            held, created = await store.record(alert, "quarantine_lot", "qa@x", db=app, audit=audit)
            assert created is True
            assert held.action == "quarantine_lot"
            assert held.signature_meaning == "responsible"
            row = await conn.fetchrow("SELECT action, reference FROM alert_response")
            assert row["action"] == "hold_batch"  # stored under its legacy name
            assert '"action": "quarantine_lot"' in row["reference"]

            again, created_again = await store.record(
                alert, "quarantine_lot", "other@x", db=app, audit=audit
            )
            assert (created_again, again.id) == (False, held.id)

            cmm, _ = await store.record(
                alert, "request_cmm_inspection", "qa@x", db=app, audit=audit
            )
            assert cmm.action == "request_cmm_inspection"
            maint, _ = await store.record(alert, "request_maintenance", "qa@x", db=app, audit=audit)
            assert maint.reason == "excursion"

            with pytest.raises(ResponseStoreUnavailable, match="not upgraded"):
                await store.record(alert, "open_ncr", "qa@x", db=app, audit=audit)
            assert store._memory == {}

            listed = await store.list_for(alert, db=app)
            assert sorted(r.action for r in listed) == [
                "quarantine_lot",
                "request_cmm_inspection",
                "request_maintenance",
            ]
            assert all(r.signature_meaning == "responsible" for r in listed)
        finally:
            await app.close()

    async def test_signed_records_block_alert_deletes_c7827_regression(self, pg) -> None:
        """[C7827][regression] The upgrade turns the cascading FK into RESTRICT."""
        import asyncpg

        conn, _server, _db = pg
        alert_id = await _legacy_cell(conn)
        await app_schema.ensure_alert_response_table(conn)
        rule = await conn.fetchval(
            "SELECT confdeltype::text FROM pg_constraint WHERE conrelid = 'alert_response'::regclass "
            "AND contype = 'f'"
        )
        assert rule == "r"
        with pytest.raises(asyncpg.RestrictViolationError):
            await conn.execute("DELETE FROM alert WHERE id = $1::uuid", alert_id)
        assert await conn.fetchval("SELECT count(*) FROM alert_response") == 3


@pytest.mark.asyncio
class TestReopenedEpisodeMetadata:
    async def test_reopen_replaces_the_first_episodes_metadata_c7828_defect(self, pg) -> None:
        """[C7828][defect] A reopened alert row carries the 2nd episode's start and lot, not the 1st."""
        from dataclasses import replace
        from datetime import UTC, datetime, timedelta

        from iot_twin.domain import lot_context
        from iot_twin.domain.catalog import list_alert_rules
        from iot_twin.services.alerts.alert_engine import AlertEngine
        from iot_twin.services.alerts.alert_responses import excursion_start

        conn, _server, _db = pg
        await app_schema.ensure_alert_table(conn)
        await app_schema.ensure_alert_event_table(conn)
        rule = replace(
            next(
                r
                for r in list_alert_rules("manufacturing")
                if r.rule_id == "mfg-alert-spindle-temp"
            ),
            duration_s=0.0,
        )
        clock = {"t": datetime(2026, 11, 18, 9, 0, 1, tzinfo=UTC)}
        engine = AlertEngine(now_fn=lambda: clock["t"])

        def reading(value: float) -> list[dict]:
            return [
                {
                    "sensor_name": "cnc_1_spindle_temp",
                    "value": value,
                    "timestamp": clock["t"].timestamp(),
                    "metadata": lot_context.lot_context_at(clock["t"]),
                }
            ]

        (first,) = await engine.evaluate(reading(66.0), [rule], industry="manufacturing", db=conn)
        clock["t"] += timedelta(seconds=40)
        await engine.evaluate(reading(50.0), [rule], industry="manufacturing", db=conn)
        clock["t"] = datetime(2026, 11, 20, 14, 33, 47, tzinfo=UTC)  # two days later
        (second,) = await engine.evaluate(reading(65.0), [rule], industry="manufacturing", db=conn)
        assert second.alert.id == first.alert.id

        # A restarted replica: no cache, everything comes from Lakebase.
        fresh = AlertEngine(now_fn=lambda: clock["t"])
        loaded = await fresh.get_alert(first.alert.id, db=conn)
        assert loaded is not None
        assert loaded.opened_at == clock["t"]
        assert loaded.metadata["excursion_started_at"] == clock["t"].isoformat()
        assert loaded.metadata["lot_id"] == lot_context.lot_id_at(clock["t"])
        assert (
            loaded.metadata["part_serial"] == lot_context.lot_context_at(clock["t"])["part_serial"]
        )
        assert loaded.metadata["lot_id"] != first.alert.metadata["lot_id"]
        assert loaded.fault_code == "VPP-SPINDLE3-TEMP"
        assert excursion_start(loaded, clock["t"]) == clock["t"]

    async def test_reopen_refreshes_a_pre_rename_fault_code_c7828_regression(self, pg) -> None:
        """[C7828][regression] A row stored with the CNC-era fault code takes the new one on reopen."""
        from dataclasses import replace
        from datetime import UTC, datetime, timedelta

        from iot_twin.domain.catalog import list_alert_rules
        from iot_twin.services.alerts.alert_engine import AlertEngine

        conn, _server, _db = pg
        await app_schema.ensure_alert_table(conn)
        await app_schema.ensure_alert_event_table(conn)
        rule = replace(
            next(
                r
                for r in list_alert_rules("manufacturing")
                if r.rule_id == "mfg-alert-spindle-temp"
            ),
            duration_s=0.0,
        )
        clock = {"t": datetime(2026, 11, 20, 9, 0, 0, tzinfo=UTC)}
        engine = AlertEngine(now_fn=lambda: clock["t"])
        old = replace(rule, fault_code="M-SPINDLE-OVERTEMP")
        read = [{"sensor_name": "cnc_1_spindle_temp", "value": 66.0}]
        (first,) = await engine.evaluate(read, [old], industry="manufacturing", db=conn)
        clock["t"] += timedelta(seconds=30)
        await engine.evaluate(
            [{"sensor_name": "cnc_1_spindle_temp", "value": 50.0}],
            [old],
            industry="manufacturing",
            db=conn,
        )
        clock["t"] += timedelta(seconds=300)
        await AlertEngine(now_fn=lambda: clock["t"]).evaluate(
            read, [rule], industry="manufacturing", db=conn
        )
        code = await conn.fetchval(
            "SELECT fault_code FROM alert WHERE id = $1::uuid", first.alert.id
        )
        assert code == "VPP-SPINDLE3-TEMP"
