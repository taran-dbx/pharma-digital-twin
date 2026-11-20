"""Action-plan audit: idempotent DDL, Lakebase store, UC mirror [qa].

Contract tests run everywhere; the live tests use a throwaway local Postgres
(skipped when no Postgres binaries are installed).
"""

from __future__ import annotations

import json
import re
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from iot_twin.models import app_schema
from iot_twin.services.genai import audit_sync
from iot_twin.services.genai.audit_store import (
    AuditRecord,
    AuditStoreUnavailable,
    GenaiAuditStore,
)
from tests._genai_fixtures import NOW, RISK


def make_record(alert_id: str | None = None, **overrides) -> AuditRecord:
    base = {
        "id": str(uuid.uuid4()),
        "alert_id": alert_id or str(uuid.uuid4()),
        "industry": "manufacturing",
        "requested_by": "qa@example.com",
        "requested_at": NOW,
        "endpoint": "databricks-claude-sonnet-5-5",
        "model": "claude-sonnet-5-5",
        "model_version": "databricks-claude-sonnet-5-5",
        "prompt_template_version": "action-plan/v1",
        "prompt_hash": "a" * 64,
        "grounding_hash": "b" * 64,
        "grounding_rows": [
            {"id": "alert:x", "source": "alert", "key": "x", "data": {"value": 64.2}},
            {"id": "risk_score:x", "source": "risk_score", "key": "x", "data": dict(RISK)},
        ],
        "response": {"plan": {"summary": {"text": "It's hot", "citations": ["alert:x"]}}},
        "grounding_check": "pass",
        "grounding_reasons": [],
        "status": "generated",
        "latency_ms": 900,
        "total_tokens": 1600,
    }
    return AuditRecord(**{**base, **overrides})


class RecordingDb:
    def __init__(self, fail_index: Exception | None = None, fail_all: Exception | None = None):
        self.statements: list[str] = []
        self.fail_index = fail_index
        self.fail_all = fail_all

    async def execute(self, query: str, *args) -> None:
        self.statements.append(query)
        if self.fail_all:
            raise self.fail_all
        if self.fail_index and query.startswith("CREATE INDEX"):
            raise self.fail_index

    async def fetchrow(self, query: str, *args):
        raise self.fail_all or RuntimeError("no rows")

    async def fetch(self, query: str, *args):
        raise self.fail_all or RuntimeError("no rows")


class InsufficientPrivilegeError(Exception):
    pass


class TestDdlContract:
    def test_columns_and_restrict_fk(self) -> None:
        ddl = " ".join(app_schema.GENAI_ACTION_PLAN_AUDIT_DDL.split())
        assert "CREATE TABLE IF NOT EXISTS genai_action_plan_audit" in ddl
        for column in (
            "requested_by TEXT NOT NULL",
            "endpoint TEXT NOT NULL",
            "prompt_template_version TEXT NOT NULL",
            "prompt_hash TEXT NOT NULL",
            "grounding_rows JSONB NOT NULL",
            "response JSONB NOT NULL DEFAULT '{}'::jsonb",
            "grounding_check TEXT NOT NULL CHECK (grounding_check IN ('pass', 'fail'))",
            "latency_ms INTEGER",
            "total_tokens INTEGER",
            "applied_by TEXT",
            "applied_at TIMESTAMPTZ",
        ):
            assert column in ddl
        assert "REFERENCES alert(id) ON DELETE RESTRICT" in ddl
        assert "CASCADE" not in ddl

    @pytest.mark.asyncio
    async def test_ensure_is_part_of_the_app_schema_and_tolerates_ownership(self) -> None:
        db = RecordingDb(fail_index=InsufficientPrivilegeError("must be owner of table"))
        await app_schema.ensure_genai_action_plan_audit_table(db)
        assert db.statements[0] == app_schema.GENAI_ACTION_PLAN_AUDIT_DDL
        everything = RecordingDb()
        await app_schema.ensure_app_schema(everything)
        assert app_schema.GENAI_ACTION_PLAN_AUDIT_DDL in everything.statements
        assert everything.statements.index(app_schema.ALERT_DDL) < everything.statements.index(
            app_schema.GENAI_ACTION_PLAN_AUDIT_DDL
        )

    @pytest.mark.asyncio
    async def test_other_errors_still_raise(self) -> None:
        with pytest.raises(RuntimeError):
            await app_schema.ensure_genai_action_plan_audit_table(
                RecordingDb(fail_index=RuntimeError("syntax error"))
            )


@pytest.mark.asyncio
class TestMemoryStore:
    async def test_record_list_get_and_apply_once(self) -> None:
        store = GenaiAuditStore()
        first = make_record()
        later = make_record(first.alert_id, requested_at=NOW + timedelta(minutes=1))
        await store.record(first)
        await store.record(later)
        assert [r.id for r in await store.list_for_alert(first.alert_id)] == [later.id, first.id]
        applied = await store.mark_applied(first.id, applied_by="qa@x", response_id="r1", now=NOW)
        assert applied.applied_by == "qa@x" and applied.applied_response_id == "r1"
        assert await store.mark_applied(first.id, applied_by="other", response_id="r2") is None
        assert (await store.get(first.id)).applied_by == "qa@x"
        assert first.applied_by is None  # records are immutable copies

    async def test_bound_database_failures_refuse_instead_of_using_memory(self) -> None:
        store = GenaiAuditStore()
        db = RecordingDb(fail_all=RuntimeError("connection reset"))
        with pytest.raises(AuditStoreUnavailable):
            await store.record(make_record(), db=db)
        with pytest.raises(AuditStoreUnavailable):
            await store.get(str(uuid.uuid4()), db=db)
        with pytest.raises(AuditStoreUnavailable):
            await store.list_for_alert(str(uuid.uuid4()), db=db)
        assert await store.list_recent() == []


async def _alert(conn) -> str:
    alert_id = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO alert (id, fingerprint, rule_id, severity) "
        "VALUES ($1::uuid, $2, 'mfg-alert-spindle-temp', 'crit')",
        alert_id,
        f"fp-{alert_id}",
    )
    return alert_id


@pytest.mark.asyncio
class TestLive:
    async def test_schema_is_idempotent_and_round_trips(self, pg) -> None:
        conn, _server, _db = pg
        await app_schema.ensure_app_schema(conn)
        await app_schema.ensure_app_schema(conn)
        alert_id = await _alert(conn)
        store = GenaiAuditStore()
        record = make_record(alert_id)
        await store.record(record, db=conn)
        await store.record(record, db=conn)  # same id: ON CONFLICT DO NOTHING
        assert await conn.fetchval("SELECT count(*) FROM genai_action_plan_audit") == 1
        loaded = await store.get(record.id, db=conn)
        assert loaded.grounding_rows == record.grounding_rows
        assert loaded.response == record.response
        assert [r.id for r in await store.list_for_alert(alert_id, db=conn)] == [record.id]
        raw = await conn.fetchval(
            "SELECT grounding_rows FROM genai_action_plan_audit WHERE id = $1::uuid", record.id
        )
        assert json.loads(raw)[0]["id"] == "alert:x"

    async def test_apply_links_the_signed_response_once(self, pg) -> None:
        conn, _server, _db = pg
        await app_schema.ensure_app_schema(conn)
        alert_id = await _alert(conn)
        response_id = await conn.fetchval(
            "INSERT INTO alert_response (alert_id, episode_opened_at, action, recorded_by) "
            "VALUES ($1::uuid, now(), 'quarantine_lot', 'qa@x') RETURNING id::text",
            alert_id,
        )
        store = GenaiAuditStore()
        record = await store.record(make_record(alert_id), db=conn)
        applied = await store.mark_applied(
            record.id, applied_by="qa@x", response_id=response_id, db=conn
        )
        assert applied.applied_by == "qa@x" and applied.applied_response_id == response_id
        assert (
            await store.mark_applied(record.id, applied_by="b", response_id=response_id, db=conn)
            is None
        )

    async def test_alert_delete_is_restricted(self, pg) -> None:
        conn, _server, _db = pg
        await app_schema.ensure_app_schema(conn)
        alert_id = await _alert(conn)
        await GenaiAuditStore().record(make_record(alert_id), db=conn)
        with pytest.raises(Exception, match="genai_action_plan_audit_alert_id_fkey"):
            await conn.execute("DELETE FROM alert WHERE id = $1::uuid", alert_id)

    async def test_non_owner_boots_against_an_existing_table(self, pg) -> None:
        import asyncpg

        conn, server, database = pg
        await app_schema.ensure_app_schema(conn)
        role = f"app_{database}"
        await conn.execute(f"CREATE ROLE {role} LOGIN")
        await conn.execute(f"GRANT USAGE, CREATE ON SCHEMA public TO {role}")
        await conn.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {role}")
        app = await asyncpg.connect(
            host=server.socket_dir, port=server.port, user=role, database=database
        )
        try:
            await app_schema.ensure_genai_action_plan_audit_table(app)
        finally:
            await app.close()


_LITERAL = re.compile(r"'((?:[^'\\]|\\.)*)'|NULL|[-0-9.]+")


def _spark_values(sql: str) -> list:
    """Parse a ``(v1, v2, ...)`` tuple the way Databricks SQL reads string literals."""
    body = sql.strip()[1:-1]
    values, pos = [], 0
    while pos < len(body):
        if body[pos] in ", ":
            pos += 1
            continue
        match = _LITERAL.match(body, pos)
        assert match is not None, f"unparseable SQL near: {body[pos : pos + 40]!r}"
        lexeme = match.group(0)
        if lexeme == "NULL":
            values.append(None)
        elif lexeme.startswith("'"):
            values.append(re.sub(r"\\(.)", r"\1", match.group(1)))
        else:
            values.append(float(lexeme))
        pos = match.end()
    return values


class _Pool:
    """``acquire()`` over one connection, like LakebaseConnectionManager."""

    def __init__(self, conn) -> None:
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.mark.asyncio
class TestLiveHardening:
    async def test_audit_is_append_only(self, pg) -> None:
        conn, _server, _db = pg
        await app_schema.ensure_app_schema(conn)
        alert_id = await _alert(conn)
        record = await GenaiAuditStore().record(make_record(alert_id), db=conn)
        with pytest.raises(Exception, match="append-only"):
            await conn.execute(
                "UPDATE genai_action_plan_audit SET grounding_check = 'fail' WHERE id = $1::uuid",
                record.id,
            )
        with pytest.raises(Exception, match="append-only"):
            await conn.execute("DELETE FROM genai_action_plan_audit WHERE id = $1::uuid", record.id)
        with pytest.raises(Exception, match="append-only"):
            await conn.execute("TRUNCATE genai_action_plan_audit")
        response_id = await conn.fetchval(
            "INSERT INTO alert_response (alert_id, episode_opened_at, action, recorded_by) "
            "VALUES ($1::uuid, now(), 'open_ncr', 'qa@x') RETURNING id::text",
            alert_id,
        )
        applied = await GenaiAuditStore().mark_applied(
            record.id, applied_by="qa@x", response_id=response_id, db=conn
        )
        assert applied is not None
        with pytest.raises(Exception, match="already applied"):
            await conn.execute(
                "UPDATE genai_action_plan_audit SET applied_by = 'someone' WHERE id = $1::uuid",
                record.id,
            )
        # Re-running the schema is a no-op on a hardened table.
        await app_schema.ensure_app_schema(conn)

    async def test_decision_and_plan_link_commit_together(self, pg, monkeypatch) -> None:
        from iot_twin.services.alerts.alert_responses import AlertResponseStore, ResponseAudit
        from iot_twin.services.genai.apply import PlanLink, sign_with_plan
        from tests._genai_fixtures import make_alert

        conn, _server, _db = pg
        await app_schema.ensure_app_schema(conn)
        alert = make_alert()
        await conn.execute(
            "INSERT INTO alert (id, fingerprint, rule_id, severity, opened_at) "
            "VALUES ($1::uuid, 'fp-atomic', $2, 'crit', $3)",
            alert.id,
            alert.rule_id,
            alert.opened_at,
        )
        audits = GenaiAuditStore()
        plan = await audits.record(make_record(alert.id), db=conn)
        audit = ResponseAudit(reason="excursion", signature_meaning="approved")

        async def broken(*_a, **_k):
            raise AuditStoreUnavailable("link failed")

        monkeypatch.setattr(audits, "mark_applied", broken)
        with pytest.raises(AuditStoreUnavailable):
            await sign_with_plan(
                AlertResponseStore(),
                audits,
                alert,
                "open_ncr",
                user="qa@x",
                audit=audit,
                link=PlanLink(plan.id, None),
                db=_Pool(conn),
            )
        assert await conn.fetchval("SELECT count(*) FROM alert_response") == 0  # rolled back

        monkeypatch.undo()
        signed = await sign_with_plan(
            AlertResponseStore(),
            audits,
            alert,
            "open_ncr",
            user="qa@x",
            audit=audit,
            link=PlanLink(plan.id, None),
            db=_Pool(conn),
        )
        assert signed.created and signed.applied.applied_response_id == signed.response.id
        assert await conn.fetchval("SELECT count(*) FROM alert_response") == 1


class TestUcMirror:
    def test_row_sql_carries_trace_and_risk(self) -> None:
        values = _spark_values(audit_sync.audit_row_sql(make_record()))
        assert len(values) == len(audit_sync.COLUMNS.split(","))
        row = dict(zip([c.strip() for c in audit_sync.COLUMNS.split(",")], values, strict=True))
        assert row["grounding_check"] == "pass" and row["plan_summary"] == "It's hot"
        assert row["risk_score"] == 0.87 and row["risk_model"] == "pharma_twin.ml.spindle_risk"
        assert json.loads(row["citation_ids"]) == ["alert:x", "risk_score:x"]
        assert json.loads(row["plan"])["summary"]["text"] == "It's hot"

    @pytest.mark.parametrize(
        "text",
        [
            "ends with a backslash \\",
            "quote \\' escape",
            "\\\\'; DROP TABLE x; --",
            "mixed ' \\ ''",
        ],
    )
    def test_backslash_quotes_round_trip(self, text: str) -> None:
        """[review #3] Model/metadata text with \\' cannot end the literal early."""
        record = make_record(
            response={"plan": {"summary": {"text": text, "citations": ["alert:x"]}}},
            model=text,
        )
        values = _spark_values(audit_sync.audit_row_sql(record))
        row = dict(zip([c.strip() for c in audit_sync.COLUMNS.split(",")], values, strict=True))
        assert row["plan_summary"] == text and row["model"] == text
        assert json.loads(row["plan"])["summary"]["text"] == text

    def test_people_are_pseudonymised_in_the_mirror(self, keyed) -> None:
        values = _spark_values(audit_sync.audit_row_sql(make_record(applied_by="qa@example.com")))
        row = dict(zip([c.strip() for c in audit_sync.COLUMNS.split(",")], values, strict=True))
        assert row["requested_by"].startswith("person-") and row["applied_by"].startswith("person-")
        assert "example.com" not in json.dumps(row)

    @pytest.mark.asyncio
    async def test_sync_replaces_rows_and_skips_empty(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_CATALOG", "cat")
        monkeypatch.setenv("UC_SCHEMA", "pharma_twin_sch")
        monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "wh")
        run = AsyncMock(return_value={})
        monkeypatch.setattr(audit_sync.ics, "execute_dbsql", run)
        assert await audit_sync.sync_genai_audit([]) == {"rows": 0, "skipped": True}
        run.assert_not_called()
        result = await audit_sync.sync_genai_audit([make_record(), make_record()])
        assert result == {"rows": 2, "skipped": False}
        sql = run.await_args_list[0].args[0]
        assert sql.startswith("INSERT OVERWRITE TABLE cat.pharma_twin_sch.intelligence_genai_audit")
        table = await audit_sync.provision_audit_table()
        assert table == "cat.pharma_twin_sch.intelligence_genai_audit"
        assert (
            "CREATE TABLE IF NOT EXISTS cat.pharma_twin_sch.intelligence_genai_audit"
            in (run.await_args_list[-1].args[0])
        )

    @pytest.mark.asyncio
    async def test_synchronizer_marks_clean_after_a_sync(self, monkeypatch) -> None:
        from iot_twin.core.state import AppState

        for key, value in {
            "UC_CATALOG": "cat",
            "UC_SCHEMA": "pharma_twin_sch",
            "DATABRICKS_SQL_WAREHOUSE_ID": "wh",
            "GENIE_SPACE_ID": "space",
        }.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setattr(audit_sync.ics, "execute_dbsql", AsyncMock(return_value={}))
        state = AppState()
        syncer = audit_sync.GenaiAuditSynchronizer(state, interval_s=1)
        state.genai_audit_synchronizer = syncer
        await GenaiAuditStore().record(make_record())  # a different store: nothing to copy
        audit_sync.mark_audit_dirty(state)
        assert syncer.dirty
        assert await syncer.run_once() == {"rows": 0, "skipped": True}
        assert not syncer.dirty
