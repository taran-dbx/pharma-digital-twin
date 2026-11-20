"""Alert / alert_event Lakebase DDL tests [qa]."""

from __future__ import annotations

import pytest

from iot_twin.models import app_schema


def _compact(sql: str) -> str:
    return " ".join(sql.split())


class TestAlertDdlContract:
    def test_alert_table_is_idempotent_with_status_severity_checks_c7401_smoke(
        self,
    ) -> None:
        """[C7401][smoke] Alert DDL is idempotent and constrains status/severity."""
        ddl = _compact(app_schema.ALERT_DDL)
        assert "CREATE TABLE IF NOT EXISTS alert" in ddl
        assert "CHECK (status IN ('open', 'acknowledged', 'cleared'))" in ddl
        assert "CHECK (severity IN ('info', 'warn', 'crit'))" in ddl
        assert "fingerprint TEXT NOT NULL UNIQUE" in ddl
        assert "version INTEGER NOT NULL DEFAULT 0" in ddl

    def test_alert_event_table_is_append_only_c7401_regression(self) -> None:
        """[C7401][regression] alert_event is append-only with typed lifecycle events."""
        ddl = _compact(app_schema.ALERT_EVENT_DDL)
        assert "CREATE TABLE IF NOT EXISTS alert_event" in ddl
        assert "updated_at" not in ddl
        assert "CHECK (event_type IN ('opened', 'updated', 'acknowledged', 'cleared'))" in ddl
        assert "REFERENCES alert(id)" in ddl

    def test_alert_indexes_are_idempotent_c7401_smoke(self) -> None:
        """[C7401][smoke] Alert indexes use IF NOT EXISTS."""
        for stmt in (*app_schema.ALERT_INDEXES, *app_schema.ALERT_EVENT_INDEXES):
            compact = _compact(stmt)
            assert "CREATE" in compact
            assert "IF NOT EXISTS" in compact
        event_sql = " ".join(_compact(s) for s in app_schema.ALERT_EVENT_INDEXES)
        assert "fingerprint" in event_sql


class TestAlertSchemaLive:
    @pytest.mark.asyncio
    async def test_ensure_creates_alert_tables_c7401_regression(self, app_backend_conn) -> None:
        """[C7401][regression] ensure_app_schema creates alert and alert_event."""
        await app_schema.ensure_app_schema(app_backend_conn)
        schema = await app_backend_conn.fetchval("SELECT current_schema()")
        for table in ("alert", "alert_event"):
            exists = await app_backend_conn.fetchval(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = $2",
                schema,
                table,
            )
            assert exists

    @pytest.mark.asyncio
    async def test_alert_status_check_rejects_invalid_c7401_defect(self, app_backend_conn) -> None:
        """[C7401][defect] Invalid status/severity fail closed."""
        await app_schema.ensure_app_schema(app_backend_conn)
        with pytest.raises(Exception):
            await app_backend_conn.execute(
                "INSERT INTO alert (fingerprint, rule_id, severity, status) "
                "VALUES ('fp-bad', 'rule', 'crit', 'nope')"
            )
        with pytest.raises(Exception):
            await app_backend_conn.execute(
                "INSERT INTO alert (fingerprint, rule_id, severity, status) "
                "VALUES ('fp-bad2', 'rule', 'fatal', 'open')"
            )

    @pytest.mark.asyncio
    async def test_alert_event_fingerprint_is_unique_c7401_regression(
        self, app_backend_conn
    ) -> None:
        """[C7401][regression] Duplicate event fingerprints are rejected."""
        await app_schema.ensure_app_schema(app_backend_conn)
        alert_id = await app_backend_conn.fetchval(
            "INSERT INTO alert (fingerprint, rule_id, severity, status) "
            "VALUES ('fp-1', 'rule-1', 'warn', 'open') RETURNING id"
        )
        await app_backend_conn.execute(
            "INSERT INTO alert_event (alert_id, event_type, fingerprint) "
            "VALUES ($1, 'opened', 'evt-fp-1')",
            alert_id,
        )
        with pytest.raises(Exception):
            await app_backend_conn.execute(
                "INSERT INTO alert_event (alert_id, event_type, fingerprint) "
                "VALUES ($1, 'opened', 'evt-fp-1')",
                alert_id,
            )

    @pytest.mark.asyncio
    async def test_ensure_alert_schema_is_idempotent_c7401_smoke(self, app_backend_conn) -> None:
        """[C7401][smoke] Re-running ensure_app_schema does not raise."""
        await app_schema.ensure_app_schema(app_backend_conn)
        await app_schema.ensure_app_schema(app_backend_conn)
