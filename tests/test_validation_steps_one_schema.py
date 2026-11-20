"""Focused unit tests asserting that zerobus validation steps use the
one-schema table layout ({UC_CATALOG}.{UC_SCHEMA}.{industry}_zerobus)
rather than the old per-industry-catalog layout (iot_{industry}.zerobus.*).

No cluster, no Kafka, no live DB — DBSQL is mocked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from iot_twin.services.validation import validation_state_machine as sm
from iot_twin.services.validation import validation_steps as vsteps
from iot_twin.services.validation.validation_service import StepContext

UC_CATALOG = "example_eu_catalog"
UC_SCHEMA = "rtm_lakets_zb"
INDUSTRY = "pharmaceutical"


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch):
    """Pin UC_CATALOG / UC_SCHEMA so industry_table() is deterministic."""
    monkeypatch.setenv("UC_CATALOG", UC_CATALOG)
    monkeypatch.setenv("UC_SCHEMA", UC_SCHEMA)


def _make_ctx(*, mode: str | None = None) -> StepContext:
    """Minimal StepContext for the zerobus steps under test."""
    setup = {
        "industry": INDUSTRY,
        "enabled_modes": ["zerobus"],
        "warehouse_id": "wh-test-1",
        "bootstrap_servers": "",  # not needed for zerobus
    }
    return StepContext(
        db=None,
        setup=setup,
        state={"cluster_id": "cl-1"},
        key=f"zerobus:{mode}" if mode else "zerobus:step",
        step=mode or "step",
        mode=mode,
    )


EXPECTED_ZB_FQN = f"{UC_CATALOG}.{UC_SCHEMA}.{INDUSTRY}_zerobus"
OLD_ZEROBUS_PATTERN = f"iot_{INDUSTRY}.zerobus."


class TestZerobusCreateTableOneSchema:
    """create_table must target {UC_CATALOG}.{UC_SCHEMA}.{industry}_zerobus."""

    @pytest.mark.asyncio
    async def test_create_table_uses_new_layout_fqn(self):
        exec_mock = AsyncMock(return_value={"data": [], "status": "ok"})
        with patch.object(vsteps, "execute_dbsql", new=exec_mock):
            result = await vsteps.create_table(_make_ctx(mode="create_table"))

        assert result.status == sm.PASSED
        assert result.data["table"] == EXPECTED_ZB_FQN

        # The DDL sent to DBSQL must reference the new FQN.
        ddl_arg: str = exec_mock.call_args[0][0]
        assert EXPECTED_ZB_FQN in ddl_arg, (
            f"Expected DDL to contain '{EXPECTED_ZB_FQN}' but got:\n{ddl_arg}"
        )

    @pytest.mark.asyncio
    async def test_create_table_never_uses_old_layout(self):
        exec_mock = AsyncMock(return_value={"data": [], "status": "ok"})
        with patch.object(vsteps, "execute_dbsql", new=exec_mock):
            await vsteps.create_table(_make_ctx(mode="create_table"))

        ddl_arg: str = exec_mock.call_args[0][0]
        assert OLD_ZEROBUS_PATTERN not in ddl_arg, (
            f"DDL still references old catalog layout: {ddl_arg}"
        )

    @pytest.mark.asyncio
    async def test_create_table_ddl_is_idempotent(self):
        """delta_table_ddl must emit CREATE TABLE IF NOT EXISTS."""
        exec_mock = AsyncMock(return_value={"data": [], "status": "ok"})
        with patch.object(vsteps, "execute_dbsql", new=exec_mock):
            await vsteps.create_table(_make_ctx(mode="create_table"))

        ddl_arg: str = exec_mock.call_args[0][0]
        assert "IF NOT EXISTS" in ddl_arg, (
            "create_table DDL is not idempotent — missing IF NOT EXISTS"
        )


class TestZerobusDbsqlReadOneSchema:
    """dbsql_read in zerobus mode must query {UC_CATALOG}.{UC_SCHEMA}.{industry}_zerobus."""

    @pytest.mark.asyncio
    async def test_dbsql_read_zerobus_uses_new_fqn(self):
        exec_mock = AsyncMock(return_value={"data": [{"n": 42}]})
        with patch.object(vsteps, "execute_dbsql", new=exec_mock):
            result = await vsteps.dbsql_read(_make_ctx(mode="zerobus"))

        assert result.status == sm.PASSED
        assert result.data["dbsql"] == EXPECTED_ZB_FQN
        assert result.data["rows"] == 42

        sql_arg: str = exec_mock.call_args[0][0]
        assert EXPECTED_ZB_FQN in sql_arg, (
            f"dbsql_read SQL does not reference '{EXPECTED_ZB_FQN}': {sql_arg}"
        )

    @pytest.mark.asyncio
    async def test_dbsql_read_zerobus_never_uses_old_layout(self):
        exec_mock = AsyncMock(return_value={"data": [{"n": 5}]})
        with patch.object(vsteps, "execute_dbsql", new=exec_mock):
            await vsteps.dbsql_read(_make_ctx(mode="zerobus"))

        sql_arg: str = exec_mock.call_args[0][0]
        assert OLD_ZEROBUS_PATTERN not in sql_arg, (
            f"dbsql_read SQL still references old catalog layout: {sql_arg}"
        )

    @pytest.mark.asyncio
    async def test_dbsql_read_zerobus_zero_rows_fails(self):
        # Zerobus dbsql_read polls to absorb the ingest→commit lag; stub sleep so
        # the bounded wait doesn't actually block the test.
        exec_mock = AsyncMock(return_value={"data": [{"n": 0}]})
        with (
            patch.object(vsteps, "execute_dbsql", new=exec_mock),
            patch.object(vsteps.asyncio, "sleep", new=AsyncMock()),
        ):
            result = await vsteps.dbsql_read(_make_ctx(mode="zerobus"))

        assert result.status == sm.FAILED
        assert EXPECTED_ZB_FQN in (result.error or "")
        assert str(vsteps._ZEROBUS_READ_WAIT_S) in (result.error or "")

    @pytest.mark.asyncio
    async def test_dbsql_read_zerobus_polls_until_rows_land(self):
        """Zerobus acks a record before the server commits it to the queryable
        Delta table, so an immediate read sees 0 rows.  dbsql_read must keep
        polling and PASS once the commit lands (LIVE-observed ~30s lag)."""
        # 0 rows on the first two polls, then the commit becomes visible.
        exec_mock = AsyncMock(
            side_effect=[
                {"data": [{"n": 0}]},
                {"data": [{"n": 0}]},
                {"data": [{"n": 100}]},
            ]
        )
        sleep_mock = AsyncMock()
        with (
            patch.object(vsteps, "execute_dbsql", new=exec_mock),
            patch.object(vsteps.asyncio, "sleep", new=sleep_mock),
        ):
            result = await vsteps.dbsql_read(_make_ctx(mode="zerobus"))

        assert result.status == sm.PASSED
        assert result.data["rows"] == 100
        assert exec_mock.await_count == 3
        # Slept between the three reads, but not after the final (successful) one.
        assert sleep_mock.await_count == 2
