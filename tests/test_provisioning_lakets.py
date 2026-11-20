"""Tests for provision_lakebase_table's LakeTS install integration (Phase 1.3)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services import provisioning_service
from tests._fakes import fake_credential


def _stub_conn() -> MagicMock:
    """Return a mocked asyncpg connection."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value="0.1.0")
    conn.close = AsyncMock(return_value=None)
    return conn


class TestProvisionLakebaseTableInstallsLakeTs:
    """After the base DDL, provision_lakebase_table must install LakeTS."""

    @pytest.mark.asyncio
    async def test_installs_lakets_and_reports_version(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS body\n")

        conn = _stub_conn()
        # First fetchval call (skip_if_installed probe) returns None, second returns version.
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0"])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                }
            )

        assert result["status"] == "created"
        assert result.get("lakets_version") == "0.1.0"
        # Two execute calls: base DDL + LakeTS body.
        execute_calls = [call.args[0] for call in conn.execute.await_args_list]
        assert any("sensor_readings" in sql for sql in execute_calls)
        assert any("LakeTS body" in sql for sql in execute_calls)

    @pytest.mark.asyncio
    async def test_skips_lakets_when_flag_false(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS body\n")

        conn = _stub_conn()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                    "install_lakets": False,
                }
            )

        assert result["status"] == "created"
        assert "lakets_version" not in result
        executed = [call.args[0] for call in conn.execute.await_args_list]
        assert not any("LakeTS body" in sql for sql in executed)

    @pytest.mark.asyncio
    async def test_lakets_failure_surfaces_as_warning_not_hard_fail(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.sql"

        conn = _stub_conn()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(missing),
                }
            )

        assert result["status"] == "created"
        assert "lakets_error" in result
        assert "LakeTS SQL not found" in result["lakets_error"]


class TestProvisionLakebaseTableAppliesTiering:
    """When lb.tiering is supplied, ChronoTable + tier policy are applied."""

    @pytest.mark.asyncio
    async def test_applies_chronotable_and_policy(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        # fetchval sequence: install probe=None, post-install version=0.1.0,
        # chronotable probe=None, chronotable create=1,
        # policy probe=None, policy apply=1
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0", None, 1, None, 1])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                    "tiering": {
                        "hot": "1 hour",
                        "drop": "7 days",
                        "chunk": "15 minutes",
                    },
                }
            )

        assert result["status"] == "created"
        assert result.get("lakets_version") == "0.1.0"
        assert result.get("tiering_applied") == {
            "hot": "1 hour",
            "drop": "7 days",
            "chunk": "15 minutes",
        }

    @pytest.mark.asyncio
    async def test_tiering_absent_skips_chronotable(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0"])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                }
            )

        assert result.get("lakets_version") == "0.1.0"
        assert "tiering_applied" not in result
        assert "tiering_error" not in result

    @pytest.mark.asyncio
    async def test_tiering_error_does_not_fail_provision(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        # install probe None, version "0.1.0", chronotable probe raises
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0", RuntimeError("registry unavailable")])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                    "tiering": {"hot": "1 hour", "drop": "7 days"},
                }
            )

        assert result["status"] == "created"
        assert result.get("lakets_version") == "0.1.0"
        assert "tiering_error" in result
        assert "registry unavailable" in result["tiering_error"]


class TestProvisionLakebaseTableEnablesSync:
    """When LakeTS/tiering is selected, enable_sync runs after the tier policy."""

    @pytest.mark.asyncio
    async def test_full_lakets_setup_calls_enable_sync_in_order(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0", None, 1, None, 1])

        order: list[str] = []

        async def _install(c, path, **kw):
            order.append("install")
            return "0.1.0"

        async def _create(c, **kw):
            order.append("create_chronotable")
            return False

        async def _policy(c, **kw):
            order.append("apply_tier_policy")
            return False

        async def _sync(c, table, schema="public"):
            order.append("enable_sync")
            return {"resource": f"LakeTS sync on {schema}.{table}", "status": "created"}

        with (
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
            patch.object(provisioning_service.lakets_service, "install_lakets", new=_install),
            patch.object(provisioning_service.lakets_service, "create_chronotable", new=_create),
            patch.object(provisioning_service.lakets_service, "apply_tier_policy", new=_policy),
            patch.object(provisioning_service.lakets_service, "enable_sync", new=_sync),
        ):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                    "tiering": {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"},
                }
            )

        assert order == [
            "install",
            "create_chronotable",
            "apply_tier_policy",
            "enable_sync",
        ]
        assert result["status"] == "created"
        assert result.get("sync_enabled") == {
            "sensor_readings": "created",
        }

    @pytest.mark.asyncio
    async def test_enable_sync_idempotent_already_enabled(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0", None, 1, None, 1])

        async def _sync(c, table, schema="public"):
            return {"resource": f"LakeTS sync on {schema}.{table}", "status": "already_exists"}

        with (
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
            patch.object(provisioning_service.lakets_service, "enable_sync", new=_sync),
        ):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                    "tiering": {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"},
                }
            )

        assert result["status"] == "created"
        assert result.get("sync_enabled") == {"sensor_readings": "already_exists"}
        # An idempotent enable_sync is NOT a failure.
        assert "sync_error" not in result

    @pytest.mark.asyncio
    async def test_enable_sync_failure_is_non_fatal(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0", None, 1, None, 1])

        async def _sync(c, table, schema="public"):
            return {
                "resource": f"LakeTS sync on {schema}.{table}",
                "status": "failed",
                "error": "shadow build failed",
            }

        with (
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
            patch.object(provisioning_service.lakets_service, "enable_sync", new=_sync),
        ):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                    "tiering": {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"},
                }
            )

        # Provisioning still succeeds; the failure is recorded, not raised.
        assert result["status"] == "created"
        assert result.get("sync_enabled") == {"sensor_readings": "failed"}
        assert "shadow build failed" in result.get("sync_error", "")

    @pytest.mark.asyncio
    async def test_no_tiering_skips_enable_sync(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.0"])

        sync_mock = AsyncMock(return_value={"resource": "x", "status": "created"})

        with (
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
            patch.object(provisioning_service.lakets_service, "enable_sync", new=sync_mock),
        ):
            result = await provisioning_service.provision_lakebase_table(
                {
                    "mode": "direct",
                    "host": "pg.example",
                    "user": "u",
                    "password": fake_credential("lakebase"),
                    "port": 5432,
                    "pg_table": "sensor_readings",
                    "lakets_sql_path": str(sql_file),
                }
            )

        sync_mock.assert_not_awaited()
        assert "sync_enabled" not in result
