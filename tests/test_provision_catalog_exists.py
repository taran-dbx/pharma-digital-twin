"""Task 3: provision into the PRE-EXISTING catalog.

The workspace catalog already exists and is owned by another SP, so
provisioning must NEVER ``CREATE CATALOG``.  It creates one schema
(``CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}``) and a set of MANAGED,
industry-prefixed Delta tables (no ``LOCATION``), then grants only the single
app SP and writes the app SP's OAuth secret as a UC secret.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services.industry import industry_catalog_service as ics
from iot_twin.services.industry import industry_provisioning_service as ips

CATALOG = "example_eu_catalog"
SCHEMA = "rtm_lakets_zb"
APP_SP = "00000000-0000-0000-0000-00000000000a"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", CATALOG)
    monkeypatch.setenv("UC_SCHEMA", SCHEMA)
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", APP_SP)


def _make_ws():
    ws = MagicMock()
    wh = MagicMock()
    wh.id = "wh-1"
    ws.warehouses.list = MagicMock(return_value=[wh])
    ws.schemas.create = MagicMock(return_value=MagicMock())
    ws.catalogs.create = MagicMock(return_value=MagicMock())
    return ws


class TestIndustryTable:
    def test_builds_industry_prefixed_fqn_in_one_schema(self) -> None:
        assert ics.industry_table("pharmaceutical", "zerobus") == (
            f"{CATALOG}.{SCHEMA}.pharmaceutical_zerobus"
        )
        assert ics.industry_table("pharmaceutical", "metrics") == (
            f"{CATALOG}.{SCHEMA}.pharmaceutical_metrics"
        )
        assert ics.industry_table("Oil & Gas", "realtime") == (
            f"{CATALOG}.{SCHEMA}.oil_gas_realtime"
        )

    def test_rejects_unknown_kind(self) -> None:
        with pytest.raises(ValueError):
            ics.industry_table("pharmaceutical", "bronze")


class TestProvisionSchemaAndTables:
    @pytest.mark.asyncio
    async def test_hyphenated_catalog_sql_is_quoted(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_CATALOG", "example-catalog")
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            await ics.provision_industry_catalog("pharmaceutical", ws=ws)
        all_sql = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        assert "CREATE SCHEMA IF NOT EXISTS `example-catalog`.rtm_lakets_zb" in all_sql
        assert (
            "CREATE TABLE IF NOT EXISTS `example-catalog`.rtm_lakets_zb.pharmaceutical_zerobus"
            in all_sql
        )

    @pytest.mark.asyncio
    async def test_creates_schema_if_not_exists_never_catalog(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.provision_industry_catalog("pharmaceutical", ws=ws)
        all_sql = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        assert f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}" in all_sql
        # The catalog pre-exists (owned by another SP) — never create it.
        assert "CREATE CATALOG" not in all_sql.upper()
        ws.catalogs.create.assert_not_called()
        assert res["catalog"] == CATALOG
        assert res["schema"] == SCHEMA

    @pytest.mark.asyncio
    async def test_creates_managed_industry_tables_with_no_location(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.provision_industry_catalog("pharmaceutical", ws=ws)
        all_sql = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        for kind in ("zerobus", "metrics", "realtime", "microbatch"):
            assert f"pharmaceutical_{kind}" in all_sql, f"missing table for {kind}"
        # Managed tables — never an external LOCATION clause.
        assert "LOCATION" not in all_sql.upper()
        assert "USING DELTA" in all_sql
        assert res["tables"]["zerobus"] == f"{CATALOG}.{SCHEMA}.pharmaceutical_zerobus"

    @pytest.mark.asyncio
    async def test_metrics_table_has_mode_column(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            await ics.provision_industry_catalog("pharmaceutical", ws=ws)
        metrics_ddl = next(
            c.args[0] for c in exec_mock.call_args_list if "pharmaceutical_metrics" in c.args[0]
        )
        assert "mode STRING" in metrics_ddl
        assert "LOCATION" not in metrics_ddl.upper()


class TestSingleSpGrants:
    @pytest.mark.asyncio
    async def test_hyphenated_catalog_grant_sql_is_quoted(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_CATALOG", "example-catalog")
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            await ics.grant_app_sp("pharmaceutical", ws=ws, principal=APP_SP)
        sqls = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        assert "USE CATALOG ON CATALOG `example-catalog`" in sqls
        assert "ON SCHEMA `example-catalog`.rtm_lakets_zb" in sqls

    @pytest.mark.asyncio
    async def test_grants_only_the_app_sp_on_the_schema(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.grant_app_sp("pharmaceutical", ws=ws, principal=APP_SP)
        sqls = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        assert f"USE CATALOG ON CATALOG {CATALOG}" in sqls
        assert f"ON SCHEMA {CATALOG}.{SCHEMA}" in sqls
        for priv in ("USE SCHEMA", "CREATE TABLE", "MODIFY", "SELECT", "READ SECRET"):
            assert priv in sqls, f"missing grant {priv}"
        # Only the single app SP appears as a grant principal.
        assert f"`{APP_SP}`" in sqls
        principals = {r["resource"].split("→")[-1].strip() for r in res["results"]}
        assert principals == {APP_SP}


class TestOrchestratorWritesUcSecret:
    @pytest.mark.asyncio
    async def test_writes_app_sp_oauth_uc_secret_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "runtime-oauth-secret")
        ws = _make_ws()
        put_mock = AsyncMock(return_value={"status": "created"})
        with (
            patch.object(ips.cat, "preflight_checks", return_value=[]),
            patch.object(
                ips.cat,
                "provision_industry_catalog",
                new=AsyncMock(
                    return_value={
                        "catalog": CATALOG,
                        "schema": SCHEMA,
                        "results": [],
                        "tables": {},
                        "metrics_table": f"{CATALOG}.{SCHEMA}.x_metrics",
                    }
                ),
            ),
            patch.object(
                ips.cat,
                "grant_app_sp",
                new=AsyncMock(return_value={"catalog": CATALOG, "results": []}),
            ),
            patch.object(ips.uc_secrets, "put_secret", new=put_mock),
        ):
            summary = await ips.provision_industry(
                "pharmaceutical",
                None,
                ws=ws,
                enable_realtime=False,
            )
        put_mock.assert_awaited_once()
        kwargs = put_mock.await_args.kwargs
        args = put_mock.await_args.args
        # Accept positional or keyword call styles.
        called = {**dict(zip(("catalog", "schema", "name", "value"), args, strict=False)), **kwargs}
        assert called["catalog"] == CATALOG
        assert called["schema"] == SCHEMA
        assert called["name"] == "app_sp_oauth"
        assert called["value"] == "runtime-oauth-secret"
        phases = [s["phase"] for s in summary["sections"]]
        assert "uc_secret" in phases
