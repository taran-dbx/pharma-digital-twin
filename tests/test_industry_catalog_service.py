"""Tests for the one-schema / managed-table / single-SP catalog service (Task 3).

Provisioning targets a PRE-EXISTING UC catalog: it creates the single schema
(never the catalog) and MANAGED industry-prefixed Delta tables, then grants only
the single app SP.  Unit tests mock the ``WorkspaceClient`` and the DBSQL executor.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services.industry import industry_catalog_service as ics

CATALOG = "example_eu_catalog"
SCHEMA = "rtm_lakets_zb"


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", CATALOG)
    monkeypatch.setenv("UC_SCHEMA", SCHEMA)


class _NotFound(Exception):
    def __str__(self) -> str:  # pragma: no cover - trivial
        return "RESOURCE_DOES_NOT_EXIST: not found"


def _make_ws(*, ext_loc=True, has_warehouse=True):
    ws = MagicMock()
    if ext_loc:
        ws.external_locations.get = MagicMock(return_value=MagicMock())
    else:
        ws.external_locations.get = MagicMock(side_effect=_NotFound())
    if has_warehouse:
        wh = MagicMock()
        wh.id = "wh-1"
        ws.warehouses.list = MagicMock(return_value=[wh])
    else:
        ws.warehouses.list = MagicMock(return_value=[])
    ws.catalogs.create = MagicMock(return_value=MagicMock())
    ws.schemas.create = MagicMock(return_value=MagicMock())
    return ws


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------
class TestNames:
    def test_catalog_name_sanitizes(self) -> None:
        assert ics.catalog_name("Pharmaceutical") == "iot_pharmaceutical"
        assert ics.catalog_name("Oil & Gas") == "iot_oil_gas"

    def test_industry_table_builds_prefixed_fqn(self) -> None:
        assert ics.industry_table("pharmaceutical", "zerobus") == (
            f"{CATALOG}.{SCHEMA}.pharmaceutical_zerobus"
        )
        assert ics.industry_table("Oil & Gas", "metrics") == (f"{CATALOG}.{SCHEMA}.oil_gas_metrics")

    def test_industry_table_rejects_unknown_kind(self) -> None:
        with pytest.raises(ValueError):
            ics.industry_table("pharmaceutical", "bronze")

    def test_uc_catalog_and_schema_required(self, monkeypatch) -> None:
        monkeypatch.delenv("UC_CATALOG", raising=False)
        with pytest.raises(RuntimeError):
            ics.industry_table("pharmaceutical", "zerobus")


# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
class TestPreflight:
    def test_all_pass_without_external_location(self, monkeypatch) -> None:
        monkeypatch.delenv("UC_EXTERNAL_LOCATION", raising=False)
        checks = ics.preflight_checks(ws=_make_ws(ext_loc=False))
        assert all(c["status"] == "pass" for c in checks)
        ics.raise_on_failed_preflight(checks)  # no raise

    def test_missing_warehouse_fails(self) -> None:
        checks = ics.preflight_checks(ws=_make_ws(has_warehouse=False))
        assert any(c["status"] == "fail" and "warehouse" in c["check"].lower() for c in checks)


# ---------------------------------------------------------------------------
# provision_industry_catalog — schema (not catalog) + managed tables
# ---------------------------------------------------------------------------
class TestProvisionCatalog:
    @pytest.mark.asyncio
    async def test_creates_schema_never_catalog(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.provision_industry_catalog("pharmaceutical", ws=ws)
        all_sql = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        assert f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}" in all_sql
        assert "CREATE CATALOG" not in all_sql.upper()
        ws.catalogs.create.assert_not_called()
        assert res["catalog"] == CATALOG and res["schema"] == SCHEMA

    @pytest.mark.asyncio
    async def test_creates_managed_tables_no_location(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.provision_industry_catalog("pharmaceutical", ws=ws)
        all_sql = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        for kind in ics.TABLE_KINDS:
            assert f"pharmaceutical_{kind}" in all_sql
        assert "LOCATION" not in all_sql.upper()
        assert "USING DELTA" in all_sql
        assert set(res["tables"]) == set(ics.TABLE_KINDS)

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

    @pytest.mark.asyncio
    async def test_records_resources_in_ledger(self) -> None:
        ws = _make_ws()
        recorded: list[dict] = []

        async def _rec(_db, **kw):
            recorded.append(kw)

        with (
            patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})),
            patch.object(ics, "record_resource", side_effect=_rec),
        ):
            await ics.provision_industry_catalog(
                "pharmaceutical",
                ws=ws,
                db=MagicMock(),
                setup_id="s1",
            )
        types = [r["resource_type"] for r in recorded]
        assert "uc_schema" in types
        assert types.count("uc_table") == len(ics.TABLE_KINDS)


# ---------------------------------------------------------------------------
# Grants — single app SP
# ---------------------------------------------------------------------------
class TestGrants:
    @pytest.mark.asyncio
    async def test_grants_single_app_sp_on_schema(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.grant_app_sp("pharmaceutical", ws=ws, principal="app-123")
        sqls = "\n".join(c.args[0] for c in exec_mock.call_args_list)
        assert f"USE CATALOG ON CATALOG {CATALOG}" in sqls
        for priv in (
            "USE SCHEMA",
            "EXTERNAL USE SCHEMA",
            "CREATE TABLE",
            "MODIFY",
            "SELECT",
            "READ SECRET",
        ):
            assert priv in sqls
        assert f"ON SCHEMA {CATALOG}.{SCHEMA}" in sqls
        assert (
            f"GRANT SELECT, MODIFY ON TABLE "
            f"{ics.sql_fqn(CATALOG, SCHEMA, 'pharmaceutical_metrics')}"
        ) in sqls
        assert "`app-123`" in sqls
        assert res["principal"] == "app-123"

    @pytest.mark.asyncio
    async def test_grants_recorded_in_ledger(self) -> None:
        ws = _make_ws()
        recorded: list[dict] = []

        async def _rec(_db, **kw):
            recorded.append(kw)

        with (
            patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})),
            patch.object(ics, "record_resource", side_effect=_rec),
        ):
            await ics.grant_app_sp(
                "pharmaceutical",
                ws=ws,
                principal="app-123",
                db=MagicMock(),
                setup_id="s1",
            )
        assert recorded and all(r["resource_type"] == "grant" for r in recorded)

    @pytest.mark.asyncio
    async def test_grant_skips_when_principal_unresolved(self, monkeypatch) -> None:
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        ws = MagicMock()
        ws.current_user.me.side_effect = Exception("no identity")
        res = await ics.grant_app_sp("pharmaceutical", ws=ws)
        assert res["principal"] is None
        assert res["results"][0]["status"] == "skipped"

    @pytest.mark.asyncio
    async def test_catalog_level_permission_denied_is_skipped_not_failed(self) -> None:
        """PERMISSION_DENIED on USE CATALOG must be recorded as 'skipped', not 'failed'.

        The app SP is not the catalog owner — the deployer grants USE CATALOG
        out-of-band.  A PERMISSION_DENIED on the catalog-level grant must never
        block provisioning; schema-level grants must still run and succeed.
        """
        ws = _make_ws()

        call_count = 0

        async def _exec_mock(sql: str, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if "USE CATALOG ON CATALOG" in sql:
                raise Exception("PERMISSION_DENIED: User does not have MANAGE on Catalog")
            # All other grants succeed (return None)

        with patch.object(ics, "execute_dbsql", new=_exec_mock):
            res = await ics.grant_app_sp("pharmaceutical", ws=ws, principal="app-sp-id")

        # Catalog-level grant → skipped (not failed)
        catalog_result = next(r for r in res["results"] if "USE CATALOG" in r["resource"])
        assert catalog_result["status"] == "skipped", (
            f"Expected 'skipped', got {catalog_result['status']!r}: {catalog_result}"
        )
        assert "out-of-band" in catalog_result.get("reason", "")

        # Schema-level grants must still have run and succeeded
        schema_results = [r for r in res["results"] if "USE CATALOG" not in r["resource"]]
        assert schema_results, "No schema-level grant results recorded"
        for r in schema_results:
            assert r["status"] != "failed", f"Schema-level grant unexpectedly failed: {r}"

    def test_resolve_app_principal_prefers_client_id(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid-123")
        assert ics.resolve_app_principal(MagicMock()) == "cid-123"

    def test_health_grants_use_current_app_principal_c7214(self, monkeypatch) -> None:
        """Legacy SP discovery must not override the single app SP [C7214][defect]."""
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "current-app-sp")
        ws = MagicMock()
        ws.service_principals.list.side_effect = AssertionError(
            "legacy service-principal discovery must not run"
        )

        assert ics.resolve_streaming_sp_principal(ws) == "current-app-sp"


# ---------------------------------------------------------------------------
# Secret scope
# ---------------------------------------------------------------------------
class TestSecretScope:
    @pytest.mark.asyncio
    async def test_creates_scope_and_records_shared(self) -> None:
        ws = MagicMock()
        ws.secrets.create_scope = MagicMock(return_value=None)
        recorded: list[dict] = []

        async def _rec(_db, **kw):
            recorded.append(kw)

        with patch.object(ics, "record_resource", side_effect=_rec):
            res = await ics.ensure_secret_scope(ws=ws, db=MagicMock(), setup_id="s1")
        assert res["status"] == "created"
        assert recorded[0]["resource_type"] == "secret_scope"

    @pytest.mark.asyncio
    async def test_scope_already_exists(self) -> None:
        ws = MagicMock()
        ws.secrets.create_scope = MagicMock(side_effect=Exception("scope already exists"))
        res = await ics.ensure_secret_scope(ws=ws)
        assert res["status"] == "already_exists"
