"""Shared UC intelligence catalog provisioning — mocked DBSQL only [qa]."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from iot_twin.services import intelligence_catalog_service as ics

CATALOG = "test_catalog"
SCHEMA = "test_schema"


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", CATALOG)
    monkeypatch.setenv("UC_SCHEMA", SCHEMA)
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "wh-1")


class TestNames:
    def test_tables_use_configured_catalog_schema_c7503_smoke(self) -> None:
        """[C7503][smoke] Reference table FQNs come from UC_CATALOG/UC_SCHEMA."""
        tables = ics.reference_tables()
        assert tables["assets"] == f"{CATALOG}.{SCHEMA}.intelligence_assets"
        assert tables["sensor_bindings"] == f"{CATALOG}.{SCHEMA}.intelligence_sensor_bindings"
        assert (
            tables["maintenance_records"] == f"{CATALOG}.{SCHEMA}.intelligence_maintenance_records"
        )
        assert tables["alert_rules"] == f"{CATALOG}.{SCHEMA}.intelligence_alert_rules"
        assert tables["alert_snapshot"] == f"{CATALOG}.{SCHEMA}.intelligence_alert_snapshot"

    def test_no_hardcoded_catalog_in_source_c7503_defect(self) -> None:
        """[C7503][defect] Service source does not hardcode workspace catalog/schema."""
        from pathlib import Path

        src = Path(ics.__file__).read_text(encoding="utf-8")
        assert "example-catalog" not in src
        assert "classic_stable" not in src
        assert "rtm_lakets_zb" not in src


class TestProvision:
    @pytest.mark.asyncio
    async def test_provision_creates_delta_tables_c7503_regression(self) -> None:
        """[C7503][regression] Provision emits Delta DDL for all reference tables."""
        ws = MagicMock()
        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            result = await ics.provision_reference_tables(ws=ws, warehouse_id="wh-1")

        assert result["catalog"] == CATALOG
        assert result["schema"] == SCHEMA
        joined = "\n".join(executed)
        for name in ics.TABLE_NAMES:
            assert "CREATE TABLE IF NOT EXISTS" in joined
            assert name in joined
            assert "USING DELTA" in joined
        assert "intelligence_alert_snapshot" in joined
        assert result["tables"]["alert_snapshot"].endswith("intelligence_alert_snapshot")

    @pytest.mark.asyncio
    async def test_sync_writes_catalog_rows_c7503_regression(self) -> None:
        """[C7503][regression] Sync loads domain catalog rows into shared tables."""
        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        alerts = [
            {
                "id": "a1",
                "industry": "manufacturing",
                "rule_id": "mfg-alert-spindle-temp",
                "status": "open",
                "severity": "crit",
                "sensor_id": "mfg-sns-cnc_1_spindle_temp",
            }
        ]
        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            result = await ics.sync_reference_data(alerts=alerts, warehouse_id="wh-1")

        joined = "\n".join(executed)
        assert "INSERT OVERWRITE TABLE" in joined
        assert "mfg-asset-cnc-1" in joined
        assert "mfg-alert-spindle-temp" in joined
        assert "a1" in joined
        assert result["rows"]["assets"] > 0
        assert result["rows"]["alert_rules"] > 0
        assert result["rows"]["alert_snapshot"] == 1

    def test_alert_snapshot_is_read_only_contract_c7503_usability(self) -> None:
        """[C7503][usability] Alert snapshot contract is documented as read-only."""
        contract = ics.alert_snapshot_contract()
        assert contract["table"].endswith("intelligence_alert_snapshot")
        assert contract["access"] == "read_only"
        assert "freshness" in contract
        assert "last_sync_at" in contract
        assert "last_error" in contract
        assert "mutate" in contract["notes"].lower() or "read" in contract["notes"].lower()

    @pytest.mark.asyncio
    async def test_hyphenated_catalog_is_quoted_c7503_defect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7503][defect] Hyphenated UC catalogs are quoted, not split-fragile."""
        monkeypatch.setenv("UC_CATALOG", "example-catalog")
        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            await ics.sync_reference_data(alerts=[], warehouse_id="wh-1")

        joined = "\n".join(executed)
        assert "`example-catalog`" in joined
        assert "INSERT OVERWRITE TABLE `example-catalog`.test_schema.intelligence_assets" in joined

    @pytest.mark.asyncio
    async def test_replace_batches_values_c7503_regression(self) -> None:
        """[C7503][regression] Snapshot SQL is batched instead of one unbounded VALUES list."""
        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        alerts = [
            {
                "id": f"a{i}",
                "industry": "manufacturing",
                "rule_id": "r",
                "status": "open",
                "severity": "warn",
                "sensor_id": "s",
            }
            for i in range(ics._VALUES_BATCH + 5)
        ]
        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            await ics.sync_reference_data(alerts=alerts, warehouse_id="wh-1")
        snapshot_sqls = [sql for sql in executed if "intelligence_alert_snapshot" in sql]
        assert any("INSERT OVERWRITE" in sql for sql in snapshot_sqls)
        assert any(sql.startswith("INSERT INTO") for sql in snapshot_sqls)
