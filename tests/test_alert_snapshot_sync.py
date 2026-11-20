"""Bounded UC alert-snapshot synchronizer [qa]."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from iot_twin.core.state import AppState
from iot_twin.services import intelligence_catalog_service as ics
from iot_twin.services.alerts import alert_snapshot_sync as snapshot_sync
from iot_twin.services.alerts.alert_engine import AlertEngine, evaluate_and_emit

CATALOG = "test_catalog"
SCHEMA = "test_schema"


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", CATALOG)
    monkeypatch.setenv("UC_SCHEMA", SCHEMA)
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "wh-1")
    monkeypatch.setenv("GENIE_SPACE_ID", "space-1")


class TestFailClosed:
    def test_missing_uc_or_genie_disables_sync_c7505_smoke(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7505][smoke] Snapshot sync fails closed without UC/warehouse/Genie config."""
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        syncer = snapshot_sync.AlertSnapshotSynchronizer(AppState())
        assert syncer.enabled is False
        assert syncer.last_error
        contract = ics.alert_snapshot_contract(syncer)
        assert contract["freshness"] in {"disabled", "stale"}
        assert contract["last_error"]


class TestEmptySnapshotSafety:
    @pytest.mark.asyncio
    async def test_empty_background_sync_does_not_delete_c7505_regression(self) -> None:
        """[C7505][regression] Empty snapshots keep the last-known-good table."""
        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            result = await snapshot_sync.sync_alert_snapshot(alerts=[], allow_empty=False)

        joined = "\n".join(executed)
        assert "DELETE FROM" not in joined
        assert "INSERT OVERWRITE TABLE" not in joined or "intelligence_alert_snapshot" not in joined
        assert result["skipped"] is True
        assert result["rows"] == 0

    @pytest.mark.asyncio
    async def test_admin_full_sync_may_clear_empty_c7505_defect(self) -> None:
        """[C7505][defect] Admin full sync may empty the snapshot when explicitly requested."""
        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            result = await snapshot_sync.sync_alert_snapshot(alerts=[], allow_empty=True)

        joined = "\n".join(executed)
        assert "DELETE FROM" in joined
        assert "intelligence_alert_snapshot" in joined
        assert result["skipped"] is False


class TestDirtyLifecycle:
    @pytest.mark.asyncio
    @pytest.mark.usefixtures("instant_alert_rules")
    async def test_lifecycle_marks_dirty_not_dbsql_c7505_regression(self) -> None:
        """[C7505][regression] Alert lifecycle marks the snapshot dirty instead of writing DBSQL."""
        state = AppState()
        syncer = snapshot_sync.AlertSnapshotSynchronizer(state)
        syncer.enabled = True
        state.alert_snapshot_synchronizer = syncer
        state.alert_engine = AlertEngine()
        with patch.object(ics, "execute_dbsql", new=AsyncMock()) as dbsql:
            await evaluate_and_emit(
                state,
                [
                    {
                        "sensor_id": "mfg-sns-cnc_1_spindle_temp",
                        "sensor_name": "cnc_1_spindle_temp",
                        "industry": "manufacturing",
                        "value": 91.0,
                    }
                ],
                industry="manufacturing",
            )
        assert syncer.dirty is True
        dbsql.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_once_copies_when_dirty_c7505_smoke(self) -> None:
        """[C7505][smoke] Dirty tick copies current alerts into UC."""
        state = AppState()
        engine = AlertEngine()
        state.alert_engine = engine
        syncer = snapshot_sync.AlertSnapshotSynchronizer(state, interval_s=60)
        syncer.enabled = True
        syncer.mark_dirty()
        with patch.object(
            snapshot_sync,
            "sync_alert_snapshot",
            new=AsyncMock(return_value={"rows": 1, "skipped": False}),
        ) as sync:
            await syncer.run_once()
        sync.assert_awaited()
        assert syncer.dirty is False
        assert syncer.last_sync_at is not None
        assert syncer.last_error is None

    @pytest.mark.asyncio
    async def test_dirty_event_during_sync_is_not_lost_c7505_defect(self) -> None:
        """[C7505][defect] A lifecycle mark during await must keep the snapshot dirty."""
        state = AppState()
        syncer = snapshot_sync.AlertSnapshotSynchronizer(state, interval_s=60)
        syncer.enabled = True
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_sync(**_kwargs):
            started.set()
            await release.wait()
            return {"rows": 1, "skipped": False}

        syncer.mark_dirty()
        with patch.object(snapshot_sync, "sync_alert_snapshot", side_effect=slow_sync):
            task = asyncio.create_task(syncer.run_once())
            await started.wait()
            syncer.mark_dirty()
            release.set()
            await task
        assert syncer.dirty is True

    @pytest.mark.asyncio
    async def test_full_sync_keeps_dirty_when_event_arrives_c7505_defect(self) -> None:
        """[C7505][defect] Admin full sync uses generation-aware clean, not dirty=False."""
        state = AppState()
        syncer = snapshot_sync.AlertSnapshotSynchronizer(state, interval_s=60)
        syncer.enabled = True
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_sync(**_kwargs):
            started.set()
            await release.wait()
            return {"rows": 1}

        syncer.mark_dirty()
        with (
            patch.object(
                snapshot_sync, "provision_reference_tables", new=AsyncMock(return_value={})
            ),
            patch.object(ics, "sync_reference_data", side_effect=slow_sync),
        ):
            task = asyncio.create_task(syncer.run_full_sync(alerts=[{"id": "a1"}]))
            await started.wait()
            syncer.mark_dirty()
            release.set()
            await task
        assert syncer.dirty is True

    @pytest.mark.asyncio
    async def test_provision_retries_until_shutdown_c7505_regression(self) -> None:
        """[C7505][regression] Transient provision failures retry with backoff until shutdown."""
        state = AppState()
        syncer = snapshot_sync.AlertSnapshotSynchronizer(state, interval_s=0.01)
        syncer.enabled = True
        attempts = {"n": 0}

        async def fail_then_ok():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("warehouse warming")
            return {"tables": {}}

        with (
            patch.object(snapshot_sync, "provision_reference_tables", side_effect=fail_then_ok),
            patch.object(
                snapshot_sync.AlertSnapshotSynchronizer,
                "run_once",
                new=AsyncMock(return_value={"rows": 0, "skipped": True}),
            ),
        ):
            task = asyncio.create_task(syncer.run_loop())
            for _ in range(50):
                if attempts["n"] >= 3:
                    break
                await asyncio.sleep(0.02)
            state.shutdown_event.set()
            await asyncio.wait_for(task, timeout=2)

        assert attempts["n"] >= 3
        assert syncer.last_error is None or syncer.retrying is False
        contract = ics.alert_snapshot_contract(syncer)
        assert contract["freshness"] in {"pending", "periodic", "error", "retry"}


class TestContract:
    def test_contract_exposes_freshness_fields_c7505_usability(self) -> None:
        """[C7505][usability] Snapshot contract includes freshness, last sync, and error."""
        syncer = snapshot_sync.AlertSnapshotSynchronizer(AppState(), interval_s=60)
        contract = ics.alert_snapshot_contract(syncer)
        assert contract["table"].endswith("intelligence_alert_snapshot")
        assert contract["access"] == "read_only"
        assert "freshness" in contract
        assert "last_sync_at" in contract
        assert "last_error" in contract
        assert int(contract["interval_s"]) == 60

    def test_interval_is_configurable_c7505_smoke(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """[C7505][smoke] Sync interval can be configured via env."""
        monkeypatch.setenv("ALERT_SNAPSHOT_SYNC_INTERVAL_S", "15")
        syncer = snapshot_sync.AlertSnapshotSynchronizer(AppState())
        assert syncer.interval_s == 15.0


class TestLifespanHook:
    @pytest.mark.asyncio
    async def test_start_attaches_task_when_configured_c7505_regression(self) -> None:
        """[C7505][regression] Startup hook records a background task on AppState when enabled."""
        state = AppState()
        with (
            patch.object(snapshot_sync, "provision_reference_tables", new=AsyncMock()),
            patch.object(
                snapshot_sync.AlertSnapshotSynchronizer,
                "run_loop",
                new=AsyncMock(),
            ),
        ):
            task = snapshot_sync.start_alert_snapshot_sync(state)
        assert task is not None
        assert state.alert_snapshot_sync_task is task
        assert state.alert_snapshot_synchronizer is not None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def test_start_skips_when_config_missing_c7505_defect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7505][defect] Missing warehouse config does not start the loop."""
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        state = AppState()
        task = snapshot_sync.start_alert_snapshot_sync(state)
        assert task is None
        assert state.alert_snapshot_sync_task is None
        assert state.alert_snapshot_synchronizer is not None
        assert state.alert_snapshot_synchronizer.enabled is False
