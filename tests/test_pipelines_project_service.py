"""Tests for the shared ``iot-pipelines`` Lakebase project provisioning.

Covers Phase 2 Task 2.3 (plan) / spec §2.1–2.2:
- ``ensure_pipelines_project`` — idempotent shared-project creation.
- ``provision_industry_branch`` — never-expiring per-industry branch, primary
  endpoint resize, read-replica creation, ledger recording, host resolution.
- ``get_read_conn`` / ``get_write_conn`` — reads route to the replica endpoint,
  writes to the primary endpoint.

Unit tests mock the Databricks ``WorkspaceClient``.  A single integration test
runs against a *throwaway* branch on the real ``iot-pipelines`` project and
``pytest.skip``s when the project is unreachable.
"""

from __future__ import annotations

import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.core.workspace_config import get_pipelines_project
from iot_twin.services.pipelines import pipeline_project_service as pps

# The test suite sets this env var so get_pipelines_project() returns a known value.
_TEST_PIPELINES_PROJECT = "iot-pipelines"


@pytest.fixture(autouse=True)
def _set_pipelines_project_env(monkeypatch):
    """Ensure PIPELINES_LAKEBASE_PROJECT is set for all tests in this module."""
    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", _TEST_PIPELINES_PROJECT)


# ---------------------------------------------------------------------------
# Helpers for the mocked WorkspaceClient
# ---------------------------------------------------------------------------
class _NotFound(Exception):
    """Stand-in for the SDK's NotFound error (message-matched by the service)."""

    def __str__(self) -> str:  # pragma: no cover - trivial
        return "RESOURCE_DOES_NOT_EXIST: not found"


def _fake_endpoint(host: str, state: str = "ACTIVE") -> MagicMock:
    """Build a MagicMock Endpoint whose status reports ACTIVE with a host."""
    ep = MagicMock()
    ep.status = MagicMock()
    ep.status.current_state = MagicMock()
    ep.status.current_state.value = state
    ep.status.current_state.name = state
    ep.status.hosts = MagicMock()
    ep.status.hosts.host = host
    return ep


def _make_ws(*, project_exists=True, branch_exists=False, replica_exists=False):
    """Build a mocked WorkspaceClient for the postgres surface."""
    ws = MagicMock()
    pg = ws.postgres

    if project_exists:
        pg.get_project = MagicMock(return_value=MagicMock())
    else:
        pg.get_project = MagicMock(side_effect=_NotFound())
    pg.create_project = MagicMock(return_value=MagicMock())

    if branch_exists:
        pg.get_branch = MagicMock(return_value=MagicMock())
    else:
        pg.get_branch = MagicMock(side_effect=_NotFound())
    pg.create_branch = MagicMock(return_value=MagicMock())

    def _get_endpoint(name: str):
        if name.endswith("/endpoints/primary"):
            return _fake_endpoint("primary.db.example.com")
        if name.endswith("/endpoints/replica"):
            if replica_exists:
                return _fake_endpoint("replica.db.example.com")
            raise _NotFound()
        raise _NotFound()

    pg.get_endpoint = MagicMock(side_effect=_get_endpoint)
    pg.update_endpoint = MagicMock(return_value=MagicMock())

    create_op = MagicMock()
    create_op.wait = MagicMock(return_value=_fake_endpoint("replica.db.example.com"))
    pg.create_endpoint = MagicMock(return_value=create_op)

    return ws


# ---------------------------------------------------------------------------
# ensure_pipelines_project
# ---------------------------------------------------------------------------
class TestEnsurePipelinesProject:
    def test_returns_existing_when_project_present(self) -> None:
        ws = _make_ws(project_exists=True)
        result = pps.ensure_pipelines_project(ws=ws)
        assert result["project"] == get_pipelines_project()
        assert result["status"] == "existing"
        ws.postgres.create_project.assert_not_called()

    def test_creates_when_project_missing(self) -> None:
        ws = _make_ws(project_exists=False)
        result = pps.ensure_pipelines_project(ws=ws)
        assert result["status"] == "created"
        ws.postgres.create_project.assert_called_once()
        # project_id must be the shared name, not a resource path
        _, kwargs = ws.postgres.create_project.call_args
        assert kwargs.get("project_id") == get_pipelines_project()


# ---------------------------------------------------------------------------
# provision_industry_branch
# ---------------------------------------------------------------------------
class TestProvisionIndustryBranch:
    @pytest.mark.asyncio
    async def test_creates_never_expiring_branch_and_replica(self) -> None:
        ws = _make_ws(branch_exists=False, replica_exists=False)
        result = await pps.provision_industry_branch("pharmaceutical", ws=ws)

        # Branch created with no_expiry=True (never expires)
        ws.postgres.create_branch.assert_called_once()
        _, kwargs = ws.postgres.create_branch.call_args
        assert kwargs["branch_id"] == "pharmaceutical"
        assert kwargs["branch"].spec.no_expiry is True

        # Primary endpoint resized (NOT created — it auto-provisions)
        ws.postgres.update_endpoint.assert_called_once()
        _, up_kwargs = ws.postgres.update_endpoint.call_args
        assert up_kwargs["name"].endswith("/branches/pharmaceutical/endpoints/primary")
        assert set(up_kwargs["update_mask"].paths) == {
            "spec.autoscaling_limit_min_cu",
            "spec.autoscaling_limit_max_cu",
        }

        # Read replica created as READ_ONLY with endpoint_id 'replica'
        ws.postgres.create_endpoint.assert_called_once()
        _, ce_kwargs = ws.postgres.create_endpoint.call_args
        assert ce_kwargs["endpoint_id"] == "replica"
        from databricks.sdk.service.postgres import EndpointType

        assert ce_kwargs["endpoint"].spec.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_ONLY

        # Return contract
        assert result["project"] == get_pipelines_project()
        assert result["branch"] == "pharmaceutical"
        assert result["primary_endpoint"].endswith("/endpoints/primary")
        assert result["replica_endpoint"].endswith("/endpoints/replica")
        assert result["primary_host"] == "primary.db.example.com"
        assert result["replica_host"] == "replica.db.example.com"

    @pytest.mark.asyncio
    async def test_idempotent_when_branch_and_replica_exist(self) -> None:
        ws = _make_ws(branch_exists=True, replica_exists=True)
        result = await pps.provision_industry_branch("pharmaceutical", ws=ws)
        ws.postgres.create_branch.assert_not_called()
        ws.postgres.create_endpoint.assert_not_called()
        # Still resolves hosts
        assert result["primary_host"] == "primary.db.example.com"
        assert result["replica_host"] == "replica.db.example.com"

    @pytest.mark.asyncio
    async def test_records_all_four_resources_in_ledger(self) -> None:
        ws = _make_ws(branch_exists=False, replica_exists=False)
        db = MagicMock()
        recorded: list[dict] = []

        async def _record(_db, **kwargs):
            recorded.append(kwargs)
            return kwargs

        with patch.object(pps, "record_resource", side_effect=_record):
            await pps.provision_industry_branch("pharmaceutical", ws=ws, db=db, setup_id="setup-1")

        types = [r["resource_type"] for r in recorded]
        assert "lakebase_branch" in types
        assert "lakebase_endpoint" in types
        assert "lakebase_read_replica" in types
        # project recorded and flagged shared (never torn down per-industry)
        proj = [r for r in recorded if r["resource_type"] == "lakebase_project"]
        assert proj and proj[0]["metadata"].get("shared") is True
        # Replica torn down before the branch (higher teardown_order)
        by_type = {r["resource_type"]: r["teardown_order"] for r in recorded}
        assert by_type["lakebase_read_replica"] > by_type["lakebase_branch"]

    @pytest.mark.asyncio
    async def test_no_ledger_write_without_setup_id(self) -> None:
        ws = _make_ws(branch_exists=False, replica_exists=False)
        with patch.object(pps, "record_resource", new=AsyncMock()) as rec:
            await pps.provision_industry_branch("pharmaceutical", ws=ws, db=MagicMock())
            rec.assert_not_called()

    @pytest.mark.asyncio
    async def test_registers_app_sp_role_on_branch(self, monkeypatch) -> None:
        # Roles are branch-scoped: provisioning a branch must register the app's
        # OWN SP as a Postgres role on it (else get_read_conn → password auth fail).
        ws = _make_ws(branch_exists=False, replica_exists=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-0d42")

        calls: list[dict] = []
        from iot_twin.services import provisioning_service

        async def _fake_role(**kwargs):
            calls.append(kwargs)
            return [{"resource": "role", "status": "created"}]

        monkeypatch.setattr(provisioning_service, "provision_lakebase_role", _fake_role)

        recorded: list[dict] = []

        async def _record(_db, **kwargs):
            recorded.append(kwargs)
            return kwargs

        with patch.object(pps, "record_resource", side_effect=_record):
            result = await pps.provision_industry_branch(
                "pharmaceutical", ws=ws, db=MagicMock(), setup_id="setup-1"
            )

        # The app SP was registered as a role on THIS branch.
        assert calls and calls[0]["sp_client_id"] == "app-sp-0d42"
        assert calls[0]["branch_id"] == "pharmaceutical"
        assert calls[0]["project_id"] == get_pipelines_project()
        # Recorded in the ledger for visibility.
        role_rows = [r for r in recorded if r["resource_type"] == "lakebase_role"]
        assert role_rows and role_rows[0]["metadata"]["sp_client_id"] == "app-sp-0d42"
        assert result["app_sp_role"]["client_id"] == "app-sp-0d42"

    @pytest.mark.asyncio
    async def test_branch_role_failure_is_non_fatal(self, monkeypatch) -> None:
        ws = _make_ws(branch_exists=False, replica_exists=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-0d42")
        from iot_twin.services import provisioning_service

        async def _boom(**kwargs):
            raise RuntimeError("lakebase project-admin required")

        monkeypatch.setattr(provisioning_service, "provision_lakebase_role", _boom)
        # Branch provisioning still returns its contract (role step is best-effort).
        result = await pps.provision_industry_branch("pharmaceutical", ws=ws)
        assert result["branch"] == "pharmaceutical"
        assert result["app_sp_role"]["results"][0]["status"] == "failed"


# ---------------------------------------------------------------------------
# get_read_conn / get_write_conn
# ---------------------------------------------------------------------------
class TestConnResolution:
    @pytest.mark.asyncio
    async def test_write_conn_targets_primary(self) -> None:
        ws = _make_ws(branch_exists=True, replica_exists=True)
        with patch.object(pps, "LakebaseConnectionManager") as mgr_cls:
            inst = mgr_cls.return_value
            inst.initialize = AsyncMock()
            await pps.get_write_conn("pharmaceutical", ws=ws)
            _, kwargs = mgr_cls.call_args
            assert kwargs["endpoint_name"].endswith("/endpoints/primary")
            assert kwargs["pg_host"] == "primary.db.example.com"
            inst.initialize.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_read_conn_targets_replica(self) -> None:
        ws = _make_ws(branch_exists=True, replica_exists=True)
        with patch.object(pps, "LakebaseConnectionManager") as mgr_cls:
            inst = mgr_cls.return_value
            inst.initialize = AsyncMock()
            await pps.get_read_conn("pharmaceutical", ws=ws)
            _, kwargs = mgr_cls.call_args
            assert kwargs["endpoint_name"].endswith("/endpoints/replica")
            assert kwargs["pg_host"] == "replica.db.example.com"
            inst.initialize.assert_awaited_once()


# ---------------------------------------------------------------------------
# Integration — throwaway branch on the real iot-pipelines project
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_integration_branch_primary_write_replica_read() -> None:
    """Create a throwaway branch, write to primary, read from replica, verify
    the replica rejects writes, then clean up.  Skips if unreachable."""
    try:
        import asyncpg  # noqa: F401
        from databricks.sdk import WorkspaceClient
    except Exception as exc:  # pragma: no cover - deps missing
        pytest.skip(f"integration deps unavailable: {exc}")

    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "example-us-east")
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", profile)
    try:
        ws = WorkspaceClient(profile=profile)
        pps.ensure_pipelines_project(ws=ws)
    except Exception as exc:  # pragma: no cover - offline / no creds
        pytest.skip(f"iot-pipelines not reachable: {exc}")

    branch = f"itest{uuid.uuid4().hex[:8]}"
    write_mgr = None
    read_mgr = None
    try:
        info = await pps.provision_industry_branch(branch, ws=ws)  # type: ignore[assignment]
    except Exception as exc:  # pragma: no cover - project unreachable/zombie
        pytest.skip(f"iot-pipelines project not operational: {exc}")
    try:
        assert info["primary_host"]
        assert info["replica_host"]

        write_mgr = await pps.get_write_conn(branch, ws=ws)
        read_mgr = await pps.get_read_conn(branch, ws=ws)

        token = uuid.uuid4().hex
        await write_mgr.execute(
            "CREATE TABLE IF NOT EXISTS itest_probe (id serial PRIMARY KEY, token text)"
        )
        await write_mgr.execute("INSERT INTO itest_probe (token) VALUES ($1)", token)

        # Replica has ~6s replication lag — poll for the row.
        seen = None
        for _ in range(15):
            try:
                seen = await read_mgr.fetchval(
                    "SELECT token FROM itest_probe WHERE token = $1", token
                )
            except Exception:
                seen = None
            if seen == token:
                break
            await __import__("asyncio").sleep(2)
        assert seen == token, "row never replicated to the read replica"

        # The read replica must reject writes.
        with pytest.raises(Exception):
            await read_mgr.execute("INSERT INTO itest_probe (token) VALUES ($1)", uuid.uuid4().hex)
    finally:
        if write_mgr is not None:
            try:
                await write_mgr.close()
            except Exception:
                pass
        if read_mgr is not None:
            try:
                await read_mgr.close()
            except Exception:
                pass
        # Tear down the throwaway branch (removes its primary endpoint too);
        # delete the replica first.
        try:
            ws.postgres.delete_endpoint(name=pps.endpoint_resource_name(branch, "replica"))
        except Exception:
            pass
        try:
            ws.postgres.delete_branch(name=pps.branch_resource_name(branch))
        except Exception:
            pass
