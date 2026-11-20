"""Regression coverage for industry-scoped realtime CDF reconciliation."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import pipeline_deploy
from iot_twin.services import cdf_reconciliation_service as cdf


def test_canonical_names_are_industry_unique_c7234(monkeypatch):
    """Canonical source and destination names do not collide [C7234][defect][regression]."""
    monkeypatch.setenv("UC_CATALOG", "example-catalog")
    monkeypatch.setenv("UC_SCHEMA", "rtm_lakets_zb")

    manufacturing = cdf.shadow_table_name(" Manufacturing ")
    pharmaceutical = cdf.shadow_table_name("pharmaceutical")

    assert manufacturing == "_shadow_manufacturing_sensor_readings"
    assert pharmaceutical == "_shadow_pharmaceutical_sensor_readings"
    assert manufacturing != pharmaceutical
    assert cdf.delta_table_name("manufacturing") == (
        "lb__shadow_manufacturing_sensor_readings_history"
    )
    assert len(manufacturing.encode()) <= 63


def test_rejects_non_curated_industry_c7235():
    """CDF names cannot be minted for arbitrary identifiers [C7235][smoke]."""
    with pytest.raises(ValueError, match="unknown curated industry"):
        cdf.shadow_table_name("customer-controlled")


@pytest.mark.asyncio
async def test_exact_wal2delta_shadow_must_stream_c7236():
    """A sibling STREAMING table cannot satisfy readiness [C7236][defect][regression]."""
    conn = MagicMock()

    async def fetch(_query, schema, shadow):
        if (schema, shadow) == (
            "lakets_cdf",
            "_shadow_pharmaceutical_sensor_readings",
        ):
            return [{"status": "STREAMING"}]
        return []

    conn.fetch = AsyncMock(side_effect=fetch)

    with pytest.raises(cdf.RealtimeCdfError, match="manufacturing"):
        await cdf.wait_for_exact_shadow_streaming(
            conn,
            "_shadow_manufacturing_sensor_readings",
            timeout_s=0,
            poll_interval_s=0,
        )

    assert conn.fetch.await_args.args[1:] == (
        "lakets_cdf",
        "_shadow_manufacturing_sensor_readings",
    )


@pytest.mark.asyncio
async def test_reconcile_updates_metadata_and_deduplicates_ledger_c7237(monkeypatch):
    """Successful reconciliation persists one exact binding [C7237][regression]."""
    monkeypatch.setenv("UC_CATALOG", "example-catalog")
    monkeypatch.setenv("UC_SCHEMA", "rtm_lakets_zb")
    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", "iot-pipelines-example")
    db = MagicMock()
    migration_conn = MagicMock()
    migration_conn.close = AsyncMock()
    poll_conn = MagicMock()
    poll_conn.close = AsyncMock()
    setup = {"id": "s1", "industry": "manufacturing"}
    deployment = {"id": "d1", "mode": "realtime"}

    monkeypatch.setattr(cdf.pipeline_setup_repo, "get_setup", AsyncMock(return_value=setup))
    monkeypatch.setattr(
        cdf.deployment_repo,
        "get_deployment_by_mode",
        AsyncMock(return_value=deployment),
    )
    update = AsyncMock(return_value=deployment)
    monkeypatch.setattr(cdf.deployment_repo, "update_deployment", update)
    monkeypatch.setattr(
        cdf.pipeline_project_service,
        "get_write_conn",
        AsyncMock(side_effect=[migration_conn, poll_conn]),
    )
    monkeypatch.setattr(
        cdf.lakets_service,
        "reconcile_chronotable_shadow",
        AsyncMock(return_value={"status": "migrated"}),
    )

    def ensure_config(*_args, **_kwargs):
        assert migration_conn.close.await_count == 1
        return {
            "name": "projects/p/branches/manufacturing/databases/d/cdf-configs/lakets_cdf",
            "cdf_config_id": "lakets_cdf",
        }

    monkeypatch.setattr(cdf.cdf_config_service, "ensure_cdf_config", ensure_config)
    monkeypatch.setattr(cdf, "wait_for_exact_shadow_streaming", AsyncMock())
    monkeypatch.setattr(
        cdf.resource_ledger,
        "list_resources",
        AsyncMock(
            return_value=[
                {
                    "id": "resource-1",
                    "resource_type": "cdf_sync",
                    "identifier": (
                        "projects/p/branches/manufacturing/databases/d/cdf-configs/lakets_cdf"
                    ),
                    "status": "active",
                }
            ]
        ),
    )
    record = AsyncMock()
    refresh = AsyncMock()
    monkeypatch.setattr(cdf.resource_ledger, "record_resource", record)
    monkeypatch.setattr(cdf.resource_ledger, "update_resource", refresh)

    binding = await cdf.reconcile_realtime_cdf(db, "s1")

    assert binding["status"] == "streaming"
    assert binding["shadow_table"] == ("lakets_cdf._shadow_manufacturing_sensor_readings")
    assert binding["delta_table"].endswith(".lb__shadow_manufacturing_sensor_readings_history")
    record.assert_not_awaited()
    refresh.assert_awaited_once()
    refreshed = refresh.await_args.kwargs
    assert refreshed["deployment_id"] == "d1"
    assert refreshed["metadata"]["source_table"] == (
        "lakets_cdf._shadow_manufacturing_sensor_readings"
    )
    assert refreshed["metadata"]["delta_table"] == binding["delta_table"]
    assert update.await_args.kwargs == {
        "cdf_enabled": True,
        "lakebase_branch": "manufacturing",
        "delta_table": binding["delta_table"],
    }
    migration_conn.close.assert_awaited_once()
    poll_conn.close.assert_awaited_once()


def test_delta_fqn_rejects_empty_namespace_c7247():
    """Reconciliation cannot emit an empty UC FQN [C7247][defect][regression]."""
    with pytest.raises(ValueError, match="catalog"):
        cdf.delta_table_fqn("manufacturing", catalog="", schema="rtm")
    with pytest.raises(ValueError, match="schema"):
        cdf.delta_table_fqn("manufacturing", catalog="main", schema="")


@pytest.mark.asyncio
async def test_failure_closes_connections_and_preserves_deployed_status_c7248(
    monkeypatch,
):
    """Failed polling clears CDF only on deployed jobs [C7248][defect][regression]."""
    monkeypatch.setenv("UC_CATALOG", "example-catalog")
    monkeypatch.setenv("UC_SCHEMA", "rtm_lakets_zb")
    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", "iot-pipelines-example")
    db = MagicMock()
    deployment = {
        "id": "d1",
        "mode": "realtime",
        "status": "deployed",
        "job_id": "42",
    }
    migration_conn = MagicMock()
    migration_conn.close = AsyncMock()
    poll_conn = MagicMock()
    poll_conn.close = AsyncMock()
    monkeypatch.setattr(
        cdf.pipeline_setup_repo,
        "get_setup",
        AsyncMock(return_value={"id": "s1", "industry": "manufacturing"}),
    )
    monkeypatch.setattr(
        cdf.deployment_repo,
        "get_deployment_by_mode",
        AsyncMock(return_value=deployment),
    )
    update = AsyncMock()
    monkeypatch.setattr(cdf.deployment_repo, "update_deployment", update)
    monkeypatch.setattr(
        cdf.pipeline_project_service,
        "get_write_conn",
        AsyncMock(side_effect=[migration_conn, poll_conn]),
    )
    monkeypatch.setattr(
        cdf.lakets_service,
        "reconcile_chronotable_shadow",
        AsyncMock(return_value={"status": "already_exists"}),
    )
    monkeypatch.setattr(
        cdf.cdf_config_service,
        "ensure_cdf_config",
        MagicMock(return_value={"name": "cdf"}),
    )
    monkeypatch.setattr(
        cdf,
        "wait_for_exact_shadow_streaming",
        AsyncMock(side_effect=cdf.RealtimeCdfError("not streaming")),
    )

    with pytest.raises(cdf.RealtimeCdfError, match="not streaming"):
        await cdf.reconcile_realtime_cdf(db, "s1")

    migration_conn.close.assert_awaited_once()
    poll_conn.close.assert_awaited_once()
    update.assert_awaited_once_with(db, "d1", cdf_enabled=False)


@pytest.mark.asyncio
async def test_explicit_admin_reconcile_endpoint_returns_binding_c7241(monkeypatch):
    """Existing realtime deployments expose reconciliation [C7241][smoke][regression]."""
    state = AppState()
    state.app_backend_db = MagicMock()
    app = FastAPI()
    app.state.app_state = state
    app.include_router(pipeline_deploy.router)
    binding = {
        "setup_id": "s1",
        "industry": "manufacturing",
        "status": "streaming",
    }
    reconcile = AsyncMock(return_value=binding)
    monkeypatch.setattr(pipeline_deploy.app_permissions, "is_admin", lambda _: True)
    monkeypatch.setattr(
        pipeline_deploy.cdf_reconciliation_service,
        "reconcile_realtime_cdf",
        reconcile,
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/admin/pipelines/s1/realtime/reconcile-cdf")

    assert response.status_code == 200
    assert response.json() == binding
    reconcile.assert_awaited_once_with(state.app_backend_db, "s1")
