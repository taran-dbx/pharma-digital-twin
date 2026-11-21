"""Opt-in OPTIMIZE of the Lakehouse Sync history table [pipeline-throughput]."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import pipeline_deploy
from iot_twin.services import cdf_history_compaction as hc


@pytest.fixture(autouse=True)
def _config(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", "example-catalog")
    monkeypatch.setenv("UC_SCHEMA", "pharma_twin")
    monkeypatch.delenv("RESOURCE_PREFIX", raising=False)
    monkeypatch.delenv(hc.OPTIMIZE_ENV, raising=False)


def test_statement_targets_only_this_deployments_history_table_c9601_smoke() -> None:
    """[C9601][smoke] Derived target: curated industry + configured pharma_twin schema."""
    assert hc.optimize_statement("manufacturing") == (
        "OPTIMIZE `example-catalog`.`pharma_twin`.`lb__shadow_manufacturing_sensor_readings_history`"
    )


@pytest.mark.parametrize(
    ("industry", "schema"),
    [
        ("manufacturing", "iot_lakehouse"),  # another app's schema
        ("manufacturing", "pharma_twinx"),  # prefix look-alike
        ("customer-controlled", "pharma_twin"),  # not a curated industry
    ],
)
def test_guards_refuse_foreign_targets_c9602_regression(industry, schema) -> None:
    """[C9602][regression] Never OPTIMIZE a table outside this deployment."""
    with pytest.raises(hc.CompactionRefused):
        hc.optimize_statement(industry, schema=schema)


def test_catalog_identifier_is_validated_c9603_regression() -> None:
    """[C9603][regression] Backticks or dots in the catalog cannot inject SQL."""
    with pytest.raises(hc.CompactionRefused):
        hc.optimize_statement("manufacturing", catalog="x`; DROP TABLE t; --")


@pytest.mark.asyncio
async def test_disabled_by_default_c9604_smoke(monkeypatch) -> None:
    """[C9604][smoke] Opt-in: without the env flag nothing runs."""
    run = AsyncMock()
    monkeypatch.setattr(hc.dbsql_service, "execute_dbsql", run)
    with pytest.raises(hc.CompactionRefused, match=hc.OPTIMIZE_ENV):
        await hc.optimize_history_table("manufacturing", "wh-1")
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_enabled_runs_the_guarded_statement_c9605_smoke(monkeypatch) -> None:
    """[C9605][smoke] When enabled, exactly the derived OPTIMIZE runs on the warehouse."""
    monkeypatch.setenv(hc.OPTIMIZE_ENV, "true")
    run = AsyncMock(return_value={"data": [{"metrics": "{numFilesRemoved: 997}"}]})
    monkeypatch.setattr(hc.dbsql_service, "execute_dbsql", run)
    result = await hc.optimize_history_table("manufacturing", "wh-1")
    assert result["status"] == "ok"
    run.assert_awaited_once_with(
        hc.optimize_statement("manufacturing"), "wh-1", hc.OPTIMIZE_TIMEOUT
    )


def _client(monkeypatch, setup):
    state = AppState(industries=["manufacturing"], update_rate_hz=1)
    state.app_backend_db = MagicMock()
    app = FastAPI()
    app.dependency_overrides[pipeline_deploy.get_app_state] = lambda: state
    app.include_router(pipeline_deploy.router)
    monkeypatch.setattr(pipeline_deploy.app_permissions, "is_admin", lambda _: True)
    monkeypatch.setattr(
        pipeline_deploy.pipeline_setup_repo, "get_setup", AsyncMock(return_value=setup)
    )
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_route_reports_409_while_disabled_c9606_regression(monkeypatch) -> None:
    """[C9606][regression] The admin route refuses (409) unless the remediation is enabled."""
    setup = {"id": "s1", "industry": "manufacturing", "warehouse_id": "wh-1"}
    async with _client(monkeypatch, setup) as client:
        response = await client.post("/api/admin/pipelines/s1/realtime/optimize-history")
    assert response.status_code == 409
    assert hc.OPTIMIZE_ENV in response.json()["detail"]


@pytest.mark.asyncio
async def test_route_runs_when_enabled_c9607_smoke(monkeypatch) -> None:
    """[C9607][smoke] Enabled + admin: the setup's warehouse runs the guarded OPTIMIZE."""
    monkeypatch.setenv(hc.OPTIMIZE_ENV, "true")
    run = AsyncMock(return_value={"data": []})
    monkeypatch.setattr(hc.dbsql_service, "execute_dbsql", run)
    setup = {"id": "s1", "industry": "manufacturing", "warehouse_id": "wh-1"}
    async with _client(monkeypatch, setup) as client:
        response = await client.post("/api/admin/pipelines/s1/realtime/optimize-history")
    assert response.status_code == 200
    assert response.json()["statement"].startswith("OPTIMIZE `example-catalog`.`pharma_twin`.")
    assert run.await_args.args[1] == "wh-1"
