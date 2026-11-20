"""Best-effort per-pipeline Genie space hook in deployment_service.deploy_pipeline."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services.deployment import deployment_service as ds
from iot_twin.services.genie import genie_space_service as gss


def _all_passed(mode: str) -> dict:
    return {"steps": {key: {"status": "passed"} for key in ds.REQUIRED_STEPS[mode]}}


def _ws() -> MagicMock:
    w = MagicMock()
    w.current_user.me.return_value = MagicMock(user_name="user@example.com")
    w.jobs.list.return_value = []
    w.jobs.create.return_value = MagicMock(job_id=4242)
    w.config.host = "https://e2.cloud.databricks.com"
    return w


def _db(setup: dict) -> MagicMock:
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)

    async def _fetchrow(query: str, *args):
        q = query.lower()
        if "from pipeline_setup" in q:
            return setup
        if "insert into pipeline_deployment" in q:
            return {"id": "dep-1"}
        if "update pipeline_deployment" in q:
            return {"id": "dep-1", "status": "deployed"}
        if "insert into provisioned_resource" in q:
            return {"id": "res-1"}
        return None

    db.fetchrow = AsyncMock(side_effect=_fetchrow)
    return db


def _setup(industry: str, mode: str, **extra) -> dict:
    return {
        "id": "setup-1",
        "industry": industry,
        "derived": {"partitions": 2},
        "cluster_spec": {"node_type": "m5d.large"},
        "validation_state": _all_passed(mode),
        **extra,
    }


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "app-secret")
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
    monkeypatch.setattr(ds.uc_secrets, "put_secret", AsyncMock(return_value={"status": "ok"}))
    monkeypatch.setattr(
        ds.cdf_reconciliation_service,
        "reconcile_realtime_cdf",
        AsyncMock(return_value={"status": "streaming"}),
    )


async def _deploy(db, mode: str, w):
    return await ds.deploy_pipeline(
        db,
        "setup-1",
        mode,
        consumer_notebook="# consumer",
        producer_notebook="# producer",
        w=w,
    )


class TestDeployGenieHook:
    @pytest.mark.asyncio
    async def test_manufacturing_deploy_provisions_space(self, monkeypatch) -> None:
        ensure = AsyncMock(return_value={"space_id": "sp-1", "action": "created", "tables": []})
        monkeypatch.setattr(gss, "ensure_pipeline_space", ensure)
        db = _db(_setup("manufacturing", "zerobus", warehouse_id="wh-setup"))
        result = await _deploy(db, "zerobus", _ws())
        assert result["genie_space"] == {"space_id": "sp-1", "action": "created", "tables": []}
        assert "warnings" not in result
        kwargs = ensure.call_args.kwargs
        assert kwargs["setup_id"] == "setup-1"
        assert kwargs["mode"] == "zerobus"
        assert kwargs["industry"] == "manufacturing"
        assert kwargs["warehouse_id"] == "wh-setup"
        assert kwargs["deployment_id"] == "dep-1"

    @pytest.mark.asyncio
    async def test_warehouse_falls_back_to_resolver(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "wh-env")
        ensure = AsyncMock(return_value={"space_id": "sp-1", "action": "updated", "tables": []})
        monkeypatch.setattr(gss, "ensure_pipeline_space", ensure)
        await _deploy(_db(_setup("manufacturing", "microbatch")), "microbatch", _ws())
        assert ensure.call_args.kwargs["warehouse_id"] == "wh-env"

    @pytest.mark.asyncio
    async def test_failure_becomes_warning_and_deploy_succeeds(self, monkeypatch) -> None:
        monkeypatch.setattr(
            gss, "ensure_pipeline_space", AsyncMock(side_effect=RuntimeError("genie quota"))
        )
        result = await _deploy(_db(_setup("manufacturing", "realtime")), "realtime", _ws())
        assert result["job_id"] == 4242
        assert "genie_space" not in result
        assert len(result["warnings"]) == 1
        assert "Genie" in result["warnings"][0] and "genie quota" in result["warnings"][0]

    @pytest.mark.asyncio
    async def test_real_service_without_warehouse_warns(self, monkeypatch) -> None:
        w = _ws()
        result = await _deploy(_db(_setup("manufacturing", "zerobus")), "zerobus", w)
        assert result["job_id"] == 4242
        assert "warehouse" in result["warnings"][0]
        w.genie.create_space.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_industry_skips_hook(self, monkeypatch) -> None:
        ensure = AsyncMock()
        monkeypatch.setattr(gss, "ensure_pipeline_space", ensure)
        result = await _deploy(_db(_setup("retail", "zerobus")), "zerobus", _ws())
        ensure.assert_not_called()
        assert "genie_space" not in result and "warnings" not in result

    @pytest.mark.asyncio
    async def test_failed_deploy_never_calls_hook(self, monkeypatch) -> None:
        ensure = AsyncMock()
        monkeypatch.setattr(gss, "ensure_pipeline_space", ensure)
        w = _ws()
        w.jobs.create.side_effect = RuntimeError("jobs api down")
        with pytest.raises(RuntimeError):
            await _deploy(_db(_setup("manufacturing", "zerobus")), "zerobus", w)
        ensure.assert_not_called()
