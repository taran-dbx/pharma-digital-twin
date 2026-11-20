"""Tests for the one-click pipeline-setup orchestrator + route.

Covers:
- ``iot_twin.services.pipelines.pipeline_setup_service.setup_pipeline_lakebase`` — step ordering,
  create_new_project branching, per-rollup create_rollup calls, deploy with all
  four ops jobs enabled, and NON-FATAL per-step failures.
- ``POST /api/setup/pipeline-lakebase`` — validation + delegation.

All external surfaces (WorkspaceClient, asyncpg pool, lakets_service.create_rollup,
deploy_lakets_ops_jobs, and the provisioning helpers) are mocked.  The LakeTS
deploy surface is owned by a sibling agent; imports are guarded so collection
never hard-fails.
"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.services.pipelines import pipeline_setup_service
from tests._fakes import fake_credential

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Secret scope *name* used by the setup payload (not a secret).
SCOPE_NAME = "iot-secrets"


def _make_state() -> MagicMock:
    state = MagicMock()
    state.db_available = True
    state.db = MagicMock()
    state.db.fetchrow = AsyncMock(return_value=None)
    return state


def _make_ws() -> MagicMock:
    w = MagicMock()
    w.current_user = MagicMock()
    w.current_user.me = MagicMock(return_value=MagicMock(user_name="app@sp.example.com"))
    w.postgres = MagicMock()
    ep = MagicMock()
    ep.name = "projects/iot-demo/branches/production/endpoints/primary"
    ep.status.hosts.host = "pg.example.com"
    w.postgres.list_endpoints = MagicMock(return_value=[ep])
    w.postgres.get_endpoint = MagicMock(return_value=ep)
    w.postgres.generate_database_credential = MagicMock(
        return_value=MagicMock(token=fake_credential("token"))
    )
    return w


def _stub_conn() -> MagicMock:
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    conn.fetchrow = AsyncMock(return_value=None)
    conn.close = AsyncMock(return_value=None)
    return conn


def _base_params(**overrides) -> dict:
    params = {
        "pipeline_id": "pipe-1",
        "create_new_project": False,
        "lb_project_id": "iot-demo",
        "lb_branch_id": "production",
        "lb_database": "databricks_postgres",
        "table_name": "sensor_readings",
        "secret_scope": SCOPE_NAME,
        "industry": "pharmaceutical",
        "tiering": {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"},
        "retention": None,
        "rollups": [],
    }
    params.update(overrides)
    return params


def _default_patchers() -> dict:
    """Return the default set of patchers (mocks) for the orchestrator."""
    return {
        "ensure_streaming_sp": patch(
            "iot_twin.services.pipelines.pipeline_setup_service.ensure_streaming_sp",
            new=AsyncMock(
                return_value={
                    "client_id": "sp-123",
                    "status": "ok",
                    "results": [{"resource": "SP", "status": "created"}],
                }
            ),
        ),
        "provision_project": patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_project_and_branch",
            new=AsyncMock(return_value=[{"resource": "project", "status": "created"}]),
        ),
        "provision_role": patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_role",
            new=AsyncMock(return_value=[{"resource": "role", "status": "created"}]),
        ),
        "provision_table": patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_table",
            new=AsyncMock(return_value={"resource": "table", "status": "created"}),
        ),
        "create_rollup": patch(
            "iot_twin.services.pipelines.pipeline_setup_service.lakets_service.create_rollup",
            new=AsyncMock(return_value={"resource": "rollup", "status": "created"}),
        ),
        "deploy_jobs": patch(
            "iot_twin.services.pipelines.pipeline_setup_service.deploy_lakets_ops_jobs",
            new=MagicMock(return_value=[{"job": "partition", "status": "created"}]),
        ),
        "ws": patch.object(pipeline_setup_service, "_new_ws", return_value=_make_ws()),
        "connect": patch("asyncpg.connect", new=AsyncMock(return_value=_stub_conn())),
    }


# ---------------------------------------------------------------------------
# Orchestrator — step ordering
# ---------------------------------------------------------------------------


class TestStepOrdering:
    @pytest.mark.asyncio
    async def test_steps_run_in_contract_order(self) -> None:
        patchers = _default_patchers()
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(), _base_params()
            )

        names = [s["step"] for s in result["steps"]]
        assert names.index("ensure_streaming_sp") < names.index("provision_lakebase_role")
        assert names.index("provision_lakebase_role") < names.index("provision_lakebase_table")
        assert names.index("provision_lakebase_table") < names.index("deploy_lakets_ops_jobs")
        assert result["lb_project_id"] == "iot-demo"
        assert result["client_id"] == "sp-123"

    @pytest.mark.asyncio
    async def test_create_new_project_true_triggers_project_creation(self) -> None:
        patchers = _default_patchers()
        proj_mock = AsyncMock(return_value=[{"resource": "project", "status": "created"}])
        patchers["provision_project"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_project_and_branch",
            new=proj_mock,
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(create_new_project=True, lb_project_id="new-proj"),
            )

        proj_mock.assert_awaited_once()
        assert proj_mock.await_args.args[0] == "new-proj"
        assert any(s["step"] == "provision_lakebase_project_and_branch" for s in result["steps"])

    @pytest.mark.asyncio
    async def test_create_new_project_false_skips_project_creation(self) -> None:
        patchers = _default_patchers()
        proj_mock = AsyncMock(return_value=[])
        patchers["provision_project"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_project_and_branch",
            new=proj_mock,
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(create_new_project=False),
            )

        proj_mock.assert_not_awaited()
        assert not any(
            s["step"] == "provision_lakebase_project_and_branch" for s in result["steps"]
        )


# ---------------------------------------------------------------------------
# Orchestrator — rollups
# ---------------------------------------------------------------------------


class TestRollups:
    @pytest.mark.asyncio
    async def test_create_rollup_called_per_entry(self) -> None:
        rollups = [
            {"name": "rollup_1m", "bucket": "1 minute", "aggregates": ["avg(value)"]},
            {"name": "rollup_5m", "bucket": "5 minutes", "aggregates": ["max(value)"]},
        ]
        patchers = _default_patchers()
        rollup_mock = AsyncMock(return_value={"resource": "rollup", "status": "created"})
        patchers["create_rollup"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.lakets_service.create_rollup",
            new=rollup_mock,
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(rollups=rollups),
            )

        assert rollup_mock.await_count == 2
        # Each rollup got its own name + bucket + aggregates forwarded.
        names = {c.kwargs["name"] for c in rollup_mock.await_args_list}
        assert names == {"rollup_1m", "rollup_5m"}
        assert len([s for s in result["steps"] if s["step"] == "create_rollup"]) == 2

    @pytest.mark.asyncio
    async def test_no_rollups_skips_create_rollup_and_connection(self) -> None:
        patchers = _default_patchers()
        rollup_mock = AsyncMock(return_value={"resource": "rollup", "status": "created"})
        connect_mock = AsyncMock(return_value=_stub_conn())
        patchers["create_rollup"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.lakets_service.create_rollup",
            new=rollup_mock,
        )
        patchers["connect"] = patch("asyncpg.connect", new=connect_mock)
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(rollups=[]),
            )

        rollup_mock.assert_not_awaited()
        connect_mock.assert_not_awaited()


# ---------------------------------------------------------------------------
# Orchestrator — deploy ops jobs
# ---------------------------------------------------------------------------


class TestDeployJobs:
    @pytest.mark.asyncio
    async def test_deploy_called_with_all_four_jobs_enabled(self) -> None:
        patchers = _default_patchers()
        deploy_mock = MagicMock(return_value=[{"job": "x", "status": "created"}])
        patchers["deploy_jobs"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.deploy_lakets_ops_jobs",
            new=deploy_mock,
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(),
            )

        deploy_mock.assert_called_once()
        kwargs = deploy_mock.call_args.kwargs
        assert kwargs["partition_manager"] is True
        assert kwargs["rollup"] is True
        assert kwargs["retention"] is True
        assert kwargs["tiering"] is True


# ---------------------------------------------------------------------------
# Orchestrator — non-fatal per-step failures
# ---------------------------------------------------------------------------


class TestNonFatal:
    @pytest.mark.asyncio
    async def test_role_failure_does_not_abort_chain(self) -> None:
        patchers = _default_patchers()
        table_mock = AsyncMock(return_value={"resource": "table", "status": "created"})
        rollup_mock = AsyncMock(return_value={"resource": "rollup", "status": "created"})
        deploy_mock = MagicMock(return_value=[{"job": "x", "status": "created"}])
        patchers["provision_role"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_role",
            new=AsyncMock(side_effect=Exception("role boom")),
        )
        patchers["provision_table"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.provision_lakebase_table",
            new=table_mock,
        )
        patchers["create_rollup"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.lakets_service.create_rollup",
            new=rollup_mock,
        )
        patchers["deploy_jobs"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.deploy_lakets_ops_jobs",
            new=deploy_mock,
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(
                    rollups=[
                        {"name": "r", "bucket": "1 minute", "aggregates": ["avg(v)"]},
                    ]
                ),
            )

        role_step = next(s for s in result["steps"] if s["step"] == "provision_lakebase_role")
        assert role_step["status"] == "failed"
        assert "role boom" in role_step["error"]
        # Downstream steps still ran.
        table_mock.assert_awaited_once()
        rollup_mock.assert_awaited_once()
        deploy_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_deploy_failure_surfaced_not_raised(self) -> None:
        patchers = _default_patchers()
        patchers["deploy_jobs"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.deploy_lakets_ops_jobs",
            new=MagicMock(side_effect=Exception("deploy boom")),
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(),
            )

        deploy_step = next(s for s in result["steps"] if s["step"] == "deploy_lakets_ops_jobs")
        assert deploy_step["status"] == "failed"
        assert "deploy boom" in deploy_step["error"]

    @pytest.mark.asyncio
    async def test_one_rollup_failure_does_not_stop_others(self) -> None:
        async def _rollup(pool, **kwargs):
            if kwargs.get("name") == "bad":
                raise Exception("rollup boom")
            return {"resource": "rollup", "status": "created"}

        patchers = _default_patchers()
        patchers["create_rollup"] = patch(
            "iot_twin.services.pipelines.pipeline_setup_service.lakets_service.create_rollup",
            new=AsyncMock(side_effect=_rollup),
        )
        with ExitStack() as stack:
            for p in patchers.values():
                stack.enter_context(p)
            result = await pipeline_setup_service.setup_pipeline_lakebase(
                _make_state(),
                _base_params(
                    rollups=[
                        {"name": "good", "bucket": "1 minute", "aggregates": ["avg(v)"]},
                        {"name": "bad", "bucket": "5 minutes", "aggregates": ["max(v)"]},
                    ]
                ),
            )

        rollup_steps = [s for s in result["steps"] if s["step"] == "create_rollup"]
        assert len(rollup_steps) == 2
        statuses = {s["status"] for s in rollup_steps}
        assert "failed" in statuses
        assert "created" in statuses


# ---------------------------------------------------------------------------
# Route — POST /api/setup/pipeline-lakebase
# ---------------------------------------------------------------------------


def _build_app() -> FastAPI:
    from iot_twin.core.state import get_app_state
    from iot_twin.routes.setup import router as setup_router

    app = FastAPI()
    app.include_router(setup_router)
    app.dependency_overrides[get_app_state] = lambda: _make_state()
    return app


async def _post(body: dict) -> tuple[int, dict]:
    app = _build_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/setup/pipeline-lakebase", json=body)
    return resp.status_code, resp.json()


class TestSetupRoute:
    @pytest.mark.asyncio
    async def test_valid_request_delegates_and_returns_steps(self) -> None:
        called: dict = {}

        async def _fake_setup(state, params):
            called["params"] = params
            return {
                "steps": [{"step": "ensure_streaming_sp", "status": "ok"}],
                "lb_project_id": params["lb_project_id"],
            }

        with patch("iot_twin.routes.setup.setup_pipeline_lakebase", new=_fake_setup):
            status, payload = await _post(
                {
                    "pipeline_id": "pipe-1",
                    "create_new_project": True,
                    "lb_project_id": "iot-demo",
                    "rollups": [{"name": "r", "bucket": "1 minute", "aggregates": ["avg(v)"]}],
                    "tiering": {"hot": "1 hour", "drop": "7 days"},
                }
            )

        assert status == 200
        assert payload["steps"][0]["step"] == "ensure_streaming_sp"
        assert called["params"]["pipeline_id"] == "pipe-1"
        assert called["params"]["create_new_project"] is True
        assert called["params"]["lb_branch_id"] == "production"  # defaulted
        assert called["params"]["lb_database"] == "databricks_postgres"  # defaulted

    @pytest.mark.asyncio
    async def test_missing_pipeline_id_is_400(self) -> None:
        status, payload = await _post({"lb_project_id": "iot-demo"})
        assert status == 400
        assert "pipeline_id" in payload["error"]

    @pytest.mark.asyncio
    async def test_missing_lb_project_id_is_400(self) -> None:
        status, payload = await _post({"pipeline_id": "pipe-1"})
        assert status == 400
        assert "lb_project_id" in payload["error"]

    @pytest.mark.asyncio
    async def test_invalid_rollup_is_400(self) -> None:
        status, payload = await _post(
            {
                "pipeline_id": "pipe-1",
                "lb_project_id": "iot-demo",
                "rollups": [{"bucket": "1 minute"}],  # missing name
            }
        )
        assert status == 400
        assert "rollup" in payload["error"].lower()

    @pytest.mark.asyncio
    async def test_invalid_tiering_is_400(self) -> None:
        status, payload = await _post(
            {
                "pipeline_id": "pipe-1",
                "lb_project_id": "iot-demo",
                "tiering": {"hot": "1 hour"},  # missing drop
            }
        )
        assert status == 400
        assert "tiering" in payload["error"].lower()
