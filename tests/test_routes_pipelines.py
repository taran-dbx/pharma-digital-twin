"""Tests for routes/pipelines.py — focused on the lakebase-health endpoint."""

from __future__ import annotations

import datetime as _dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState


def _build_app_with_state(state: AppState) -> FastAPI:
    from iot_twin.routes.pipelines import router as pipelines_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(pipelines_router)
    return app


def _mock_db_for_health(row: dict | None = None, raises: Exception | None = None) -> MagicMock:
    db = MagicMock()
    if raises is not None:
        db.fetchrow = AsyncMock(side_effect=raises)
    else:
        db.fetchrow = AsyncMock(return_value=row)
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


def _state_with_db(db: MagicMock, available: bool = True) -> AppState:
    state = AppState()
    state.db = db
    state.db_available = available
    state.db_error = None
    return state


def _mock_pipeline_lakebase(db: MagicMock) -> MagicMock:
    lb = MagicMock()
    lb.fetch = db.fetch
    lb.fetchrow = db.fetchrow
    lb.fetchval = db.fetchval
    lb.execute = db.execute
    return lb


def _patch_pipeline_lakebase(db: MagicMock | None):
    handle = _mock_pipeline_lakebase(db) if db is not None else None
    return patch("iot_twin.routes.pipelines.get_pipeline_lakebase", AsyncMock(return_value=handle))


class TestLakebaseHealth:
    """GET /api/pipelines/:id/lakebase-health returns sink observability signals."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_shape(self) -> None:
        latest_at = _dt.datetime(2026, 4, 21, 10, 15, 30, tzinfo=_dt.UTC)
        db = _mock_db_for_health(row={"rows_5m": 1500, "latest_at": latest_at, "latency_sec": 2.5})
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_5m"] == 1500
        assert body["latest_at"] == "2026-04-21T10:15:30+00:00"
        assert body["latency_sec"] == 2.5
        # 1500 / 300 = 5.0
        assert body["inserts_per_sec_recent"] == 5.0
        assert "error" not in body

    @pytest.mark.asyncio
    async def test_no_rows_returns_zeroes(self) -> None:
        db = _mock_db_for_health(row={"rows_5m": 0, "latest_at": None, "latency_sec": None})
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")

        assert resp.status_code == 200
        body = resp.json()
        assert body == {
            "rows_5m": 0,
            "latest_at": None,
            "latency_sec": None,
            "inserts_per_sec_recent": 0.0,
        }

    @pytest.mark.asyncio
    async def test_db_unavailable_returns_zeroes_with_error(self) -> None:
        state = AppState()
        state.db = None
        state.db_available = False
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        # No lakebase resolvable → endpoint returns default payload with error
        with _patch_pipeline_lakebase(None):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_5m"] == 0
        assert body["latest_at"] is None
        assert body["latency_sec"] is None
        assert body["inserts_per_sec_recent"] == 0.0
        assert "error" in body

    @pytest.mark.asyncio
    async def test_db_error_returns_zeroes_with_error(self) -> None:
        db = _mock_db_for_health(raises=RuntimeError("connection reset"))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_5m"] == 0
        assert body["inserts_per_sec_recent"] == 0.0
        assert "connection reset" in body["error"]

    @pytest.mark.asyncio
    async def test_industry_health_requires_matching_deployment_c7266(self) -> None:
        """[C7266][defect][security] Reject cross-industry Lakebase reads."""
        state = _state_with_db(_mock_db_for_health())
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with (
            patch(
                "iot_twin.routes.pipelines._is_industry_deployment",
                AsyncMock(return_value=False),
            ),
            patch(
                "iot_twin.routes.pipelines.get_industry_lakebase",
                AsyncMock(),
            ) as resolve_industry,
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get(
                    "/api/pipelines/deployment-1/lakebase-health",
                    params={"industry": "pharmaceutical"},
                )

        assert response.status_code == 404
        assert response.json()["error"] == ("Realtime industry deployment not found")
        resolve_industry.assert_not_awaited()


# ---------------------------------------------------------------------------
# POST /api/pipelines/:id/heal-lakebase
# ---------------------------------------------------------------------------
def _pipeline_row(
    *,
    pipeline_id: str = "pipe-heal",
    user_email: str = "anonymous",
    source_config: dict | str | None = None,
    resources: dict | str | None = None,
) -> dict:
    """Build a minimal asyncpg-like row the route handler can consume."""
    import json as _json

    if source_config is None:
        source_config = {
            "lb_project_id": "proj-1",
            "lb_branch_id": "br-main",
            "lb_database": "databricks_postgres",
        }
    if resources is None:
        resources = {"job_id": 42}
    return {
        "id": pipeline_id,
        "user_email": user_email,
        "name": "heal-me",
        "status": "running",
        "industry": None,
        "sensor_config": "[]",
        "source_type": "lakebase",
        "source_config": _json.dumps(source_config)
        if isinstance(source_config, dict)
        else source_config,
        "sink_config": "{}",
        "resources": _json.dumps(resources) if isinstance(resources, dict) else resources,
        "created_at": _dt.datetime(2026, 4, 21, tzinfo=_dt.UTC),
        "updated_at": _dt.datetime(2026, 4, 21, tzinfo=_dt.UTC),
    }


def _mock_db_for_heal(row: dict | None) -> MagicMock:
    db = MagicMock()
    db.fetchrow = AsyncMock(return_value=row)
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


def _make_ws_mock(
    *,
    sp_client_id: str = "sp-uuid-123",
    active_run_ids: list[int] | None = None,
    new_run_id: int | None = 999,
) -> MagicMock:
    ws = MagicMock()
    ws.current_user = MagicMock()
    me = MagicMock()
    me.user_name = sp_client_id
    ws.current_user.me = MagicMock(return_value=me)
    ws.jobs = MagicMock()
    runs = []
    for rid in active_run_ids or []:
        r = MagicMock()
        r.run_id = rid
        runs.append(r)
    ws.jobs.list_runs = MagicMock(return_value=runs)
    ws.jobs.cancel_run = MagicMock(return_value=None)
    triggered = MagicMock()
    triggered.run_id = new_run_id
    ws.jobs.run_now = MagicMock(return_value=triggered)
    return ws


class TestHealLakebase:
    """POST /api/pipelines/:id/heal-lakebase re-applies OAuth-native provisioning."""

    @pytest.mark.asyncio
    async def test_heal_happy_path(self) -> None:
        db = _mock_db_for_heal(_pipeline_row())
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_ws_mock(active_run_ids=[101, 102], new_run_id=500)
        provision_mock = AsyncMock(return_value=[{"role": "created"}])
        lakets_mock = AsyncMock(return_value={"installed": True, "version": "0.1.0"})
        grant_mock = MagicMock(return_value=None)
        ops_jobs_mock = MagicMock(
            return_value=[{"job": "retention", "job_id": 1, "status": "created"}]
        )

        with (
            patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws),
            patch("iot_twin.routes.pipelines.provision_lakebase_role", provision_mock),
            patch("iot_twin.routes.pipelines.install_lakets_if_missing", lakets_mock),
            patch("iot_twin.routes.pipelines.grant_app_can_use", grant_mock),
            patch(
                "iot_twin.routes.pipelines._app_name", MagicMock(return_value="iot-digital-twin")
            ),
            patch("iot_twin.routes.pipelines.deploy_lakets_ops_jobs", ops_jobs_mock),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/pipelines/pipe-heal/heal-lakebase")

        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["pipeline_id"] == "pipe-heal"
        assert body["endpoint"] == "projects/proj-1/branches/br-main/endpoints/primary"
        steps_by_name = {s["step"]: s for s in body["steps"]}
        assert set(steps_by_name) == {
            "provision_role",
            "install_lakets",
            "grant_app_access",
            "deploy_lakets_jobs",
            "restart_job",
        }

        # grant CAN_USE called with the resolved app name + SP id
        grant_mock.assert_called_once()
        g_args = grant_mock.call_args
        assert g_args.args[0] is ws or g_args.kwargs.get("w") is ws
        assert "iot-digital-twin" in g_args.args or "iot-digital-twin" in g_args.kwargs.values()
        assert "sp-uuid-123" in g_args.args or "sp-uuid-123" in g_args.kwargs.values()

        # LakeTS ops jobs deployed for the pipeline's project/branch/database
        ops_jobs_mock.assert_called_once()
        oj_kwargs = ops_jobs_mock.call_args.kwargs
        oj_args = ops_jobs_mock.call_args.args
        assert "proj-1" in oj_args or oj_kwargs.get("project_id") == "proj-1"
        assert "br-main" in oj_args or oj_kwargs.get("branch_id") == "br-main"
        assert steps_by_name["deploy_lakets_jobs"]["results"] == [
            {"job": "retention", "job_id": 1, "status": "created"}
        ]

        # provision called with SP + pipeline params
        provision_mock.assert_awaited_once()
        kwargs = provision_mock.await_args.kwargs
        assert kwargs["sp_client_id"] == "sp-uuid-123"
        assert kwargs["project_id"] == "proj-1"
        assert kwargs["branch_id"] == "br-main"
        assert kwargs["database"] == "databricks_postgres"
        assert kwargs["endpoint_name"] == "projects/proj-1/branches/br-main/endpoints/primary"
        assert tuple(kwargs["sink_tables"]) == ("sensor_readings",)
        assert steps_by_name["provision_role"]["results"] == [{"role": "created"}]

        # lakets install called with endpoint + database
        lakets_mock.assert_awaited_once()
        lk_kwargs = lakets_mock.await_args.kwargs
        assert lk_kwargs["endpoint_name"] == "projects/proj-1/branches/br-main/endpoints/primary"
        assert lk_kwargs["database"] == "databricks_postgres"
        assert steps_by_name["install_lakets"]["result"] == {"installed": True, "version": "0.1.0"}

        # job restart — cancelled active runs then triggered a new one
        ws.jobs.list_runs.assert_called_once_with(job_id=42, active_only=True)
        assert ws.jobs.cancel_run.call_count == 2
        ws.jobs.run_now.assert_called_once_with(job_id=42)
        assert steps_by_name["restart_job"]["new_run_id"] == 500
        assert sorted(steps_by_name["restart_job"]["cancelled"]) == [101, 102]
        assert "error" not in steps_by_name["restart_job"]

    @pytest.mark.asyncio
    async def test_heal_missing_pipeline_returns_404(self) -> None:
        db = _mock_db_for_heal(None)
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=_make_ws_mock()),
            patch("iot_twin.routes.pipelines.provision_lakebase_role", AsyncMock()),
            patch("iot_twin.routes.pipelines.install_lakets_if_missing", AsyncMock()),
            patch("iot_twin.routes.pipelines.grant_app_can_use", MagicMock()),
            patch("iot_twin.routes.pipelines.deploy_lakets_ops_jobs", MagicMock(return_value=[])),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/pipelines/pipe-missing/heal-lakebase")

        assert resp.status_code == 404
        assert "not found" in resp.json()["error"].lower()

    @pytest.mark.asyncio
    async def test_heal_missing_sink_config_returns_400(self) -> None:
        # source_config without lb_project_id should yield 400 with clear error
        row = _pipeline_row(
            source_config={"lb_branch_id": "br-main", "lb_database": "databricks_postgres"}
        )
        db = _mock_db_for_heal(row)
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with (
            patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=_make_ws_mock()),
            patch("iot_twin.routes.pipelines.provision_lakebase_role", AsyncMock()),
            patch("iot_twin.routes.pipelines.install_lakets_if_missing", AsyncMock()),
            patch("iot_twin.routes.pipelines.grant_app_can_use", MagicMock()),
            patch("iot_twin.routes.pipelines.deploy_lakets_ops_jobs", MagicMock(return_value=[])),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/pipelines/pipe-heal/heal-lakebase")

        assert resp.status_code == 400
        err = resp.json()["error"]
        assert "lb_project_id" in err
        assert "missing" in err.lower() or "required" in err.lower()

    @pytest.mark.asyncio
    async def test_heal_provision_failure_returns_partial_results(self) -> None:
        db = _mock_db_for_heal(_pipeline_row())
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_ws_mock(active_run_ids=[], new_run_id=777)
        provision_mock = AsyncMock(side_effect=RuntimeError("permission denied"))
        lakets_mock = AsyncMock(return_value={"installed": True})

        with (
            patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws),
            patch("iot_twin.routes.pipelines.provision_lakebase_role", provision_mock),
            patch("iot_twin.routes.pipelines.install_lakets_if_missing", lakets_mock),
            patch("iot_twin.routes.pipelines.grant_app_can_use", MagicMock()),
            patch(
                "iot_twin.routes.pipelines._app_name", MagicMock(return_value="iot-digital-twin")
            ),
            patch("iot_twin.routes.pipelines.deploy_lakets_ops_jobs", MagicMock(return_value=[])),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/pipelines/pipe-heal/heal-lakebase")

        assert resp.status_code == 200, resp.text
        body = resp.json()

        steps_by_name = {s["step"]: s for s in body["steps"]}
        # provision step reports the error, doesn't break the response
        assert "error" in steps_by_name["provision_role"]
        assert "permission denied" in steps_by_name["provision_role"]["error"]
        # subsequent steps still ran
        lakets_mock.assert_awaited_once()
        assert steps_by_name["install_lakets"]["result"] == {"installed": True}
        assert steps_by_name["restart_job"]["new_run_id"] == 777


# ---------------------------------------------------------------------------
# DELETE /api/pipelines/:id  — UC table drop during cleanup
# ---------------------------------------------------------------------------
def _delete_pipeline_row(
    *,
    pipeline_id: str = "pipe-del",
    user_email: str = "anonymous",
    resources: dict | None = None,
) -> dict:
    """Minimal asyncpg-like row consumed by delete_pipeline."""
    import json as _json

    if resources is None:
        resources = {"uc_catalog": "iot_factory_abc123"}
    return {
        "id": pipeline_id,
        "user_email": user_email,
        "name": "del-me",
        "status": "running",
        "industry": None,
        "sensor_config": "[]",
        "source_type": "zerobus",
        "source_config": "{}",
        "sink_config": "{}",
        "resources": _json.dumps(resources),
        "created_at": _dt.datetime(2026, 4, 21, tzinfo=_dt.UTC),
        "updated_at": _dt.datetime(2026, 4, 21, tzinfo=_dt.UTC),
    }


def _mock_db_for_delete(row: dict | None) -> MagicMock:
    db = MagicMock()
    db.fetchrow = AsyncMock(return_value=row)
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


def _make_delete_ws_mock() -> MagicMock:
    """WorkspaceClient mock whose UC/job/cluster/schema/table deletes all succeed."""
    ws = MagicMock()
    ws.jobs = MagicMock()
    ws.jobs.delete = MagicMock(return_value=None)
    ws.clusters = MagicMock()
    ws.clusters.permanent_delete = MagicMock(return_value=None)
    ws.schemas = MagicMock()
    ws.schemas.delete = MagicMock(return_value=None)
    ws.tables = MagicMock()
    ws.tables.delete = MagicMock(return_value=None)
    return ws


class TestDeletePipelineUcSafety:
    """Regression coverage for destructive UC cleanup during pipeline delete."""

    @pytest.mark.asyncio
    async def test_zerobus_delete_never_deletes_shared_schema_c7113_defect_regression(
        self,
    ) -> None:
        db = _mock_db_for_delete(
            _delete_pipeline_row(
                resources={
                    "uc_catalog": "example_eu_catalog",
                    "uc_schema": "rtm_lakets_zb",
                }
            )
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        ws = _make_delete_ws_mock()

        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        ws.schemas.delete.assert_not_called()
        ws.tables.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_table_ownership_metadata_fails_closed_c7113_defect_regression(
        self,
    ) -> None:
        db = _mock_db_for_delete(
            _delete_pipeline_row(
                resources={
                    "uc_catalog": "shared_catalog",
                    "owned_uc_tables": [
                        {"full_name": "shared_catalog.shared_schema.target_zerobus"}
                    ],
                }
            )
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        ws = _make_delete_ws_mock()

        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        ws.tables.delete.assert_not_called()
        assert any("ownership" in str(entry).lower() for entry in resp.json()["cleanup_log"])

    @pytest.mark.asyncio
    async def test_cleanup_failure_is_surfaced_without_false_success_c7113_defect_regression(
        self,
    ) -> None:
        db = _mock_db_for_delete(_delete_pipeline_row(resources={"job_id": "42"}))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        ws = _make_delete_ws_mock()
        ws.jobs.delete.side_effect = RuntimeError("permission denied")

        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 502, resp.text
        assert resp.json()["deleted"] is False
        assert "permission denied" in resp.text
        status_updates = [
            call
            for call in db.execute.await_args_list
            if "UPDATE pipelines SET status = 'deleted'" in call.args[0]
        ]
        assert status_updates == []

    @pytest.mark.asyncio
    async def test_missing_job_is_idempotent_and_pipeline_delete_completes(
        self,
    ) -> None:
        db = _mock_db_for_delete(_delete_pipeline_row(resources={"job_id": "42"}))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        ws = _make_delete_ws_mock()
        ws.jobs.delete.side_effect = NotFound("job not found")

        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        assert resp.json()["deleted"] is True

    @pytest.mark.asyncio
    async def test_catalog_location_alone_does_not_authorize_table_delete(self) -> None:
        db = _mock_db_for_delete(
            _delete_pipeline_row(resources={"uc_catalog": "iot_factory_abc123"})
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_delete_ws_mock()
        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["deleted"] is True

        ws.tables.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_only_exactly_owned_table_is_deleted_and_sibling_is_preserved(
        self,
    ) -> None:
        db = _mock_db_for_delete(
            _delete_pipeline_row(
                resources={
                    "owned_uc_tables": [
                        {
                            "full_name": "shared.schema.target_zerobus",
                            "pipeline_id": "pipe-del",
                        },
                        {
                            "full_name": "shared.schema.sibling_zerobus",
                            "pipeline_id": "pipe-sibling",
                        },
                    ],
                }
            )
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_delete_ws_mock()
        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        called = {c.kwargs.get("full_name") for c in ws.tables.delete.call_args_list}
        assert called == {"shared.schema.target_zerobus"}

    @pytest.mark.asyncio
    async def test_cold_tier_table_outside_catalog_is_skipped(self) -> None:
        # A tracked cold-tier table that does NOT belong to this pipeline's
        # catalog must never be dropped (no shared-catalog blast radius).
        db = _mock_db_for_delete(
            _delete_pipeline_row(
                resources={
                    "uc_catalog": "iot_factory_abc123",
                    "cold_tier_table": "shared_prod.cold.sensor_history",
                }
            )
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_delete_ws_mock()
        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        called = {c.kwargs.get("full_name") for c in ws.tables.delete.call_args_list}
        assert "shared_prod.cold.sensor_history" not in called
        assert "iot_factory_abc123.raw.iot_sensor_data" not in called

    @pytest.mark.asyncio
    async def test_not_found_table_is_treated_as_success(self) -> None:
        db = _mock_db_for_delete(
            _delete_pipeline_row(
                resources={
                    "owned_uc_tables": [
                        {
                            "full_name": "iot_factory_abc123.raw.iot_sensor_data",
                            "pipeline_id": "pipe-del",
                        }
                    ]
                }
            )
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_delete_ws_mock()
        ws.tables.delete = MagicMock(side_effect=NotFound("table does not exist"))
        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["deleted"] is True
        entries = [e for e in body["cleanup_log"] if isinstance(e, dict)]
        raw_entry = next(
            e
            for e in entries
            if e.get("resource") == "UC table iot_factory_abc123.raw.iot_sensor_data"
        )
        assert raw_entry["status"] == "not_found"

    @pytest.mark.asyncio
    async def test_drop_failure_is_fatal_and_surfaced(self) -> None:
        db = _mock_db_for_delete(
            _delete_pipeline_row(
                resources={
                    "owned_uc_tables": [
                        {
                            "full_name": "iot_factory_abc123.raw.iot_sensor_data",
                            "pipeline_id": "pipe-del",
                        }
                    ]
                }
            )
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_delete_ws_mock()
        ws.tables.delete = MagicMock(side_effect=RuntimeError("permission denied"))
        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 502, resp.text
        body = resp.json()
        assert body["deleted"] is False
        entries = [e for e in body["cleanup_log"] if isinstance(e, dict)]
        raw_entry = next(
            e
            for e in entries
            if e.get("resource") == "UC table iot_factory_abc123.raw.iot_sensor_data"
        )
        assert raw_entry["status"] == "failed"
        assert "permission denied" in raw_entry.get("error", "")

    @pytest.mark.asyncio
    async def test_no_catalog_skips_uc_table_drop(self) -> None:
        # A pipeline that never provisioned a UC catalog has nothing to drop.
        db = _mock_db_for_delete(_delete_pipeline_row(resources={}))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        ws = _make_delete_ws_mock()
        with patch("iot_twin.routes.pipelines.WorkspaceClient", return_value=ws):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.delete("/api/pipelines/pipe-del")

        assert resp.status_code == 200, resp.text
        ws.tables.delete.assert_not_called()
