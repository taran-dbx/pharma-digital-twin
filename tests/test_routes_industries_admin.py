"""Tests for routes/industries_admin.py — admin pipeline management API.

Unit tests drive the routes through an httpx ``AsyncClient`` with the repos,
provisioning service, and teardown service mocked, plus the ``is_admin`` gate
and app-backend DB pool stubbed. A skippable live smoke test ensures the
app-backend tables against the real ``iot-app-backend`` project.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import industries_admin
from iot_twin.services import resource_ledger, teardown_service
from iot_twin.services.pipelines import pipeline_setup_repo
from iot_twin.services.validation import validation_state_machine as sm

MOD = "iot_twin.routes.industries_admin"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _state(with_db: bool = True) -> AppState:
    state = AppState()
    if with_db:
        db = MagicMock()
        db.fetch = AsyncMock(return_value=[])
        db.fetchrow = AsyncMock(return_value=None)
        db.execute = AsyncMock(return_value="DELETE 1")
        state.app_backend_db = db
        state.app_backend_available = True
    return state


def test_job_status_lookups_run_concurrently_c7251() -> None:
    """[C7251][performance][regression] Jobs API latency is not additive."""
    barrier = threading.Barrier(2, timeout=1)

    def pipeline_status(job_id, *, w):
        barrier.wait()
        return {"state": "RUNNING", "tasks": [], "job_id": job_id}

    deployments = [
        {"mode": "realtime", "job_id": "j1"},
        {"mode": "microbatch", "job_id": "j2"},
    ]
    clients = [MagicMock(), MagicMock()]
    with (
        patch(f"{MOD}.deployment_service._new_ws", MagicMock(side_effect=clients)) as new_ws,
        patch(
            f"{MOD}.deployment_service.pipeline_status", side_effect=pipeline_status
        ) as status_mock,
    ):
        result = industries_admin._job_running_lookup(deployments)

    assert set(result) == {"j1", "j2"}
    assert new_ws.call_count == 2
    assert {id(call.kwargs["w"]) for call in status_mock.call_args_list} == {
        id(client) for client in clients
    }


def test_runtime_reconciliation_stops_all_tasks_on_failure_c7259() -> None:
    """[C7259][defect][regression] A broken task pause-cancels its whole job."""
    client = MagicMock()
    degraded = {
        "state": "RUNNING",
        "degraded": True,
        "restartable": True,
        "detail": "data_producer: Driver stopped unexpectedly",
        "tasks": [{"task": "data_producer", "result": "FAILED"}],
    }
    with (
        patch(f"{MOD}.deployment_service._new_ws", return_value=client),
        patch(f"{MOD}.deployment_service.pipeline_status", return_value=degraded),
        patch(
            f"{MOD}.deployment_service.stop_pipeline",
            return_value={"state": "STOPPED", "paused": True},
        ) as stop,
    ):
        result = industries_admin._job_running_lookup(
            [{"mode": "realtime", "job_id": "j1"}],
            reconcile_failures=True,
        )

    stop.assert_called_once_with("j1", w=client, continuous=True)
    assert result["j1"]["running"] is False
    assert result["j1"]["degraded"] is False
    assert result["j1"]["restartable"] is True
    assert "data_producer" in result["j1"]["detail"]


def test_public_runtime_lookup_never_mutates_jobs_c7259() -> None:
    """[C7259][security][regression] Read-only public polls cannot stop jobs."""
    client = MagicMock()
    with (
        patch(f"{MOD}.deployment_service._new_ws", return_value=client),
        patch(
            f"{MOD}.deployment_service.pipeline_status",
            return_value={"state": "RUNNING", "degraded": True, "tasks": []},
        ),
        patch(f"{MOD}.deployment_service.stop_pipeline") as stop,
    ):
        industries_admin._job_running_lookup([{"mode": "realtime", "job_id": "j1"}])

    stop.assert_not_called()


def test_zerobus_failure_cancels_without_continuous_pause_c7259() -> None:
    client = MagicMock()
    with (
        patch(f"{MOD}.deployment_service._new_ws", return_value=client),
        patch(
            f"{MOD}.deployment_service.pipeline_status",
            return_value={"state": "RUNNING", "degraded": True, "tasks": []},
        ),
        patch(
            f"{MOD}.deployment_service.stop_pipeline",
            return_value={"state": "STOPPED"},
        ) as stop,
    ):
        industries_admin._job_running_lookup(
            [{"mode": "zerobus", "job_id": "j1"}],
            reconcile_failures=True,
        )

    stop.assert_called_once_with("j1", w=client, continuous=False)


def _app(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(industries_admin.router)
    return app


def _client(state: AppState) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=_app(state)), base_url="http://test")


def _admin(value: bool = True):
    return patch(f"{MOD}.app_permissions.is_admin", MagicMock(return_value=value))


# ---------------------------------------------------------------------------
# Admin gating + DB availability
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestGuards:
    async def test_non_admin_forbidden(self) -> None:
        with _admin(False):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 403

    async def test_db_unavailable_returns_503(self) -> None:
        with _admin(True):
            async with _client(_state(with_db=False)) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/setup
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestCreateOrUpdateSetup:
    async def test_creates_new_draft_setup(self) -> None:
        created = {"id": "s1", "industry": "pharmaceutical", "status": "draft"}
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=None),
            ),
            patch(
                f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock(return_value=created)
            ) as create_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={
                        "flavor": "classic",
                        "source_type": "kafka",
                        "enabled_modes": ["realtime"],
                    },
                )
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is True
        assert body["setup"]["id"] == "s1"
        # created with draft status + one-per-industry enforced via prior lookup
        _, kwargs = create_mock.call_args
        assert kwargs["industry"] == "pharmaceutical"
        assert kwargs["status"] == "draft"

    async def test_persists_frequency_for_each_selected_mode(self) -> None:
        mode_configs = {
            "realtime": {"rate_hz": 1200},
            "zerobus": {"rate_hz": 3000},
        }
        created = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "draft",
            "mode_configs": mode_configs,
        }
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=None),
            ),
            patch(
                f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock(return_value=created)
            ) as create_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={
                        "enabled_modes": ["realtime", "zerobus"],
                        "mode_configs": mode_configs,
                    },
                )

        assert resp.status_code == 200
        assert resp.json()["setup"]["mode_configs"] == mode_configs
        assert create_mock.call_args.kwargs["mode_configs"] == mode_configs

    async def test_create_rejects_mode_config_without_selected_modes(self) -> None:
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=None),
            ),
            patch(f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()) as create_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"mode_configs": {"zerobus": {"rate_hz": 2000}}},
                )

        assert resp.status_code == 422
        create_mock.assert_not_awaited()

    async def test_updates_existing_setup_in_place(self) -> None:
        existing = {"id": "s1", "industry": "pharmaceutical"}
        updated = {"id": "s1", "industry": "pharmaceutical", "flavor": "classic"}
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=existing),
            ),
            patch(
                f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock(return_value=updated)
            ) as update_mock,
            patch(f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()) as create_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"flavor": "classic"},
                )
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is False
        assert body["setup"]["flavor"] == "classic"
        # one-per-industry: no second row created
        create_mock.assert_not_awaited()
        # only non-null fields forwarded to update
        args, kwargs = update_mock.call_args
        assert args[1] == "s1"
        assert kwargs == {"flavor": "classic"}

    async def test_rejects_config_for_a_mode_not_selected_on_existing_setup(self) -> None:
        existing = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime"],
        }
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=existing),
            ),
            patch(f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"mode_configs": {"zerobus": {"rate_hz": 2000}}},
                )

        assert resp.status_code == 422
        update_mock.assert_not_awaited()

    async def test_rejects_partial_mode_config_replacement(self) -> None:
        existing = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime", "zerobus"],
            "mode_configs": {
                "realtime": {"rate_hz": 1200},
                "zerobus": {"rate_hz": 2400},
            },
        }
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=existing),
            ),
            patch(f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"mode_configs": {"realtime": {"rate_hz": 1800}}},
                )

        assert resp.status_code == 422
        update_mock.assert_not_awaited()

    async def test_create_only_on_existing_industry_rejected_409(self) -> None:
        existing = {"id": "s1", "industry": "pharmaceutical"}
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=existing),
            ),
            patch(f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()) as create_mock,
            patch(f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"create_only": True, "flavor": "classic"},
                )
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["setup_id"] == "s1"
        # neither a second create nor an in-place update happens
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()

    async def test_create_only_409_serializes_uuid_setup_id(self) -> None:
        """Regression: real Lakebase returns the ``id`` column as a
        ``uuid.UUID``. FastAPI renders the HTTPException detail with plain
        ``json.dumps`` (no jsonable_encoder), so a raw UUID there raised
        TypeError → the intended 409 surfaced as a 500. The route must coerce
        ``setup_id`` to str so the guard returns a clean 409.
        """
        import uuid as _uuid

        setup_uuid = _uuid.UUID("00000000-0000-0000-0000-00000000000f")
        existing = {"id": setup_uuid, "industry": "pharmaceutical"}
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=existing),
            ),
            patch(f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()),
            patch(f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()),
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"create_only": True},
                )
        assert resp.status_code == 409
        assert resp.json()["detail"]["setup_id"] == str(setup_uuid)


# ---------------------------------------------------------------------------
# PATCH /api/admin/pipelines/{setup_id}/modes/{mode}/config
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestUpdateModeConfig:
    async def test_rejects_frequency_outside_supported_range(self) -> None:
        with _admin(True), patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock()) as get_mock:
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes/zerobus/config",
                    json={"rate_hz": 50},
                )

        assert resp.status_code == 422
        get_mock.assert_not_awaited()

    async def test_rejects_fractional_frequency(self) -> None:
        with _admin(True), patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock()) as get_mock:
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes/zerobus/config",
                    json={"rate_hz": 1500.5},
                )

        assert resp.status_code == 422
        get_mock.assert_not_awaited()

    async def test_rejects_frequency_change_while_mode_is_running(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime"],
            "mode_configs": {"realtime": {"rate_hz": 2000}},
        }
        deployments = [{"mode": "realtime", "job_id": "42"}]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(
                f"{MOD}._job_running_lookup",
                MagicMock(return_value={"42": {"running": True, "starting": False}}),
            ),
            patch(f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes/realtime/config",
                    json={"rate_hz": 1500},
                )

        assert resp.status_code == 409
        assert resp.json()["detail"] == "stop the pipeline before changing its frequency"
        update_mock.assert_not_awaited()

    async def test_rejects_change_when_runtime_state_cannot_be_confirmed(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime"],
            "mode_configs": {"realtime": {"rate_hz": 2000}},
        }
        deployments = [{"mode": "realtime", "job_id": "42"}]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(f"{MOD}._job_running_lookup", MagicMock(return_value={})),
            patch(f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes/realtime/config",
                    json={"rate_hz": 1500},
                )

        assert resp.status_code == 503
        assert resp.json()["detail"] == "could not confirm that the pipeline is stopped"
        update_mock.assert_not_awaited()

    async def test_rejects_change_when_jobs_api_status_lookup_fails(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime"],
            "mode_configs": {"realtime": {"rate_hz": 2000}},
        }
        deployments = [{"mode": "realtime", "job_id": "42"}]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(f"{MOD}.deployment_service._new_ws", MagicMock(return_value=MagicMock())),
            patch(
                f"{MOD}.deployment_service.pipeline_status",
                MagicMock(side_effect=RuntimeError("Jobs API unavailable")),
            ),
            patch(f"{MOD}.pipeline_setup_repo.update_mode_config", AsyncMock()) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes/realtime/config",
                    json={"rate_hz": 1500},
                )

        assert resp.status_code == 503
        update_mock.assert_not_awaited()

    async def test_updates_only_the_stopped_modes_frequency(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime", "zerobus"],
            "mode_configs": {
                "realtime": {"rate_hz": 2000},
                "zerobus": {"rate_hz": 3000},
            },
        }
        updated = {
            **setup,
            "mode_configs": {
                "realtime": {"rate_hz": 1500},
                "zerobus": {"rate_hz": 3000},
            },
        }
        deployments = [{"mode": "realtime", "job_id": "42"}]
        state = _state()
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(
                f"{MOD}._job_running_lookup",
                MagicMock(return_value={"42": {"running": False, "starting": False}}),
            ) as runtime_lookup,
            patch(
                f"{MOD}.pipeline_setup_repo.update_mode_config",
                AsyncMock(return_value=updated),
                create=True,
            ) as update_mock,
        ):
            async with _client(state) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes/realtime/config",
                    json={"rate_hz": 1500},
                )

        assert resp.status_code == 200
        assert resp.json()["mode_config"] == {"rate_hz": 1500}
        assert runtime_lookup.call_args.kwargs["reconcile_failures"] is False
        update_mock.assert_awaited_once_with(
            state.app_backend_db,
            "s1",
            "realtime",
            {"rate_hz": 1500},
        )


# ---------------------------------------------------------------------------
# PATCH /api/admin/pipelines/{setup_id}/modes
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestUpdatePipelineModes:
    async def test_noop_mode_update_is_allowed_while_validation_runs(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime"],
            "validation_state": {
                "steps": {
                    "realtime:write_test": {
                        "status": sm.RUNNING,
                        "error": None,
                        "updated_at": None,
                    }
                }
            },
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock()) as list_deployments,
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes",
                    json={"enabled_modes": ["realtime"]},
                )

        assert resp.status_code == 200
        list_deployments.assert_not_awaited()

    async def test_rejects_mode_change_while_validation_is_running(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime"],
            "validation_state": {
                "steps": {
                    "realtime:write_test": {
                        "status": sm.RUNNING,
                        "error": None,
                        "updated_at": None,
                    }
                }
            },
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock()) as list_deployments,
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes",
                    json={"enabled_modes": ["zerobus"]},
                )

        assert resp.status_code == 409
        list_deployments.assert_not_awaited()

    async def test_add_mode_preserves_existing_validation_results(self) -> None:
        state = sm.initial_state(["realtime", "microbatch"])
        for key in state["sequence"]:
            state = sm.set_step(state, key, sm.PASSED)
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime", "microbatch"],
            "validation_state": state,
        }
        updated = {**setup, "enabled_modes": ["realtime", "microbatch", "zerobus"]}

        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])),
            patch(
                f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock(return_value=updated)
            ) as update_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes",
                    json={"enabled_modes": ["realtime", "microbatch", "zerobus"]},
                )

        assert resp.status_code == 200
        fields = update_mock.await_args.kwargs
        assert fields["enabled_modes"] == ["realtime", "microbatch", "zerobus"]
        assert fields["validation_state"]["steps"]["realtime:write_test"]["status"] == sm.PASSED
        assert fields["validation_state"]["steps"]["zerobus:create_table"]["status"] == sm.PENDING

    async def test_remove_mode_deletes_its_job_and_deployment(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime", "zerobus"],
            "validation_state": sm.initial_state(["realtime", "zerobus"]),
        }
        deployment = {"id": "d-zb", "mode": "zerobus", "job_id": "9001"}
        job_resource = {
            "id": "r-zb",
            "deployment_id": "d-zb",
            "resource_type": "job",
            "identifier": "9001",
        }
        delete_job = AsyncMock()

        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=[deployment]),
            ),
            patch(
                f"{MOD}.resource_ledger.list_resources",
                AsyncMock(return_value=[job_resource]),
            ),
            patch(
                f"{MOD}.deployment_service.make_job_deleter",
                MagicMock(return_value=delete_job),
            ),
            patch(
                f"{MOD}.deployment_repo.delete_deployment", AsyncMock(return_value=True)
            ) as delete_deployment,
            patch(f"{MOD}.resource_ledger.mark_resource_status", AsyncMock()) as mark_resource,
            patch(
                f"{MOD}.pipeline_setup_repo.update_setup",
                AsyncMock(return_value={**setup, "enabled_modes": ["realtime"]}),
            ),
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes",
                    json={"enabled_modes": ["realtime"]},
                )

        assert resp.status_code == 200
        delete_job.assert_awaited_once_with(job_resource)
        delete_deployment.assert_awaited_once()
        assert delete_deployment.await_args.args[1] == "d-zb"
        mark_resource.assert_awaited_once()
        assert resp.json()["removed"] == ["zerobus"]

    async def test_remove_failed_deploy_without_job_drops_metadata_only(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "enabled_modes": ["realtime", "zerobus"],
            "validation_state": sm.initial_state(["realtime", "zerobus"]),
        }
        deployment = {
            "id": "d-zb",
            "mode": "zerobus",
            "status": "deploying",
            "job_id": None,
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=[deployment]),
            ),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
            patch(f"{MOD}.deployment_service.make_job_deleter") as delete_job,
            patch(
                f"{MOD}.deployment_repo.delete_deployment", AsyncMock(return_value=True)
            ) as delete_deployment,
            patch(
                f"{MOD}.pipeline_setup_repo.update_setup",
                AsyncMock(return_value={**setup, "enabled_modes": ["realtime"]}),
            ),
        ):
            async with _client(_state()) as c:
                resp = await c.patch(
                    "/api/admin/pipelines/s1/modes",
                    json={"enabled_modes": ["realtime"]},
                )

        assert resp.status_code == 200
        delete_job.assert_not_called()
        delete_deployment.assert_awaited_once()


# ---------------------------------------------------------------------------
# GET /api/admin/industries/{industry}/setup
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestGetSetupForIndustry:
    async def test_404_when_no_setup(self) -> None:
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=None),
            ),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/industries/pharmaceutical/setup")
        assert resp.status_code == 404

    async def test_returns_existing_setup_with_contract(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "draft",
            "enabled_modes": ["realtime"],
        }
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=setup),
            ),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/industries/pharmaceutical/setup")
        assert resp.status_code == 200
        body = resp.json()
        assert body["setup"]["id"] == "s1"
        assert body["pipeline"]["setup_id"] == "s1"
        assert body["pipeline"]["status"] == "draft"


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/provision
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestProvision:
    async def test_404_when_no_setup(self) -> None:
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=None),
            ),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 404

    async def test_setup_id_must_match_path_industry_c7268(self) -> None:
        """[C7268][defect][security] Prevent cross-industry provisioning."""
        setup = {"id": "s1", "industry": "manufacturing"}
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup",
                AsyncMock(return_value=setup),
            ),
            patch(
                f"{MOD}.industry_provisioning_service.provision_industry",
                AsyncMock(),
            ) as provision,
        ):
            async with _client(_state()) as client:
                response = await client.post(
                    "/api/admin/industries/pharmaceutical/provision",
                    json={"setup_id": "s1"},
                )

        assert response.status_code == 409
        provision.assert_not_awaited()

    async def test_provisions_and_returns_resources(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical", "warehouse_id": "wh1"}
        summary = {"industry": "pharmaceutical", "sections": [], "catalog": "iot_pharmaceutical"}
        resources = [{"id": "r1", "resource_type": "uc_catalog"}]
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=setup),
            ),
            patch(
                f"{MOD}.industry_provisioning_service.provision_industry",
                AsyncMock(return_value=summary),
            ) as prov_mock,
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 200
        body = resp.json()
        assert body["setup_id"] == "s1"
        assert body["resources"] == resources
        args, kwargs = prov_mock.call_args
        assert args[0] == "pharmaceutical"
        assert args[1] == "s1"
        assert kwargs["warehouse_id"] == "wh1"

    async def test_zerobus_only_setup_passes_enable_realtime_false(self) -> None:
        # A zerobus-only setup must NOT trigger realtime Lakebase branch
        # provisioning (which may not exist on this workspace) — the route
        # gates enable_realtime on enabled_modes.
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "warehouse_id": "wh1",
            "enabled_modes": ["zerobus"],
        }
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=setup),
            ),
            patch(
                f"{MOD}.industry_provisioning_service.provision_industry",
                AsyncMock(return_value={"sections": []}),
            ) as prov_mock,
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 200
        assert prov_mock.call_args.kwargs["enable_realtime"] is False

    async def test_realtime_setup_passes_enable_realtime_true(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "warehouse_id": "wh1",
            "enabled_modes": ["zerobus", "realtime"],
        }
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=setup),
            ),
            patch(
                f"{MOD}.industry_provisioning_service.provision_industry",
                AsyncMock(return_value={"sections": []}),
            ) as prov_mock,
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 200
        assert prov_mock.call_args.kwargs["enable_realtime"] is True

    async def test_failed_preflight_returns_409(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical"}
        with (
            _admin(True),
            patch(
                f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
                AsyncMock(return_value=setup),
            ),
            patch(
                f"{MOD}.industry_provisioning_service.provision_industry",
                AsyncMock(side_effect=RuntimeError("external location missing")),
            ),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 409
        assert "external location" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestListPipelines:
    async def test_public_runtime_serves_stale_cache_while_refreshing_c7253(
        self,
    ) -> None:
        """[C7253][performance][regression] Gallery does not await live refresh."""
        state = _state()
        cached = {"pipelines": [{"industry": "pharmaceutical", "status": "running"}]}
        state.industry_runtime_cache = cached
        state.industry_runtime_cached_at = 0.0
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()

        async def blocked_refresh(_state, _generation):
            refresh_started.set()
            await release_refresh.wait()
            return cached

        with patch(f"{MOD}._refresh_industry_runtime_cache", blocked_refresh):
            async with _client(state) as c:
                resp = await asyncio.wait_for(c.get("/api/industries/runtime"), timeout=0.2)
            await refresh_started.wait()

        assert resp.status_code == 200
        assert resp.json() == cached
        assert not release_refresh.is_set()
        release_refresh.set()
        await state.industry_runtime_refresh_task

    async def test_invalidated_refresh_cannot_republish_old_state_c7254(
        self,
    ) -> None:
        """[C7254][defect][regression] Mutation generation rejects stale writes."""
        state = _state()
        cached = {"pipelines": [{"industry": "pharmaceutical", "status": "running"}]}
        state.industry_runtime_cache = cached
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()

        async def delayed_items(_state, *, include_resources):
            assert include_resources is False
            refresh_started.set()
            await release_refresh.wait()
            return []

        with patch(f"{MOD}._list_pipeline_items", side_effect=delayed_items):
            task = asyncio.create_task(industries_admin._refresh_industry_runtime_cache(state, 0))
            await refresh_started.wait()
            state.invalidate_industry_runtime_cache()
            release_refresh.set()
            await task

        assert state.industry_runtime_cache == cached
        assert state.industry_runtime_cached_at == 0.0
        assert state.industry_runtime_cache_generation == 1

    async def test_public_runtime_cold_start_times_out_c7255(self) -> None:
        """[C7255][defect][regression] Cold cache cannot hang gallery requests."""
        state = _state()
        release_refresh = asyncio.Event()

        async def blocked_refresh(_state, _generation):
            await release_refresh.wait()
            return {"pipelines": []}

        with (
            patch(f"{MOD}._refresh_industry_runtime_cache", blocked_refresh),
            patch(
                f"{MOD}._INDUSTRY_RUNTIME_COLD_START_TIMEOUT_SECONDS",
                0.01,
            ),
        ):
            async with _client(state) as c:
                resp = await c.get("/api/industries/runtime")
            release_refresh.set()
            await state.industry_runtime_refresh_task

        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "2"

    async def test_public_runtime_returns_sanitized_deployed_state_c7252(self) -> None:
        """[C7252][defect][regression] Non-admin viewers receive live card state."""
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "deployed",
            "bootstrap_servers": "secret-broker:9198",
            "mode_configs": {"realtime": {"rate_hz": 500}},
            "enabled_modes": ["realtime"],
        }
        deployments = [{"id": "d1", "mode": "realtime", "status": "deployed", "job_id": "j1"}]
        resources = AsyncMock(return_value=[])
        with (
            _admin(False),
            patch(f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=[setup])),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(f"{MOD}.resource_ledger.list_resources", resources),
            patch(
                f"{MOD}._job_running_lookup",
                MagicMock(
                    return_value={
                        "j1": {
                            "running": False,
                            "restartable": True,
                            "detail": "data_producer: driver stopped",
                            "tasks": [{"task": "producer", "message": "secret detail"}],
                        }
                    }
                ),
            ),
            patch(f"{MOD}._flowing_lookup", AsyncMock(return_value={"realtime": True})),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/industries/runtime")

        assert resp.status_code == 200
        pipeline = resp.json()["pipelines"][0]
        assert pipeline["status"] == "stopped"
        assert pipeline["setup"] == {
            "id": "s1",
            "industry": "pharmaceutical",
            "mode_configs": {"realtime": {"rate_hz": 500}},
        }
        assert pipeline["deployments"] == [{"mode": "realtime", "status": "deployed"}]
        assert "job_id" not in pipeline["deployments"][0]
        assert "bootstrap_servers" not in pipeline["setup"]
        assert "resource_health" not in pipeline
        assert pipeline["modes"][0]["runtime_detail"] == "data_producer: driver stopped"
        assert pipeline["modes"][0]["restartable"] is True
        assert "tasks" not in pipeline["modes"][0]
        resources.assert_not_awaited()

    async def test_pipeline_repository_lookups_run_concurrently_c7249(self) -> None:
        """[C7249][performance][regression] Gallery avoids serial setup lookups."""
        setups = [
            {"id": f"s{index}", "industry": f"industry-{index}", "status": "draft"}
            for index in range(6)
        ]
        active_deployments = 0
        max_active_deployments = 0
        active_resources = 0
        max_active_resources = 0

        async def list_deployments(_db, _setup_id):
            nonlocal active_deployments, max_active_deployments
            active_deployments += 1
            max_active_deployments = max(max_active_deployments, active_deployments)
            await asyncio.sleep(0.01)
            active_deployments -= 1
            return []

        async def list_resources(_db, _setup_id):
            nonlocal active_resources, max_active_resources
            active_resources += 1
            max_active_resources = max(max_active_resources, active_resources)
            await asyncio.sleep(0.01)
            active_resources -= 1
            return []

        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=setups)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(side_effect=list_deployments),
            ),
            patch(
                f"{MOD}.resource_ledger.list_resources",
                AsyncMock(side_effect=list_resources),
            ),
            patch(f"{MOD}._job_running_lookup", MagicMock(return_value={})),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")

        assert resp.status_code == 200
        assert max_active_deployments == industries_admin._LIST_PIPELINE_CONCURRENCY
        assert max_active_resources == industries_admin._LIST_PIPELINE_CONCURRENCY

    async def test_lists_with_contract_and_resource_health(self) -> None:
        setups = [
            {
                "id": "s1",
                "industry": "pharmaceutical",
                "status": "deployed",
                "enabled_modes": ["realtime"],
            }
        ]
        # Deployed realtime but CDF not done -> unhealthy + not startable + stopped.
        deployments = [{"id": "d1", "mode": "realtime", "status": "deployed", "job_id": "j1"}]
        resources = [
            {"id": "r1", "resource_type": "uc_catalog", "status": "active"},
            {"id": "r2", "resource_type": "job", "status": "active"},
        ]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=setups)),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)),
            patch(f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": False})),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 200
        items = resp.json()["pipelines"]
        assert len(items) == 1
        pipe = items[0]
        # contract fields
        assert pipe["setup_id"] == "s1"
        assert pipe["industry"] == "pharmaceutical"
        assert pipe["status"] == "stopped"
        assert pipe["health"] == "unhealthy"
        assert pipe["startable"] is False
        assert pipe["ready"] is False
        assert pipe["modes"][0] == {
            "mode": "realtime",
            "status": "stopped",
            "health": "unhealthy",
            "startable": False,
            "runtime_detail": None,
            "tasks": [],
            "restartable": False,
            "deployed": True,
            "flowing": False,
        }
        # resource ledger rollup preserved under resource_health
        assert pipe["resource_health"]["overall"] == "healthy"
        assert pipe["resource_health"]["jobs_running"] == 1

    async def test_draft_pipeline_when_no_deployments(self) -> None:
        setups = [
            {
                "id": "s1",
                "industry": "pharmaceutical",
                "status": "draft",
                "enabled_modes": ["realtime"],
            }
        ]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=setups)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        pipe = resp.json()["pipelines"][0]
        assert pipe["status"] == "draft"
        assert pipe["health"] == "draft"
        assert pipe["startable"] is False

    async def test_running_and_flowing_reports_running(self) -> None:
        # Deployed + RUNNING job + fresh data in the sink → running.
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "deployed",
            "enabled_modes": ["microbatch"],
            "validation_state": {
                "steps": {k: {"status": sm.PASSED} for k in sm.mode_required_keys("microbatch")}
            },
        }
        deployments = [{"id": "d1", "mode": "microbatch", "status": "deployed", "job_id": "j1"}]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=[setup])),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
            patch(f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": True})),
            patch(f"{MOD}._flowing_lookup", AsyncMock(return_value={"microbatch": True})),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        pipe = resp.json()["pipelines"][0]
        assert pipe["status"] == "running"
        assert pipe["modes"][0]["status"] == "running"
        assert pipe["modes"][0]["flowing"] is True

    async def test_running_without_freshness_still_reports_running_c7264(self) -> None:
        """[C7264][defect][regression] Sink freshness cannot strand RUNNING jobs."""
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "deployed",
            "enabled_modes": ["microbatch"],
            "validation_state": {
                "steps": {key: {"status": sm.PASSED} for key in sm.mode_required_keys("microbatch")}
            },
        }
        deployments = [{"id": "d1", "mode": "microbatch", "status": "deployed", "job_id": "j1"}]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=[setup])),
            patch(
                f"{MOD}.deployment_repo.list_deployments",
                AsyncMock(return_value=deployments),
            ),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
            patch(f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": True})),
            patch(f"{MOD}._flowing_lookup", AsyncMock(return_value={})),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        pipe = resp.json()["pipelines"][0]
        assert pipe["status"] == "running"
        assert pipe["modes"][0]["status"] == "running"
        assert pipe["modes"][0]["flowing"] is False


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines/{setup_id}/health
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestPipelineHealth:
    async def test_404_when_setup_missing(self) -> None:
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=None)),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/nope/health")
        assert resp.status_code == 404

    async def test_returns_grouped_resource_health(self) -> None:
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "deployed",
            "enabled_modes": ["realtime"],
        }
        resources = [
            {
                "id": "r1",
                "resource_type": "uc_catalog",
                "identifier": "iot_x",
                "status": "active",
                "metadata": {},
            },
            {
                "id": "r2",
                "resource_type": "job",
                "identifier": "j",
                "status": "error",
                "metadata": {},
            },
        ]
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")
        assert resp.status_code == 200
        body = resp.json()
        # per-mode contract + ledger rollup under resource_health
        assert body["setup_id"] == "s1"
        assert body["status"] == "draft"  # no deployment rows
        assert body["resource_health"]["overall"] == "degraded"
        assert body["resources"]["uc_catalog"][0]["health"] == "healthy"
        assert body["resources"]["job"][0]["health"] == "degraded"

    async def test_no_ws_falls_back_to_deploy_gate_view(self) -> None:
        # Default path: no WorkspaceClient (no creds) -> Tier-1 checks skipped, the
        # endpoint still returns the ledger view + an empty checks list + e2e desc.
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "draft",
            "enabled_modes": ["realtime"],
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
            patch(f"{MOD}._gather_api_health", AsyncMock(return_value=None)),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")
        body = resp.json()
        assert body["checks"] == []
        assert body["cluster_state"] is None
        assert body["health"] == "draft"  # deploy-gate view preserved
        assert body["e2e_test"]["tier"] == "e2e"
        assert body["e2e_test"]["spins_interactive_cluster"] is True

    async def test_tier1_checks_become_primary_health(self) -> None:
        # When the Tier-1 sweep runs, its rollup is the primary health/startable
        # and checks[] + cluster_state are surfaced; deploy-gate view is preserved.
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "deployed",
            "enabled_modes": ["microbatch"],
            "validation_state": {
                "steps": {key: {"status": sm.PASSED} for key in sm.mode_required_keys("microbatch")}
            },
        }
        deployments = [{"id": "d1", "mode": "microbatch", "status": "deployed", "job_id": "j1"}]
        api = {
            "checks": [
                {"name": "uc_catalog_schemas", "tier": "api", "status": "pass", "detail": "ok"},
                {"name": "kafka_topics", "tier": "e2e", "status": "skip", "detail": "cluster-only"},
            ],
            "health": "healthy",
            "startable": True,
            "cluster_state": "TERMINATED",
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=deployments)),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
            patch(f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": True})),
            patch(
                f"{MOD}._flowing_lookup",
                AsyncMock(return_value={"microbatch": True}),
            ),
            patch(f"{MOD}._gather_api_health", AsyncMock(return_value=api)),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")
        body = resp.json()
        assert body["health"] == "healthy"
        assert body["startable"] is True
        assert body["cluster_state"] == "TERMINATED"
        assert len(body["checks"]) == 2
        # deploy-gate rollup preserved for admin detail
        assert "deploy_health" in body and "deploy_startable" in body
        # Tier-2 topic check is present and marked e2e
        topic = [c for c in body["checks"] if c["name"] == "kafka_topics"][0]
        assert topic["tier"] == "e2e"

    async def test_validated_draft_is_not_healthy_until_running(self) -> None:
        validation = sm.initial_state(["zerobus"])
        for key in validation["sequence"]:
            validation = sm.set_step(validation, key, sm.PASSED)
        setup = {
            "id": "s1",
            "industry": "pharmaceutical",
            "status": "draft",
            "enabled_modes": ["zerobus"],
            "validation_state": validation,
        }
        api = {
            "checks": [
                {
                    "name": "delta_tables",
                    "tier": "api",
                    "status": "pass",
                    "detail": "ok",
                }
            ],
            "health": "healthy",
            "startable": True,
            "cluster_state": None,
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])),
            patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])),
            patch(f"{MOD}._gather_api_health", AsyncMock(return_value=api)),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")

        body = resp.json()
        assert body["health"] == "draft"
        assert body["validation_passed"] is True
        assert body["startable"] is False


# ---------------------------------------------------------------------------
# DELETE /api/admin/pipelines/{setup_id}
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestDeletePipeline:
    async def test_404_when_setup_missing(self) -> None:
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=None)),
        ):
            async with _client(_state()) as c:
                resp = await c.delete("/api/admin/pipelines/nope")
        assert resp.status_code == 404

    async def test_tears_down_and_returns_result(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical"}
        teardown_result = {"success": True, "setup_id": "s1", "deleted_setup": True, "steps": []}
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.teardown_service.teardown_setup",
                AsyncMock(return_value=teardown_result),
            ) as td_mock,
        ):
            async with _client(_state()) as c:
                resp = await c.delete("/api/admin/pipelines/s1")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        td_mock.assert_awaited_once()

    async def test_teardown_failure_is_http_error_c7113_defect_regression(self) -> None:
        """[C7113][defect][regression] Failed cleanup is never reported complete."""
        setup = {"id": "s1", "industry": "pharmaceutical"}
        teardown_result = {
            "success": False,
            "setup_id": "s1",
            "deleted_setup": False,
            "steps": [
                {
                    "resource_type": "job",
                    "identifier": "42",
                    "action": "failed",
                    "error": "permission denied",
                }
            ],
        }
        with (
            _admin(True),
            patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)),
            patch(
                f"{MOD}.teardown_service.teardown_setup",
                AsyncMock(return_value=teardown_result),
            ),
        ):
            async with _client(_state()) as c:
                resp = await c.delete("/api/admin/pipelines/s1")

        assert resp.status_code == 502
        assert resp.json()["detail"]["success"] is False
        assert resp.json()["detail"]["deleted_setup"] is False


# ---------------------------------------------------------------------------
# Health rollup unit tests
# ---------------------------------------------------------------------------
class TestRollupHealth:
    def test_draft_setup_reports_draft(self) -> None:
        out = industries_admin._rollup_health({"status": "draft"}, [], [])
        assert out["overall"] == "draft"

    def test_no_resources_is_unknown(self) -> None:
        out = industries_admin._rollup_health({"status": "deployed"}, [], [])
        assert out["overall"] == "unknown"

    def test_all_active_is_healthy(self) -> None:
        res = [{"resource_type": "uc_catalog", "status": "active"}]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "healthy"

    def test_mixed_is_degraded(self) -> None:
        res = [
            {"resource_type": "uc_catalog", "status": "active"},
            {"resource_type": "job", "status": "error"},
        ]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "degraded"

    def test_none_active_is_down(self) -> None:
        res = [{"resource_type": "job", "status": "error"}]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "down"

    def test_deleted_resources_ignored(self) -> None:
        res = [
            {"resource_type": "uc_catalog", "status": "active"},
            {"resource_type": "job", "status": "deleted"},
        ]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "healthy"
        assert out["resources_total"] == 1


# ---------------------------------------------------------------------------
# Live smoke test (skippable) — ensure tables against real iot-app-backend
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture()
async def schema_ready(app_backend_conn):
    from iot_twin.models import app_schema

    await app_schema.ensure_app_schema(app_backend_conn)
    return app_backend_conn


@pytest.mark.asyncio
async def test_smoke_setup_provision_list_delete_roundtrip(schema_ready) -> None:
    """End-to-end against the live project: create → list → health → delete."""
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(
        db, industry="smoke_test_industry", status="draft", enabled_modes=["realtime"]
    )
    await resource_ledger.record_resource(
        db,
        setup_id=setup["id"],
        resource_type="uc_catalog",
        identifier="iot_smoke",
        teardown_order=10,
    )

    resources = await resource_ledger.list_resources(db, setup["id"])
    assert len(resources) == 1
    health = industries_admin._rollup_health(setup, [], resources)
    assert health["overall"] == "draft"  # status=draft

    result = await teardown_service.teardown_setup(db, setup["id"])
    assert result["deleted_setup"] is True
    assert await pipeline_setup_repo.get_setup(db, setup["id"]) is None
