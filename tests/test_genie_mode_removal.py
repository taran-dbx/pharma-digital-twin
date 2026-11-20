"""Removing a mode trashes its Genie space before the deployment cascade."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services import resource_deleters
from iot_twin.services.teardown_service import teardown_setup
from iot_twin.services.validation import validation_state_machine as sm
from tests.test_routes_industries_admin import MOD, _admin, _client, _state

SETUP = {
    "id": "s1",
    "industry": "manufacturing",
    "enabled_modes": ["realtime", "zerobus"],
    "validation_state": sm.initial_state(["realtime", "zerobus"]),
}
DEPLOYMENT = {"id": "d-zb", "mode": "zerobus", "job_id": "9001"}
JOB = {
    "id": "r-job",
    "setup_id": "s1",
    "deployment_id": "d-zb",
    "resource_type": "job",
    "identifier": "9001",
    "status": "active",
}
SPACE_ZB = {
    "id": "r-sp",
    "setup_id": "s1",
    "deployment_id": "d-zb",
    "resource_type": "genie_space",
    "identifier": "sp-zb",
    "status": "active",
    "metadata": {"mode": "zerobus"},
}
SPACE_RT = {
    "id": "r-sp-rt",
    "setup_id": "s1",
    "deployment_id": "d-rt",
    "resource_type": "genie_space",
    "identifier": "sp-rt",
    "status": "active",
    "metadata": {"mode": "realtime"},
}


async def _remove_zerobus(resources: list[dict], trash: AsyncMock, events: list[str]):
    delete_job = AsyncMock(side_effect=lambda _r: events.append("job"))

    async def _delete_deployment(_db, _dep_id):
        events.append("delete_deployment")
        return True

    with (
        _admin(True),
        patch(f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=SETUP)),
        patch(f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[DEPLOYMENT])),
        patch(f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)),
        patch(f"{MOD}.deployment_service.make_job_deleter", MagicMock(return_value=delete_job)),
        patch(f"{MOD}.genie_space_service.make_genie_space_deleter", MagicMock(return_value=trash)),
        patch(
            f"{MOD}.deployment_repo.delete_deployment", AsyncMock(side_effect=_delete_deployment)
        ),
        patch(f"{MOD}.resource_ledger.mark_resource_status", AsyncMock()),
        patch(
            f"{MOD}.pipeline_setup_repo.update_setup",
            AsyncMock(return_value={**SETUP, "enabled_modes": ["realtime"]}),
        ),
    ):
        async with _client(_state()) as c:
            return await c.patch(
                "/api/admin/pipelines/s1/modes", json={"enabled_modes": ["realtime"]}
            )


@pytest.mark.asyncio
class TestModeRemovalTrashesSpace:
    async def test_space_trashed_before_deployment_delete(self) -> None:
        events: list[str] = []
        trash = AsyncMock(side_effect=lambda _r: events.append("trash"))
        resp = await _remove_zerobus([JOB, SPACE_ZB, SPACE_RT], trash, events)
        assert resp.status_code == 200
        trash.assert_awaited_once_with(SPACE_ZB)
        assert events.index("trash") < events.index("delete_deployment")

    async def test_other_modes_space_untouched(self) -> None:
        trash = AsyncMock()
        await _remove_zerobus([JOB, SPACE_ZB, SPACE_RT], trash, [])
        assert [c.args[0]["identifier"] for c in trash.await_args_list] == ["sp-zb"]

    async def test_setup_scoped_space_row_matched_by_mode(self) -> None:
        setup_scoped = {**SPACE_ZB, "deployment_id": None}
        trash = AsyncMock()
        await _remove_zerobus([JOB, setup_scoped], trash, [])
        trash.assert_awaited_once_with(setup_scoped)

    async def test_trash_failure_keeps_deployment(self) -> None:
        events: list[str] = []
        trash = AsyncMock(side_effect=RuntimeError("permission denied"))
        resp = await _remove_zerobus([JOB, SPACE_ZB], trash, events)
        assert resp.status_code == 502
        assert "delete_deployment" not in events


@pytest.mark.asyncio
async def test_full_teardown_still_trashes_remaining_space() -> None:
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[SPACE_RT])
    db.fetchrow = AsyncMock(return_value={"id": "r-sp-rt"})
    db.execute = AsyncMock(return_value="DELETE 1")
    ws = MagicMock()
    registry = resource_deleters.make_all_deleters(ws=ws, warehouse_id="wh")
    result = await teardown_setup(db, "s1", registry)
    ws.genie.trash_space.assert_called_once_with("sp-rt")
    assert result["steps"][0]["action"] == "deleted"
