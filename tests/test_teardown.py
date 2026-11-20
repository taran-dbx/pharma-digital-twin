"""Tests for the teardown orchestrator (Phase 1, Task 1.4).

Unit tests use a mocked DB to assert the orchestrator dispatches per-resource
deleters in DESCENDING teardown_order, marks each ledger row deleted, and then
deletes the setup (FK cascade removes metadata).  An integration test against
the live iot-app-backend project proves nothing is orphaned after teardown.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from iot_twin.models import app_schema
from iot_twin.services import resource_ledger, teardown_service
from iot_twin.services.deployment import deployment_repo
from iot_twin.services.pipelines import pipeline_setup_repo

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Unit — mocked DB
# ---------------------------------------------------------------------------
async def test_teardown_dispatches_in_descending_order(mock_db) -> None:
    # resources_in_teardown_order() maps db.fetch() results; feed descending rows.
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "r3",
                "setup_id": "setup-1",
                "deployment_id": "dep-1",
                "resource_type": "job",
                "identifier": "j",
                "metadata": {},
                "teardown_order": 90,
            },
            {
                "id": "r2",
                "resource_type": "kafka_topic",
                "identifier": "t",
                "setup_id": "setup-1",
                "metadata": {},
                "teardown_order": 50,
            },
            {
                "id": "r1",
                "resource_type": "uc_catalog",
                "identifier": "c",
                "setup_id": "setup-1",
                "metadata": {},
                "teardown_order": 10,
            },
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r", "status": "deleted", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    calls: list[str] = []

    async def _rec(res):
        calls.append(res["resource_type"])
        return {"status": "deleted"}

    deleters = {
        "job": AsyncMock(side_effect=_rec),
        "kafka_topic": AsyncMock(side_effect=_rec),
        "uc_catalog": AsyncMock(side_effect=_rec),
    }

    result = await teardown_service.teardown_setup(mock_db, "setup-1", deleters=deleters)

    assert calls == ["job", "kafka_topic"]
    assert result["success"] is True
    assert result["deleted_setup"] is True
    assert len(result["steps"]) == 3
    # every ledger row marked deleted (mark_resource_status → db.fetchrow UPDATE)
    assert mock_db.fetchrow.await_count == 3


async def test_teardown_skips_unknown_resource_type(mock_db) -> None:
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "r1",
                "resource_type": "mystery_type",
                "identifier": "x",
                "setup_id": "setup-1",
                "metadata": {},
                "teardown_order": 5,
            },
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "status": "deleted", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    result = await teardown_service.teardown_setup(mock_db, "setup-1", deleters={})

    assert result["steps"][0]["action"] == "skipped"
    # unknown type still marked deleted and does not fail the teardown
    assert result["success"] is True
    assert mock_db.fetchrow.await_count == 1


async def test_teardown_reports_deleter_failure(mock_db) -> None:
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "r1",
                "resource_type": "job",
                "identifier": "j",
                "metadata": {},
                "setup_id": "setup-1",
                "deployment_id": "dep-1",
                "teardown_order": 5,
            },
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    async def _boom(res):
        raise RuntimeError("delete failed")

    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters={"job": AsyncMock(side_effect=_boom)}
    )

    assert result["success"] is False
    assert result["steps"][0]["action"] == "failed"
    assert "delete failed" in result["steps"][0]["error"]


async def test_teardown_failure_does_not_cascade_orphan(mock_db) -> None:
    # A failed deleter must NOT drop the setup row — keeping the ledger row
    # (marked ``failed``) is the only record that the cloud resource still
    # exists, so it is surfaced for retry instead of silently orphaned.
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "r1",
                "setup_id": "setup-1",
                "deployment_id": "dep-1",
                "resource_type": "job",
                "identifier": "j",
                "metadata": {},
                "teardown_order": 10,
            },
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    async def _boom(res):
        raise RuntimeError("boom")

    with patch.object(
        teardown_service.pipeline_setup_repo, "delete_setup", AsyncMock()
    ) as del_mock:
        result = await teardown_service.teardown_setup(
            mock_db, "setup-1", deleters={"job": AsyncMock(side_effect=_boom)}
        )

    del_mock.assert_not_awaited()
    assert result["success"] is False
    assert result["deleted_setup"] is False
    # the failed row is marked ``failed`` (not ``deleted``) before we bail out
    assert mock_db.fetchrow.await_args.args[1] == "failed"


async def test_teardown_skips_shared_resources(mock_db) -> None:
    # Shared SP / secret-scope / iot-pipelines project rows carry
    # ``metadata.shared`` and must never be deleted per-industry — even when a
    # deleter is registered for their type.
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "r1",
                "resource_type": "service_principal",
                "setup_id": "setup-1",
                "identifier": "iot-sss-industries",
                "metadata": {"shared": True},
                "teardown_order": 1,
            },
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    sp_deleter = AsyncMock()
    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters={"service_principal": sp_deleter}
    )

    sp_deleter.assert_not_awaited()  # shared guard fires before dispatch
    assert result["steps"][0]["action"] == "skipped"
    assert "shared" in result["steps"][0]["reason"].lower()
    assert result["success"] is True
    assert mock_db.fetchrow.await_args.args[1] == "skipped"


async def test_teardown_preserves_catalog_schema_and_sibling_resources_c7113(
    mock_db,
) -> None:
    """[C7113][defect][regression] Setup delete cannot cross ownership boundaries."""
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "target-job",
                "setup_id": "setup-1",
                "deployment_id": "deployment-1",
                "resource_type": "job",
                "identifier": "101",
                "metadata": {"notebook_path": "/Workspace/target"},
                "teardown_order": 90,
            },
            {
                "id": "target-table",
                "setup_id": "setup-1",
                "deployment_id": "deployment-1",
                "resource_type": "uc_table",
                "identifier": "shared.schema.target_zerobus",
                "metadata": {"managed": True},
                "teardown_order": 20,
            },
            {
                "id": "sibling-table",
                "setup_id": "setup-2",
                "deployment_id": "deployment-2",
                "resource_type": "uc_table",
                "identifier": "shared.schema.sibling_zerobus",
                "metadata": {},
                "teardown_order": 20,
            },
            {
                "id": "shared-schema",
                "setup_id": "setup-1",
                "deployment_id": None,
                "resource_type": "uc_schema",
                "identifier": "shared.schema",
                "metadata": {},
                "teardown_order": 15,
            },
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")
    job_delete = AsyncMock()
    table_delete = AsyncMock()
    schema_delete = AsyncMock()

    result = await teardown_service.teardown_setup(
        mock_db,
        "setup-1",
        deleters={
            "job": job_delete,
            "uc_table": table_delete,
            "uc_schema": schema_delete,
        },
    )

    job_delete.assert_awaited_once()
    table_delete.assert_awaited_once()
    assert table_delete.await_args.args[0]["identifier"] == "shared.schema.target_zerobus"
    schema_delete.assert_not_awaited()
    assert result["success"] is True
    assert [step["action"] for step in result["steps"]] == [
        "deleted",
        "deleted",
        "skipped",
        "skipped",
    ]


async def test_uc_table_without_pipeline_ownership_fails_closed_c7113(
    mock_db,
) -> None:
    """[C7113][defect][regression] Ambiguous UC table ownership preserves data."""
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "table-1",
                "setup_id": None,
                "deployment_id": None,
                "resource_type": "uc_table",
                "identifier": "shared.schema.target_zerobus",
                "metadata": {"managed": True},
                "teardown_order": 20,
            }
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "table-1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")
    table_delete = AsyncMock()

    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters={"uc_table": table_delete}
    )

    table_delete.assert_not_awaited()
    assert result["steps"][0]["action"] == "skipped"
    assert "ownership" in result["steps"][0]["reason"].lower()


async def test_setup_owned_uc_table_is_deleted_without_deployment_id_c7113(
    mock_db,
) -> None:
    """[C7113][defect][regression] Exact setup ledger ownership permits deletion."""
    mock_db.fetch = AsyncMock(
        return_value=[
            {
                "id": "table-1",
                "setup_id": "setup-1",
                "deployment_id": None,
                "resource_type": "uc_table",
                "identifier": "shared.schema.target_zerobus",
                "metadata": {"managed": True, "kind": "zerobus"},
                "teardown_order": 20,
            }
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "table-1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")
    table_delete = AsyncMock()

    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters={"uc_table": table_delete}
    )

    table_delete.assert_awaited_once()
    assert result["steps"][0]["action"] == "deleted"
    assert result["deleted_setup"] is True


# ---------------------------------------------------------------------------
# Integration — live iot-app-backend, nothing orphaned
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture()
async def schema_ready(app_backend_conn):
    await app_schema.ensure_app_schema(app_backend_conn)
    return app_backend_conn


async def test_teardown_leaves_nothing_orphaned(schema_ready) -> None:
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(db, industry="pharmaceutical")
    dep = await deployment_repo.create_deployment(db, setup_id=setup["id"], mode="realtime")
    await resource_ledger.record_resource(
        db,
        setup_id=setup["id"],
        resource_type="uc_catalog",
        identifier="iot_pharmaceutical",
        teardown_order=10,
    )
    await resource_ledger.record_resource(
        db,
        setup_id=setup["id"],
        deployment_id=dep["id"],
        resource_type="kafka_topic",
        identifier="iot.pharmaceutical.realtime",
        teardown_order=50,
    )

    order: list[str] = []

    async def _rec(res):
        order.append(res["resource_type"])
        return {"status": "deleted"}

    result = await teardown_service.teardown_setup(
        db,
        setup["id"],
        deleters={"uc_catalog": _rec, "kafka_topic": _rec},
    )

    assert result["success"] is True
    # descending teardown_order
    assert order == ["kafka_topic"]
    # setup gone + cascade wiped deployments and resources — nothing orphaned
    assert await pipeline_setup_repo.get_setup(db, setup["id"]) is None
    assert await deployment_repo.list_deployments(db, setup["id"]) == []
    assert await resource_ledger.list_resources(db, setup["id"]) == []
