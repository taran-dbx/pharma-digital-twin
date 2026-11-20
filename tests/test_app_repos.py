"""Integration tests for the app-backend repositories (Phase 1, Task 1.3).

Exercises CRUD on ``pipeline_setup_repo`` / ``deployment_repo`` /
``resource_ledger`` against the live ``iot-app-backend`` project (throwaway
schema per test).  JSONB fields must round-trip as Python dicts/lists and the
ledger must return resources in ascending (list) and descending (teardown)
``teardown_order``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import pytest_asyncio

from iot_twin.models import app_schema
from iot_twin.services import resource_ledger
from iot_twin.services.deployment import deployment_repo
from iot_twin.services.pipelines import pipeline_setup_repo

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture()
async def schema_ready(app_backend_conn):
    await app_schema.ensure_app_schema(app_backend_conn)
    return app_backend_conn


# ---------------------------------------------------------------------------
# pipeline_setup_repo
# ---------------------------------------------------------------------------
async def test_create_and_get_setup(schema_ready) -> None:
    db = schema_ready
    row = await pipeline_setup_repo.create_setup(
        db,
        industry="pharmaceutical",
        flavor="classic",
        source_type="kafka",
        bootstrap_servers="b-1:9198",
        derived={"region": "us-east-1", "replication_factor": 3},
        enabled_modes=["realtime", "microbatch"],
        mode_configs={
            "realtime": {"rate_hz": 1200},
            "microbatch": {"rate_hz": 2400},
        },
        user_email="admin@example.com",
    )
    assert row["industry"] == "pharmaceutical"
    assert row["derived"]["region"] == "us-east-1"
    assert row["enabled_modes"] == ["realtime", "microbatch"]
    assert row["mode_configs"]["realtime"]["rate_hz"] == 1200
    assert row["status"] == "draft"

    fetched = await pipeline_setup_repo.get_setup(db, row["id"])
    assert fetched["id"] == row["id"]
    by_ind = await pipeline_setup_repo.get_setup_by_industry(db, "pharmaceutical")
    assert by_ind["id"] == row["id"]

    updated = await pipeline_setup_repo.update_mode_config(
        db, row["id"], "realtime", {"rate_hz": 1800}
    )
    assert updated["mode_configs"] == {
        "realtime": {"rate_hz": 1800},
        "microbatch": {"rate_hz": 2400},
    }


async def test_industry_uniqueness_enforced(schema_ready) -> None:
    db = schema_ready
    await pipeline_setup_repo.create_setup(db, industry="energy")
    with pytest.raises(asyncpg.UniqueViolationError):
        await pipeline_setup_repo.create_setup(db, industry="energy")


async def test_update_and_list_setup(schema_ready) -> None:
    db = schema_ready
    row = await pipeline_setup_repo.create_setup(db, industry="mining")
    updated = await pipeline_setup_repo.update_setup(
        db, row["id"], status="validating", warehouse_id="wh-123"
    )
    assert updated["status"] == "validating"
    assert updated["warehouse_id"] == "wh-123"

    await pipeline_setup_repo.create_setup(db, industry="retail")
    all_setups = await pipeline_setup_repo.list_setups(db)
    assert {s["industry"] for s in all_setups} == {"mining", "retail"}


async def test_delete_setup(schema_ready) -> None:
    db = schema_ready
    row = await pipeline_setup_repo.create_setup(db, industry="aviation")
    assert await pipeline_setup_repo.delete_setup(db, row["id"]) is True
    assert await pipeline_setup_repo.get_setup(db, row["id"]) is None


# ---------------------------------------------------------------------------
# deployment_repo
# ---------------------------------------------------------------------------
async def test_create_list_deployment(schema_ready) -> None:
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(db, industry="pharmaceutical")
    dep = await deployment_repo.create_deployment(
        db,
        setup_id=setup["id"],
        mode="realtime",
        partition_count=6,
        lakebase_endpoints={"primary": "ep-a", "replica": "ep-b"},
    )
    assert dep["mode"] == "realtime"
    assert dep["partition_count"] == 6
    assert dep["lakebase_endpoints"]["replica"] == "ep-b"

    await deployment_repo.create_deployment(db, setup_id=setup["id"], mode="microbatch")
    deps = await deployment_repo.list_deployments(db, setup["id"])
    assert {d["mode"] for d in deps} == {"realtime", "microbatch"}

    by_mode = await deployment_repo.get_deployment_by_mode(db, setup["id"], "realtime")
    assert by_mode["id"] == dep["id"]


async def test_deployment_unique_setup_mode(schema_ready) -> None:
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(db, industry="energy")
    await deployment_repo.create_deployment(db, setup_id=setup["id"], mode="realtime")
    with pytest.raises(asyncpg.UniqueViolationError):
        await deployment_repo.create_deployment(db, setup_id=setup["id"], mode="realtime")


async def test_update_deployment(schema_ready) -> None:
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(db, industry="mining")
    dep = await deployment_repo.create_deployment(db, setup_id=setup["id"], mode="zerobus")
    updated = await deployment_repo.update_deployment(
        db, dep["id"], status="deployed", job_id="job-99"
    )
    assert updated["status"] == "deployed"
    assert updated["job_id"] == "job-99"


# ---------------------------------------------------------------------------
# resource_ledger
# ---------------------------------------------------------------------------
async def test_record_and_list_resources(schema_ready) -> None:
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
        parent_ref="example",
        teardown_order=50,
        metadata={"partitions": 3},
    )
    await resource_ledger.record_resource(
        db,
        setup_id=setup["id"],
        resource_type="uc_schema",
        identifier="iot_pharmaceutical.realtime",
        teardown_order=20,
    )

    listed = await resource_ledger.list_resources(db, setup["id"])
    assert [r["teardown_order"] for r in listed] == [10, 20, 50]

    teardown = await resource_ledger.resources_in_teardown_order(db, setup["id"])
    assert [r["teardown_order"] for r in teardown] == [50, 20, 10]
    # metadata round-trips as a dict
    topic = next(r for r in teardown if r["resource_type"] == "kafka_topic")
    assert topic["metadata"]["partitions"] == 3


async def test_mark_resource_status(schema_ready) -> None:
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(db, industry="retail")
    res = await resource_ledger.record_resource(
        db,
        setup_id=setup["id"],
        resource_type="job",
        identifier="iot-retail-realtime",
        teardown_order=90,
    )
    updated = await resource_ledger.mark_resource_status(db, res["id"], "deleted")
    assert updated["status"] == "deleted"


async def test_update_resource_refreshes_cdf_binding_c7253() -> None:
    """Active CDF rows refresh deployment and metadata [C7253][regression]."""
    db = MagicMock()
    db.fetchrow = AsyncMock(
        return_value={
            "id": "r1",
            "deployment_id": "d1",
            "metadata": {"source_table": ("lakets_cdf._shadow_manufacturing_sensor_readings")},
        }
    )

    updated = await resource_ledger.update_resource(
        db,
        "r1",
        deployment_id="d1",
        metadata={"source_table": "lakets_cdf._shadow_manufacturing_sensor_readings"},
    )

    query = db.fetchrow.await_args.args[0]
    assert "UPDATE provisioned_resource" in query
    assert "deployment_id" in query
    assert "metadata" in query
    assert "updated_at" not in query
    assert updated["metadata"]["source_table"].endswith("_shadow_manufacturing_sensor_readings")
