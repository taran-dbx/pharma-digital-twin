"""Tests for the per-resource-type teardown deleters (Phase 7 bug fix).

The delete-pipeline path used to register ONLY a ``job`` deleter, so every other
``resource_type`` in the ledger (UC catalog, Lakebase branch/endpoints, external
metrics table + its S3 objects, secrets, SP, grants) was marked ``skipped`` and
then FK-cascade-removed — silently orphaning the real cloud resource.

``iot_twin.services.resource_deleters.make_all_deleters`` builds a deleter for every
``resource_type`` the provisioning services record, so nothing is silently
skipped.  These tests mock the WorkspaceClient / DBSQL / boto3 / Lakebase so no
live resource is touched.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from databricks.sdk.errors import NotFound

from iot_twin.services import resource_deleters
from iot_twin.services.resource_deleters import make_all_deleters
from iot_twin.services.resource_ledger import RESOURCE_TYPES
from iot_twin.services.teardown_service import SkipResource

pytestmark = pytest.mark.asyncio


# Every resource_type actually written to the ledger across the provisioning
# services (confirmed by call-site audit).  ``lakebase_project`` is recorded by
# pipeline_project_service but is not in RESOURCE_TYPES, so it is listed here.
RECORDED_TYPES = {
    "service_principal",
    "secret_scope",
    "uc_catalog",
    "uc_schema",
    "uc_volume",
    "metrics_table",
    "grant",
    "uc_table",
    "job",
    "lakebase_project",
    "lakebase_branch",
    "lakebase_endpoint",
    "lakebase_read_replica",
    "cdf_sync",
}


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------
async def test_every_recorded_type_has_a_registered_deleter() -> None:
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh1")
    missing = RECORDED_TYPES - set(reg)
    assert not missing, f"no deleter registered for recorded types: {missing}"


async def test_registry_covers_every_ledger_resource_type() -> None:
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh1")
    missing = set(RESOURCE_TYPES) - set(reg)
    assert not missing, f"RESOURCE_TYPES not covered: {missing}"


# ---------------------------------------------------------------------------
# UC deleters (DBSQL)
# ---------------------------------------------------------------------------
async def test_pipeline_teardown_never_drops_uc_catalog_c7113() -> None:
    run_sql = AsyncMock()
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh", run_sql=run_sql)
    with pytest.raises(SkipResource):
        await reg["uc_catalog"]({"identifier": "iot_pharma", "metadata": {}})
    run_sql.assert_not_awaited()


async def test_pipeline_teardown_never_drops_shared_uc_schema_c7113() -> None:
    run_sql = AsyncMock()
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh", run_sql=run_sql)
    with pytest.raises(SkipResource):
        await reg["uc_schema"]({"identifier": "iot_p.metrics", "metadata": {}})
    run_sql.assert_not_awaited()


async def test_uc_volume_drop_remains_pipeline_scoped() -> None:
    run_sql = AsyncMock()
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh", run_sql=run_sql)
    await reg["uc_volume"]({"identifier": "iot_p.checkpoints.stream_checkpoints", "metadata": {}})
    assert (
        run_sql.await_args.args[0] == "DROP VOLUME IF EXISTS iot_p.checkpoints.stream_checkpoints"
    )


async def test_uc_table_chronotable_is_skipped_with_note() -> None:
    run_sql = AsyncMock()
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh", run_sql=run_sql)
    with pytest.raises(SkipResource):
        await reg["uc_table"](
            {"identifier": "pharma.public.sensor_readings", "metadata": {"chronotable": True}}
        )
    run_sql.assert_not_awaited()


async def test_uc_table_real_table_is_dropped() -> None:
    ws = MagicMock()
    run_sql = AsyncMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh", run_sql=run_sql)
    await reg["uc_table"](
        {
            "identifier": "iot_p.raw.events",
            "deployment_id": "deployment-1",
            "_ownership_verified": True,
            "metadata": {},
        }
    )
    ws.tables.delete.assert_called_once_with(full_name="iot_p.raw.events")
    run_sql.assert_not_awaited()


async def test_uc_table_delete_is_idempotent_when_already_absent() -> None:
    ws = MagicMock()
    ws.tables.delete.side_effect = NotFound("table not found")
    reg = make_all_deleters(ws=ws, warehouse_id="wh", run_sql=AsyncMock())

    await reg["uc_table"](
        {
            "identifier": "iot_p.raw.events",
            "_ownership_verified": True,
            "metadata": {},
        }
    )

    ws.tables.delete.assert_called_once_with(full_name="iot_p.raw.events")


# ---------------------------------------------------------------------------
# Metrics table — drop table AND delete the external S3 prefix
# ---------------------------------------------------------------------------
async def test_metrics_table_drops_table_and_deletes_s3_prefix() -> None:
    ws = MagicMock()
    run_sql = AsyncMock()
    delete_s3 = AsyncMock()
    reg = make_all_deleters(
        ws=ws,
        warehouse_id="wh",
        run_sql=run_sql,
        delete_s3_prefix=delete_s3,
    )
    loc = "s3://example-shared-bucket/iot-digital-twin/iot_p/metrics/streaming_metrics/"
    await reg["metrics_table"](
        {
            "identifier": "iot_p.metrics.streaming_metrics",
            "deployment_id": "deployment-1",
            "_ownership_verified": True,
            "metadata": {"external": True, "location": loc},
        }
    )
    ws.tables.delete.assert_called_once_with(full_name="iot_p.metrics.streaming_metrics")
    run_sql.assert_not_awaited()
    delete_s3.assert_awaited_once_with(loc)


async def test_metrics_table_without_location_skips_s3() -> None:
    ws = MagicMock()
    run_sql = AsyncMock()
    delete_s3 = AsyncMock()
    reg = make_all_deleters(
        ws=ws,
        warehouse_id="wh",
        run_sql=run_sql,
        delete_s3_prefix=delete_s3,
    )
    await reg["metrics_table"](
        {
            "identifier": "iot_p.metrics.streaming_metrics",
            "deployment_id": "deployment-1",
            "_ownership_verified": True,
            "metadata": {},
        }
    )
    ws.tables.delete.assert_called_once()
    run_sql.assert_not_awaited()
    delete_s3.assert_not_awaited()


async def test_parse_s3_url_splits_bucket_and_prefix() -> None:
    bucket, prefix = resource_deleters._parse_s3_url("s3://my-bucket/a/b/c/")
    assert bucket == "my-bucket"
    assert prefix == "a/b/c/"


# ---------------------------------------------------------------------------
# Kafka topic — DeleteTopic denied (§1.8): skip with a clear note
# ---------------------------------------------------------------------------
async def test_kafka_topic_skipped_with_note() -> None:
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh")
    with pytest.raises(SkipResource) as exc:
        await reg["kafka_topic"]({"identifier": "iot.pharma.realtime", "metadata": {}})
    note = str(exc.value).lower()
    assert "deletetopic" in note or "1.8" in note


async def test_cdf_sync_is_deleted_via_postgres_api_c7205() -> None:
    """Automated CDF is removed before its Lakebase branch [C7205][regression]."""
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    parent = "projects/iot-pipelines/branches/pharma/databases/databricks_postgres"

    await reg["cdf_sync"](
        {
            "identifier": f"{parent}/cdf-configs/lakets_cdf",
            "metadata": {
                "parent": parent,
                "cdf_config_id": "lakets_cdf",
            },
        }
    )

    ws.api_client.do.assert_called_once_with(
        "DELETE",
        f"/api/2.0/postgres/{parent}/cdf-configs/lakets_cdf",
        query={"force": "false"},
    )


# ---------------------------------------------------------------------------
# Shared resources are never deleted by their deleter
# ---------------------------------------------------------------------------
async def test_shared_service_principal_skipped() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    with pytest.raises(SkipResource):
        await reg["service_principal"](
            {"identifier": "iot-sss-industries", "metadata": {"shared": True}}
        )
    ws.service_principals.delete.assert_not_called()


async def test_shared_secret_scope_skipped() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    with pytest.raises(SkipResource):
        await reg["secret_scope"]({"identifier": "iot-secrets", "metadata": {"shared": True}})
    ws.secrets.delete_scope.assert_not_called()


async def test_shared_lakebase_project_skipped() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    with pytest.raises(SkipResource):
        await reg["lakebase_project"](
            {"identifier": "projects/iot-pipelines", "metadata": {"shared": True}}
        )


# ---------------------------------------------------------------------------
# Lakebase endpoints / branch
# ---------------------------------------------------------------------------
async def test_primary_endpoint_skipped_removed_with_branch() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    with pytest.raises(SkipResource):
        await reg["lakebase_endpoint"](
            {
                "identifier": "projects/iot-pipelines/branches/pharma/endpoints/primary",
                "metadata": {"endpoint_type": "READ_WRITE"},
            }
        )
    ws.postgres.delete_endpoint.assert_not_called()


async def test_read_replica_endpoint_deleted() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    name = "projects/iot-pipelines/branches/pharma/endpoints/replica"
    await reg["lakebase_read_replica"](
        {"identifier": name, "metadata": {"endpoint_type": "READ_ONLY"}}
    )
    ws.postgres.delete_endpoint.assert_called_once_with(name=name)


async def test_lakebase_branch_deleted() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    name = "projects/iot-pipelines/branches/pharma"
    await reg["lakebase_branch"]({"identifier": name, "metadata": {}})
    ws.postgres.delete_branch.assert_called_once_with(name=name)


# ---------------------------------------------------------------------------
# Grant — best-effort revoke (never fails the teardown)
# ---------------------------------------------------------------------------
async def test_grant_revokes_best_effort() -> None:
    run_sql = AsyncMock()
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh", run_sql=run_sql)
    await reg["grant"](
        {
            "identifier": "iot_p:USE CATALOG:app-id",
            "metadata": {"sql": "GRANT USE CATALOG ON CATALOG iot_p TO `app-id`"},
        }
    )
    stmt = run_sql.await_args.args[0]
    assert stmt.startswith("REVOKE")
    assert "FROM `app-id`" in stmt


async def test_grant_swallows_revoke_error() -> None:
    run_sql = AsyncMock(side_effect=RuntimeError("object dropped"))
    reg = make_all_deleters(ws=MagicMock(), warehouse_id="wh", run_sql=run_sql)
    # Best-effort: a failed revoke must NOT raise (would otherwise mark the whole
    # teardown failed even though the object is already gone).
    await reg["grant"](
        {
            "identifier": "iot_p:USE CATALOG:app-id",
            "metadata": {"sql": "GRANT USE CATALOG ON CATALOG iot_p TO `app-id`"},
        }
    )


# ---------------------------------------------------------------------------
# Job — reuses the existing job deleter
# ---------------------------------------------------------------------------
async def test_job_deleter_cancels_and_deletes() -> None:
    ws = MagicMock()
    reg = make_all_deleters(ws=ws, warehouse_id="wh")
    await reg["job"]({"identifier": "12345", "metadata": {"mode": "realtime"}})
    ws.jobs.delete.assert_called_once_with(job_id=12345)
