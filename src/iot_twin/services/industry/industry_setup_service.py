"""LakeTS setup on an industry's Lakebase branch (spec §1.9–1.10, plan 2.3).

Given a never-expiring branch on the shared ``iot-pipelines`` project (created by
``pipeline_project_service.provision_industry_branch``), this module:

1. installs ``lakets.sql`` on the branch's ``databricks_postgres`` DB,
2. creates ``sensor_readings`` from the SSOT (``iot_twin.models.sensor_schema`` —
   standalone-sequence PK, not BIGSERIAL, so ``create_chronotable`` can drop the
   renamed original cleanly),
3. registers it as a ChronoTable (``event_time`` / 15-minute chunks),
4. pre-creates a ~24h forward window of partitions (96 × 15-minute chunks) so the
   real-time writer never hits a "no partition" error before the Partition
   Manager job takes over,
5. configures the one-minute sensor RollUp, invalidation triggers, and Last
   Value Cache,
6. deploys the four LakeTS operational jobs (partition manager / rollup / tiering
   / retention) via the existing ``lakets_deploy_service``.

Every provisioned resource (LakeTS install, ChronoTable, jobs) is recorded in the
``provisioned_resource`` ledger.  This module is **additive**.
"""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from iot_twin.core.workspace_config import get_pipelines_project
from iot_twin.models.sensor_schema import pg_chronotable_ddl
from iot_twin.services import cdf_reconciliation_service
from iot_twin.services.lakets import lakets_deploy_service, lakets_service
from iot_twin.services.resource_ledger import record_resource

logger = logging.getLogger("iot_digital_twin.services.industry_setup")

# --- Constants -------------------------------------------------------------
SENSOR_TABLE = "sensor_readings"
TIME_COLUMN = "event_time"
# Schema enable_sync creates the CDC shadow mirror in (lakets_cdf._shadow_<table>).
CDF_SHADOW_SCHEMA = "lakets_cdf"
CHUNK_INTERVAL = "15 minutes"
# ~24h forward window at 15-minute chunks (spec §1.10 partition pre-create).
DEFAULT_FUTURE_PARTITIONS = 96
PG_DATABASE = "databricks_postgres"

# Ledger teardown_order.  Jobs are torn down before the Lakebase branch/UC
# resources they depend on (reverse-dependency order); the ChronoTable lives
# inside the branch and is removed when the branch is deleted.
TEARDOWN_ORDER_JOB = 50
TEARDOWN_ORDER_CHRONOTABLE = 35

_CHRONOTABLE_ID_SQL = (
    "SELECT id FROM lakets._chronotable_registry WHERE schema_name = $1 AND table_name = $2"
)
_ENSURE_PARTITIONS_SQL = "SELECT lakets._ensure_partitions($1, p_future_count := $2)"


def _new_ws() -> Any:
    return WorkspaceClient()


async def setup_lakets_on_branch(
    branch: str,
    *,
    conn: Any,
    industry: str | None = None,
    ws: Any | None = None,
    project_id: str | None = None,
    future_count: int = DEFAULT_FUTURE_PARTITIONS,
    chunk: str = CHUNK_INTERVAL,
    lakets_sql_path: str | None = None,
    deploy_ops_jobs: bool = True,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Install LakeTS + create the ChronoTable + pre-create partitions + deploy
    ops jobs on ``branch``'s primary (write) connection ``conn``.

    ``conn`` is an initialized write connection (asyncpg-style
    ``execute``/``fetchval``) to the branch's ``databricks_postgres`` DB.

    Returns a summary dict; records LakeTS install, ChronoTable, and each ops
    job in the ledger.  Idempotent throughout.
    """
    project_id = project_id or get_pipelines_project()
    industry = cdf_reconciliation_service.normalize_industry(industry or branch)
    ws = ws or _new_ws()
    result: dict[str, Any] = {"branch": branch, "steps": []}

    def _step(name: str, status: str, **extra: Any) -> None:
        result["steps"].append({"step": name, "status": status, **extra})

    # 1. Install LakeTS (idempotent).
    sql_path = lakets_service.resolve_lakets_source(lakets_sql_path)
    version = await lakets_service.install_lakets(conn, sql_path, skip_if_installed=True)
    result["lakets_version"] = version
    _step("install_lakets", "ok", version=version)

    # 2. Create sensor_readings from the SSOT (standalone-sequence PK).
    await conn.execute(pg_chronotable_ddl(SENSOR_TABLE))
    _step("create_sensor_readings", "ok", table=SENSOR_TABLE)

    # 3. Register as a ChronoTable (15-minute chunks on event_time).
    already = await lakets_service.create_chronotable(
        conn,
        table=SENSOR_TABLE,
        time_col=TIME_COLUMN,
        chunk=chunk,
    )
    _step("create_chronotable", "already_exists" if already else "created")
    await _record(
        db,
        setup_id,
        deployment_id,
        resource_type="uc_table",  # PG ChronoTable inside the branch
        identifier=f"{branch}.public.{SENSOR_TABLE}",
        parent_ref=branch,
        teardown_order=TEARDOWN_ORDER_CHRONOTABLE,
        metadata={"chronotable": True, "chunk": chunk, "time_column": TIME_COLUMN},
    )

    # 4. Pre-create the ~24h forward partition window (hard-gate prerequisite).
    chrono_id = await conn.fetchval(_CHRONOTABLE_ID_SQL, "public", SENSOR_TABLE)
    if chrono_id is None:
        _step("precreate_partitions", "failed", error="ChronoTable id not found after create")
    else:
        created = await conn.fetchval(_ENSURE_PARTITIONS_SQL, chrono_id, future_count)
        result["partitions_created"] = created
        _step("precreate_partitions", "ok", future_count=future_count, created=created)

    # 4b. Configure the default one-minute sensor RollUp and Last Value Cache.
    rollup_result = await lakets_service.create_rollup(
        conn,
        table=SENSOR_TABLE,
        name="sensor_1m",
        bucket="1 minute",
        group_columns=["sensor_name"],
        aggregates=[
            {"fn": "count", "column": "value", "as": "sample_count"},
            {"fn": "avg", "column": "value", "as": "avg_value"},
            {"fn": "min", "column": "value", "as": "min_value"},
            {"fn": "max", "column": "value", "as": "max_value"},
            {
                "fn": "stddev_samp",
                "column": "value",
                "as": "stddev_value",
            },
        ],
    )
    _step(
        "create_rollup",
        rollup_result.get("status", "failed"),
        error=rollup_result.get("error"),
    )

    if rollup_result.get("status") == "failed":
        _step(
            "enable_rollup_invalidation",
            "skipped",
            error="RollUp creation or validation failed",
        )
    else:
        invalidation_result = await lakets_service.enable_rollup_invalidation(conn, "sensor_1m")
        _step(
            "enable_rollup_invalidation",
            invalidation_result.get("status", "failed"),
            error=invalidation_result.get("error"),
        )

    lvc_result = await lakets_service.enable_lvc(
        conn,
        table=SENSOR_TABLE,
        key_columns=["sensor_name"],
        value_columns=[
            "event_time",
            "industry",
            "sensor_type",
            "unit",
            "value",
            "fault_active",
        ],
        schema="public",
    )
    _step(
        "enable_lvc",
        lvc_result.get("status", "failed"),
        error=lvc_result.get("error"),
    )

    # 4c. Prepare the branch's industry-unique CDF source. The atomic helper
    #     migrates legacy shadows without deleting them and backfills only once.
    shadow = cdf_reconciliation_service.shadow_table_name(industry)
    sync_result = await lakets_service.reconcile_chronotable_shadow(
        conn,
        shadow_table=shadow,
        table=SENSOR_TABLE,
        schema="public",
    )
    result["cdf_shadow"] = sync_result.get("status", "failed")
    _step(
        "enable_sync",
        sync_result.get("status", "failed"),
        shadow=f"{CDF_SHADOW_SCHEMA}.{shadow}",
        error=sync_result.get("error"),
    )

    # 5. Deploy the LakeTS operational jobs (partition manager / rollup /
    #    tiering / retention).
    if deploy_ops_jobs:
        jobs = lakets_deploy_service.deploy_lakets_ops_jobs(ws, project_id, branch, PG_DATABASE)
        result["ops_jobs"] = jobs
        for job in jobs:
            _step(
                f"deploy_job:{job.get('job')}",
                job.get("status", "unknown"),
                job_id=job.get("job_id"),
            )
            await _record(
                db,
                setup_id,
                deployment_id,
                resource_type="job",
                identifier=str(job.get("job_id")),
                parent_ref=branch,
                teardown_order=TEARDOWN_ORDER_JOB,
                metadata={"job_name": job.get("job"), "kind": "lakets_ops"},
            )

    return result


async def _record(
    db: Any | None,
    setup_id: Any | None,
    deployment_id: Any | None,
    *,
    resource_type: str,
    identifier: str,
    parent_ref: str | None = None,
    teardown_order: int = 0,
    metadata: dict | None = None,
) -> None:
    """Record a resource in the ledger when a db + setup_id are provided."""
    if db is None or setup_id is None:
        return
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type=resource_type,
        identifier=identifier,
        parent_ref=parent_ref,
        teardown_order=teardown_order,
        metadata=metadata,
    )
