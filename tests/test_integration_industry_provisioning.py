"""End-to-end integration test for the industry provisioning engine.

Provisions a THROWAWAY industry (``spiketest``) against the real
``example-us-east`` workspace + ``iot-pipelines`` Lakebase project:

- canonical ``iot_spiketest`` catalog + 5 schemas + checkpoint volume +
  external metrics table,
- a never-expiring Lakebase branch + primary/replica endpoints,
- LakeTS install + ``sensor_readings`` ChronoTable + ~24h partition pre-create,
- shared-SP grants on the catalog.

Asserts each resource exists, then performs FULL cleanup (drop catalog CASCADE +
best-effort S3-object delete, delete the branch, drop the ChronoTable).
``pytest.skip`` when the workspace / project is unreachable.  Ops-job deploy is
disabled (``deploy_ops_jobs=False``) to keep the run fast and avoid leaving
scheduled jobs behind.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

INDUSTRY = "spiketest"
# Task 3: provision into the PRE-EXISTING catalog + single schema from env.
CATALOG = os.getenv("UC_CATALOG", "")
SCHEMA = os.getenv("UC_SCHEMA", "")
_VENDORED_LAKETS = str(
    Path(__file__).resolve().parent.parent / "src" / "iot_twin" / "assets" / "lakets.sql"
)


def _ws_or_skip():
    try:
        from databricks.sdk import WorkspaceClient
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"databricks-sdk unavailable: {exc}")
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "example-us-east")
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", profile)
    try:
        ws = WorkspaceClient(profile=profile)
        ws.current_user.me()
        return ws
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"workspace {profile} unreachable: {exc}")


def _warehouse_or_skip(ws):
    whs = list(ws.warehouses.list())
    if not whs:
        pytest.skip("no SQL warehouse available")
    return whs[0].id


async def _catalog_exists(ws, wh, catalog: str) -> bool:
    from iot_twin.services.dbsql_service import execute_dbsql

    res = await execute_dbsql(f"SHOW CATALOGS LIKE '{catalog}'", wh)
    return res["row_count"] > 0


@pytest.mark.asyncio
async def test_integration_full_provision_and_teardown() -> None:
    from iot_twin.services.dbsql_service import execute_dbsql
    from iot_twin.services.industry import industry_catalog_service as ics
    from iot_twin.services.industry import industry_provisioning_service as ips
    from iot_twin.services.pipelines import pipeline_project_service as pps

    if not CATALOG or not SCHEMA:
        pytest.skip("UC_CATALOG / UC_SCHEMA not set — integration provisioning skipped")

    ws = _ws_or_skip()
    wh = _warehouse_or_skip(ws)

    # Pre-flight must pass (external location + warehouse) or the environment
    # can't support the test.
    checks = ics.preflight_checks(ws=ws, warehouse_id=wh)
    if any(c["status"] == "fail" for c in checks):
        pytest.skip(f"pre-flight failed: {checks}")

    try:
        summary = await ips.provision_industry(
            INDUSTRY,
            setup_id=None,
            ws=ws,
            warehouse_id=wh,
            enable_realtime=True,
            deploy_ops_jobs=False,
            lakets_sql_path=_VENDORED_LAKETS,
        )

        assert summary["catalog"] == CATALOG
        assert summary["schema"] == SCHEMA
        assert summary["branch"] == INDUSTRY

        # 1. The single schema exists in the pre-existing catalog.
        assert await _catalog_exists(ws, wh, CATALOG)
        tabs = await execute_dbsql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}", wh)
        names = {r.get("tableName") for r in tabs["data"]}
        # 2. Managed industry-prefixed tables exist.
        assert {
            f"{INDUSTRY}_zerobus",
            f"{INDUSTRY}_metrics",
            f"{INDUSTRY}_realtime",
            f"{INDUSTRY}_microbatch",
        } <= names

        # 3. Lakebase branch is present with a resolvable primary host.
        info = summary["sections"]
        branch_section = next(s for s in info if s["phase"] == "lakebase_branch")
        assert branch_section["result"]["primary_host"]

        # 5. LakeTS ChronoTable was created (registered).
        lakets_section = next(s for s in info if s["phase"] == "lakets_setup")
        steps = {s["step"]: s["status"] for s in lakets_section["result"]["steps"]}
        assert steps.get("create_chronotable") in ("created", "already_exists")
        assert steps.get("precreate_partitions") == "ok"

    finally:
        # ---- CLEANUP -----------------------------------------------------
        # The catalog PRE-EXISTS and is shared — NEVER drop it.  Drop only the
        # throwaway industry-prefixed managed tables (managed → data removed too).
        for kind in ("realtime", "microbatch", "zerobus", "metrics"):
            try:
                await execute_dbsql(
                    f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{INDUSTRY}_{kind}", wh
                )
            except Exception:
                pass

        # Delete the Lakebase branch (removes its primary endpoint); delete the
        # replica first.
        try:
            ws.postgres.delete_endpoint(name=pps.endpoint_resource_name(INDUSTRY, "replica"))
        except Exception:
            pass
        try:
            ws.postgres.delete_branch(name=pps.branch_resource_name(INDUSTRY))
        except Exception:
            pass
