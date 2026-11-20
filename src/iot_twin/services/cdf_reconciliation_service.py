"""Industry-scoped Lakebase CDF reconciliation.

CDF configuration is shared at the Unity Catalog schema level, but every
industry must expose a distinct Postgres shadow table. Lakebase derives the
Delta destination name from that source table, so source identity is the
isolation boundary.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

from iot_twin.core.workspace_config import get_pipelines_project
from iot_twin.services import cdf_config_service, resource_ledger
from iot_twin.services.deployment import deployment_repo
from iot_twin.services.industry import industry_catalog_service
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.lakets import lakets_service
from iot_twin.services.pipelines import pipeline_project_service, pipeline_setup_repo

logger = logging.getLogger("iot_digital_twin.services.cdf_reconciliation")

CDF_SHADOW_SCHEMA = cdf_config_service.DEFAULT_POSTGRES_SCHEMA
SOURCE_SCHEMA = "public"
SOURCE_TABLE = "sensor_readings"
POLL_TIMEOUT_S = 90
POLL_INTERVAL_S = 2
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_WAL2DELTA_STATUS_SQL = """
SELECT t.status
FROM wal2delta.tables t
JOIN pg_class c ON c.oid = t.table_oid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
"""


class RealtimeCdfError(RuntimeError):
    """Realtime CDF could not be proven ready for job deployment or start."""


def normalize_industry(industry: str) -> str:
    """Return a canonical curated industry identifier or raise."""
    normalized = industry.strip().lower() if isinstance(industry, str) else ""
    if normalized not in CURATED_INDUSTRIES:
        raise ValueError(f"unknown curated industry: {industry!r}")
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"industry is not Postgres identifier-safe: {industry!r}")
    return normalized


def shadow_table_name(industry: str) -> str:
    """Return the industry-unique LakeTS CDF shadow table name."""
    shadow = f"_shadow_{normalize_industry(industry)}_{SOURCE_TABLE}"
    if len(shadow.encode("utf-8")) > 63:
        raise ValueError(f"CDF shadow identifier exceeds 63 bytes: {shadow!r}")
    return shadow


def delta_table_name(industry: str) -> str:
    """Return Lakebase's deterministic Delta history-table name."""
    return f"lb_{shadow_table_name(industry)}_history"


def delta_table_fqn(
    industry: str,
    *,
    catalog: str | None = None,
    schema: str | None = None,
) -> str:
    """Return the configured one-schema Unity Catalog destination."""
    resolved_catalog = industry_catalog_service.uc_catalog_name() if catalog is None else catalog
    resolved_schema = industry_catalog_service.uc_schema_name() if schema is None else schema
    for label, namespace in (
        ("catalog", resolved_catalog),
        ("schema", resolved_schema),
    ):
        if not isinstance(namespace, str) or not namespace.strip() or "." in namespace:
            raise ValueError(f"CDF {label} must be a non-empty UC identifier")
    return f"{resolved_catalog.strip()}.{resolved_schema.strip()}.{delta_table_name(industry)}"


async def wait_for_exact_shadow_streaming(
    conn: Any,
    shadow: str,
    *,
    timeout_s: float = POLL_TIMEOUT_S,
    poll_interval_s: float = POLL_INTERVAL_S,
) -> None:
    """Wait until the exact shadow row in wal2delta.tables is STREAMING."""
    deadline = time.monotonic() + timeout_s
    last_status = "missing"
    while True:
        rows = await conn.fetch(_WAL2DELTA_STATUS_SQL, CDF_SHADOW_SCHEMA, shadow)
        statuses = [str(row.get("status") or "").upper() for row in (rows or [])]
        if "STREAMING" in statuses:
            return
        if statuses:
            last_status = ",".join(statuses)
        if time.monotonic() >= deadline:
            raise RealtimeCdfError(
                f"{CDF_SHADOW_SCHEMA}.{shadow} did not reach STREAMING "
                f"within {timeout_s:g}s (last status: {last_status})"
            )
        await asyncio.sleep(poll_interval_s)


async def _record_cdf_once(
    db: Any,
    *,
    setup_id: Any,
    deployment_id: Any,
    config: dict,
    parent: str,
    destination: str,
    shadow: str,
    branch: str,
) -> None:
    resources = await resource_ledger.list_resources(db, setup_id)
    catalog = industry_catalog_service.uc_catalog_name()
    schema = industry_catalog_service.uc_schema_name()
    metadata = {
        "parent": parent,
        "cdf_config_id": config.get("cdf_config_id", cdf_config_service.DEFAULT_POSTGRES_SCHEMA),
        "catalog": catalog,
        "schema": schema,
        "postgres_schema": CDF_SHADOW_SCHEMA,
        "source_table": f"{CDF_SHADOW_SCHEMA}.{shadow}",
        "delta_table": destination,
        "database": pipeline_project_service.PG_DATABASE,
        "force_delete_tables": False,
    }
    existing = next(
        (
            resource
            for resource in resources
            if resource.get("resource_type") == "cdf_sync"
            and resource.get("identifier") == config["name"]
            and resource.get("status") == "active"
        ),
        None,
    )
    parent_ref = pipeline_project_service.branch_resource_name(branch)
    if existing is not None:
        await resource_ledger.update_resource(
            db,
            existing["id"],
            deployment_id=deployment_id,
            parent_ref=parent_ref,
            teardown_order=cdf_config_service.TEARDOWN_ORDER,
            metadata=metadata,
        )
        return
    await resource_ledger.record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type="cdf_sync",
        identifier=config["name"],
        parent_ref=parent_ref,
        teardown_order=cdf_config_service.TEARDOWN_ORDER,
        metadata=metadata,
    )


async def reconcile_realtime_cdf(
    db: Any,
    setup_id: Any,
    *,
    ws: Any | None = None,
    poll_timeout_s: float = POLL_TIMEOUT_S,
    poll_interval_s: float = POLL_INTERVAL_S,
) -> dict:
    """Reconcile and prove the exact industry CDF binding for a deployment."""
    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise RealtimeCdfError(f"pipeline_setup {setup_id!r} not found")
    try:
        industry = normalize_industry(setup.get("industry"))
    except ValueError as exc:
        raise RealtimeCdfError(str(exc)) from exc

    deployment = await deployment_repo.get_deployment_by_mode(db, setup_id, "realtime")
    if deployment is None:
        raise RealtimeCdfError(f"realtime deployment for pipeline_setup {setup_id!r} not found")

    shadow = shadow_table_name(industry)
    branch = industry
    migration_conn = None
    poll_conn = None
    try:
        catalog = industry_catalog_service.uc_catalog_name()
        schema = industry_catalog_service.uc_schema_name()
        destination = delta_table_fqn(industry, catalog=catalog, schema=schema)
        project = get_pipelines_project()
        migration_conn = await pipeline_project_service.get_write_conn(
            branch, ws=ws, project_id=project
        )
        migration = await lakets_service.reconcile_chronotable_shadow(
            migration_conn,
            shadow_table=shadow,
            table=SOURCE_TABLE,
            schema=SOURCE_SCHEMA,
        )
        if migration.get("status") == "failed":
            raise RealtimeCdfError(
                migration.get("error") or "industry CDF shadow reconciliation failed"
            )
        await _close_conn(migration_conn, "migration")
        migration_conn = None

        parent = cdf_config_service.database_parent(branch, project_id=project)
        config = await asyncio.to_thread(
            cdf_config_service.ensure_cdf_config,
            parent,
            catalog=catalog,
            schema=schema,
            ws=ws,
        )
        poll_conn = await pipeline_project_service.get_write_conn(branch, ws=ws, project_id=project)
        await wait_for_exact_shadow_streaming(
            poll_conn,
            shadow,
            timeout_s=poll_timeout_s,
            poll_interval_s=poll_interval_s,
        )
        await _record_cdf_once(
            db,
            setup_id=setup_id,
            deployment_id=deployment["id"],
            config=config,
            parent=parent,
            destination=destination,
            shadow=shadow,
            branch=branch,
        )
        await deployment_repo.update_deployment(
            db,
            deployment["id"],
            cdf_enabled=True,
            lakebase_branch=branch,
            delta_table=destination,
        )
        return {
            "setup_id": setup_id,
            "deployment_id": deployment["id"],
            "industry": industry,
            "branch": branch,
            "shadow_table": f"{CDF_SHADOW_SCHEMA}.{shadow}",
            "delta_table": destination,
            "cdf_config": config["name"],
            "status": "streaming",
            "migration": migration["status"],
        }
    except RealtimeCdfError:
        await _mark_failed(db, deployment)
        raise
    except Exception as exc:
        await _mark_failed(db, deployment)
        raise RealtimeCdfError(f"realtime CDF reconciliation failed for {industry}: {exc}") from exc
    finally:
        await _close_conn(migration_conn, "migration")
        await _close_conn(poll_conn, "poll")


async def _close_conn(conn: Any | None, purpose: str) -> None:
    if conn is None:
        return
    try:
        await conn.close()
    except Exception:
        logger.warning("Failed to close CDF %s connection", purpose, exc_info=True)


async def _mark_failed(db: Any, deployment: dict) -> None:
    try:
        fields: dict[str, Any] = {"cdf_enabled": False}
        if not deployment.get("job_id") and deployment.get("status") in {"pending", "deploying"}:
            fields["status"] = "failed"
        await deployment_repo.update_deployment(db, deployment["id"], **fields)
    except Exception:
        logger.warning("Could not persist failed CDF status", exc_info=True)
