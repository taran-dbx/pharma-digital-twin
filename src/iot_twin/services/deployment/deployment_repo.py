"""Async CRUD for the ``pipeline_deployment`` table (app-backend, spec §2.3).

One row per (setup, mode); UNIQUE(setup_id, mode).  FK ``setup_id`` cascades on
setup delete.  Returns plain dicts with JSONB columns decoded.
"""

from __future__ import annotations

from typing import Any

from iot_twin.services.db.app_repo_common import (
    DBExecutor,
    build_insert,
    build_update,
    row_to_dict,
)

TABLE = "pipeline_deployment"
JSON_FIELDS = ("metrics_config", "lakebase_endpoints")


def _to_dict(row: Any) -> dict | None:
    return row_to_dict(row, JSON_FIELDS)


async def create_deployment(
    db: DBExecutor,
    *,
    setup_id: Any,
    mode: str,
    status: str = "pending",
    partition_count: int | None = None,
    cdf_enabled: bool | None = None,
    metrics_config: dict | None = None,
    lakebase_branch: str | None = None,
    lakebase_endpoints: dict | None = None,
    delta_table: str | None = None,
    checkpoint_path: str | None = None,
    job_id: str | None = None,
    notebook_path: str | None = None,
) -> dict:
    """Insert a deployment for a (setup, mode) pair and return the row."""
    values = {
        "setup_id": setup_id,
        "mode": mode,
        "status": status,
        "partition_count": partition_count,
        "cdf_enabled": cdf_enabled,
        "metrics_config": metrics_config,
        "lakebase_branch": lakebase_branch,
        "lakebase_endpoints": lakebase_endpoints,
        "delta_table": delta_table,
        "checkpoint_path": checkpoint_path,
        "job_id": job_id,
        "notebook_path": notebook_path,
    }
    query, args = build_insert(TABLE, values, JSON_FIELDS)
    row = await db.fetchrow(query, *args)
    return _to_dict(row)


async def get_deployment(db: DBExecutor, deployment_id: Any) -> dict | None:
    """Return the deployment row by id, or None."""
    row = await db.fetchrow(f"SELECT * FROM {TABLE} WHERE id = $1", deployment_id)
    return _to_dict(row)


async def get_deployment_by_mode(db: DBExecutor, setup_id: Any, mode: str) -> dict | None:
    """Return the deployment for a (setup, mode) pair, or None."""
    row = await db.fetchrow(
        f"SELECT * FROM {TABLE} WHERE setup_id = $1 AND mode = $2",
        setup_id,
        mode,
    )
    return _to_dict(row)


async def list_deployments(db: DBExecutor, setup_id: Any) -> list[dict]:
    """Return all deployments for a setup ordered by creation time."""
    rows = await db.fetch(
        f"SELECT * FROM {TABLE} WHERE setup_id = $1 ORDER BY created_at ASC",
        setup_id,
    )
    return [_to_dict(r) for r in rows]


async def update_deployment(db: DBExecutor, deployment_id: Any, **fields: Any) -> dict | None:
    """Update the given columns on a deployment and return the updated row."""
    if not fields:
        return await get_deployment(db, deployment_id)
    query, args = build_update(TABLE, deployment_id, fields, JSON_FIELDS)
    row = await db.fetchrow(query, *args)
    return _to_dict(row)


async def delete_deployment(db: DBExecutor, deployment_id: Any) -> bool:
    """Delete a single deployment. Returns True if a row was deleted."""
    result = await db.execute(f"DELETE FROM {TABLE} WHERE id = $1", deployment_id)
    if isinstance(result, str) and result:
        try:
            return int(result.split()[-1]) > 0
        except (ValueError, IndexError):
            return False
    return False
