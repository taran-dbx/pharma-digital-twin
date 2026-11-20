"""Normalized provisioned-resource ledger (app-backend, spec §2.3/§2.4).

Every resource provisioned for a pipeline (UC catalog/schema/table/volume,
Kafka topic, Lakebase branch/endpoint/replica, job, secret scope/secret, SP,
grant, CDF sync, metrics table) is recorded here with a ``teardown_order`` and
``parent_ref`` so delete can walk resources in reverse dependency order.

The ledger is the single source of truth used both to delete and to (re)create
resources.  ``resources_in_teardown_order`` returns rows in DESCENDING
``teardown_order`` — the order the teardown orchestrator deletes them in.
"""

from __future__ import annotations

from typing import Any

from iot_twin.services.db.app_repo_common import DBExecutor, build_insert, build_update, row_to_dict

TABLE = "provisioned_resource"
JSON_FIELDS = ("metadata",)

# Resource types recognized by the ledger (spec §2.3).  Not enforced by the DB
# so new types can be added without a migration, but documented here.
RESOURCE_TYPES = (
    "uc_catalog",
    "uc_schema",
    "uc_table",
    "uc_volume",
    "kafka_topic",
    "lakebase_branch",
    "lakebase_endpoint",
    "lakebase_read_replica",
    "job",
    "secret_scope",
    "secret",
    "service_principal",
    "grant",
    "cdf_sync",
    "metrics_table",
    "genie_space",
)


def _to_dict(row: Any) -> dict | None:
    return row_to_dict(row, JSON_FIELDS)


async def record_resource(
    db: DBExecutor,
    *,
    setup_id: Any,
    resource_type: str,
    identifier: str,
    deployment_id: Any | None = None,
    parent_ref: str | None = None,
    teardown_order: int = 0,
    status: str = "active",
    metadata: dict | None = None,
) -> dict:
    """Record a provisioned resource and return the created ledger row."""
    values = {
        "setup_id": setup_id,
        "deployment_id": deployment_id,
        "resource_type": resource_type,
        "identifier": identifier,
        "parent_ref": parent_ref,
        "teardown_order": teardown_order,
        "status": status,
        "metadata": metadata,
    }
    query, args = build_insert(TABLE, values, JSON_FIELDS)
    row = await db.fetchrow(query, *args)
    return _to_dict(row)


async def get_resource(db: DBExecutor, resource_id: Any) -> dict | None:
    """Return a single ledger row by id, or None."""
    row = await db.fetchrow(f"SELECT * FROM {TABLE} WHERE id = $1", resource_id)
    return _to_dict(row)


async def update_resource(db: DBExecutor, resource_id: Any, **fields: Any) -> dict | None:
    """Refresh mutable binding metadata on an existing ledger row."""
    if not fields:
        return await get_resource(db, resource_id)
    query, args = build_update(
        TABLE,
        resource_id,
        fields,
        JSON_FIELDS,
        touch_updated_at=False,
    )
    row = await db.fetchrow(query, *args)
    return _to_dict(row)


async def list_resources(db: DBExecutor, setup_id: Any) -> list[dict]:
    """Return all ledger rows for a setup in ASCENDING teardown_order."""
    rows = await db.fetch(
        f"SELECT * FROM {TABLE} WHERE setup_id = $1 ORDER BY teardown_order ASC, created_at ASC",
        setup_id,
    )
    return [_to_dict(r) for r in rows]


async def resources_in_teardown_order(db: DBExecutor, setup_id: Any) -> list[dict]:
    """Return ledger rows for a setup in DESCENDING teardown_order.

    This is the order the teardown orchestrator deletes resources in (reverse
    dependency order).
    """
    rows = await db.fetch(
        f"SELECT * FROM {TABLE} WHERE setup_id = $1 ORDER BY teardown_order DESC, created_at DESC",
        setup_id,
    )
    return [_to_dict(r) for r in rows]


async def mark_resource_status(db: DBExecutor, resource_id: Any, status: str) -> dict | None:
    """Set the ``status`` on a ledger row (e.g. 'deleted') and return it."""
    row = await db.fetchrow(
        f"UPDATE {TABLE} SET status = $1 WHERE id = $2 RETURNING *",
        status,
        resource_id,
    )
    return _to_dict(row)
