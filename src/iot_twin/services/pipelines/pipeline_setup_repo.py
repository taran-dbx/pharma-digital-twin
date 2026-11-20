"""Async CRUD for the ``pipeline_setup`` table (app-backend, spec §2.3/§2.4).

One row per industry (UNIQUE ``industry``).  All functions take any async DB
executor (``LakebaseConnectionManager`` / ``PipelineLakebaseHandle`` / raw
``asyncpg.Connection``) and return plain dicts with JSONB columns decoded.
"""

from __future__ import annotations

import json
from typing import Any

from iot_twin.services.db.app_repo_common import (
    DBExecutor,
    build_insert,
    build_update,
    row_to_dict,
)

TABLE = "pipeline_setup"
JSON_FIELDS = (
    "derived",
    "enabled_modes",
    "mode_configs",
    "cluster_spec",
    "validation_state",
)


def _to_dict(row: Any) -> dict | None:
    return row_to_dict(row, JSON_FIELDS)


async def create_setup(
    db: DBExecutor,
    *,
    industry: str,
    flavor: str | None = None,
    source_type: str | None = None,
    bootstrap_servers: str | None = None,
    derived: dict | None = None,
    service_credential: str | None = None,
    enabled_modes: list | None = None,
    mode_configs: dict | None = None,
    cluster_spec: dict | None = None,
    warehouse_id: str | None = None,
    validation_state: dict | None = None,
    status: str = "draft",
    user_email: str | None = None,
) -> dict:
    """Insert a new pipeline setup and return the created row."""
    values = {
        "industry": industry,
        "flavor": flavor,
        "source_type": source_type,
        "bootstrap_servers": bootstrap_servers,
        "derived": derived,
        "service_credential": service_credential,
        "enabled_modes": enabled_modes,
        "mode_configs": mode_configs,
        "cluster_spec": cluster_spec,
        "warehouse_id": warehouse_id,
        "validation_state": validation_state,
        "status": status,
        "user_email": user_email,
    }
    query, args = build_insert(TABLE, values, JSON_FIELDS)
    row = await db.fetchrow(query, *args)
    return _to_dict(row)


async def get_setup(db: DBExecutor, setup_id: Any) -> dict | None:
    """Return the setup row by id, or None."""
    row = await db.fetchrow(f"SELECT * FROM {TABLE} WHERE id = $1", setup_id)
    return _to_dict(row)


async def get_setup_by_industry(db: DBExecutor, industry: str) -> dict | None:
    """Return the setup row for an industry, or None."""
    row = await db.fetchrow(f"SELECT * FROM {TABLE} WHERE industry = $1", industry)
    return _to_dict(row)


async def list_setups(db: DBExecutor) -> list[dict]:
    """Return all setup rows ordered by creation time."""
    rows = await db.fetch(f"SELECT * FROM {TABLE} ORDER BY created_at ASC")
    return [_to_dict(r) for r in rows]


async def update_setup(db: DBExecutor, setup_id: Any, **fields: Any) -> dict | None:
    """Update the given columns on a setup and return the updated row.

    With no fields, returns the current row unchanged.
    """
    if not fields:
        return await get_setup(db, setup_id)
    query, args = build_update(TABLE, setup_id, fields, JSON_FIELDS)
    row = await db.fetchrow(query, *args)
    return _to_dict(row)


async def update_mode_config(
    db: DBExecutor,
    setup_id: Any,
    mode: str,
    config: dict[str, Any],
) -> dict | None:
    """Atomically merge one mode's config without replacing sibling modes."""
    row = await db.fetchrow(
        f"UPDATE {TABLE} SET mode_configs = jsonb_set("
        "COALESCE(mode_configs, '{}'::jsonb), ARRAY[$2::text], "
        "COALESCE(mode_configs -> $2, '{}'::jsonb) || $3::jsonb, true"
        "), updated_at = now() WHERE id = $1::uuid RETURNING *",
        setup_id,
        mode,
        json.dumps(config),
    )
    return _to_dict(row)


async def delete_setup(db: DBExecutor, setup_id: Any) -> bool:
    """Delete a setup (FK cascade removes deployments + resources).

    Returns True if a row was deleted.
    """
    result = await db.execute(f"DELETE FROM {TABLE} WHERE id = $1", setup_id)
    return _rows_affected(result) > 0


def _rows_affected(command_tag: Any) -> int:
    """Parse the trailing integer from an asyncpg command tag (e.g. 'DELETE 1')."""
    if isinstance(command_tag, str) and command_tag:
        try:
            return int(command_tag.split()[-1])
        except (ValueError, IndexError):
            return 0
    return 0
