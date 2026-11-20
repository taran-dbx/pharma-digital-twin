"""Databricks SQL Statement Execution helper for querying UC tables."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.dbsql")

_CATALOG_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


def validate_catalog(catalog: str) -> bool:
    """Return True if the catalog name is safe for SQL interpolation."""
    return bool(_CATALOG_RE.fullmatch(catalog))


# Reserved user_settings key holding APP-GLOBAL (service-principal-scoped)
# config. DBSQL queries always run as the app service principal, so the
# warehouse is shared app-wide rather than tied to any human user.
APP_GLOBAL_SETTINGS_KEY = "__app__"


async def _warehouse_from_settings_row(db: Any, key: str) -> str:
    """Return the saved warehouse_id for a user_settings key, or '' if none."""
    row = await db.fetchrow("SELECT settings FROM user_settings WHERE user_email = $1", key)
    if not row:
        return ""
    settings = (
        json.loads(row["settings"]) if isinstance(row["settings"], str) else row["settings"]
    ) or {}
    return settings.get("warehouse", {}).get("warehouse_id", "") or ""


async def resolve_warehouse_id(db: Any, db_available: bool, user_email: str) -> str:
    """Resolve the SQL warehouse for DBSQL queries.

    DBSQL always executes as the APP SERVICE PRINCIPAL (``execute_dbsql`` uses
    the ambient ``WorkspaceClient`` = the app SP in the Apps runtime), so the
    warehouse is resolved app-globally rather than per-user. Resolution order:

        1. ``DATABRICKS_SQL_WAREHOUSE_ID`` env (explicit ops override)
        2. the shared ``__app__`` global config row
        3. the calling user's saved warehouse (legacy fallback)

    This keeps queries working regardless of which user (or ``anonymous``)
    initiates the request.
    """
    env_wh = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if env_wh:
        return env_wh
    if db_available and db:
        try:
            wh = await _warehouse_from_settings_row(db, APP_GLOBAL_SETTINGS_KEY)
            if wh:
                return wh
            wh = await _warehouse_from_settings_row(db, user_email)
            if wh:
                return wh
        except Exception:
            pass
    return ""


def _execute_dbsql_sync(sql: str, warehouse_id: str, timeout: str) -> dict:
    """Blocking SQL execution via the SDK Statement Execution API.

    Uses the WorkspaceClient so auth works under the app's SP OAuth (a raw
    httpx call with ``w.config.token`` returns 401 in the Apps runtime because
    there is no PAT — the token is empty/invalid).  Run via ``asyncio.to_thread``
    so the event loop is never blocked.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout=timeout,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    state = resp.status.state if resp.status else None
    if state not in (StatementState.SUCCEEDED, None):
        err = resp.status.error.message if resp.status and resp.status.error else str(state)
        raise RuntimeError(f"DBSQL statement {state}: {err}")

    manifest = resp.manifest
    columns = (
        [c.name for c in manifest.schema.columns]
        if manifest and manifest.schema and manifest.schema.columns
        else []
    )
    data_array = (resp.result.data_array if resp.result else None) or []
    rows: list[dict] = [dict(zip(columns, chunk, strict=False)) for chunk in data_array]
    return {"columns": columns, "data": rows, "row_count": len(rows)}


async def execute_dbsql(
    sql: str,
    warehouse_id: str,
    timeout: str = "50s",
) -> dict:
    """Execute a SQL statement via the Databricks SQL Statement Execution API.

    Returns {"columns": [...], "data": [...], "row_count": int}.
    Raises on API errors.
    """
    import asyncio

    return await asyncio.to_thread(_execute_dbsql_sync, sql, warehouse_id, timeout)


async def get_pipeline_catalog(
    db: Any,
    db_available: bool,
    pipeline_id: str,
    user_email: str,
    is_admin: bool = False,
) -> str | None:
    """Resolve UC catalog from a pipeline's resources. Returns None if unavailable.

    ``is_admin`` lifts the per-user ownership filter so an admin can inspect
    curated-industry pipelines (owned by a fixed system identity, not the
    admin's own email — see ``iot_twin.services.industry.industry_service._INDUSTRY_PIPELINE_OWNER``).
    """
    if not db_available or not db:
        return None
    if is_admin:
        row = await db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid",
            pipeline_id,
        )
    else:
        row = await db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid AND user_email = $2",
            pipeline_id,
            user_email,
        )
    if not row:
        return None
    resources = (
        row["resources"]
        if isinstance(row["resources"], dict)
        else json.loads(row["resources"] or "{}")
    )
    return resources.get("uc_catalog")
