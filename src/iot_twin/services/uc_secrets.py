"""Unity Catalog secret read/write helper (Task 3).

Unity Catalog secrets are catalog-scoped (``{catalog}.{schema}.{name}``) and
distinct from workspace secret scopes.  The app SP stores its own OAuth
client-secret here (``{catalog}.{schema}.app_sp_oauth``) so the Zerobus
producer notebooks can read it at runtime with only ``READ SECRET`` on the
schema — no static credential is baked into notebook source.

Both helpers use the native SDK ``ws.secrets_uc`` surface (databricks-sdk
≥ 0.133.0) and run in a worker thread so the event loop is never blocked.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.uc_secrets")


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _fqn(catalog: str, schema: str, name: str) -> str:
    """Return the fully-qualified UC secret name ``{catalog}.{schema}.{name}``."""
    return f"{catalog}.{schema}.{name}"


def _is_already_exists(exc: Exception) -> bool:
    return "already exists" in str(exc).lower()


def _put_sync(ws: Any, catalog: str, schema: str, name: str, value: str) -> dict:
    """Create-or-update a UC secret via the native SDK ``secrets_uc`` API (blocking)."""
    from databricks.sdk.common.types.fieldmask import FieldMask
    from databricks.sdk.service.catalog import Secret

    fqn = _fqn(catalog, schema, name)
    try:
        ws.secrets_uc.create_secret(
            Secret(catalog_name=catalog, schema_name=schema, name=name, value=value)
        )
        return {"resource": f"UC secret '{fqn}'", "status": "created"}
    except Exception as exc:
        if not _is_already_exists(exc):
            return {"resource": f"UC secret '{fqn}'", "status": "failed", "error": str(exc)[:200]}
        # Already present — update the value in place.  Best-effort: a failing
        # update (wrong API form, permission denied, network blip) must NOT hard-
        # fail provisioning on a teardown→re-provision re-run, so mirror the
        # create path's contract and surface the error instead of raising.
        try:
            ws.secrets_uc.update_secret(
                fqn,
                Secret(catalog_name=catalog, schema_name=schema, name=name, value=value),
                FieldMask(["value"]),
            )
        except Exception as update_exc:
            return {
                "resource": f"UC secret '{fqn}'",
                "status": "failed",
                "error": str(update_exc)[:200],
            }
        return {"resource": f"UC secret '{fqn}'", "status": "updated"}


def _get_sync(ws: Any, catalog: str, schema: str, name: str) -> str:
    """Read a UC secret value via the native SDK ``secrets_uc`` API (blocking)."""
    fqn = _fqn(catalog, schema, name)
    result = ws.secrets_uc.get_secret(fqn, include_value=True)
    return result.effective_value or ""


async def put_secret(
    catalog: str,
    schema: str,
    name: str,
    value: str,
    *,
    ws: Any | None = None,
) -> dict:
    """Create or update the UC secret ``{catalog}.{schema}.{name}`` (idempotent).

    Returns a ``{resource, status}`` dict (``created`` / ``updated`` / ``failed``).
    """
    ws = ws or _new_ws()
    return await asyncio.to_thread(_put_sync, ws, catalog, schema, name, value)


async def get_secret(
    catalog: str,
    schema: str,
    name: str,
    *,
    ws: Any | None = None,
) -> str:
    """Return the value of the UC secret ``{catalog}.{schema}.{name}``."""
    ws = ws or _new_ws()
    return await asyncio.to_thread(_get_sync, ws, catalog, schema, name)
