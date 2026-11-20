"""Lakebase CDF configuration API helpers.

The pinned Databricks SDK does not yet expose typed CDF-config methods, so this
module wraps the documented Postgres REST API. CDF configs are immutable:
``ensure_cdf_config`` reuses an exact match and rejects a conflicting config.
"""

from __future__ import annotations

import time
from typing import Any

from databricks.sdk import WorkspaceClient

from iot_twin.core.workspace_config import get_pipelines_project

DEFAULT_DATABASE_ID = "databricks-postgres"
DEFAULT_POSTGRES_SCHEMA = "lakets_cdf"
TEARDOWN_ORDER = 38
OPERATION_TIMEOUT_S = 300
OPERATION_POLL_S = 2


class CdfConfigMismatch(RuntimeError):
    """An existing immutable CDF config points at a different destination."""


def _new_ws() -> Any:
    return WorkspaceClient()


def database_parent(
    branch: str,
    *,
    project_id: str | None = None,
    database_id: str = DEFAULT_DATABASE_ID,
) -> str:
    """Return the database resource name used by the CDF-config API."""
    project = project_id or get_pipelines_project()
    return f"projects/{project}/branches/{branch}/databases/{database_id}"


def config_name(parent: str, cdf_config_id: str) -> str:
    """Return the full CDF config resource name."""
    return f"{parent}/cdf-configs/{cdf_config_id}"


def _path(name: str) -> str:
    return f"/api/2.0/postgres/{name}"


def _is_not_found(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    error_code = getattr(exc, "error_code", None)
    normalized_code = getattr(error_code, "value", error_code)
    return status_code == 404 or str(normalized_code).upper() in {
        "NOT_FOUND",
        "RESOURCE_DOES_NOT_EXIST",
    }


def _wait_operation(
    operation: dict | None,
    *,
    ws: Any,
    timeout_s: int = OPERATION_TIMEOUT_S,
    poll_s: int = OPERATION_POLL_S,
) -> dict:
    """Wait for a Postgres long-running operation and return its response."""
    current = operation or {}
    operation_name = current.get("name")
    if "done" not in current and "/operations/" not in (operation_name or ""):
        return current
    deadline = time.monotonic() + timeout_s
    while not current.get("done"):
        if not operation_name:
            raise RuntimeError("CDF operation did not return an operation name")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"CDF operation {operation_name} did not finish within {timeout_s}s")
        time.sleep(poll_s)
        current = ws.api_client.do("GET", _path(operation_name))
    error = current.get("error")
    if error:
        code = error.get("error_code") if isinstance(error, dict) else None
        message = error.get("message") if isinstance(error, dict) else str(error)
        raise RuntimeError(f"CDF operation failed ({code or 'UNKNOWN'}): {message}")
    response = current.get("response")
    return response if isinstance(response, dict) else {}


def get_cdf_config(
    parent: str,
    cdf_config_id: str = DEFAULT_POSTGRES_SCHEMA,
    *,
    ws: Any | None = None,
) -> dict | None:
    """Return one CDF config, or ``None`` when it does not exist."""
    client = ws or _new_ws()
    try:
        return client.api_client.do("GET", _path(config_name(parent, cdf_config_id)))
    except Exception as exc:
        if _is_not_found(exc):
            return None
        raise


def ensure_cdf_config(
    parent: str,
    *,
    catalog: str,
    schema: str,
    postgres_schema: str = DEFAULT_POSTGRES_SCHEMA,
    cdf_config_id: str | None = None,
    ws: Any | None = None,
) -> dict:
    """Create an immutable CDF config or reuse an identical existing config."""
    client = ws or _new_ws()
    config_id = cdf_config_id or postgres_schema
    existing = get_cdf_config(parent, config_id, ws=client)
    expected = {
        "catalog": catalog,
        "schema": schema,
        "postgres_schema": postgres_schema,
    }
    if existing is not None:
        mismatch = {
            key: {"expected": value, "actual": existing.get(key)}
            for key, value in expected.items()
            if existing.get(key) != value
        }
        if mismatch:
            raise CdfConfigMismatch(
                f"CDF config {config_name(parent, config_id)} is immutable and "
                f"does not match the requested target: {mismatch}"
            )
        return {
            **existing,
            "name": config_name(parent, config_id),
            "cdf_config_id": existing.get("cdf_config_id") or config_id,
            "status": "existing",
        }

    response = client.api_client.do(
        "POST",
        _path(f"{parent}/cdf-configs"),
        query={"cdf_config_id": config_id},
        body=expected,
    )
    _wait_operation(response, ws=client)
    created = get_cdf_config(parent, config_id, ws=client)
    if created is None:
        raise RuntimeError(
            f"CDF config {config_name(parent, config_id)} was not found after create"
        )
    return {
        **expected,
        **created,
        "name": config_name(parent, config_id),
        "cdf_config_id": created.get("cdf_config_id") or config_id,
        "status": "created",
    }


def delete_cdf_config(
    parent: str,
    cdf_config_id: str = DEFAULT_POSTGRES_SCHEMA,
    *,
    force: bool = False,
    ws: Any | None = None,
) -> bool:
    """Delete a CDF config; return whether it existed."""
    client = ws or _new_ws()
    try:
        operation = client.api_client.do(
            "DELETE",
            _path(config_name(parent, cdf_config_id)),
            query={"force": str(force).lower()},
        )
        _wait_operation(operation, ws=client)
    except Exception as exc:
        if _is_not_found(exc):
            return False
        raise
    return True
