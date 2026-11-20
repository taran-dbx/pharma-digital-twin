"""Health, metrics, and app-defaults endpoints."""

from __future__ import annotations

import os

from fastapi import APIRouter, Depends

from iot_twin.core.state import AppState, get_app_state
from iot_twin.core.workspace_config import (
    get_pipelines_project,
    get_secret_scope,
    get_service_credential,
)

router = APIRouter()


@router.get("/api/health")
async def health(state: AppState = Depends(get_app_state)):
    return {
        "status": "ok",
        "db_available": state.db_available,
        "db_error": state.db_error,
        "industries": state.industries,
        "databricks_client_id": os.getenv("DATABRICKS_CLIENT_ID", "(not set)"),
        "databricks_host": os.getenv("DATABRICKS_HOST", "(not set)"),
        "zerobus_client_id": os.getenv("ZEROBUS_CLIENT_ID", "(not set)")[:8] + "...",
        "zerobus_endpoint": os.getenv("ZEROBUS_SERVER_ENDPOINT", "(not set)"),
    }


@router.get("/api/app-defaults")
async def app_defaults():
    """Return workspace-level defaults the frontend can auto-populate.

    NOTE: Only non-secret values are returned. The client_secret is indicated
    as present/absent but never exposed.
    """
    # Parse the app's Lakebase endpoint so the wizard can pre-populate new
    # pipelines with the same project (ops can override per pipeline).
    # Format: "projects/<project_id>/branches/<branch_id>/endpoints/<endpoint_id>"
    lakebase_endpoint = os.getenv("LAKEBASE_ENDPOINT", "")
    lakebase_project_id = ""
    lakebase_branch_id = ""
    parts = lakebase_endpoint.split("/")
    if len(parts) >= 4 and parts[0] == "projects" and parts[2] == "branches":
        lakebase_project_id = parts[1]
        lakebase_branch_id = parts[3]

    # Zerobus OAuth builds "<workspace_url>/oidc/v1/token"; a bare host (no
    # scheme) raises "No scheme supplied" and produces an invalid UC token.
    # Ensure the workspace URL the frontend receives carries an https:// scheme.
    workspace_url = os.getenv("DATABRICKS_HOST", "")
    if workspace_url and not workspace_url.startswith("http"):
        workspace_url = f"https://{workspace_url}"

    return {
        "zerobus_endpoint": os.getenv("ZEROBUS_SERVER_ENDPOINT", ""),
        # The UC secret `app_sp_oauth` stores DATABRICKS_CLIENT_SECRET, so the
        # notebook must use the matching app service-principal client id.
        "zerobus_client_id": os.getenv("DATABRICKS_CLIENT_ID", ""),
        "zerobus_client_secret_set": bool(os.getenv("DATABRICKS_CLIENT_SECRET")),
        "workspace_url": workspace_url,
        "warehouse_id": os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
        "lakebase_endpoint": lakebase_endpoint,
        "lakebase_project_id": lakebase_project_id,
        "lakebase_branch_id": lakebase_branch_id,
        "lakebase_database": os.getenv("PGDATABASE", ""),
        # Workspace-specific values consumed by the TS notebook generators so
        # they never contain hardcoded workspace names or resource identifiers.
        "service_credential": get_service_credential(),
        "pipelines_project": get_pipelines_project(),
        "storage_base": os.getenv("UC_STORAGE_BASE", ""),
        "secret_scope": get_secret_scope(),
        "msk_bootstrap": os.getenv("MSK_BOOTSTRAP_SERVERS", ""),
        # One-schema layout (Task 3/4): pre-existing catalog + single schema for
        # all industry-prefixed managed tables (e.g. {industry}_zerobus, {industry}_metrics).
        "uc_catalog": os.getenv("UC_CATALOG", ""),
        "uc_schema": os.getenv("UC_SCHEMA", ""),
    }


@router.get("/metrics")
async def metrics():
    return {"status": "healthy"}
