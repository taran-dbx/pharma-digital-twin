"""Settings endpoints — validation, status, and provisioning."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state
from iot_twin.models.sql_ddl import LAKEBASE_ROLE_NAME
from iot_twin.services.app_permissions import _app_name, grant_app_can_use
from iot_twin.services.provisioning_service import (
    ensure_streaming_sp,
    provision_lakebase_project_and_branch,
    provision_lakebase_role,
    provision_lakebase_table,
    provision_metrics_table,
    provision_secrets_scope,
    provision_uc_catalog,
    save_direct_creds_to_secrets,
)
from iot_twin.services.settings_service import (
    status_lakebase,
    status_secrets,
    status_uc,
    status_warehouse,
    validate_cluster,
    validate_lakebase,
    validate_secrets,
    validate_uc,
)

logger = logging.getLogger("iot_digital_twin.routes.settings")

router = APIRouter(tags=["settings"])


@router.post("/api/settings/validate")
async def validate_settings(request: Request, user_email: str = Depends(get_current_user)):
    """Validate all settings sections concurrently and return pass/fail/warn per check."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    uc_result, lb_result, sec_result, cl_result = await asyncio.gather(
        validate_uc(body),
        validate_lakebase(body),
        validate_secrets(body),
        validate_cluster(body),
    )

    return {
        "uc": uc_result,
        "lakebase": lb_result,
        "secrets": sec_result,
        "cluster": cl_result,
    }


@router.get("/api/settings/status")
async def get_settings_status(
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Return current workspace resource state for the settings page."""
    # Load user settings to know what to probe
    settings: dict = {}
    if state.db_available and state.db:
        row = await state.db.fetchrow(
            "SELECT settings FROM user_settings WHERE user_email = $1", user_email
        )
        if row:
            settings = (
                json.loads(row["settings"]) if isinstance(row["settings"], str) else row["settings"]
            )

    uc_status, lb_status, sec_status, wh_status = await asyncio.gather(
        status_uc(settings),
        status_lakebase(settings),
        status_secrets(settings),
        status_warehouse(),
    )

    return {
        "uc": uc_status,
        "lakebase": lb_status,
        "secrets": sec_status,
        "warehouse": wh_status,
    }


@router.post("/api/settings/provision")
async def provision_settings(request: Request, user_email: str = Depends(get_current_user)):
    """Provision all settings resources then re-validate."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    provisioned: list[dict] = []

    # Run provisioning concurrently
    secrets_scope = (body.get("secrets", {}).get("scope") or "").strip()
    uc = body.get("uc", {})
    lb = body.get("lakebase", {})

    tasks = []
    if secrets_scope:
        tasks.append(("secrets", provision_secrets_scope(secrets_scope)))
    if uc.get("catalog_pattern"):
        tasks.append(("uc", provision_uc_catalog(uc)))
    tasks.append(("lakebase", provision_lakebase_table(lb)))

    results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)

    for (label, _), result in zip(tasks, results, strict=True):
        if isinstance(result, Exception):
            provisioned.append({"resource": label, "status": "failed", "error": str(result)[:200]})
        elif isinstance(result, list):
            provisioned.extend(result)
        elif isinstance(result, dict):
            provisioned.append(result)

    # Re-validate after provisioning
    uc_result, lb_result, sec_result, cl_result = await asyncio.gather(
        validate_uc(body),
        validate_lakebase(body),
        validate_secrets(body),
        validate_cluster(body),
    )

    lakets_entry = next(
        (p for p in provisioned if "lakets_version" in p or "lakets_error" in p),
        {},
    )

    return {
        "provisioned": provisioned,
        "lakets": {
            "version": lakets_entry.get("lakets_version"),
            "error": lakets_entry.get("lakets_error"),
        },
        "validation": {
            "uc": uc_result,
            "lakebase": lb_result,
            "secrets": sec_result,
            "cluster": cl_result,
        },
    }


@router.post("/api/settings/provision/uc")
async def provision_uc_settings(request: Request, user_email: str = Depends(get_current_user)):
    """Provision UC catalog, schemas, volume, and metrics table from UC settings."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    uc = body.get("uc", {})
    provisioned = await provision_uc_catalog(uc)

    # After creating schemas, provision the streaming_metrics table in the metrics schema
    pattern = uc.get("catalog_pattern", "iot_{name}")
    catalog_name = (
        pattern.replace("{name}", "default") if "{name}" in pattern else (pattern or "iot_default")
    )
    if "metrics" in uc.get("schemas", ["raw", "metrics", "checkpoints"]):
        import os

        zb_client_id = os.getenv("ZEROBUS_CLIENT_ID", "")
        metrics_results = await provision_metrics_table(
            catalog=catalog_name,
            schema="metrics",
            table_name="streaming_metrics",
            grant_to=zb_client_id or None,
        )
        provisioned.extend(metrics_results)

    validation = await validate_uc({"uc": uc})
    return {"provisioned": provisioned, "validation": validation}


@router.post("/api/settings/provision/secrets")
async def provision_secrets_settings(request: Request, user_email: str = Depends(get_current_user)):
    """Provision the secrets scope from secrets settings."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    secrets = body.get("secrets", {})
    scope_name = (secrets.get("scope") or "").strip()
    provisioned: list[dict] = []

    if not scope_name:
        provisioned.append(
            {"resource": "Secret scope", "status": "failed", "error": "Scope name is empty"}
        )
    else:
        # Scope + streaming SP + OAuth secret + app grant — delegated to the
        # reusable provisioning helper.  Module-level (patchable) references
        # are injected so route-level test patches still take effect.
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        sp_result = await ensure_streaming_sp(
            w,
            scope_name,
            provision_scope=provision_secrets_scope,
            grant_fn=grant_app_can_use,
            app_name_fn=_app_name,
        )
        provisioned.extend(sp_result["results"])

    validation = await validate_secrets({"secrets": secrets})
    return {"provisioned": provisioned, "validation": validation}


@router.post("/api/settings/provision/lakebase")
async def provision_lakebase_settings(
    request: Request, user_email: str = Depends(get_current_user)
):
    """Provision Lakebase: project, branch, role, tables, and credentials."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    lb = body.get("lakebase", {})
    secrets = body.get("secrets", {})
    lb_user = (body.get("lb_user") or "").strip()
    lb_password = (body.get("lb_password") or "").strip()
    mode = lb.get("mode", "autoscale")

    provisioned: list[dict] = []
    credentials: dict | None = None

    if mode == "autoscale":
        project_id = (lb.get("project_id") or "").strip()
        branch_id = (lb.get("branch_id") or "production").strip()

        if not project_id:
            validation = await validate_lakebase({"lakebase": lb})
            return {
                "provisioned": [
                    {"resource": "Lakebase", "status": "failed", "error": "project_id is empty"}
                ],
                "validation": validation,
            }

        # Step 1: Create project + branch
        proj_results = await provision_lakebase_project_and_branch(project_id, branch_id)
        provisioned.extend(proj_results)
        if any(r.get("status") == "failed" for r in proj_results):
            validation = await validate_lakebase({"lakebase": lb})
            return {"provisioned": provisioned, "validation": validation}

        # Step 2: Register the current SP as an OAuth-auth Postgres role.
        # The old flow created a password-authenticated ``iot_app_user`` role;
        # the new flow grants the SP itself DML on the sink tables and uses
        # short-lived OAuth credentials per connection.
        import secrets as secrets_mod

        database = (lb.get("database") or "databricks_postgres").strip()
        role_password = secrets_mod.token_urlsafe(24)  # kept for back-compat UI
        role_results = await provision_lakebase_role(
            project_id=project_id,
            branch_id=branch_id,
            database=database,
        )
        provisioned.extend(role_results)
        if any(r.get("status") == "failed" for r in role_results):
            validation = await validate_lakebase({"lakebase": lb})
            return {"provisioned": provisioned, "validation": validation}

        # Step 3: Store role credentials in workspace secrets
        scope = (secrets.get("scope") or "").strip()
        user_key = secrets.get("lb_user_key", "lb-user")
        pass_key = secrets.get("lb_password_key", "lb-password")
        if scope:
            cred_results = await save_direct_creds_to_secrets(
                secrets, LAKEBASE_ROLE_NAME, role_password
            )
            provisioned.extend(cred_results)
            credentials = {
                "pg_role": LAKEBASE_ROLE_NAME,
                "secret_scope": scope,
                "user_key": user_key,
                "password_key": pass_key,
            }

        # Step 4: Create tables
        # If password was set successfully, use the new role's credentials.
        # Otherwise (password skipped/failed), use autoscale OAuth credentials.
        password_set = any(
            r.get("resource", "").endswith("password") and r.get("status") == "created"
            for r in role_results
        )
        table_lb = dict(lb)
        if password_set:
            from databricks.sdk import WorkspaceClient as _WC

            _w = _WC()
            parent = f"projects/{project_id}/branches/{branch_id}"
            _endpoints = list(_w.postgres.list_endpoints(parent=parent))
            if _endpoints:
                _ep = _w.postgres.get_endpoint(name=_endpoints[0].name)
                table_lb["host"] = _ep.status.hosts.host
                table_lb["port"] = 5432
                table_lb["user"] = LAKEBASE_ROLE_NAME
                table_lb["password"] = role_password
                table_lb["mode"] = "direct"
        # else: keep autoscale mode — provision_lakebase_table will use OAuth
        table_result = await provision_lakebase_table(table_lb)
        provisioned.append(table_result)

    else:
        # Direct mode: use provided credentials
        table_lb = dict(lb)
        if lb_user:
            table_lb["user"] = lb_user
            table_lb["password"] = lb_password
        table_result = await provision_lakebase_table(table_lb)
        provisioned.append(table_result)

        # Save provided creds to workspace secrets
        if lb_user and lb_password and secrets:
            scope = (secrets.get("scope") or "").strip()
            user_key = secrets.get("lb_user_key", "lb-user")
            pass_key = secrets.get("lb_password_key", "lb-password")
            cred_results = await save_direct_creds_to_secrets(secrets, lb_user, lb_password)
            provisioned.extend(cred_results)
            if scope:
                credentials = {
                    "pg_role": lb_user,
                    "secret_scope": scope,
                    "user_key": user_key,
                    "password_key": pass_key,
                }

    validation = await validate_lakebase({"lakebase": lb})
    result = {"provisioned": provisioned, "validation": validation}
    if credentials:
        result["credentials"] = credentials
    return result
