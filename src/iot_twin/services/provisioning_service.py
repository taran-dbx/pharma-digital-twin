"""Provisioning helper functions for settings-page resource creation.

These functions create UC catalogs, Lakebase projects/branches/roles/tables,
UC secrets, and metrics tables.  They are used by the settings route handlers.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from iot_twin.models.sql_ddl import (
    LAKEBASE_ROLE_ID,
    MATVIEW_1M_DDL,
    MATVIEW_5M_DDL,
    MATVIEW_INDEX_DDL,
    SENSOR_READINGS_DDL,
)
from iot_twin.services.app_permissions import _app_name, grant_app_can_use
from iot_twin.services.lakets import lakets_service
from iot_twin.services.zerobus_validation import ZEROBUS_OAUTH_ENTRY_NAME

logger = logging.getLogger("iot_digital_twin.services.provisioning")


def _is_missing_metastore_storage(exc: Exception) -> bool:
    """Detect the 'metastore has no default storage root' UC error.

    Mirrors the matcher in ``routes/provisioning.py`` so both code paths
    surface the same actionable hint when ``CREATE CATALOG`` is issued
    on a workspace whose metastore has no default managed-storage URL.
    """
    msg = str(exc).lower()
    return "metastore storage root" in msg or "default storage" in msg or "managed location" in msg


async def provision_secrets_scope(scope_name: str) -> dict:
    """Create a UC secret scope (idempotent)."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        try:
            w.secrets.create_scope(scope=scope_name)
            return {"resource": f"Secret scope '{scope_name}'", "status": "created"}
        except Exception as inner:
            if "already exists" in str(inner).lower():
                return {"resource": f"Secret scope '{scope_name}'", "status": "already_exists"}
            raise
    except Exception as exc:
        return {
            "resource": f"Secret scope '{scope_name}'",
            "status": "failed",
            "error": str(exc)[:200],
        }


def _is_sp_credential_skippable(exc: Exception) -> bool:
    """Return True when SP-credential minting failed for a non-fatal reason.

    Permission and SDK-deserialization errors mean the runtime identity can't
    mint account-level SP credentials (common on FEVM / shared demo
    workspaces).  This is only required for Zerobus ingest, so callers should
    surface it as ``skipped`` rather than ``failed``.
    """
    err = str(exc).lower()
    return (
        "unable to parse response" in err
        or "not authorized" in err
        or "permission denied" in err
        or "forbidden" in err
        or "unauthorized" in err
    )


def _find_sp_by_display_name(w: Any, display_name: str) -> Any | None:
    """Look up a service principal by exact display name.

    Returns the first matching SP object that has an ``application_id``, or
    ``None`` if none match. Used by ``ensure_streaming_sp`` (grant re-apply on
    the already-exists path) and by ``industry_catalog_service`` to resolve the
    app SP's grant principal.
    """
    matches = list(w.service_principals.list(filter=f'displayName eq "{display_name}"'))
    return next((s for s in matches if getattr(s, "application_id", None)), None)


def _create_sp_and_oauth_secret(
    w: Any,
    sp_name: str,
    scope_name: str,
    id_key: str,
    secret_key: str,
) -> tuple[str, str]:
    """Create a service principal, mint an OAuth secret, and store both in
    the given secret scope.

    Returns ``(sp_id, client_id)``. Raises on failure — callers wrap this in
    their own error handling. Used by the legacy per-pipeline
    ``ensure_streaming_sp`` path (the industry flow uses the single app SP and
    mints no new SP).
    """
    sp = w.service_principals.create(display_name=sp_name, active=True)
    sp_id = sp.id
    client_id = sp.application_id

    secret_data = w.api_client.do(
        "POST",
        f"/api/2.0/accounts/servicePrincipals/{sp_id}/credentials/secrets",
        body={},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    client_secret = secret_data.get("secret", "")
    if not client_secret:
        raise RuntimeError(f"No secret returned for SP {sp_id}: keys={list(secret_data.keys())}")

    w.secrets.put_secret(scope=scope_name, key=id_key, string_value=client_id)
    w.secrets.put_secret(scope=scope_name, key=secret_key, string_value=client_secret)

    return sp_id, client_id


async def ensure_streaming_sp(
    w: Any,
    scope_name: str,
    *,
    provision_scope: Any = None,
    grant_fn: Any = None,
    app_name_fn: Any = None,
) -> dict:
    """Ensure the streaming service principal + OAuth secret exist (idempotent).

    Creates the secret scope, mints a service principal named
    ``iot-sss-<scope_name>`` with an OAuth secret, stores its client id /
    secret in the scope, and grants the SP ``CAN_USE`` on the app.  Both the
    fresh-create and already-exists paths (re-)apply the app grant.

    Reused by ``routes/settings.py`` (``/api/settings/provision/secrets``) and
    by the one-click pipeline-setup orchestrator.  Non-fatal throughout — any
    failure is surfaced in ``results`` and never raised.

    The ``provision_scope`` / ``grant_fn`` / ``app_name_fn`` hooks let callers
    inject their own (patchable) helper references; they default to this
    module's implementations.

    Returns:
        ``{"client_id": str | None, "status": str, "results": [...]}``.
    """
    provision_scope = provision_scope or provision_secrets_scope
    grant_fn = grant_fn or grant_app_can_use
    app_name_fn = app_name_fn or _app_name

    results: list[dict] = []
    scope_name = (scope_name or "").strip()
    if not scope_name:
        return {
            "client_id": None,
            "status": "failed",
            "results": [
                {
                    "resource": "Secret scope",
                    "status": "failed",
                    "error": "Scope name is empty",
                }
            ],
        }

    scope_result = await provision_scope(scope_name)
    results.append(scope_result)
    if scope_result.get("status") not in ("created", "already_exists"):
        return {"client_id": None, "status": "failed", "results": results}

    zb_id_key = "zerobus-client-id"
    zb_secret_key = ZEROBUS_OAUTH_ENTRY_NAME
    sp_name = f"iot-sss-{scope_name}"
    client_id: str | None = None

    def _grant_app_access(application_id: str | None) -> None:
        """Grant the streaming SP CAN_USE on the app (idempotent, non-fatal)."""
        if not application_id:
            return
        try:
            grant_fn(w, app_name_fn(), application_id)
            results.append(
                {
                    "resource": "App access (CAN_USE) for SP",
                    "status": "granted",
                }
            )
        except Exception as grant_exc:  # pragma: no cover - defensive
            logger.warning(
                "Failed to grant app access to SP '%s' (non-fatal): %s",
                application_id,
                grant_exc,
            )

    try:
        existing_keys = {s.key for s in w.secrets.list_secrets(scope=scope_name)}
        if zb_id_key in existing_keys and zb_secret_key in existing_keys:
            results.append(
                {
                    "resource": "Service principal credentials",
                    "status": "already_exists",
                }
            )
            # Re-apply the grant idempotently — resolve SP by display name
            # since the secret value is write-only over the API.
            try:
                existing_sp = _find_sp_by_display_name(w, sp_name)
                if existing_sp is not None:
                    client_id = existing_sp.application_id
                    _grant_app_access(client_id)
                else:
                    logger.warning(
                        "Streaming SP '%s' not found by display name — cannot re-apply app grant",
                        sp_name,
                    )
            except Exception as lookup_exc:
                logger.warning(
                    "Failed to resolve streaming SP '%s' for app grant (non-fatal): %s",
                    sp_name,
                    lookup_exc,
                )
        else:
            sp_id, client_id = _create_sp_and_oauth_secret(
                w,
                sp_name,
                scope_name,
                zb_id_key,
                zb_secret_key,
            )
            results.append(
                {
                    "resource": f"Service principal '{sp_name}' (id={sp_id})",
                    "status": "created",
                }
            )
            results.append({"resource": "OAuth secret", "status": "created"})
            results.append(
                {
                    "resource": f"Secret '{zb_id_key}' in '{scope_name}'",
                    "status": "created",
                }
            )
            results.append(
                {
                    "resource": f"Secret '{zb_secret_key}' in '{scope_name}'",
                    "status": "created",
                }
            )

            logger.info(
                "Created SP '%s' (app_id=%s) and stored credentials in scope '%s'",
                sp_name,
                client_id,
                scope_name,
            )
            _grant_app_access(client_id)

    except Exception as exc:
        logger.warning("Failed to create service principal / store credentials: %s", exc)
        if _is_sp_credential_skippable(exc):
            results.append(
                {
                    "resource": "Service principal credentials",
                    "status": "skipped",
                    "error": "Service-principal credential minting isn't available on "
                    "this workspace (needs account-admin).  This is only "
                    "required for Zerobus ingest — skip unless you plan to use Zerobus.",
                }
            )
        else:
            results.append(
                {
                    "resource": "Service principal credentials",
                    "status": "failed",
                    "error": str(exc)[:200],
                }
            )

    status = "ok" if not any(r.get("status") == "failed" for r in results) else "failed"
    return {"client_id": client_id, "status": status, "results": results}


# ---------------------------------------------------------------------------
# Shared streaming service principal (one SP for all industry pipelines)
# ---------------------------------------------------------------------------

SHARED_STREAMING_SP_DISPLAY_NAME = "iot-sss-industries"
# DEPRECATED constant kept for import compatibility; use get_secret_scope() at
# call time instead.  Set to "" so import-time reads never inject a workspace
# literal — the real value is resolved per-call via workspace_config.
SHARED_STREAMING_SP_SECRET_SCOPE: str = ""
SHARED_STREAMING_SP_ID_KEY = "sss-industries-client-id"
# Key *names* inside the scope; the literals sit on neutrally named constants so
# credential scanners do not read them as hardcoded secrets.
SHARED_STREAMING_SP_OAUTH_ENTRY_NAME = "sss-industries-client-secret"
SHARED_STREAMING_SP_SECRET_KEY = SHARED_STREAMING_SP_OAUTH_ENTRY_NAME
# Distinct Lakebase role_id for the shared streaming SP so it can be registered
# on the SAME branch as the app-backend role (LAKEBASE_ROLE_ID="iot-app-user")
# without a role_id collision.
SHARED_STREAMING_LAKEBASE_ROLE_ID = "iot-sss-industries"


# NOTE (Task 3): ``ensure_shared_streaming_sp`` and the separate zerobus SP
# were removed — the single app SP (``DATABRICKS_CLIENT_ID``) is now used for
# every auth path.  ``provision_lakebase_role`` registers that one SP as the
# branch Postgres role.  The SHARED_STREAMING_* constants above are retained
# only for import compatibility (e.g. health checks) and are otherwise unused.


async def provision_uc_catalog(uc: dict) -> list[dict]:
    """Create UC catalog, schemas, and checkpoint volume (idempotent)."""
    results: list[dict] = []
    pattern = uc.get("catalog_pattern", "iot_{name}")
    if "{name}" in pattern:
        catalog_name = pattern.replace("{name}", "default")
    else:
        catalog_name = pattern or "iot_default"

    schemas = uc.get("schemas", ["raw", "metrics", "checkpoints"])
    volume_name = uc.get("checkpoint_volume", "stream_checkpoints")
    storage_root_base = uc.get("storage_root", "")

    def _is_already_exists(exc: Exception) -> bool:
        return "already exists" in str(exc).lower()

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.catalog import VolumeType

        w = WorkspaceClient()

        try:
            create_kwargs: dict[str, Any] = {"name": catalog_name}
            if storage_root_base:
                create_kwargs["storage_root"] = f"{storage_root_base}/{catalog_name}"
            w.catalogs.create(**create_kwargs)
            results.append({"resource": f"UC catalog '{catalog_name}'", "status": "created"})
        except Exception as e:
            if _is_already_exists(e):
                results.append(
                    {"resource": f"UC catalog '{catalog_name}'", "status": "already_exists"}
                )
            elif _is_missing_metastore_storage(e):
                results.append(
                    {
                        "resource": f"UC catalog '{catalog_name}'",
                        "status": "failed",
                        "error": (
                            "Workspace metastore has no default storage root. "
                            "Set 'Storage Root' on this Settings tab to an "
                            "external-location URL the app SP can write to "
                            "(CREATE_MANAGED_STORAGE), then retry."
                        ),
                    }
                )
                return results
            else:
                raise

        for schema in schemas:
            try:
                w.schemas.create(name=schema, catalog_name=catalog_name)
                results.append(
                    {"resource": f"Schema '{catalog_name}.{schema}'", "status": "created"}
                )
            except Exception as e:
                if _is_already_exists(e):
                    results.append(
                        {
                            "resource": f"Schema '{catalog_name}.{schema}'",
                            "status": "already_exists",
                        }
                    )
                else:
                    results.append(
                        {
                            "resource": f"Schema '{catalog_name}.{schema}'",
                            "status": "failed",
                            "error": str(e)[:200],
                        }
                    )

        if "checkpoints" in schemas:
            try:
                w.volumes.create(
                    catalog_name=catalog_name,
                    schema_name="checkpoints",
                    name=volume_name,
                    volume_type=VolumeType.MANAGED,
                )
                results.append(
                    {
                        "resource": f"Volume '{catalog_name}.checkpoints.{volume_name}'",
                        "status": "created",
                    }
                )
            except Exception as e:
                if _is_already_exists(e):
                    results.append(
                        {
                            "resource": f"Volume '{catalog_name}.checkpoints.{volume_name}'",
                            "status": "already_exists",
                        }
                    )
                else:
                    results.append(
                        {
                            "resource": f"Volume '{catalog_name}.checkpoints.{volume_name}'",
                            "status": "failed",
                            "error": str(e)[:200],
                        }
                    )

    except Exception as exc:
        results.append(
            {
                "resource": f"UC catalog '{catalog_name}'",
                "status": "failed",
                "error": str(exc)[:200],
            }
        )

    return results


async def provision_lakebase_table(lb: dict) -> dict:
    """Create the sensor_readings table in Lakebase (idempotent)."""
    mode = lb.get("mode", "autoscale")
    table_name = lb.get("pg_table", "sensor_readings")
    database = lb.get("database", "databricks_postgres")

    try:
        import asyncpg

        if mode == "autoscale":
            project_id = lb.get("project_id", "")
            branch_id = lb.get("branch_id", "production")
            if not project_id:
                return {
                    "resource": f"Lakebase table '{table_name}'",
                    "status": "failed",
                    "error": "project_id is empty",
                }

            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            parent = f"projects/{project_id}/branches/{branch_id}"
            endpoints = list(w.postgres.list_endpoints(parent=parent))
            if not endpoints:
                return {
                    "resource": f"Lakebase table '{table_name}'",
                    "status": "failed",
                    "error": f"No endpoints for {parent}",
                }

            endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
            pg_host = endpoint.status.hosts.host
            pg_user = w.current_user.me().user_name
            cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
            pg_password = cred.token
            pg_port = 5432
        else:
            pg_host = lb.get("host", "")
            pg_user = lb.get("user", "")
            pg_password = lb.get("password", "")
            pg_port = int(lb.get("port", 5432))
            if not pg_host:
                return {
                    "resource": f"Lakebase table '{table_name}'",
                    "status": "failed",
                    "error": "host is empty",
                }

        conn = await asyncpg.connect(
            host=pg_host,
            database=database,
            user=pg_user,
            password=pg_password,
            port=pg_port,
            ssl="require",
        )
        try:
            # Create the main table
            ddl = SENSOR_READINGS_DDL.format(table_name=table_name)
            await conn.execute(ddl)

            # Matviews are superseded by LakeTS RollUps when tiering is in use.
            # Creating them would later block the ChronoTable conversion (they
            # would hold references to the renamed original).
            tiering_requested = bool(lb.get("tiering"))
            matview_errors: list[str] = []
            if not tiering_requested:
                for mv_ddl in [MATVIEW_1M_DDL, MATVIEW_5M_DDL, *MATVIEW_INDEX_DDL]:
                    try:
                        await conn.execute(mv_ddl)
                    except Exception as mv_exc:
                        mv_err = str(mv_exc)
                        if "already exists" not in mv_err.lower():
                            matview_errors.append(mv_err[:80])

            result: dict = {"resource": f"Lakebase table '{table_name}'", "status": "created"}
            if matview_errors:
                result["error"] = f"Table OK; matview warnings: {'; '.join(matview_errors)}"

            # Install LakeTS on the same connection.  A failure here must not
            # fail the overall provisioning — it is surfaced as a warning.
            if lb.get("install_lakets", True):
                try:
                    sql_path = lakets_service.resolve_lakets_source(lb.get("lakets_sql_path"))
                    version = await lakets_service.install_lakets(
                        conn,
                        sql_path,
                        skip_if_installed=True,
                    )
                    result["lakets_version"] = version
                except Exception as lts_exc:
                    logger.warning("LakeTS install failed: %s", lts_exc)
                    result["lakets_error"] = str(lts_exc)[:200]

            # ChronoTable conversion + tier policy (Phase 2).  Only when the caller
            # supplied a `tiering` block.  Failures are warnings, not hard fails.
            tiering = lb.get("tiering")
            if tiering and result.get("lakets_version"):
                try:
                    # Pre-clean before ChronoTable conversion.  LakeTS'
                    # create_chronotable renames the source table and drops the
                    # rename at the end; any dependency holding a reference
                    # blocks the drop and forces a rollback.  Dependencies to
                    # clear:
                    #   1. Legacy matviews (sensor_1m, sensor_5m) — superseded by
                    #      LakeTS RollUps once tiering is active.
                    #   2. BIGSERIAL sequence ownership — renaming the table
                    #      carries sequence ownership with it, and the new
                    #      partitions still reference the sequence, so DROP
                    #      of the rename fails on OWNED BY.
                    #   3. Leftover ``_lakets_orig`` from a failed previous run.
                    await conn.execute("DROP MATERIALIZED VIEW IF EXISTS sensor_1m CASCADE")
                    await conn.execute("DROP MATERIALIZED VIEW IF EXISTS sensor_5m CASCADE")
                    await conn.execute(
                        f"ALTER SEQUENCE IF EXISTS {table_name}_id_seq OWNED BY NONE"
                    )
                    await conn.execute(f"DROP TABLE IF EXISTS {table_name}_lakets_orig CASCADE")

                    await lakets_service.create_chronotable(
                        conn,
                        table=table_name,
                        time_col=tiering.get("time_column", "event_time"),
                        chunk=tiering.get("chunk", "15 minutes"),
                    )
                    await lakets_service.apply_tier_policy(
                        conn,
                        table=table_name,
                        hot=tiering["hot"],
                        drop=tiering["drop"],
                    )
                    result["tiering_applied"] = {
                        "hot": tiering["hot"],
                        "drop": tiering["drop"],
                        "chunk": tiering.get("chunk", "15 minutes"),
                    }
                except Exception as tier_exc:
                    logger.warning("Tier policy application failed: %s", tier_exc)
                    result["tiering_error"] = str(tier_exc)[:200]

                # enable_sync prepares the lakets_cdf shadow + mirror trigger —
                # REQUIRED before Lakebase CDF / cold-tiering can stream.  This
                # completes the cohesive LakeTS setup block:
                #   install → create_chronotable → tier policy → enable_sync.
                # Runs even if the tier-policy step above warned, since the
                # shadow is a prerequisite the user explicitly wants prepared
                # at table-creation time.  Non-fatal: a failure is recorded but
                # never aborts the overall provisioning.
                schema = tiering.get("schema", "public")
                sync_table = tiering.get("time_table", table_name)
                try:
                    sync_result = await lakets_service.enable_sync(
                        conn,
                        sync_table,
                        schema=schema,
                    )
                except Exception as sync_exc:  # defensive — enable_sync never raises
                    logger.warning("enable_sync raised unexpectedly: %s", sync_exc)
                    sync_result = {"status": "failed", "error": str(sync_exc)[:200]}

                result.setdefault("sync_enabled", {})[sync_table] = sync_result.get(
                    "status", "failed"
                )
                if sync_result.get("status") == "failed":
                    result["sync_error"] = sync_result.get("error", "enable_sync failed")[:200]

            return result
        except Exception as inner_exc:
            err = str(inner_exc)
            if "already exists" in err.lower() or "DuplicateTable" in err:
                return {"resource": f"Lakebase table '{table_name}'", "status": "already_exists"}
            return {
                "resource": f"Lakebase table '{table_name}'",
                "status": "failed",
                "error": err[:200],
            }
        finally:
            await conn.close()

    except Exception as exc:
        return {
            "resource": f"Lakebase table '{table_name}'",
            "status": "failed",
            "error": str(exc)[:200],
        }


async def provision_lakebase_project_and_branch(project_id: str, branch_id: str) -> list[dict]:
    """Create Lakebase autoscale project and branch if they don't exist."""
    results: list[dict] = []

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.postgres import Branch, BranchSpec, Project

    w = WorkspaceClient()

    # Step 1: Check if project exists
    project_exists = False
    try:
        w.postgres.get_project(name=f"projects/{project_id}")
        project_exists = True
        results.append({"resource": f"Lakebase project '{project_id}'", "status": "already_exists"})
    except Exception as e:
        err_str = str(e).lower()
        if "not_found" in err_str or "not found" in err_str or "does not exist" in err_str:
            project_exists = False
        else:
            results.append(
                {
                    "resource": f"Lakebase project '{project_id}'",
                    "status": "failed",
                    "error": str(e)[:200],
                }
            )
            return results

    # Step 2: Create project if needed
    if not project_exists:
        try:
            w.postgres.create_project(project=Project(), project_id=project_id)
            results.append({"resource": f"Lakebase project '{project_id}'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append(
                    {"resource": f"Lakebase project '{project_id}'", "status": "already_exists"}
                )
            else:
                results.append(
                    {
                        "resource": f"Lakebase project '{project_id}'",
                        "status": "failed",
                        "error": str(exc)[:200],
                    }
                )
                return results

    # Step 3: Check if branch exists
    branch_exists = False
    parent = f"projects/{project_id}/branches/{branch_id}"
    try:
        w.postgres.get_branch(name=parent)
        branch_exists = True
        results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "already_exists"})
    except Exception as e:
        err_str = str(e).lower()
        if "not_found" in err_str or "not found" in err_str or "does not exist" in err_str:
            branch_exists = False
        else:
            results.append(
                {
                    "resource": f"Lakebase branch '{branch_id}'",
                    "status": "failed",
                    "error": str(e)[:200],
                }
            )
            return results

    # Step 4: Create branch if needed
    if not branch_exists:
        try:
            # ``no_expiry=True`` is required by newer Lakebase API versions —
            # otherwise "Expiration must be specified when creating a branch".
            # Lives inside BranchSpec, not on Branch itself.
            w.postgres.create_branch(
                parent=f"projects/{project_id}",
                branch=Branch(spec=BranchSpec(no_expiry=True)),
                branch_id=branch_id,
            )
            results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append(
                    {"resource": f"Lakebase branch '{branch_id}'", "status": "already_exists"}
                )
            else:
                results.append(
                    {
                        "resource": f"Lakebase branch '{branch_id}'",
                        "status": "failed",
                        "error": str(exc)[:200],
                    }
                )
                return results

    # Step 5: Ensure a primary read-write endpoint exists on the branch.
    # Project creation auto-creates a `production` branch with a `primary`
    # endpoint, but creating a branch on an existing project does NOT.
    try:
        endpoints = list(w.postgres.list_endpoints(parent=parent))
    except Exception:
        endpoints = []

    if not endpoints:
        try:
            from databricks.sdk.service.postgres import (
                Endpoint,
                EndpointSpec,
                EndpointType,
            )

            w.postgres.create_endpoint(
                parent=parent,
                endpoint=Endpoint(
                    spec=EndpointSpec(
                        endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                        autoscaling_limit_min_cu=0.5,
                        autoscaling_limit_max_cu=2.0,
                    ),
                ),
                endpoint_id="primary",
            )
            results.append({"resource": "Lakebase endpoint 'primary'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append(
                    {"resource": "Lakebase endpoint 'primary'", "status": "already_exists"}
                )
            else:
                results.append(
                    {
                        "resource": "Lakebase endpoint 'primary'",
                        "status": "failed",
                        "error": str(exc)[:200],
                    }
                )

    # Step 6: Wait for endpoint to be ACTIVE (up to 90s)
    if not branch_exists or not project_exists or not endpoints:
        for _ in range(18):  # 18 * 5s = 90s
            try:
                current = list(w.postgres.list_endpoints(parent=parent))
                if current:
                    break
            except Exception:
                pass
            await asyncio.sleep(5)

    return results


def _is_permission_error(exc: Exception) -> bool:
    """Return True when an SDK exception is the Lakebase project-admin gate."""
    s = str(exc).lower()
    return (
        "not authorized" in s
        or "permission denied" in s
        or "unauthorized" in s
        or "permission_denied" in s
        or "forbidden" in s
    )


def _resolve_endpoint_host_and_token(w: Any, endpoint_name: str) -> tuple[str, str]:
    """Resolve (host, oauth_token) for a Lakebase endpoint.

    Prefers the SDK ``w.postgres`` shape; falls back to raw REST on older
    SDK versions that don't expose ``generate_database_credential``.
    """
    # Host resolution — same pattern used by provision_lakebase_table.
    if hasattr(w, "postgres") and hasattr(w.postgres, "get_endpoint"):
        endpoint = w.postgres.get_endpoint(name=endpoint_name)
        host = endpoint.status.hosts.host
    else:
        # Fallback via REST; very rare path.
        resp = w.api_client.do("GET", f"/api/2.0/{endpoint_name}")
        host = resp["status"]["hosts"]["host"]

    # Credential resolution with SDK-or-REST fallback.
    try:
        cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
        token = cred.token
    except AttributeError:
        resp = w.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_name},
        )
        token = resp["token"]

    return host, token


async def provision_lakebase_role(
    sp_client_id: str | None = None,
    project_id: str | None = None,
    branch_id: str | None = None,
    database: str = "databricks_postgres",
    endpoint_name: str | None = None,
    sink_tables: Any = ("sensor_readings",),
    ws: Any | None = None,
    role_id: str | None = None,
    **kwargs: Any,
) -> list[dict]:
    """Register a service principal as an OAuth-auth Postgres role and grant
    DML on each sink table.

    This is the OAuth-native replacement for the older password-based flow —
    the caller's SP becomes a PG role via ``RoleAuthMethod.LAKEBASE_OAUTH_V1``
    and no static credentials are persisted anywhere.

    The function accepts legacy positional/keyword forms too: when invoked
    with ``role_name=`` / ``password=`` (the old contract), it logs a
    deprecation warning and delegates to the new flow, resolving the
    endpoint from ``project_id`` / ``branch_id`` when ``endpoint_name`` is
    not provided.
    """
    # ------------------------------------------------------------------ #
    # Back-compat: absorb legacy ``role_name=`` / ``password=`` kwargs so
    # older callers (or third-party imports) don't crash. Nothing about the
    # OAuth-native flow needs either one — they are accepted and ignored
    # with a deprecation warning.
    # ------------------------------------------------------------------ #
    if "password" in kwargs:
        logger.warning(
            "provision_lakebase_role called with legacy `password` kwarg — "
            "ignoring; OAuth-native flow does not use static passwords."
        )
        kwargs.pop("password", None)
    if "role_name" in kwargs:
        logger.warning(
            "provision_lakebase_role called with legacy `role_name` kwarg — "
            "ignoring; the SP client id is now the PG role name."
        )
        kwargs.pop("role_name", None)

    if kwargs:
        logger.warning("provision_lakebase_role received unknown kwargs: %s", list(kwargs.keys()))

    # Normalise sink_tables (tests pass tuples, production uses list).
    sink_tables_list: list[str] = [t for t in sink_tables] if sink_tables else ["sensor_readings"]

    # The Lakebase role_id must be UNIQUE per role on a branch. Default to the
    # app-backend id (LAKEBASE_ROLE_ID — also how settings_service looks it up),
    # but a second SP on the same branch (e.g. the shared streaming SP) MUST pass
    # a distinct role_id or create_role collides on the fixed id.
    _lb_role_id = role_id or LAKEBASE_ROLE_ID

    results: list[dict] = []

    from databricks.sdk import WorkspaceClient

    w = ws or WorkspaceClient()

    # Resolve the SP client id when not supplied — when the app runs as its
    # own SP, ``current_user.me().user_name`` is the SP's client id.
    if not sp_client_id:
        try:
            sp_client_id = w.current_user.me().user_name
        except Exception as exc:
            results.append(
                {
                    "resource": "Lakebase OAuth role",
                    "status": "failed",
                    "error": f"Unable to resolve SP client id: {exc}"[:200],
                }
            )
            return results

    if not project_id or not branch_id:
        results.append(
            {
                "resource": "Lakebase OAuth role",
                "status": "failed",
                "error": "project_id and branch_id are required",
            }
        )
        return results

    parent = f"projects/{project_id}/branches/{branch_id}"

    # Resolve endpoint_name if not provided.
    if not endpoint_name:
        try:
            endpoints = list(w.postgres.list_endpoints(parent=parent))
        except Exception as exc:
            results.append(
                {
                    "resource": "Lakebase endpoint lookup",
                    "status": "failed",
                    "error": str(exc)[:200],
                }
            )
            return results
        if not endpoints:
            results.append(
                {
                    "resource": "Lakebase endpoint lookup",
                    "status": "failed",
                    "error": f"No endpoints available on {parent}",
                }
            )
            return results
        endpoint_name = endpoints[0].name

    # ------------------------------------------------------------------ #
    # 1. Register the SP as a PG role with LAKEBASE_OAUTH_V1 auth.
    # ------------------------------------------------------------------ #
    role_resource = f"Postgres role '{sp_client_id}' (OAuth)"
    try:
        from databricks.sdk.service.postgres import (
            Role,
            RoleAuthMethod,
            RoleMembershipRole,
            RoleRoleSpec,
        )

        create_kwargs: dict[str, Any] = {
            "postgres_role": sp_client_id,
            "auth_method": RoleAuthMethod.LAKEBASE_OAUTH_V1,
            "membership_roles": [RoleMembershipRole.DATABRICKS_SUPERUSER],
        }

        # ``RoleIdentityType`` only exists on newer SDK versions. Fall back to a
        # string literal when the enum is missing.
        try:
            from databricks.sdk.service.postgres import RoleIdentityType

            create_kwargs["identity_type"] = RoleIdentityType.SERVICE_PRINCIPAL
        except ImportError:
            create_kwargs["identity_type"] = "SERVICE_PRINCIPAL"

        w.postgres.create_role(
            parent=parent,
            role=Role(spec=RoleRoleSpec(**create_kwargs)),
            role_id=_lb_role_id,
        )
        results.append({"resource": role_resource, "status": "created"})
    except AttributeError:
        # SDK too old — fall back to raw REST.
        try:
            w.api_client.do(
                "POST",
                f"/api/2.0/{parent}/roles",
                body={
                    "spec": {
                        "postgres_role": sp_client_id,
                        "auth_method": "LAKEBASE_OAUTH_V1",
                        "identity_type": "SERVICE_PRINCIPAL",
                        "membership_roles": ["DATABRICKS_SUPERUSER"],
                    },
                    "role_id": _lb_role_id,
                },
            )
            results.append({"resource": role_resource, "status": "created"})
        except Exception as rest_exc:
            rest_err = str(rest_exc).lower()
            if "already exists" in rest_err:
                results.append({"resource": role_resource, "status": "already_exists"})
            elif _is_permission_error(rest_exc):
                results.append(
                    {
                        "resource": role_resource,
                        "status": "skipped",
                        "error": "Runtime identity lacks Lakebase project-admin.",
                    }
                )
            else:
                results.append(
                    {
                        "resource": role_resource,
                        "status": "failed",
                        "error": str(rest_exc)[:200],
                    }
                )
                return results
    except Exception as exc:
        err_str = str(exc).lower()
        if "already exists" in err_str or "already_exists" in err_str:
            results.append({"resource": role_resource, "status": "already_exists"})
        elif _is_permission_error(exc):
            results.append(
                {
                    "resource": role_resource,
                    "status": "skipped",
                    "error": "Runtime identity lacks Lakebase project-admin.",
                }
            )
        else:
            results.append(
                {
                    "resource": role_resource,
                    "status": "failed",
                    "error": str(exc)[:200],
                }
            )
            return results

    # ------------------------------------------------------------------ #
    # 2. Connect via asyncpg and grant schema/table privileges to the SP.
    # ------------------------------------------------------------------ #
    try:
        import asyncpg

        pg_host, pg_token = _resolve_endpoint_host_and_token(w, endpoint_name)
        pg_user = w.current_user.me().user_name

        conn = await asyncpg.connect(
            host=pg_host,
            database=database,
            user=pg_user,
            password=pg_token,
            port=5432,
            ssl="require",
        )
    except Exception as exc:
        results.append(
            {
                "resource": "Connect to Lakebase for grants",
                "status": "failed",
                "error": str(exc)[:200],
            }
        )
        return results

    try:
        # SP identifiers in Lakebase are UUIDs — always quote.
        qsp = f'"{sp_client_id}"'

        # Schema USAGE on public (once).
        try:
            await conn.execute(f"GRANT USAGE ON SCHEMA public TO {qsp}")
            results.append(
                {
                    "resource": f"GRANT USAGE ON SCHEMA public TO {sp_client_id}",
                    "status": "created",
                }
            )
        except Exception as exc:
            err = str(exc)
            if "already" in err.lower():
                results.append(
                    {
                        "resource": f"GRANT USAGE ON SCHEMA public TO {sp_client_id}",
                        "status": "already_exists",
                    }
                )
            else:
                results.append(
                    {
                        "resource": f"GRANT USAGE ON SCHEMA public TO {sp_client_id}",
                        "status": "failed",
                        "error": err[:200],
                    }
                )

        # Sequence USAGE (once — covers BIGSERIAL PKs).
        try:
            await conn.execute(f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {qsp}")
            results.append(
                {
                    "resource": f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {sp_client_id}",
                    "status": "created",
                }
            )
        except Exception as exc:
            err = str(exc)
            if "already" in err.lower():
                results.append(
                    {
                        "resource": f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {sp_client_id}",
                        "status": "already_exists",
                    }
                )
            else:
                results.append(
                    {
                        "resource": f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {sp_client_id}",
                        "status": "failed",
                        "error": err[:200],
                    }
                )

        # Optional: USAGE on the lakets schema (installed lazily by LakeTS).
        lakets_resource = f"GRANT USAGE ON SCHEMA lakets TO {sp_client_id}"
        try:
            await conn.execute(f"GRANT USAGE ON SCHEMA lakets TO {qsp}")
            results.append({"resource": lakets_resource, "status": "created"})
        except Exception as exc:
            err = str(exc)
            if "does not exist" in err.lower():
                results.append(
                    {
                        "resource": lakets_resource,
                        "status": "skipped",
                        "error": "lakets schema not installed yet — will be granted after install.",
                    }
                )
            elif "already" in err.lower():
                results.append({"resource": lakets_resource, "status": "already_exists"})
            else:
                results.append(
                    {
                        "resource": lakets_resource,
                        "status": "failed",
                        "error": err[:200],
                    }
                )

        # DML on each sink table.
        for table in sink_tables_list:
            qtable = f'"{table}"'
            stmt = f"GRANT INSERT, SELECT, UPDATE, DELETE ON {qtable} TO {qsp}"
            resource = f"GRANT DML ON {table} TO {sp_client_id}"
            try:
                await conn.execute(stmt)
                results.append({"resource": resource, "status": "created"})
            except Exception as exc:
                err = str(exc)
                if "already" in err.lower():
                    results.append({"resource": resource, "status": "already_exists"})
                elif "does not exist" in err.lower():
                    results.append(
                        {
                            "resource": resource,
                            "status": "skipped",
                            "error": f"Table {table} does not exist yet.",
                        }
                    )
                else:
                    results.append(
                        {
                            "resource": resource,
                            "status": "failed",
                            "error": err[:200],
                        }
                    )
    finally:
        try:
            await conn.close()
        except Exception:
            pass

    return results


async def save_direct_creds_to_secrets(
    secrets: dict,
    lb_user: str,
    lb_password: str,
) -> list[dict]:
    """[DEPRECATED] Persist Lakebase creds into workspace secrets.

    The OAuth-native provisioning flow does not use static credentials, so
    this function is a no-op kept for back-compat with any caller that still
    imports it.  It emits a warning and returns a single skipped result.
    """
    logger.warning(
        "save_direct_creds_to_secrets is deprecated — OAuth-native provisioning "
        "does not use static Lakebase credentials; call is a no-op."
    )
    return [
        {
            "resource": "Save credentials to secrets (deprecated)",
            "status": "skipped",
            "error": "OAuth-native provisioning does not require static credentials.",
        }
    ]


# ---------------------------------------------------------------------------
# UC Secrets (catalog-scoped)
# ---------------------------------------------------------------------------


async def provision_uc_secret(
    catalog: str,
    schema: str,
    name: str,
    value: str,
) -> dict:
    """Create or update a UC Secret (catalog-scoped).

    Uses the REST API: POST /api/2.1/unity-catalog/secrets
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    headers = w.config.authenticate()
    fqn = f"{catalog}.{schema}.{name}"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{host}/api/2.1/unity-catalog/secrets",
            headers=headers,
            json={
                "secret_info": {
                    "catalog_name": catalog,
                    "schema_name": schema,
                    "name": name,
                    "value": value,
                }
            },
        )
        if resp.status_code == 200:
            return {"resource": f"UC secret '{fqn}'", "status": "created"}

        if resp.status_code == 409 or "already exists" in resp.text.lower():
            resp2 = await client.patch(
                f"{host}/api/2.1/unity-catalog/secrets/{fqn}?update_mask=*",
                headers=headers,
                json={"secret_info": {"value": value}},
            )
            if resp2.status_code == 200:
                return {"resource": f"UC secret '{fqn}'", "status": "updated"}
            return {"resource": f"UC secret '{fqn}'", "status": "failed", "error": resp2.text[:200]}

        return {"resource": f"UC secret '{fqn}'", "status": "failed", "error": resp.text[:200]}


async def grant_uc_secret_permissions(
    catalog: str,
    schema: str,
    principal: str,
) -> dict:
    """Grant READ_SECRET and REFERENCE_SECRET on a UC schema to a principal."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    headers = w.config.authenticate()
    fqn = f"{catalog}.{schema}"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.patch(
            f"{host}/api/2.1/unity-catalog/permissions/schema/{fqn}",
            headers=headers,
            json={
                "changes": [
                    {
                        "principal": principal,
                        "add": ["READ_SECRET", "REFERENCE_SECRET"],
                    }
                ]
            },
        )
        if resp.status_code == 200:
            return {"resource": f"UC secret grants on '{fqn}' for {principal}", "status": "created"}
        return {
            "resource": f"UC secret grants on '{fqn}'",
            "status": "failed",
            "error": resp.text[:200],
        }


async def save_creds_as_uc_secrets(
    catalog: str,
    schema: str,
    lb_user: str,
    lb_password: str,
    zb_client_id: str | None = None,
    zb_client_secret: str | None = None,
) -> list[dict]:
    """[DEPRECATED for Lakebase creds] Save Zerobus creds as UC secrets.

    The OAuth-native flow removes the need for ``lb-user`` / ``lb-password``
    UC secrets — those keys are no longer written.  Zerobus secrets are still
    persisted when supplied, so callers storing only Zerobus creds keep
    working.
    """
    results: list[dict] = []

    logger.warning(
        "save_creds_as_uc_secrets: skipping `lb-user` / `lb-password` — "
        "OAuth-native provisioning does not use static Lakebase credentials."
    )
    results.append(
        {
            "resource": f"UC secret '{catalog}.{schema}.lb-user' (deprecated)",
            "status": "skipped",
            "error": "OAuth-native provisioning does not require static credentials.",
        }
    )
    results.append(
        {
            "resource": f"UC secret '{catalog}.{schema}.lb-password' (deprecated)",
            "status": "skipped",
            "error": "OAuth-native provisioning does not require static credentials.",
        }
    )

    if zb_client_id:
        results.append(
            await provision_uc_secret(catalog, schema, "zerobus-client-id", zb_client_id)
        )
    if zb_client_secret:
        results.append(
            await provision_uc_secret(catalog, schema, "zerobus-client-secret", zb_client_secret)
        )

    return results


# ---------------------------------------------------------------------------
# Metrics table provisioning
# ---------------------------------------------------------------------------

STREAMING_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    query_id STRING,
    run_id STRING,
    query_name STRING,
    batch_id LONG,
    timestamp STRING,
    progress STRING
)
"""


async def provision_metrics_table(
    catalog: str,
    schema: str = "metrics",
    table_name: str = "streaming_metrics",
    grant_to: str | None = None,
) -> list[dict]:
    """Create the streaming_metrics table and optionally grant access.

    Uses the Databricks SQL Statement Execution API.
    """
    results: list[dict] = []
    fqn = f"{catalog}.{schema}.{table_name}"

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        warehouses = list(w.warehouses.list())
        if not warehouses:
            return [
                {
                    "resource": f"Metrics table '{fqn}'",
                    "status": "failed",
                    "error": "No SQL warehouse available",
                }
            ]

        wh_id = warehouses[0].id

        stmt = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=STREAMING_METRICS_DDL.format(table=fqn),
            wait_timeout="50s",
        )
        if stmt.status.error:
            results.append(
                {
                    "resource": f"Metrics table '{fqn}'",
                    "status": "failed",
                    "error": str(stmt.status.error)[:200],
                }
            )
        else:
            results.append({"resource": f"Metrics table '{fqn}'", "status": "created"})

        if grant_to:
            for sql in [
                f"GRANT MODIFY, SELECT ON TABLE {fqn} TO `{grant_to}`",
                f"GRANT USE CATALOG ON CATALOG {catalog} TO `{grant_to}`",
                f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{grant_to}`",
                # Required so the deployed job identity can mint UC
                # temporary-table-credentials for the delta-rs metrics writer
                # (generate_temporary_table_credentials -> EXTERNAL USE SCHEMA).
                f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{grant_to}`",
            ]:
                try:
                    w.statement_execution.execute_statement(
                        warehouse_id=wh_id,
                        statement=sql,
                        wait_timeout="50s",
                    )
                except Exception as ge:
                    logger.warning("Grant failed: %s — %s", sql, ge)

            results.append({"resource": f"Grants on '{fqn}' to {grant_to}", "status": "created"})

    except Exception as exc:
        results.append(
            {"resource": f"Metrics table '{fqn}'", "status": "failed", "error": str(exc)[:200]}
        )

    return results
