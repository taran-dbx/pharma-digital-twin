"""Provisioning endpoints — UC catalogs/schemas and Lakebase PG tables."""

from __future__ import annotations

import json
import logging
import os
import re
import traceback

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state
from iot_twin.models.sql_ddl import (
    MATVIEW_1M_DDL,
    MATVIEW_5M_DDL,
    MATVIEW_INDEX_DDL,
    SENSOR_READINGS_DDL,
)
from iot_twin.services import provisioning_service
from iot_twin.services.pipelines.pipeline_service import track_resource

logger = logging.getLogger("iot_digital_twin.routes.provisioning")

router = APIRouter()


_MISSING_STORAGE_ROOT_HINT = (
    "Catalog provisioning failed: this workspace's metastore has no default "
    "storage root configured. Open Settings → Unity Catalog and set "
    "'Storage root' to an external location URL (e.g. s3://my-bucket/uc-root) "
    "that the app's service principal has CREATE_MANAGED_STORAGE on, then "
    "retry the pipeline. The catalog will be created at "
    "<storage_root>/<catalog_name>."
)


def _is_missing_metastore_storage(exc: Exception) -> bool:
    """Detect the 'metastore has no default storage root' UC error.

    UC raises this when ``CREATE CATALOG`` is issued without an explicit
    ``storage_root`` and the metastore's own default location is missing
    or invalid. The message we match against is the one produced by the
    Catalogs API today; we keep the matcher loose so future wording
    changes still trigger the user-friendly hint.
    """
    msg = str(exc).lower()
    return "metastore storage root" in msg or "default storage" in msg or "managed location" in msg


async def _load_user_settings(state: AppState, user_email: str) -> dict:
    """Return the calling user's saved settings JSON, or ``{}``.

    Used to resolve fallback config (e.g. UC ``storage_root``) when the
    workspace metastore has no default managed-storage location.
    """
    if not user_email or not state.db_available or not state.db:
        return {}
    try:
        row = await state.db.fetchrow(
            "SELECT settings FROM user_settings WHERE user_email = $1", user_email
        )
    except Exception as exc:
        logger.warning("Failed to load user_settings for %s: %s", user_email, exc)
        return {}
    if not row:
        return {}
    raw = row["settings"]
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw) if raw is not None else {}
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def _resolve_uc_storage_root(
    state: AppState,
    user_email: str,
    body_value: str | None = None,
) -> str:
    """Resolve the UC catalog ``storage_root`` to use for catalog creation.

    Resolution order (first non-empty wins):
        1. Explicit ``storage_root`` in the request body.
        2. ``uc.storage_root`` from the user's saved global settings.
        3. ``UC_STORAGE_ROOT`` environment variable.

    Returns ``""`` if none are configured, in which case the caller falls
    back to the metastore's default managed-storage root (which may not
    exist on workspaces without one configured).
    """
    if body_value and body_value.strip():
        return body_value.strip().rstrip("/")
    settings = await _load_user_settings(state, user_email)
    saved = ((settings.get("uc") or {}).get("storage_root") or "").strip()
    if saved:
        return saved.rstrip("/")
    env_root = (os.getenv("UC_STORAGE_ROOT") or "").strip()
    return env_root.rstrip("/") if env_root else ""


async def _load_pipeline_sink_config(state: AppState, pipeline_id: str | None) -> dict:
    """Return the pipeline's ``sink_config`` dict, or ``{}`` if unavailable.

    Handles missing pipeline_id, unavailable DB, missing row, malformed JSON,
    and non-dict payloads so callers can treat the result as a plain dict.
    """
    if not pipeline_id or not state.db_available or not state.db:
        return {}
    try:
        row = await state.db.fetchrow(
            "SELECT sink_config FROM pipelines WHERE id = $1::uuid", pipeline_id
        )
    except Exception as exc:
        logger.warning("Failed to load sink_config for pipeline %s: %s", pipeline_id, exc)
        return {}
    if not row:
        return {}
    raw = row["sink_config"]
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_valid_tiering(sink_config: dict) -> dict | None:
    """Return a well-formed tiering block or None.

    A valid tiering block is a dict with non-empty ``hot`` and ``drop`` keys.
    ``chunk`` is optional — the provisioning service applies a default when
    absent. Any other shape (missing keys, wrong types, non-dict) yields None
    so the caller skips LakeTS tiering and falls back to default behaviour.
    """
    tiering = sink_config.get("tiering") if isinstance(sink_config, dict) else None
    if not isinstance(tiering, dict):
        return None
    hot = tiering.get("hot")
    drop = tiering.get("drop")
    if not isinstance(hot, str) or not hot.strip():
        return None
    if not isinstance(drop, str) or not drop.strip():
        return None
    clean: dict = {"hot": hot, "drop": drop}
    chunk = tiering.get("chunk")
    if isinstance(chunk, str) and chunk.strip():
        clean["chunk"] = chunk
    return clean


def _resolve_workspace_id(w) -> int | None:
    """Return the current workspace id, or ``None`` if undeterminable.

    Preferred source is the SDK's ``WorkspaceClient.get_workspace_id()``.
    Falls back to the ``DATABRICKS_WORKSPACE_ID`` env var when the SDK
    method is unavailable (older SDKs) or raises. Returns ``None`` when
    neither yields a usable integer so the caller can skip binding.
    """
    getter = getattr(w, "get_workspace_id", None)
    if callable(getter):
        try:
            ws_id = getter()
            if ws_id is not None:
                return int(ws_id)
        except Exception as exc:  # SDK call failed — fall through to env
            logger.warning("get_workspace_id() failed: %s", exc)
    env_id = (os.getenv("DATABRICKS_WORKSPACE_ID") or "").strip()
    if env_id:
        try:
            return int(env_id)
        except ValueError:
            logger.warning("DATABRICKS_WORKSPACE_ID is not an integer: %r", env_id)
    return None


def _bind_catalog_to_workspace(w, catalog_name: str) -> dict:
    """Bind ``catalog_name`` READ_WRITE to the current workspace (idempotent).

    A freshly-created catalog is not guaranteed to be accessible from the
    workspace that created it depending on metastore isolation defaults;
    this caused a production failure where a consumer's checkpoint Volume
    catalog was reported "not accessible in current workspace". We use
    ``add`` (merge) semantics so existing bindings are preserved.

    Always non-fatal: any failure is logged and reported as ``skipped`` so
    provisioning continues. Returns a provisioned-result entry describing
    the outcome (``bound`` | ``skipped``).
    """
    resource = f"Workspace binding for catalog {catalog_name}"
    workspace_id = _resolve_workspace_id(w)
    if workspace_id is None:
        logger.warning(
            "Skipping workspace binding for catalog '%s': workspace id unavailable "
            "(no get_workspace_id() and no DATABRICKS_WORKSPACE_ID)",
            catalog_name,
        )
        return {"resource": resource, "status": "skipped", "reason": "workspace_id_unavailable"}
    try:
        from databricks.sdk.service.catalog import (
            WorkspaceBinding,
            WorkspaceBindingBindingType,
        )

        w.workspace_bindings.update_bindings(
            securable_type="catalog",
            securable_name=catalog_name,
            add=[
                WorkspaceBinding(
                    workspace_id=workspace_id,
                    binding_type=WorkspaceBindingBindingType.BINDING_TYPE_READ_WRITE,
                )
            ],
        )
        logger.info("Bound catalog '%s' READ_WRITE to workspace %s", catalog_name, workspace_id)
        return {"resource": resource, "status": "bound", "workspace_id": workspace_id}
    except Exception as exc:
        logger.warning(
            "Failed to bind catalog '%s' to workspace %s (non-fatal): %s",
            catalog_name,
            workspace_id,
            exc,
        )
        return {"resource": resource, "status": "skipped", "reason": str(exc)}


def _grant_zerobus_sp(w, catalog_name: str, schemas: list[str], sp_app_id: str) -> list[dict]:
    """Grant the Zerobus service principal UC privileges (idempotent).

    The Zerobus Ingest client authenticates as a service principal identified
    by its ``application_id`` (the OAuth client UUID). For it to write to the
    per-pipeline Delta table it needs USE_CATALOG on the catalog and
    USE_SCHEMA/MODIFY/SELECT/CREATE_TABLE on the schemas. Granting at the
    schema level cascades to every table the client creates underneath.

    Critical: the UC permissions REST API expects the securable *type* as a
    lowercase string token (``"catalog"`` / ``"schema"``) interpolated into
    the request path. Passing the SDK ``SecurableType`` enum stringifies to
    ``"SecurableType.SCHEMA"`` and produces a malformed request that throws —
    the original bug, which was then swallowed by a broad ``except`` so the
    grant silently never applied. We pass the lowercase string explicitly.

    Each grant's outcome is captured in the returned list (``created`` |
    ``failed`` with an ``error``) so the caller can surface it in the response.
    Failures are logged at error level but never raised: a missing grant is
    reported, not fatal to the rest of provisioning.
    """
    results: list[dict] = []
    try:
        from databricks.sdk.service.catalog import PermissionsChange, Privilege
    except ImportError as exc:
        logger.error("SDK grant enums unavailable; cannot grant Zerobus SP: %s", exc)
        return [
            {
                "resource": f"Zerobus SP grants on {catalog_name}",
                "principal": sp_app_id,
                "status": "failed",
                "error": f"SDK grant enums unavailable: {exc}",
            }
        ]

    def _apply(securable_type: str, full_name: str, privileges: list) -> None:
        try:
            w.grants.update(
                securable_type=securable_type,
                full_name=full_name,
                changes=[PermissionsChange(principal=sp_app_id, add=list(privileges))],
            )
            results.append(
                {
                    "resource": f"{securable_type}:{full_name}",
                    "principal": sp_app_id,
                    "privileges": [p.value for p in privileges],
                    "status": "created",
                }
            )
        except Exception as grant_exc:
            logger.error(
                "Zerobus SP grant on %s '%s' for principal %s FAILED (non-fatal): %s",
                securable_type,
                full_name,
                sp_app_id,
                grant_exc,
            )
            results.append(
                {
                    "resource": f"{securable_type}:{full_name}",
                    "principal": sp_app_id,
                    "privileges": [p.value for p in privileges],
                    "status": "failed",
                    "error": str(grant_exc),
                }
            )

    # Catalog first so USE_CATALOG exists before schema-level access is used.
    _apply("catalog", catalog_name, [Privilege.USE_CATALOG])
    for schema in schemas:
        _apply(
            "schema",
            f"{catalog_name}.{schema}",
            [
                Privilege.USE_SCHEMA,
                Privilege.MODIFY,
                Privilege.SELECT,
                Privilege.CREATE_TABLE,
            ],
        )
    return results


def _extract_sse_metrics(sink_config: dict) -> list[str]:
    """Return the list of SSE metrics from sink_config, or ``[]`` if malformed."""
    if not isinstance(sink_config, dict):
        return []
    metrics = sink_config.get("sse_metrics")
    if not isinstance(metrics, list):
        return []
    return [m for m in metrics if isinstance(m, str)]


@router.post("/api/provision/uc")
async def provision_uc(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Create a Unity Catalog catalog and schema (idempotent).

    Expects ``{"catalog": "...", "schema": "..."}``.
    Skips creation silently when resources already exist so the caller
    can safely re-run provisioning without hitting 409 errors.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    catalog_name = (body.get("catalog") or "").strip()
    schema_name = (body.get("schema") or "").strip()

    if not catalog_name or not schema_name:
        return JSONResponse(
            status_code=422,
            content={"error": "Both 'catalog' and 'schema' are required"},
        )

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        def _is_already_exists(exc: Exception) -> bool:
            """Check if exception indicates resource already exists."""
            try:
                from databricks.sdk.errors import AlreadyExists

                if isinstance(exc, AlreadyExists):
                    return True
            except ImportError:
                pass
            return "already exists" in str(exc).lower()

        catalog_created = False
        try:
            storage_root = await _resolve_uc_storage_root(
                state, user_email, body.get("storage_root")
            )
            create_kwargs = {"name": catalog_name}
            if storage_root:
                create_kwargs["storage_root"] = f"{storage_root}/{catalog_name}"
            w.catalogs.create(**create_kwargs)
            catalog_created = True
        except Exception as e:
            if _is_already_exists(e):
                logger.info("Catalog '%s' already exists — skipping", catalog_name)
            elif _is_missing_metastore_storage(e):
                return JSONResponse(
                    status_code=409,
                    content={"error": _MISSING_STORAGE_ROOT_HINT},
                )
            else:
                raise

        schema_created = False
        try:
            w.schemas.create(name=schema_name, catalog_name=catalog_name)
            schema_created = True
        except Exception as e:
            if _is_already_exists(e):
                logger.info("Schema '%s.%s' already exists — skipping", catalog_name, schema_name)
            else:
                raise

        status = "created" if (catalog_created or schema_created) else "already_exists"

        pipeline_id = body.get("pipeline_id")
        if pipeline_id:
            await track_resource(
                state.db, state.db_available, pipeline_id, "uc_catalog", catalog_name
            )
            await track_resource(
                state.db, state.db_available, pipeline_id, "uc_schema", schema_name
            )

        return {"catalog": catalog_name, "schema": schema_name, "status": status}

    except Exception as exc:
        logger.error("UC provisioning failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/provision/pipeline-catalog")
async def provision_pipeline_catalog(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Provision a dedicated UC catalog for a pipeline.

    Creates:
      <catalog>/raw/           -- schema for ingested data
      <catalog>/metrics/       -- schema for streaming metrics
      <catalog>/checkpoints/   -- schema + managed volume for SSS checkpoints

    Grants USE CATALOG, USE SCHEMA, MODIFY, SELECT to the Zerobus service
    principal so that Zerobus Ingest and the streaming job can write.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    pipeline_id = (body.get("pipeline_id") or "").strip()
    pipeline_name = (body.get("pipeline_name") or "").strip()
    if not pipeline_id:
        return JSONResponse(status_code=422, content={"error": "'pipeline_id' is required"})

    safe_name = re.sub(r"[^a-z0-9]", "_", (pipeline_name or "pipe").lower())[:20].strip("_")
    short_id = pipeline_id.replace("-", "")[:6]
    catalog_pattern = (body.get("catalog_pattern") or "").strip()
    catalog_isolation = (body.get("catalog_isolation_mode") or "per-pipeline").strip()

    if catalog_isolation == "shared":
        # Shared mode: all pipelines use the same catalog
        if catalog_pattern and "{name}" in catalog_pattern:
            catalog_name = catalog_pattern.replace("{name}", "shared")
        else:
            catalog_name = catalog_pattern or "iot_shared"
    elif catalog_pattern and "{name}" in catalog_pattern:
        catalog_name = catalog_pattern.replace("{name}", f"{safe_name}_{short_id}")
    else:
        catalog_name = f"iot_{safe_name}_{short_id}"

    schemas = ["raw", "metrics", "checkpoints"]
    volume_name = "stream_checkpoints"

    # -- Idempotent early-return: if this pipeline already owns ANY catalog --
    if pipeline_id and state.db_available and state.db:
        row = await state.db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id
        )
        existing_res = json.loads(row["resources"] or "{}") if row else {}
        existing_catalog = existing_res.get("uc_catalog")
        if existing_catalog:
            volume_path = (
                existing_res.get("checkpoint_volume")
                or f"/Volumes/{existing_catalog}/checkpoints/{volume_name}"
            )
            return {
                "catalog": existing_catalog,
                "schemas": schemas,
                "volume": volume_path,
                "status": "exists",
            }

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.errors import AlreadyExists
        from databricks.sdk.service.catalog import VolumeType

        w = WorkspaceClient()

        try:
            storage_root = await _resolve_uc_storage_root(
                state, user_email, body.get("storage_root")
            )
            create_kwargs = {"name": catalog_name}
            if storage_root:
                create_kwargs["storage_root"] = f"{storage_root}/{catalog_name}"
            w.catalogs.create(**create_kwargs)
        except AlreadyExists:
            pass
        except Exception as e:
            if _is_missing_metastore_storage(e):
                return JSONResponse(
                    status_code=409,
                    content={"error": _MISSING_STORAGE_ROOT_HINT},
                )
            raise

        for schema in schemas:
            try:
                w.schemas.create(name=schema, catalog_name=catalog_name)
            except AlreadyExists:
                pass

        try:
            w.volumes.create(
                catalog_name=catalog_name,
                schema_name="checkpoints",
                name=volume_name,
                volume_type=VolumeType.MANAGED,
            )
        except AlreadyExists:
            pass

        # Explicitly bind the just-created catalog to THIS workspace so it is
        # always accessible here (guards against metastore isolation defaults
        # that leave a fresh catalog unbound). Idempotent and non-fatal.
        workspace_binding = _bind_catalog_to_workspace(w, catalog_name)

        zb_client_id = os.getenv("ZEROBUS_CLIENT_ID", "")
        grants: list[dict] = []
        if zb_client_id:
            grants = _grant_zerobus_sp(w, catalog_name, schemas, zb_client_id)
            failed = [g for g in grants if g.get("status") == "failed"]
            if failed:
                logger.error(
                    "Zerobus SP grants partially failed for catalog '%s': %d of %d "
                    "grant(s) failed — the SP may be unable to write to the Delta "
                    "table. Failures: %s",
                    catalog_name,
                    len(failed),
                    len(grants),
                    [g.get("resource") for g in failed],
                )

        volume_path = f"/Volumes/{catalog_name}/checkpoints/{volume_name}"
        if pipeline_id:
            await track_resource(
                state.db, state.db_available, pipeline_id, "uc_catalog", catalog_name
            )
            await track_resource(
                state.db, state.db_available, pipeline_id, "checkpoint_volume", volume_path
            )
            await track_resource(
                state.db, state.db_available, pipeline_id, "catalog_schemas", json.dumps(schemas)
            )

        return {
            "catalog": catalog_name,
            "schemas": schemas,
            "volume": volume_path,
            "status": "created",
            "workspace_binding": workspace_binding,
            "grants": grants,
        }

    except Exception as exc:
        logger.error("Pipeline catalog provisioning failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/pipelines/{pipeline_id}/secrets")
async def put_pipeline_secrets(
    pipeline_id: str,
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Store key-value pairs in a UC secret scope tied to this pipeline."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    secrets = body.get("secrets")
    if not secrets or not isinstance(secrets, dict):
        return JSONResponse(status_code=422, content={"error": "'secrets' dict is required"})

    scope_name: str | None = None
    if state.db_available and state.db:
        row = await state.db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id
        )
        if row:
            existing = json.loads(row["resources"] or "{}") if row else {}
            scope_name = existing.get("secret_scope")

    if not scope_name:
        short_id = pipeline_id.replace("-", "")[:8]
        scope_name = f"iot-pipeline-{short_id}"

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.errors import AlreadyExists

        w = WorkspaceClient()
        try:
            w.secrets.create_scope(scope=scope_name)
        except AlreadyExists:
            pass

        stored_keys: list[str] = []
        for key, value in secrets.items():
            w.secrets.put_secret(scope=scope_name, key=key, string_value=str(value))
            stored_keys.append(key)

        await track_resource(state.db, state.db_available, pipeline_id, "secret_scope", scope_name)
        return {"scope": scope_name, "stored_keys": stored_keys}

    except Exception as exc:
        logger.error("Failed to store secrets for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/pipelines/{pipeline_id}/secrets/keys")
async def list_pipeline_secret_keys(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """List which secret keys exist in this pipeline's UC secret scope."""
    scope_name: str | None = None
    if state.db_available and state.db:
        row = await state.db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id
        )
        if row:
            existing = json.loads(row["resources"] or "{}") if row else {}
            scope_name = existing.get("secret_scope")

    if not scope_name:
        return {"scope": None, "keys": []}

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        secret_list = w.secrets.list_secrets(scope=scope_name)
        keys = [s.key for s in secret_list if s.key]
        return {"scope": scope_name, "keys": keys}
    except Exception as exc:
        logger.warning("Failed to list secrets for pipeline %s: %s", pipeline_id, exc)
        return {"scope": scope_name, "keys": []}


@router.post("/api/provision/lakebase-table")
async def provision_lakebase_table(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Create the IoT sensor_readings table in a Lakebase PostgreSQL database.

    Supports two connection modes:
    - **autoscale**: resolves the host via the Databricks SDK from project_id/branch_id,
      generates an OAuth token, and connects.
    - **direct**: connects using the provided host/port/user/password.

    Returns 409 if the table already exists.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    mode = body.get("mode", "direct")
    table_name = (body.get("table_name") or "").strip()
    database = body.get("database", "databricks_postgres")
    pipeline_id = body.get("pipeline_id")

    if not table_name:
        return JSONResponse(status_code=422, content={"error": "'table_name' is required"})

    # Wizard-driven deploy: pull tiering + sse_metrics from the pipeline's
    # persisted sink_config so LakeTS ChronoTable conversion + tier policy
    # flow through the shared provisioning service. Settings-tab callers
    # (no pipeline_id) keep the original, tiering-free fast path.
    sink_config = await _load_pipeline_sink_config(state, pipeline_id)
    tiering = _extract_valid_tiering(sink_config)
    sse_metrics = _extract_sse_metrics(sink_config)

    if tiering:
        lb: dict = {
            "mode": mode,
            "database": database,
            "pg_table": table_name,
            "tiering": tiering,
        }
        if mode == "autoscale":
            project_id = body.get("project_id", "")
            branch_id = body.get("branch_id") or "production"
            if not project_id:
                lb_endpoint = os.getenv("LAKEBASE_ENDPOINT", "")
                parts = lb_endpoint.split("/")
                if len(parts) >= 4 and parts[0] == "projects" and parts[2] == "branches":
                    project_id = parts[1]
                    branch_id = branch_id or parts[3]
            if not project_id:
                return JSONResponse(
                    status_code=422,
                    content={
                        "error": "project_id is required (and could not be derived from LAKEBASE_ENDPOINT)"
                    },
                )
            lb["project_id"] = project_id
            lb["branch_id"] = branch_id
        else:
            lb["host"] = body.get("host", "")
            lb["user"] = body.get("user", "")
            lb["password"] = body.get("password", "")
            lb["port"] = int(body.get("port", 5432))
            if not lb["host"] or not lb["user"]:
                return JSONResponse(
                    status_code=422,
                    content={"error": "'host' and 'user' are required for direct mode"},
                )

        svc_result = await provisioning_service.provision_lakebase_table(lb)
        if svc_result.get("status") == "failed":
            return JSONResponse(
                status_code=500, content={"error": svc_result.get("error", "Provisioning failed")}
            )
        if svc_result.get("status") == "already_exists":
            return JSONResponse(
                status_code=409, content={"error": f"Table '{table_name}' already exists"}
            )

        if pipeline_id:
            await track_resource(
                state.db, state.db_available, pipeline_id, "lakebase_table", table_name
            )

        response: dict = {
            "table": table_name,
            "database": database,
            "status": "created",
            "sse_metrics": sse_metrics,
        }
        if "lakets_version" in svc_result:
            response["lakets_version"] = svc_result["lakets_version"]
        if "tiering_applied" in svc_result:
            response["tiering_applied"] = svc_result["tiering_applied"]
        if "tiering_error" in svc_result:
            response["tiering_error"] = svc_result["tiering_error"]
        if "sync_enabled" in svc_result:
            response["sync_enabled"] = svc_result["sync_enabled"]
        if "sync_error" in svc_result:
            response["sync_error"] = svc_result["sync_error"]
        return response

    try:
        import asyncpg

        if mode == "autoscale":
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            project_id = body.get("project_id", "")
            branch_id = body.get("branch_id", "")

            if not project_id or not branch_id:
                lb_endpoint = os.getenv("LAKEBASE_ENDPOINT", "")
                parts = lb_endpoint.split("/")
                if len(parts) >= 4 and parts[0] == "projects" and parts[2] == "branches":
                    project_id = project_id or parts[1]
                    branch_id = branch_id or parts[3]

            if not project_id:
                return JSONResponse(
                    status_code=422,
                    content={
                        "error": "project_id is required (and could not be derived from LAKEBASE_ENDPOINT)"
                    },
                )
            branch_id = branch_id or "production"
            parent = f"projects/{project_id}/branches/{branch_id}"

            endpoints = list(w.postgres.list_endpoints(parent=parent))
            if not endpoints:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"No Lakebase endpoints found for {parent}"},
                )
            endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
            pg_host = endpoint.status.hosts.host
            pg_user = w.current_user.me().user_name
            cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
            pg_password = cred.token

        else:
            pg_host = body.get("host", "")
            pg_user = body.get("user", "")
            pg_password = body.get("password", "")
            if not pg_host or not pg_user:
                return JSONResponse(
                    status_code=422,
                    content={"error": "'host' and 'user' are required for direct mode"},
                )

        pg_port = int(body.get("port", 5432))

        conn = await asyncpg.connect(
            host=pg_host,
            database=database,
            user=pg_user,
            password=pg_password,
            port=pg_port,
            ssl="require",
        )

        try:
            ddl = SENSOR_READINGS_DDL.format(table_name=table_name)
            await conn.execute(ddl)
            await conn.execute(MATVIEW_1M_DDL)
            await conn.execute(MATVIEW_5M_DDL)
            for idx_ddl in MATVIEW_INDEX_DDL:
                await conn.execute(idx_ddl)
            logger.info("Provisioned table '%s' with materialized views", table_name)
        except asyncpg.exceptions.DuplicateTableError:
            return JSONResponse(
                status_code=409,
                content={"error": f"Table '{table_name}' already exists"},
            )
        finally:
            await conn.close()

        if pipeline_id:
            await track_resource(
                state.db, state.db_available, pipeline_id, "lakebase_table", table_name
            )

        response_no_tier: dict = {"table": table_name, "database": database, "status": "created"}
        if sse_metrics:
            response_no_tier["sse_metrics"] = sse_metrics
        return response_no_tier

    except Exception as exc:
        logger.error("Lakebase table provisioning failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})
