"""Pipeline CRUD endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state
from iot_twin.core.workspace_config import get_pipelines_project
from iot_twin.routes.sensor_data import _is_industry_deployment, _is_undefined_relation
from iot_twin.services import app_permissions
from iot_twin.services.pipelines.pipeline_db import get_industry_lakebase, get_pipeline_lakebase
from iot_twin.services.pipelines.pipeline_project_service import (
    PG_DATABASE,
    PRIMARY_ENDPOINT_ID,
    endpoint_resource_name,
)
from iot_twin.services.pipelines.pipeline_service import pipeline_row_to_dict
from iot_twin.services.producer_service import stop_data_producer
from iot_twin.services.source_registry import check_source_available, claim_source, release_source

# Import the OAuth-native Lakebase provisioning helpers. These are being
# introduced by a sibling refactor; tolerate their absence so this module still
# imports cleanly during the interim. Tests should always patch these symbols
# via ``iot_twin.routes.pipelines.<name>`` so the patch target exists either way.
try:
    from iot_twin.services.provisioning_service import provision_lakebase_role  # type: ignore
except ImportError:  # pragma: no cover — transitional fallback

    async def provision_lakebase_role(*_args: Any, **_kwargs: Any) -> list[dict]:  # type: ignore[no-redef]
        raise RuntimeError(
            "provision_lakebase_role is not yet available in iot_twin.services.provisioning_service"
        )


try:
    from iot_twin.services.lakets.lakets_service import install_lakets_if_missing  # type: ignore
except ImportError:  # pragma: no cover — transitional fallback

    async def install_lakets_if_missing(*_args: Any, **_kwargs: Any) -> dict:  # type: ignore[no-redef]
        raise RuntimeError(
            "install_lakets_if_missing is not yet available in iot_twin.services.lakets.lakets_service"
        )


# App-permission grant (CAN_USE) for the streaming SP — fixes the SP's 401
# when it authenticates back to the deployed app.
from iot_twin.services.app_permissions import _app_name, grant_app_can_use  # noqa: E402

# LakeTS operational jobs (partition manager, rollup, retention). Owned by a
# sibling refactor in iot_twin.services.lakets.lakets_deploy_service; tolerate its absence so
# this module still imports during the interim. Tests patch this symbol via
# ``iot_twin.routes.pipelines.deploy_lakets_ops_jobs`` so the patch target exists either way.
try:
    from iot_twin.services.lakets.lakets_deploy_service import deploy_lakets_ops_jobs  # type: ignore
except ImportError:  # pragma: no cover — transitional fallback

    def deploy_lakets_ops_jobs(*_args: Any, **_kwargs: Any) -> list[dict]:  # type: ignore[no-redef]
        raise RuntimeError(
            "deploy_lakets_ops_jobs is not yet available in iot_twin.services.lakets.lakets_deploy_service"
        )


logger = logging.getLogger("iot_digital_twin.routes.pipelines")
router = APIRouter()


@router.get("/api/pipelines")
async def list_pipelines(
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        "SELECT * FROM pipelines WHERE user_email = $1 AND status != 'deleted' ORDER BY created_at DESC",
        user_email,
    )
    return [pipeline_row_to_dict(r) for r in rows]


@router.post("/api/pipelines")
async def create_pipeline(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse(status_code=422, content={"error": "'name' is required"})

    dup = await state.db.fetchrow(
        "SELECT id FROM pipelines WHERE user_email = $1 AND LOWER(name) = LOWER($2) AND status != 'deleted'",
        user_email,
        name,
    )
    if dup:
        return JSONResponse(
            status_code=409, content={"error": f"A pipeline named '{name}' already exists"}
        )

    source_type = body.get("source_type")
    source_config = body.get("source_config")
    if source_type and source_config:
        try:
            availability = await check_source_available(
                db=state.db, source_type=source_type, source_config=source_config
            )
        except (ValueError, KeyError) as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        if not availability["available"]:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "Source already claimed by another pipeline",
                    "claimed_by_pipeline": availability.get("claimed_by_pipeline"),
                    "claimed_by": availability.get("claimed_by"),
                },
            )

    # Persist source_type / source_config on the row at create time so a
    # pipeline can never sit in the source_type=null window (which made
    # zerobus pipelines render as SSE until the source-settings autosave ran).
    row = await state.db.fetchrow(
        """
        INSERT INTO pipelines (user_email, name, industry, source_type, source_config)
        VALUES ($1, $2, $3, $4, COALESCE($5::jsonb, '{}'::jsonb))
        RETURNING *
        """,
        user_email,
        name,
        body.get("industry"),
        source_type,
        json.dumps(source_config) if source_config else None,
    )

    if source_type and source_config and row:
        try:
            await claim_source(
                db=state.db,
                pipeline_id=str(row["id"]),
                source_type=source_type,
                source_config=source_config,
                user_email=user_email,
                partition_count=body.get("partition_count"),
            )
        except Exception as exc:
            logger.warning("claim_source failed after pipeline insert: %s", exc)

    return pipeline_row_to_dict(row)


@router.post("/api/pipelines/check-source")
async def check_source(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Check whether a source is available (not claimed by any active pipeline)."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    body = await request.json()
    source_type = body.get("source_type")
    source_config = body.get("source_config")
    if not source_type or not source_config:
        return JSONResponse(
            status_code=400,
            content={"detail": "source_type and source_config required"},
        )
    try:
        result = await check_source_available(
            db=state.db, source_type=source_type, source_config=source_config
        )
        return JSONResponse(content=result)
    except (ValueError, KeyError) as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})


@router.get("/api/pipelines/{pipeline_id}")
async def get_pipeline(
    pipeline_id: str,
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    # Admins may inspect any pipeline (curated-industry pipelines are owned by
    # a fixed system identity, not the admin's own email — see
    # ``iot_twin.services.industry.industry_service._INDUSTRY_PIPELINE_OWNER``). End users remain
    # scoped to their own rows.
    if app_permissions.is_admin(request):
        row = await state.db.fetchrow(
            "SELECT * FROM pipelines WHERE id = $1::uuid",
            pipeline_id,
        )
    else:
        row = await state.db.fetchrow(
            "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2",
            pipeline_id,
            user_email,
        )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})
    return pipeline_row_to_dict(row)


@router.put("/api/pipelines/{pipeline_id}")
async def update_pipeline(
    pipeline_id: str,
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    # Admins may modify any pipeline (curated-industry pipelines are owned by a
    # fixed system identity, not the admin's own email); end users stay scoped
    # to their own rows. See ``get_pipeline`` for the same admin/owner split.
    caller_is_admin = app_permissions.is_admin(request)
    if caller_is_admin:
        existing = await state.db.fetchrow(
            "SELECT id, user_email FROM pipelines WHERE id = $1::uuid",
            pipeline_id,
        )
    else:
        existing = await state.db.fetchrow(
            "SELECT id, user_email FROM pipelines WHERE id = $1::uuid AND user_email = $2",
            pipeline_id,
            user_email,
        )
    if not existing:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    # Duplicate-name check runs within the owning user's namespace (names are
    # unique per owner, not globally — see ``create_pipeline``).
    owner_email = existing["user_email"]
    if "name" in body and body["name"]:
        new_name = body["name"].strip()
        dup = await state.db.fetchrow(
            "SELECT id FROM pipelines WHERE user_email = $1 AND LOWER(name) = LOWER($2) AND id != $3::uuid AND status != 'deleted'",
            owner_email,
            new_name,
            pipeline_id,
        )
        if dup:
            return JSONResponse(
                status_code=409, content={"error": f"A pipeline named '{new_name}' already exists"}
            )

    allowed = {
        "name",
        "status",
        "industry",
        "sensor_config",
        "source_type",
        "source_config",
        "sink_config",
        "resources",
    }
    merge_jsonb = {"source_config", "sink_config", "resources"}
    sets: list[str] = ["updated_at = NOW()"]
    params: list[object] = []
    idx = 1
    for key in allowed:
        if key in body:
            idx += 1
            val = body[key]
            if key in ("sensor_config", "source_config", "sink_config", "resources"):
                val = json.dumps(val)
            if key in merge_jsonb:
                sets.append(f"{key} = COALESCE({key}, '{{}}'::jsonb) || ${idx}::jsonb")
            else:
                sets.append(f"{key} = ${idx}")
            params.append(val)

    query = f"UPDATE pipelines SET {', '.join(sets)} WHERE id = $1::uuid RETURNING *"
    row = await state.db.fetchrow(query, pipeline_id, *params)
    return pipeline_row_to_dict(row)


@router.delete("/api/pipelines/{pipeline_id}")
async def delete_pipeline(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Delete a pipeline and clean up all provisioned resources except notebooks."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id,
        user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    resources = json.loads(row["resources"]) if row["resources"] else {}

    if pipeline_id in state.producer_states:
        try:
            await stop_data_producer(state.producer_states, pipeline_id)
        except Exception:
            pass

    cleanup_log: list[Any] = []
    cleanup_errors: list[str] = []

    def _is_not_found(exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        message = str(exc).lower()
        return (
            isinstance(exc, NotFound)
            or status_code == 404
            or "not found" in message
            or "does not exist" in message
        )

    def _uc_tables_to_drop(log: list[Any]) -> list[str]:
        """Return only tables carrying exact pipeline ownership metadata.

        Historical ``uc_catalog`` / ``uc_schema`` / ``cold_tier_table`` fields
        identify locations, not ownership.  Inferring a conventional table name
        from them can target a sibling deployment in the shared schema, so those
        fields deliberately authorize no UC deletion.
        """
        tracked = resources.get("owned_uc_tables")
        if not isinstance(tracked, list):
            return []
        targets: list[str] = []
        for item in tracked:
            if not isinstance(item, dict):
                log.append("Skipped UC table — malformed ownership metadata")
                continue
            full_name = item.get("full_name")
            owner = item.get("pipeline_id")
            if (
                owner != pipeline_id
                or not isinstance(full_name, str)
                or len(full_name.split(".")) != 3
            ):
                log.append(f"Skipped UC table {full_name!r} — ownership is not proven")
                continue
            targets.append(full_name)
        return targets

    def _sdk_cleanup() -> list[Any]:
        log: list[Any] = []
        try:
            w = WorkspaceClient()

            if resources.get("job_id"):
                try:
                    w.jobs.delete(job_id=int(resources["job_id"]))
                    log.append(f"Deleted job {resources['job_id']}")
                except Exception as exc:
                    if _is_not_found(exc):
                        log.append(f"Job {resources['job_id']} already absent")
                    else:
                        message = f"Job delete failed: {exc}"
                        log.append(message)
                        cleanup_errors.append(message)

            if resources.get("cluster_id"):
                try:
                    w.clusters.permanent_delete(cluster_id=resources["cluster_id"])
                    log.append(f"Deleted cluster {resources['cluster_id']}")
                except Exception as exc:
                    if _is_not_found(exc):
                        log.append(f"Cluster {resources['cluster_id']} already absent")
                    else:
                        message = f"Cluster delete failed: {exc}"
                        log.append(message)
                        cleanup_errors.append(message)

            # Drop the pipeline's CDF-enabled / cold-tier Unity Catalog table(s)
            # via the SDK. Each drop is best-effort, idempotent (not-found is
            # success), and scoped to this pipeline's own catalog.
            for full_name in _uc_tables_to_drop(log):
                entry: dict[str, Any] = {
                    "resource": f"UC table {full_name}",
                }
                try:
                    w.tables.delete(full_name=full_name)
                    entry["status"] = "dropped"
                except NotFound:
                    entry["status"] = "not_found"
                except Exception as exc:
                    entry["status"] = "failed"
                    entry["error"] = str(exc)
                    cleanup_errors.append(f"UC table delete failed for {full_name}: {exc}")
                    logger.warning(
                        "UC table drop failed for %s (pipeline %s): %s",
                        full_name,
                        pipeline_id,
                        exc,
                    )
                log.append(entry)
        except Exception as exc:
            message = f"SDK init failed: {exc}"
            log.append(message)
            cleanup_errors.append(message)
        return log

    try:
        cleanup_log = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _sdk_cleanup),
            timeout=15,
        )
    except TimeoutError:
        message = "SDK cleanup timed out after 15s; resources may be orphaned"
        cleanup_log.append(message)
        cleanup_errors.append(message)

    if resources.get("lakebase_table"):
        table_name = resources["lakebase_table"]
        cleanup_log.append(
            f"Preserved Lakebase table {table_name!r} — deployment ownership "
            "is not represented in the legacy pipeline resource record"
        )

    if cleanup_errors:
        return JSONResponse(
            status_code=502,
            content={
                "deleted": False,
                "error": "; ".join(cleanup_errors),
                "cleanup_log": cleanup_log,
            },
        )

    try:
        await release_source(db=state.db, pipeline_id=pipeline_id)
    except Exception as exc:
        logger.warning("release_source failed during pipeline delete: %s", exc)
        return JSONResponse(
            status_code=502,
            content={
                "deleted": False,
                "error": f"Source release failed: {exc}",
                "cleanup_log": cleanup_log,
            },
        )

    await state.db.execute(
        "UPDATE pipelines SET status = 'deleted', updated_at = NOW() WHERE id = $1::uuid",
        pipeline_id,
    )
    return {"deleted": True, "cleanup_log": cleanup_log}


@router.post("/api/pipelines/{pipeline_id}/reset")
async def reset_pipeline(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Stop the running job/cluster and reset pipeline to step2_complete for re-deployment.

    Preserves UC catalog, secrets, and Lakebase tables.
    """
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2 AND status != 'deleted'",
        pipeline_id,
        user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    resources = (
        json.loads(row["resources"])
        if isinstance(row["resources"], str)
        else (row["resources"] or {})
    )
    cleanup_log: list[str] = []

    # Stop job and cluster in background
    def _cleanup() -> list[str]:
        log: list[str] = []
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()

            job_id = resources.get("job_id")
            if job_id:
                try:
                    w.jobs.cancel_all_runs(job_id=int(job_id))
                    log.append(f"Cancelled runs for job {job_id}")
                except Exception as exc:
                    log.append(f"Job cancel failed: {exc}")

            # Note: do NOT delete the validation cluster here — it's the user's
            # all-purpose cluster for testing. Job clusters are ephemeral and
            # auto-deleted when the job stops.
        except Exception as exc:
            log.append(f"SDK init failed: {exc}")
        return log

    try:
        cleanup_log = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _cleanup),
            timeout=15,
        )
    except TimeoutError:
        cleanup_log.append("Cleanup timed out after 15s")

    # Clear job/cluster/notebook from resources, keep catalog/secrets/tables
    keep_keys = {
        "uc_catalog",
        "uc_schema",
        "secret_scope",
        "checkpoint_volume",
        "catalog_schemas",
        "lakebase_table",
        "job_id",
    }
    cleaned_resources = {k: v for k, v in resources.items() if k in keep_keys}

    updated = await state.db.fetchrow(
        """UPDATE pipelines
           SET status = 'step2_complete', resources = $2::jsonb, updated_at = NOW()
           WHERE id = $1::uuid
           RETURNING *""",
        pipeline_id,
        json.dumps(cleaned_resources),
    )

    # Stop any in-memory producer
    if pipeline_id in state.producer_states:
        try:
            await stop_data_producer(state.producer_states, pipeline_id)
        except Exception:
            pass

    return {
        "pipeline": pipeline_row_to_dict(updated),
        "cleanup_log": cleanup_log,
    }


async def _heal_industry_lakebase(industry: str) -> dict[str, Any] | JSONResponse:
    """Re-apply role + LakeTS provisioning to an industry-first branch.

    Mirrors the legacy ``heal_lakebase`` role/LakeTS steps against the
    deterministic ``iot-pipelines`` project/branch (no legacy ``pipelines``
    row, so no ``job_id`` to restart — the "restart_job" step is reported as
    skipped). Never raises: every step is wrapped in its own try/except and
    reported in the response, matching the legacy endpoint's shape.
    """
    lb_project_id = get_pipelines_project()
    lb_branch_id = industry
    lb_database = PG_DATABASE
    endpoint_name = endpoint_resource_name(
        lb_branch_id,
        PRIMARY_ENDPOINT_ID,
        lb_project_id,
    )

    try:
        w = WorkspaceClient()
        sp_client_id = w.current_user.me().user_name
    except Exception as exc:
        logger.warning(
            "heal_lakebase(industry=%s): failed to resolve SP client id: %s", industry, exc
        )
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to resolve service principal: {exc}"},
        )

    steps: list[dict[str, Any]] = []

    try:
        provision_results = await provision_lakebase_role(
            sp_client_id=sp_client_id,
            project_id=lb_project_id,
            branch_id=lb_branch_id,
            database=lb_database,
            endpoint_name=endpoint_name,
            sink_tables=("sensor_readings",),
        )
        steps.append({"step": "provision_role", "results": provision_results})
    except Exception as exc:
        logger.exception("heal_lakebase(industry=%s): provision_lakebase_role failed", industry)
        steps.append({"step": "provision_role", "error": str(exc), "results": []})

    try:
        lakets_result = await install_lakets_if_missing(
            endpoint_name=endpoint_name,
            database=lb_database,
        )
        steps.append({"step": "install_lakets", "result": lakets_result})
    except Exception as exc:
        logger.exception("heal_lakebase(industry=%s): install_lakets_if_missing failed", industry)
        steps.append({"step": "install_lakets", "error": str(exc), "result": None})

    grant_step: dict[str, Any] = {"step": "grant_app_access"}
    try:
        app_name = _app_name()
        grant_app_can_use(w, app_name, sp_client_id)
        grant_step["app_name"] = app_name
        grant_step["sp"] = sp_client_id
        grant_step["status"] = "granted"
    except Exception as exc:  # pragma: no cover — grant is itself non-raising
        logger.warning("heal_lakebase(industry=%s): grant_app_can_use failed: %s", industry, exc)
        grant_step["error"] = str(exc)
    steps.append(grant_step)

    steps.append(
        {
            "step": "restart_job",
            "status": "skipped",
            "reason": "Industry-first pipelines have no legacy job_id to restart",
        }
    )

    return {
        "industry": industry,
        "endpoint": endpoint_name,
        "steps": steps,
    }


@router.post("/api/pipelines/{pipeline_id}/heal-lakebase")
async def heal_lakebase(
    pipeline_id: str,
    industry: str | None = Query(
        None,
        description="Industry id — for industry-first pipelines with no legacy "
        "`pipelines` row, re-applies role + LakeTS provisioning to the "
        "deterministic iot-pipelines branch instead (no job to restart).",
    ),
    state: AppState = Depends(get_app_state),
    user_email: str = Depends(get_current_user),
):
    """Re-apply the OAuth-native Lakebase provisioning to an existing pipeline.

    Idempotent. Safe to call multiple times. Triggers a job restart so the
    updated consumer notebook takes effect. Each step (provision, install lakets,
    restart job) is wrapped in its own try/except so a single failure does not
    break the whole endpoint — every step result is reported in the response.

    Industry-first pipelines have no row in the legacy ``pipelines`` table, so
    when ``industry`` is passed and no legacy row is found, this instead heals
    the deterministic ``iot-pipelines`` branch directly (role + LakeTS install
    only — there is no legacy ``job_id`` to restart).
    """
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2 AND status != 'deleted'",
        pipeline_id,
        user_email,
    )
    if not row:
        if industry:
            return await _heal_industry_lakebase(industry)
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    pipeline = pipeline_row_to_dict(row)
    source_config = pipeline.get("source_config") or {}
    if isinstance(source_config, str):
        try:
            source_config = json.loads(source_config)
        except (json.JSONDecodeError, TypeError):
            source_config = {}

    lb_project_id = source_config.get("lb_project_id")
    lb_branch_id = source_config.get("lb_branch_id")
    lb_database = source_config.get("lb_database")

    missing = [
        name
        for name, val in (
            ("lb_project_id", lb_project_id),
            ("lb_branch_id", lb_branch_id),
            ("lb_database", lb_database),
        )
        if not val
    ]
    if missing:
        return JSONResponse(
            status_code=400,
            content={
                "error": (
                    "Pipeline source_config is missing required Lakebase fields: "
                    f"{', '.join(missing)}"
                ),
            },
        )

    endpoint_name = f"projects/{lb_project_id}/branches/{lb_branch_id}/endpoints/primary"

    # Resolve SP client ID via WorkspaceClient().current_user.me().user_name.
    # When running as a deployed Databricks App, this is the SP's UUID.
    try:
        w = WorkspaceClient()
        sp_client_id = w.current_user.me().user_name
    except Exception as exc:
        logger.warning("heal_lakebase: failed to resolve SP client id: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to resolve service principal: {exc}"},
        )

    steps: list[dict[str, Any]] = []

    # Step 5: provision Lakebase role
    try:
        provision_results = await provision_lakebase_role(
            sp_client_id=sp_client_id,
            project_id=lb_project_id,
            branch_id=lb_branch_id,
            database=lb_database,
            endpoint_name=endpoint_name,
            sink_tables=("sensor_readings",),
        )
        steps.append({"step": "provision_role", "results": provision_results})
    except Exception as exc:
        logger.exception("heal_lakebase: provision_lakebase_role failed for %s", pipeline_id)
        steps.append(
            {
                "step": "provision_role",
                "error": str(exc),
                "results": [],
            }
        )

    # Step 6: install LakeTS (idempotent)
    try:
        lakets_result = await install_lakets_if_missing(
            endpoint_name=endpoint_name,
            database=lb_database,
        )
        steps.append({"step": "install_lakets", "result": lakets_result})
    except Exception as exc:
        logger.exception("heal_lakebase: install_lakets_if_missing failed for %s", pipeline_id)
        steps.append(
            {
                "step": "install_lakets",
                "error": str(exc),
                "result": None,
            }
        )

    # Step 6b: grant the streaming SP CAN_USE on the app so its OAuth token is
    # accepted when it calls back into the app (otherwise: 401). Non-fatal.
    grant_step: dict[str, Any] = {"step": "grant_app_access"}
    try:
        app_name = _app_name()
        grant_app_can_use(w, app_name, sp_client_id)
        grant_step["app_name"] = app_name
        grant_step["sp"] = sp_client_id
        grant_step["status"] = "granted"
    except Exception as exc:  # pragma: no cover — grant is itself non-raising
        logger.warning("heal_lakebase: grant_app_can_use failed for %s: %s", pipeline_id, exc)
        grant_step["error"] = str(exc)
    steps.append(grant_step)

    # Step 6c: deploy LakeTS operational jobs (partition manager, rollup,
    # retention) for the pipeline's Lakebase project/branch/database. Non-fatal.
    deploy_step: dict[str, Any] = {"step": "deploy_lakets_jobs"}
    try:
        ops_results = deploy_lakets_ops_jobs(
            w,
            lb_project_id,
            lb_branch_id,
            database=lb_database,
        )
        deploy_step["results"] = ops_results
    except Exception as exc:
        logger.exception("heal_lakebase: deploy_lakets_ops_jobs failed for %s", pipeline_id)
        deploy_step["error"] = str(exc)
        deploy_step["results"] = []
    steps.append(deploy_step)

    # Step 7: TODO: regenerate consumer notebook via generate_sss_consumer_notebook()
    # once frontend template is updated — skipped for now; user can manually re-run
    # deploy from wizard.

    # Step 8: cancel active runs and trigger a fresh run.
    resources = pipeline.get("resources") or {}
    if isinstance(resources, str):
        try:
            resources = json.loads(resources)
        except (json.JSONDecodeError, TypeError):
            resources = {}
    job_id_raw = resources.get("job_id") if isinstance(resources, dict) else None

    cancelled_runs: list[int] = []
    new_run_id: int | None = None
    restart_error: str | None = None
    try:
        if not job_id_raw:
            raise ValueError("Pipeline has no job_id to restart")
        job_id = int(job_id_raw)
        active_runs = list(w.jobs.list_runs(job_id=job_id, active_only=True) or [])
        for run in active_runs:
            run_id = getattr(run, "run_id", None)
            if run_id is None:
                continue
            try:
                w.jobs.cancel_run(run_id=run_id)
                cancelled_runs.append(int(run_id))
            except Exception as cancel_exc:
                logger.warning(
                    "heal_lakebase: failed to cancel run %s for job %s: %s",
                    run_id,
                    job_id,
                    cancel_exc,
                )
        triggered = w.jobs.run_now(job_id=job_id)
        maybe_run_id = getattr(triggered, "run_id", None)
        if maybe_run_id is not None:
            new_run_id = int(maybe_run_id)
    except Exception as exc:
        logger.exception("heal_lakebase: restart failed for pipeline %s", pipeline_id)
        restart_error = str(exc)

    restart_step: dict[str, Any] = {
        "step": "restart_job",
        "new_run_id": new_run_id,
        "cancelled": cancelled_runs,
    }
    if restart_error is not None:
        restart_step["error"] = restart_error
    steps.append(restart_step)

    return {
        "pipeline_id": pipeline_id,
        "endpoint": endpoint_name,
        "steps": steps,
    }


@router.post("/api/pipelines/{pipeline_id}/clone")
async def clone_pipeline(
    pipeline_id: str,
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Clone an existing pipeline's configuration into a new draft pipeline."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    source = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2 AND status != 'deleted'",
        pipeline_id,
        user_email,
    )
    if not source:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    new_name = (body.get("name") or "").strip()
    if not new_name:
        return JSONResponse(status_code=422, content={"error": "'name' is required"})

    dup = await state.db.fetchrow(
        "SELECT id FROM pipelines WHERE user_email = $1 AND LOWER(name) = LOWER($2) AND status != 'deleted'",
        user_email,
        new_name,
    )
    if dup:
        return JSONResponse(
            status_code=409, content={"error": f"A pipeline named '{new_name}' already exists"}
        )

    # Copy config fields, reset resources and status
    sensor_config = source["sensor_config"] if source["sensor_config"] else "[]"
    source_config = source["source_config"] if source["source_config"] else "{}"
    sink_config = source["sink_config"] if source["sink_config"] else "{}"

    row = await state.db.fetchrow(
        """
        INSERT INTO pipelines (user_email, name, industry, sensor_config, source_type, source_config, sink_config)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7::jsonb)
        RETURNING *
        """,
        user_email,
        new_name,
        source["industry"],
        sensor_config,
        source["source_type"],
        source_config,
        sink_config,
    )
    return pipeline_row_to_dict(row)


@router.get("/api/pipelines/{pipeline_id}/lakebase-health")
async def get_lakebase_health(
    pipeline_id: str,
    industry: str | None = Query(
        None,
        description="Industry id — resolves the deterministic iot-pipelines "
        "branch directly (for industry-first dashboards with no legacy "
        "`pipelines` row).",
    ),
    state: AppState = Depends(get_app_state),
):
    """Return Lakebase sink health signals for the dashboard observability card.

    Queries ``sensor_readings`` on the app's Lakebase pool to expose:
    - ``rows_5m``: row count inserted in the last 5 minutes.
    - ``latest_at``: ISO timestamp of the most recent ``event_time`` (or null).
    - ``latency_sec``: seconds between now and ``latest_at`` (or null).
    - ``inserts_per_sec_recent``: ``rows_5m / 300`` rounded to 2 dp.

    On DB error the same shape is returned with zeroes plus an ``error`` key so
    the client never has to special-case a transport failure. When ``industry``
    is passed, an ``undefined table`` error (realtime not deployed yet on that
    branch) degrades to the zeroed payload without an ``error`` key rather than
    surfacing a spurious failure.
    """
    default_payload: dict[str, Any] = {
        "rows_5m": 0,
        "latest_at": None,
        "latency_sec": None,
        "inserts_per_sec_recent": 0.0,
    }

    if industry and not await _is_industry_deployment(state, pipeline_id, industry, "realtime"):
        return JSONResponse(
            status_code=404,
            content={"error": "Realtime industry deployment not found"},
        )
    lb = (
        await get_industry_lakebase(state, industry)
        if industry
        else await get_pipeline_lakebase(state, pipeline_id)
    )
    if lb is None:
        return {**default_payload, "error": "Pipeline Lakebase not resolvable"}

    try:
        row = await lb.fetchrow(
            """
            SELECT
                COUNT(*) AS rows_5m,
                MAX(event_time) AS latest_at,
                CASE
                    WHEN MAX(event_time) IS NOT NULL
                        THEN EXTRACT(EPOCH FROM NOW() - MAX(event_time))
                    ELSE NULL
                END AS latency_sec
            FROM sensor_readings
            WHERE event_time > NOW() - INTERVAL '5 minutes'
            """
        )
    except Exception as exc:
        if industry and _is_undefined_relation(exc):
            logger.info(
                "lakebase-health: sensor_readings not provisioned yet for industry %s",
                industry,
            )
            return default_payload
        logger.warning("lakebase-health query failed for pipeline %s: %s", pipeline_id, exc)
        return {**default_payload, "error": str(exc)}

    if not row:
        return default_payload

    rows_5m = int(row["rows_5m"] or 0)
    latest_at_raw = row["latest_at"]
    latency_raw = row["latency_sec"]

    latest_at_iso: str | None = None
    if latest_at_raw is not None:
        try:
            latest_at_iso = latest_at_raw.isoformat()
        except AttributeError:
            latest_at_iso = str(latest_at_raw)

    latency_sec: float | None = None
    if latency_raw is not None:
        try:
            latency_sec = round(float(latency_raw), 2)
        except (TypeError, ValueError):
            latency_sec = None

    inserts_per_sec_recent = round(rows_5m / 300.0, 2)

    return {
        "rows_5m": rows_5m,
        "latest_at": latest_at_iso,
        "latency_sec": latency_sec,
        "inserts_per_sec_recent": inserts_per_sec_recent,
    }


@router.get("/api/pipeline/{pipeline_id}/history")
async def pipeline_history(
    pipeline_id: str,
    days: int = Query(90, ge=1, le=365, description="Lookback window in days"),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query historical sensor data from the pipeline's UC catalog via DBSQL."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT resources FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id,
        user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    resources = (
        row["resources"]
        if isinstance(row["resources"], dict)
        else json.loads(row["resources"] or "{}")
    )
    catalog = resources.get("uc_catalog")
    if not catalog:
        return JSONResponse(status_code=400, content={"error": "Pipeline has no UC catalog"})

    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
    table = f"{catalog}.raw.iot_sensor_data"

    try:
        import httpx

        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        if not token:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            token = w.config.token

        sql = f"""
            SELECT sensor_name, sensor_type, industry,
                   DATE_TRUNC('HOUR', timestamp) AS hour,
                   AVG(value) AS avg_value,
                   MIN(value) AS min_value,
                   MAX(value) AS max_value,
                   COUNT(*) AS cnt
            FROM {table}
            WHERE timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), {days})
            GROUP BY 1, 2, 3, 4
            ORDER BY hour DESC
            LIMIT 5000
        """

        warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
        if state.db_available and state.db:
            try:
                settings_row = await state.db.fetchrow(
                    "SELECT settings FROM user_settings WHERE user_email = $1", user_email
                )
                if settings_row:
                    user_settings = (
                        json.loads(settings_row["settings"])
                        if isinstance(settings_row["settings"], str)
                        else settings_row["settings"]
                    )
                    saved_wh = user_settings.get("warehouse", {}).get("warehouse_id", "")
                    if saved_wh:
                        warehouse_id = saved_wh
            except Exception:
                pass
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{host}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "statement": sql,
                    "warehouse_id": warehouse_id,
                    "wait_timeout": "50s",
                    "disposition": "INLINE",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        result = data.get("result", {})
        columns = [
            c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])
        ]
        rows_data = []
        for chunk in result.get("data_array", []):
            rows_data.append(dict(zip(columns, chunk, strict=False)))

        return {"columns": columns, "data": rows_data, "row_count": len(rows_data)}

    except Exception as exc:
        logger.error("Pipeline history query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": "Internal server error"})
