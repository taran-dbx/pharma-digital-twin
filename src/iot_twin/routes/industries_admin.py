"""Admin pipeline management API (spec §2.3–2.9, plan Task 2.4 + Phase 6 backend).

Exposes the app-backend data model (``pipeline_setup`` / ``pipeline_deployment``
/ ``provisioned_resource`` in the ``iot-app-backend`` project) and the
provisioning + teardown engine over admin-gated HTTP endpoints:

    POST   /api/admin/industries/{industry}/setup      create/update a setup (one per industry)
    POST   /api/admin/industries/{industry}/provision  run provision_industry(); return resources
    GET    /api/admin/pipelines                         management list + rolled-up health
    GET    /api/admin/pipelines/{setup_id}/health       per-resource health for one pipeline
    DELETE /api/admin/pipelines/{setup_id}              teardown (reverse order) + cascade-delete

All endpoints are gated by ``app_permissions.is_admin`` (same check the rest of
the app uses) and require the additive app-backend DB pool
(``state.app_backend_db``), returning 503 when it is unavailable so a Lakebase
outage degrades gracefully instead of 500-ing.

Additive only: this router never touches ``PGHOST`` / ``LAKEBASE_ENDPOINT`` / the
legacy ``iot-lakets-demo`` connection or the ``pipelines`` / ``user_settings`` /
``source_bindings`` wiring.

TODO (Phase 4 — separate task): the validation-flow routes
``POST /api/admin/industries/{industry}/validate`` (start / resume the persisted
validation state machine) and ``GET /api/admin/industries/{industry}/validate/state``
(poll live step state) will be added here, backed by ``services/validation/validation_service.py``.
They are intentionally NOT implemented in this task.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from iot_twin.core.state import AppState, get_app_state
from iot_twin.core.workspace_config import (
    get_app_sp_client_id,
    get_uc_catalog,
    get_uc_schema,
)
from iot_twin.services import (
    app_permissions,
    dbsql_service,
    health_checks,
    resource_deleters,
    resource_ledger,
    teardown_service,
)
from iot_twin.services.deployment import deployment_repo, deployment_service
from iot_twin.services.genie import genie_space_service
from iot_twin.services.industry import industry_provisioning_service
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.pipelines import (
    pipeline_freshness,
    pipeline_project_service,
    pipeline_setup_repo,
    pipeline_view,
)
from iot_twin.services.validation import validation_state_machine

logger = logging.getLogger("iot_digital_twin.routes.industries_admin")

router = APIRouter()


def _normalize_curated_industry(industry: str) -> str:
    normalized = industry.strip().lower()
    if normalized not in CURATED_INDUSTRIES:
        raise HTTPException(status_code=400, detail="unknown industry")
    return normalized


# Resource statuses that count as live/healthy in the ledger.
_HEALTHY_STATUSES = {"active"}
_IGNORED_STATUSES = {"deleted"}
_MIN_RATE_HZ = 100
_MAX_RATE_HZ = 5000
_LIST_PIPELINE_CONCURRENCY = 3
_INDUSTRY_RUNTIME_CACHE_TTL_SECONDS = 15.0
_INDUSTRY_RUNTIME_COLD_START_TIMEOUT_SECONDS = 10.0


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------
def _require_admin(request: Request) -> None:
    """Raise 403 unless the caller passes the admin check."""
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")


def _require_db(state: AppState) -> Any:
    """Return the app-backend DB pool, or raise 503 if it is unavailable."""
    db = getattr(state, "app_backend_db", None)
    if db is None:
        raise HTTPException(
            status_code=503,
            detail="app-backend metadata database is unavailable",
        )
    return db


def _validate_mode_configs(value: Any) -> dict[str, dict[str, int]]:
    """Validate the persisted per-mode producer frequencies."""
    if not isinstance(value, dict):
        raise HTTPException(status_code=422, detail="mode_configs must be an object")

    normalized: dict[str, dict[str, int]] = {}
    for mode, config in value.items():
        if mode not in deployment_service.VALID_MODES:
            raise HTTPException(status_code=422, detail=f"unknown mode '{mode}'")
        if not isinstance(config, dict):
            raise HTTPException(status_code=422, detail=f"mode_configs.{mode} must be an object")
        rate_hz = config.get("rate_hz")
        if (
            isinstance(rate_hz, bool)
            or not isinstance(rate_hz, int)
            or rate_hz < _MIN_RATE_HZ
            or rate_hz > _MAX_RATE_HZ
        ):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"mode_configs.{mode}.rate_hz must be between {_MIN_RATE_HZ} and {_MAX_RATE_HZ}"
                ),
            )
        normalized[mode] = {"rate_hz": int(rate_hz)}
    return normalized


# ---------------------------------------------------------------------------
# Live job-running lookup (Jobs API) — resolves REAL running state per job so
# ``status`` is never a fabricated "idle/running".  Best-effort: a missing
# WorkspaceClient (tests / no creds) or a Jobs-API error degrades a deployed
# mode to "stopped" (not running) rather than 500-ing the whole list.
# ---------------------------------------------------------------------------
def _job_running_lookup(
    deployments: list[dict], *, reconcile_failures: bool = False
) -> dict[str, dict]:
    """Return ``{job_id: {running, degraded, starting, detail, tasks}}`` per deployed mode.

    Per-mode runtime derived from the Jobs API: ``running`` (run life-cycle RUNNING),
    ``starting`` (run coming up — PENDING/STARTING, not yet RUNNING), ``degraded``
    (run up but a task crashed — e.g. producer died so data isn't flowing), a human
    ``detail``, and the per-task states for the UI.
    """
    job_ids = pipeline_view.collect_job_ids(deployments)
    if not job_ids:
        return {}
    job_modes = {
        str(deployment["job_id"]): str(deployment.get("mode") or "")
        for deployment in deployments
        if deployment.get("job_id")
    }
    lookup: dict[str, dict] = {}

    def fetch_status(job_id: str) -> tuple[str, dict]:
        # WorkspaceClient's HTTP transport is not thread-safe. Each worker owns
        # its client while jobs are resolved concurrently.
        w = deployment_service._new_ws()
        status = deployment_service.pipeline_status(job_id, w=w)
        if reconcile_failures and status.get("degraded"):
            stopped = deployment_service.stop_pipeline(
                job_id,
                w=w,
                continuous=job_modes.get(str(job_id)) != "zerobus",
            )
            if stopped.get("state") == "STOPPED":
                status = {
                    **status,
                    "state": "FAILED",
                    "degraded": False,
                    "restartable": True,
                    "failure_stopped": True,
                }
            else:
                logger.error("Fail-stop could not fully stop job %s: %s", job_id, stopped)
        return str(job_id), status

    # Each job status requires two network calls (latest run + task details).
    # Resolve independent jobs concurrently so page latency is bounded by the
    # slowest job instead of the sum across every deployed pipeline.
    with ThreadPoolExecutor(max_workers=min(len(job_ids), 8)) as executor:
        futures = {executor.submit(fetch_status, job_id): job_id for job_id in job_ids}
        for future in as_completed(futures):
            job_id = futures[future]
            try:
                key, status = future.result()
                lookup[key] = {
                    "running": status.get("state") == "RUNNING",
                    "starting": status.get("state") == "STARTING",
                    "degraded": bool(status.get("degraded")),
                    "restartable": bool(status.get("restartable")),
                    "failure_stopped": bool(status.get("failure_stopped")),
                    "detail": status.get("detail"),
                    "tasks": status.get("tasks") or [],
                }
            except Exception as exc:  # pragma: no cover - best-effort
                logger.warning("pipeline_status(%s) failed; state is unknown: %s", job_id, exc)
    return lookup


# ---------------------------------------------------------------------------
# Live data-freshness lookup ("is data flowing?") — independent observability
# for a RUNNING job. Best-effort: any Lakebase/DBSQL error yields "not flowing"
# without changing the Databricks lifecycle status or 500-ing the list.
# ---------------------------------------------------------------------------
async def _flowing_lookup(
    state: AppState, setup: dict, deployments: list[dict], running: dict[str, dict]
) -> dict[str, bool]:
    """Return ``{mode: flowing}`` for the pipeline's deployed, RUNNING modes."""
    try:
        return await pipeline_freshness.flowing_lookup(state, setup, deployments, running)
    except Exception as exc:  # pragma: no cover - flowing_lookup is self-guarding
        logger.warning("freshness lookup failed; modes report not-flowing: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Health rollup (ledger-derived; live checks are Phase 4)
# ---------------------------------------------------------------------------
def _rollup_health(setup: dict, deployments: list[dict], resources: list[dict]) -> dict:
    """Compute an overall health verdict + counts from ledger + deployment rows.

    Draft setups report ``status='draft'`` (not startable / "coming soon").
    Otherwise: ``healthy`` when every non-deleted resource is active,
    ``degraded`` when some are active and some are not, ``down`` when none are
    active, and ``unknown`` when there are no resources yet.
    """
    live = [r for r in resources if r.get("status") not in _IGNORED_STATUSES]
    total = len(live)
    healthy = sum(1 for r in live if r.get("status") in _HEALTHY_STATUSES)

    if setup.get("status") == "draft":
        overall = "draft"
    elif total == 0:
        overall = "unknown"
    elif healthy == total:
        overall = "healthy"
    elif healthy == 0:
        overall = "down"
    else:
        overall = "degraded"

    jobs = [r for r in live if r.get("resource_type") == "job"]
    jobs_running = sum(1 for r in jobs if r.get("status") in _HEALTHY_STATUSES)

    return {
        "overall": overall,
        "resources_healthy": healthy,
        "resources_total": total,
        "jobs_running": jobs_running,
        "jobs_total": len(jobs),
        "enabled_modes": setup.get("enabled_modes") or [],
        "deployment_count": len(deployments),
    }


# ---------------------------------------------------------------------------
# Tier-1 live API/DB health checks (spec §1.16) — fast, NO interactive cluster.
# Best-effort: a missing WorkspaceClient (tests / no creds) degrades to ``None``
# so the endpoint still returns the ledger-backed inventory instead of 500-ing.
# ---------------------------------------------------------------------------
async def _gather_api_health(setup: dict, deployments: list[dict]) -> dict | None:
    """Run the Tier-1 API/DB health sweep for one pipeline, or ``None`` if no ws."""
    try:
        ws = deployment_service._new_ws()
    except Exception as exc:  # pragma: no cover - no creds in tests
        logger.info("Tier-1 API health checks skipped (no workspace client): %s", exc)
        return None

    warehouse_id = setup.get("warehouse_id")
    run_sql = None
    if warehouse_id:

        async def run_sql(sql: str) -> dict:  # noqa: A001 - local binding
            return await dbsql_service.execute_dbsql(sql, warehouse_id)

    async def open_read_conn(branch: str):
        return await pipeline_project_service.get_read_conn(branch, ws=ws)

    try:
        return await health_checks.gather_checks(
            setup,
            deployments,
            ws=ws,
            run_sql=run_sql,
            open_read_conn=open_read_conn,
        )
    except Exception as exc:  # pragma: no cover - gather_checks is self-guarding
        logger.warning("Tier-1 API health checks failed: %s", exc)
        return None


def _e2e_test_descriptor(industry: str, enabled_modes: list[str]) -> dict:
    """Describe the selected modes' opt-in Tier-2 end-to-end test."""
    has_cluster_modes = bool(set(enabled_modes or []) & {"realtime", "microbatch"})
    if has_cluster_modes:
        description = (
            "Opt-in end-to-end test: spins the interactive validation cluster "
            "(~10 min) for selected Kafka modes and also exercises any selected "
            "Zerobus lane. Verifies produce→consume→land→metrics end to end."
        )
    else:
        description = (
            "Opt-in Zerobus end-to-end test: runs serverless auth, produce, "
            "durable acknowledgement and DBSQL read-back. No interactive "
            "cluster or Kafka resources are used."
        )
    return {
        "tier": "e2e",
        "available": True,
        "spins_interactive_cluster": has_cluster_modes,
        "eta_minutes": 10 if has_cluster_modes else 3,
        "description": description,
        "start_endpoint": f"POST /api/admin/industries/{industry}/validate/start",
        "state_endpoint": f"GET /api/admin/industries/{industry}/validate/state",
    }


def _group_resource_health(resources: list[dict]) -> dict:
    """Group ledger rows by ``resource_type`` with a per-resource health pill."""
    groups: dict[str, list[dict]] = {}
    for res in resources:
        status = res.get("status")
        if status in _HEALTHY_STATUSES:
            pill = "healthy"
        elif status in _IGNORED_STATUSES:
            pill = "deleted"
        else:
            pill = "degraded"
        groups.setdefault(res.get("resource_type") or "unknown", []).append(
            {
                "id": res.get("id"),
                "identifier": res.get("identifier"),
                "parent_ref": res.get("parent_ref"),
                "teardown_order": res.get("teardown_order"),
                "status": status,
                "health": pill,
                "metadata": res.get("metadata") or {},
            }
        )
    return groups


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/setup — create/update (one per industry)
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/setup")
async def create_or_update_setup(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Create or update the single ``pipeline_setup`` for an industry (draft).

    Enforces one-setup-per-industry (the DB has ``UNIQUE(industry)``): if a setup
    already exists for ``industry`` it is UPDATED in place; otherwise a new draft
    row is created.

    A caller that explicitly intends to create a *brand-new* pipeline for an
    already-configured industry (body/query ``create_only`` / ``intent=create``)
    is rejected with **409** and pointed at the existing ``setup_id`` — so a
    second pipeline is never silently created for the same industry.
    """
    _require_admin(request)
    db = _require_db(state)
    industry = _normalize_curated_industry(industry)

    try:
        body = await request.json()
    except Exception:
        body = {}

    requested_modes = body.get("enabled_modes")
    if requested_modes is not None:
        if not isinstance(requested_modes, list) or not requested_modes:
            raise HTTPException(
                status_code=422,
                detail="enabled_modes must contain at least one mode",
            )
        invalid_modes = [
            mode for mode in requested_modes if mode not in deployment_service.VALID_MODES
        ]
        if invalid_modes:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"unknown modes {invalid_modes}; expected {deployment_service.VALID_MODES}"
                ),
            )

    mode_configs = body.get("mode_configs")
    if mode_configs is not None:
        mode_configs = _validate_mode_configs(mode_configs)
        if requested_modes is None:
            raise HTTPException(
                status_code=422,
                detail="enabled_modes is required when mode_configs is provided",
            )
        if set(mode_configs) != set(requested_modes):
            raise HTTPException(
                status_code=422,
                detail="mode_configs must include exactly the selected modes",
            )

    create_only = bool(body.get("create_only")) or (
        body.get("intent") == "create" or request.query_params.get("intent") == "create"
    )

    fields = {
        "flavor": body.get("flavor"),
        "source_type": body.get("source_type"),
        "bootstrap_servers": body.get("bootstrap_servers"),
        "enabled_modes": body.get("enabled_modes"),
        "mode_configs": mode_configs,
        "cluster_spec": body.get("cluster_spec"),
        "warehouse_id": body.get("warehouse_id"),
    }

    existing = await pipeline_setup_repo.get_setup_by_industry(db, industry)
    if existing is not None:
        if create_only:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": f"a pipeline setup already exists for industry '{industry}'",
                    # ``existing["id"]`` is a ``uuid.UUID`` (asyncpg maps the pg
                    # uuid column) — coerce to str, since FastAPI renders the
                    # HTTPException detail with plain json.dumps (no
                    # jsonable_encoder), and a raw UUID there raises TypeError
                    # → the intended 409 would otherwise surface as a 500.
                    "setup_id": str(existing["id"]),
                    "industry": industry,
                },
            )
        requested_modes = fields.get("enabled_modes")
        if isinstance(requested_modes, list):
            removed_modes = set(existing.get("enabled_modes") or []) - set(requested_modes)
            if removed_modes:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"removing modes requires PATCH /api/admin/pipelines/{existing['id']}/modes"
                    ),
                )
        update_fields = {k: v for k, v in fields.items() if v is not None}
        setup = await pipeline_setup_repo.update_setup(db, existing["id"], **update_fields)
        state.invalidate_industry_runtime_cache()
        return {"created": False, "setup": setup}

    caller_email = app_permissions._caller_email(request)
    setup = await pipeline_setup_repo.create_setup(
        db,
        industry=industry,
        status="draft",
        user_email=caller_email,
        **fields,
    )
    state.invalidate_industry_runtime_cache()
    return {"created": True, "setup": setup}


# ---------------------------------------------------------------------------
# PATCH /api/admin/pipelines/{setup_id}/modes/{mode}/config
# ---------------------------------------------------------------------------
@router.patch("/api/admin/pipelines/{setup_id}/modes/{mode}/config")
async def update_mode_config(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Persist a stopped mode's producer frequency for its next deployment."""
    _require_admin(request)
    if mode not in deployment_service.VALID_MODES:
        raise HTTPException(status_code=422, detail=f"unknown mode '{mode}'")

    try:
        body = await request.json()
    except Exception:
        body = {}
    requested = _validate_mode_configs({mode: {"rate_hz": body.get("rate_hz")}})

    db = _require_db(state)
    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")
    if mode not in (setup.get("enabled_modes") or []):
        raise HTTPException(status_code=404, detail="pipeline mode is not configured")

    deployments = await deployment_repo.list_deployments(db, setup_id)
    deployment = next((item for item in deployments if item.get("mode") == mode), None)
    if deployment and deployment.get("job_id"):
        job_id = str(deployment["job_id"])
        runtime = (
            await asyncio.to_thread(
                _job_running_lookup,
                [deployment],
                reconcile_failures=False,
            )
        ).get(job_id)
        if runtime is None:
            raise HTTPException(
                status_code=503,
                detail="could not confirm that the pipeline is stopped",
            )
        if runtime.get("running") or runtime.get("starting"):
            raise HTTPException(
                status_code=409,
                detail="stop the pipeline before changing its frequency",
            )

    updated = await pipeline_setup_repo.update_mode_config(db, setup_id, mode, requested[mode])
    state.invalidate_industry_runtime_cache()
    return {
        "mode": mode,
        "mode_config": (updated or {}).get("mode_configs", {}).get(mode),
        "applied": "on next start",
    }


# ---------------------------------------------------------------------------
# GET /api/admin/industries/{industry}/setup — existing setup or 404
# ---------------------------------------------------------------------------
@router.get("/api/admin/industries/{industry}/setup")
async def get_setup_for_industry(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Return the industry's single ``pipeline_setup`` (with its contract view), or 404.

    Lets the frontend detect that an industry is already configured (one-per-
    industry) before offering a "create" path.
    """
    _require_admin(request)
    db = _require_db(state)
    industry = _normalize_curated_industry(industry)

    setup = await pipeline_setup_repo.get_setup_by_industry(db, industry)
    if setup is None:
        raise HTTPException(status_code=404, detail=f"no pipeline setup for industry '{industry}'")
    if setup.get("industry") != industry:
        raise HTTPException(
            status_code=409,
            detail="setup_id does not belong to the requested industry",
        )

    deployments = await deployment_repo.list_deployments(db, setup["id"])
    running = await asyncio.to_thread(_job_running_lookup, deployments, reconcile_failures=False)
    flowing = await _flowing_lookup(state, setup, deployments, running)
    return {
        "setup": setup,
        "deployments": deployments,
        "pipeline": pipeline_view.build_pipeline_view(setup, deployments, running, flowing),
    }


def _mode_genie_spaces(resources: list[dict], mode: str, deployment_id: Any) -> list[dict]:
    """Active genie_space rows for ``mode`` (deployment-attached or setup-scoped)."""
    return [
        resource
        for resource in resources
        if resource.get("resource_type") == genie_space_service.RESOURCE_TYPE
        and (resource.get("status") or "active") == "active"
        and (
            str(resource.get("deployment_id")) == str(deployment_id)
            or (resource.get("metadata") or {}).get("mode") == mode
        )
    ]


async def _trash_mode_genie_spaces(
    db: Any, resources: list[dict], mode: str, deployment_id: Any, attached: list[dict]
) -> None:
    """Trash a removed mode's Genie space(s); 502 (deployment kept) on failure."""
    spaces = _mode_genie_spaces(resources, mode, deployment_id)
    if not spaces:
        return
    deleter = genie_space_service.make_genie_space_deleter()
    attached_ids = {resource.get("id") for resource in attached}
    for space in spaces:
        try:
            await deleter(space)
        except Exception as exc:
            logger.exception("Failed to trash %s Genie space %s", mode, space.get("identifier"))
            raise HTTPException(
                status_code=502,
                detail=f"failed to remove {mode} Genie space {space.get('identifier')}",
            ) from exc
        if space.get("id") not in attached_ids:
            await resource_ledger.mark_resource_status(db, space.get("id"), "deleted")


# ---------------------------------------------------------------------------
# PATCH /api/admin/pipelines/{setup_id}/modes — incremental add/remove
# ---------------------------------------------------------------------------
@router.patch("/api/admin/pipelines/{setup_id}/modes")
async def update_pipeline_modes(
    setup_id: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Update modes while leaving retained running lanes untouched.

    Added lanes enter the validation state as pending. Removed lanes have their
    job cancelled/deleted before the deployment row is removed; shared data
    objects remain. The setup's selected modes are authoritative for health.
    """
    _require_admin(request)
    db = _require_db(state)
    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")

    try:
        body = await request.json()
    except Exception:
        body = {}
    requested = body.get("enabled_modes")
    if not isinstance(requested, list) or not requested:
        raise HTTPException(status_code=422, detail="enabled_modes must contain at least one mode")
    invalid = [mode for mode in requested if mode not in deployment_service.VALID_MODES]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=f"unknown modes {invalid}; expected {deployment_service.VALID_MODES}",
        )

    desired = [mode for mode in validation_state_machine.MODE_ORDER if mode in requested]
    configured = setup.get("enabled_modes") or []
    current = [mode for mode in validation_state_machine.MODE_ORDER if mode in configured]
    added = [mode for mode in desired if mode not in current]
    removed = [mode for mode in current if mode not in desired]
    if not added and not removed:
        return {
            "setup": setup,
            "added": [],
            "removed": [],
            "retained": desired,
        }
    if any(
        record.get("status") == validation_state_machine.RUNNING
        for record in (setup.get("validation_state") or {}).get("steps", {}).values()
    ):
        raise HTTPException(
            status_code=409,
            detail="cannot change modes while validation is running",
        )

    deployments = await deployment_repo.list_deployments(db, setup_id)
    dep_by_mode = {deployment.get("mode"): deployment for deployment in deployments}
    resources = await resource_ledger.list_resources(db, setup_id) if removed else []

    working_modes = list(current)
    working_state = setup.get("validation_state") or {}
    for mode in removed:
        deployment = dep_by_mode.get(mode)
        attached: list[dict] = []
        if deployment is not None:
            deployment_id = deployment.get("id")
            attached = [
                resource
                for resource in resources
                if str(resource.get("deployment_id")) == str(deployment_id)
            ]
            job_resource = next(
                (resource for resource in attached if resource.get("resource_type") == "job"),
                None,
            )
            job_id = deployment.get("job_id") or (
                job_resource.get("identifier") if job_resource else None
            )
            if not job_id and pipeline_view.is_deployed(deployment):
                raise HTTPException(
                    status_code=409,
                    detail=f"cannot safely remove {mode}: deployed job id is missing",
                )
            if job_id:
                resource = job_resource or {
                    "identifier": str(job_id),
                    "resource_type": "job",
                }
                try:
                    await deployment_service.make_job_deleter()(resource)
                except Exception as exc:
                    logger.exception("Failed to remove %s job %s", mode, job_id)
                    raise HTTPException(
                        status_code=502,
                        detail=f"failed to remove {mode} job {job_id}",
                    ) from exc
            # The genie_space ledger row cascades with the deployment, so trash
            # the workspace space first or it would be orphaned.
            await _trash_mode_genie_spaces(db, resources, mode, deployment_id, attached)
            await deployment_repo.delete_deployment(db, deployment_id)
            for resource in attached:
                await resource_ledger.mark_resource_status(db, resource["id"], "deleted")

        working_modes.remove(mode)
        update_fields: dict[str, Any] = {"enabled_modes": list(working_modes)}
        if working_state.get("sequence"):
            working_state = validation_state_machine.reconcile_modes(working_state, working_modes)
            update_fields["validation_state"] = working_state
        await pipeline_setup_repo.update_setup(db, setup_id, **update_fields)

    update_fields: dict[str, Any] = {"enabled_modes": desired}
    if working_state.get("sequence"):
        working_state = validation_state_machine.reconcile_modes(working_state, desired)
        update_fields["validation_state"] = working_state
    updated = await pipeline_setup_repo.update_setup(db, setup_id, **update_fields)
    state.invalidate_industry_runtime_cache()
    return {
        "setup": updated,
        "added": added,
        "removed": removed,
        "retained": [mode for mode in desired if mode in current],
    }


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/provision — run the provisioning engine
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/provision")
async def provision_industry_endpoint(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Provision all resources for an industry and return the ledger.

    Resolves the setup by industry (or by ``setup_id`` in the body), then calls
    ``industry_provisioning_service.provision_industry`` bound to the
    app-backend DB so every resource is recorded in ``provisioned_resource``.
    """
    _require_admin(request)
    db = _require_db(state)
    industry = _normalize_curated_industry(industry)

    try:
        body = await request.json()
    except Exception:
        body = {}

    setup_id = body.get("setup_id")
    if setup_id:
        setup = await pipeline_setup_repo.get_setup(db, setup_id)
    else:
        setup = await pipeline_setup_repo.get_setup_by_industry(db, industry)

    if setup is None:
        raise HTTPException(
            status_code=404,
            detail=f"no pipeline setup for industry '{industry}' — create one first",
        )
    if setup.get("industry") != industry:
        raise HTTPException(
            status_code=409,
            detail="setup_id does not belong to the requested industry",
        )

    # Only provision the realtime Lakebase branch when the setup actually
    # enables the realtime mode — a zerobus/microbatch-only industry must not
    # touch the realtime Lakebase project (which may not exist on this workspace).
    enable_realtime = "realtime" in (setup.get("enabled_modes") or [])

    try:
        summary = await industry_provisioning_service.provision_industry(
            setup["industry"],
            setup["id"],
            db=db,
            warehouse_id=setup.get("warehouse_id"),
            enable_realtime=enable_realtime,
        )
    except RuntimeError as exc:  # failed pre-flight is a clean 409 for the admin
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    resources = await resource_ledger.list_resources(db, setup["id"])
    state.invalidate_industry_runtime_cache()
    return {"setup_id": setup["id"], "summary": summary, "resources": resources}


async def _list_pipeline_items(
    state: AppState, *, include_resources: bool, reconcile_failures: bool = False
) -> list[dict]:
    """Build live pipeline views with bounded concurrent external lookups."""
    db = _require_db(state)
    setups = await pipeline_setup_repo.list_setups(db)
    lookup_limit = asyncio.Semaphore(_LIST_PIPELINE_CONCURRENCY)

    async def list_deployments(setup: dict) -> list[dict]:
        async with lookup_limit:
            return await deployment_repo.list_deployments(db, setup["id"])

    deployment_rows = await asyncio.gather(*(list_deployments(setup) for setup in setups))
    all_deployments = {
        setup["id"]: deployments for setup, deployments in zip(setups, deployment_rows, strict=True)
    }
    combined: list[dict] = []
    for deployments in deployment_rows:
        combined.extend(deployments)

    # The SDK lookup is synchronous and network-bound. Keep it off the event
    # loop so unrelated app requests remain responsive during Jobs API calls.
    running = await asyncio.to_thread(
        _job_running_lookup,
        combined,
        reconcile_failures=reconcile_failures,
    )

    async def build_item(setup: dict) -> dict:
        deployments = all_deployments[setup["id"]]
        async with lookup_limit:
            if include_resources:
                resources, flowing = await asyncio.gather(
                    resource_ledger.list_resources(db, setup["id"]),
                    _flowing_lookup(state, setup, deployments, running),
                )
            else:
                resources = []
                flowing = await _flowing_lookup(state, setup, deployments, running)
        view = pipeline_view.build_pipeline_view(setup, deployments, running, flowing)
        item = {
            **view,
            "setup": setup,
            "deployments": deployments,
        }
        if include_resources:
            item["resource_health"] = _rollup_health(setup, deployments, resources)
        return item

    return list(await asyncio.gather(*(build_item(setup) for setup in setups)))


def _public_runtime_payload(items: list[dict]) -> dict:
    """Allowlist the non-sensitive runtime fields used by industry cards."""
    return {
        "pipelines": [
            {
                "setup_id": item.get("setup_id"),
                "industry": item.get("industry"),
                "status": item.get("status"),
                "health": item.get("health"),
                "startable": item.get("startable"),
                "ready": item.get("ready"),
                "validation_passed": item.get("validation_passed"),
                "modes": [
                    {
                        key: mode.get(key)
                        for key in (
                            "mode",
                            "status",
                            "health",
                            "startable",
                            "deployed",
                            "flowing",
                            "runtime_detail",
                            "restartable",
                        )
                    }
                    for mode in item.get("modes") or []
                ],
                "setup": {
                    "id": item["setup"]["id"],
                    "industry": item["setup"]["industry"],
                    "mode_configs": item["setup"].get("mode_configs") or {},
                },
                "deployments": [
                    {
                        "mode": deployment.get("mode"),
                        "status": deployment.get("status"),
                    }
                    for deployment in item["deployments"]
                ],
            }
            for item in items
        ]
    }


async def _refresh_industry_runtime_cache(state: AppState, generation: int) -> dict:
    items = await _list_pipeline_items(state, include_resources=False)
    payload = _public_runtime_payload(items)
    if generation == state.industry_runtime_cache_generation:
        state.industry_runtime_cache = payload
        state.industry_runtime_cached_at = time.monotonic()
    return payload


def _runtime_refresh_done(state: AppState, task: asyncio.Task) -> None:
    """Retrieve background failures so asyncio never reports an orphaned task."""
    state.industry_runtime_refresh_tasks.discard(task)
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception:
        logger.warning("Industry runtime cache refresh failed", exc_info=True)


def start_industry_runtime_refresh(state: AppState) -> asyncio.Task:
    """Start one refresh for the current cache generation."""
    generation = state.industry_runtime_cache_generation
    current = state.industry_runtime_refresh_task
    if (
        current is not None
        and not current.done()
        and state.industry_runtime_refresh_generation != generation
    ):
        current.cancel()
    task = asyncio.create_task(_refresh_industry_runtime_cache(state, generation))
    state.industry_runtime_refresh_tasks.add(task)
    task.add_done_callback(lambda done: _runtime_refresh_done(state, done))
    state.industry_runtime_refresh_task = task
    state.industry_runtime_refresh_generation = generation
    return task


@router.get("/api/industries/runtime")
async def list_industry_runtime(state: AppState = Depends(get_app_state)):
    """Return cached live state immediately and refresh stale state in background."""
    cached = state.industry_runtime_cache
    task = state.industry_runtime_refresh_task
    generation = state.industry_runtime_cache_generation
    cache_age = time.monotonic() - state.industry_runtime_cached_at
    refresh_matches = (
        task is not None
        and not task.done()
        and state.industry_runtime_refresh_generation == generation
    )

    if cached is not None:
        if cache_age >= _INDUSTRY_RUNTIME_CACHE_TTL_SECONDS and not refresh_matches:
            start_industry_runtime_refresh(state)
        return cached

    if not refresh_matches:
        task = start_industry_runtime_refresh(state)
    try:
        payload = await asyncio.wait_for(
            asyncio.shield(task),
            timeout=_INDUSTRY_RUNTIME_COLD_START_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        raise HTTPException(
            status_code=503,
            detail="Industry runtime state is still loading",
            headers={"Retry-After": "2"},
        ) from exc

    if state.industry_runtime_cache is None:
        if payload is not None:
            return payload
        raise HTTPException(
            status_code=503,
            detail="Industry runtime state is temporarily unavailable",
            headers={"Retry-After": "2"},
        )
    return state.industry_runtime_cache


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines — management list + rolled-up health
# ---------------------------------------------------------------------------
@router.get("/api/admin/pipelines")
async def list_pipelines(request: Request, state: AppState = Depends(get_app_state)):
    """Return every setup with its live contract and ledger health rollup."""
    _require_admin(request)
    items = await _list_pipeline_items(state, include_resources=True, reconcile_failures=False)
    return {"pipelines": items}


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines/{setup_id}/health — per-resource health
# ---------------------------------------------------------------------------
@router.get("/api/admin/pipelines/{setup_id}/health")
async def pipeline_health(
    setup_id: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Return per-pipeline health: **Tier-1 live API/DB checks** + ledger inventory.

    Primary health is the fast Tier-1 sweep (spec §1.16) — UC/DBSQL, Lakebase
    endpoints, the ChronoTable + CDF sync, grants/secrets, metrics freshness, and
    Jobs-API run state — run from the app backend with **no interactive cluster**.
    Each check is ``{name, tier, status, detail}``; they aggregate into the
    top-level ``health`` / ``startable`` and surface the informational
    ``cluster_state``.  The opt-in Tier-2 interactive end-to-end test (which spins
    the cluster ~10 min) is described under ``e2e_test``.

    The deploy-gate rollup (``build_pipeline_view``) is preserved under
    ``deploy_health`` / ``deploy_startable`` and the ledger inventory under
    ``resource_health`` / ``resources`` for admin detail.  When no WorkspaceClient
    is available (tests / no creds), the live checks are skipped and the endpoint
    falls back to the deploy-gate view.
    """
    _require_admin(request)
    db = _require_db(state)

    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")

    deployments = await deployment_repo.list_deployments(db, setup_id)
    resources = await resource_ledger.list_resources(db, setup_id)
    running = await asyncio.to_thread(_job_running_lookup, deployments, reconcile_failures=False)
    flowing = await _flowing_lookup(state, setup, deployments, running)

    view = pipeline_view.build_pipeline_view(setup, deployments, running, flowing)
    response = {
        **view,
        "uc_catalog": get_uc_catalog(),
        "uc_schema": get_uc_schema(),
        "app_sp_client_id": get_app_sp_client_id(),
        "setup": setup,
        "deployments": deployments,
        "resource_health": _rollup_health(setup, deployments, resources),
        "resources": _group_resource_health(resources),
        "e2e_test": _e2e_test_descriptor(
            setup.get("industry") or "", setup.get("enabled_modes") or []
        ),
        "validation_passed": bool(setup.get("enabled_modes"))
        and all(
            pipeline_view.gate_open(setup, mode) for mode in (setup.get("enabled_modes") or [])
        ),
    }

    # Tier-1 live health is the PRIMARY health for this endpoint (spec §1.16).
    api = await _gather_api_health(setup, deployments)
    if api is not None:
        response["checks"] = api["checks"]
        response["cluster_state"] = api["cluster_state"]
        response["deploy_health"] = view["health"]
        response["deploy_startable"] = view["startable"]
        if api["health"] == pipeline_view.HEALTH_UNHEALTHY:
            primary_health = pipeline_view.HEALTH_UNHEALTHY
        elif view["status"] == pipeline_view.STATUS_RUNNING:
            primary_health = api["health"]
        elif view["status"] == pipeline_view.STATUS_DRAFT:
            primary_health = pipeline_view.HEALTH_DRAFT
        elif view["status"] == pipeline_view.STATUS_STARTING:
            primary_health = pipeline_view.HEALTH_UNKNOWN
        else:
            primary_health = pipeline_view.HEALTH_UNHEALTHY
        response["health"] = primary_health
        response["startable"] = bool(
            view["startable"] and api["health"] != pipeline_view.HEALTH_UNHEALTHY
        )
        response["ready"] = response["startable"]
    else:
        response["checks"] = []
        response["cluster_state"] = None
    return response


# ---------------------------------------------------------------------------
# DELETE /api/admin/pipelines/{setup_id} — teardown + cascade-delete
# ---------------------------------------------------------------------------
@router.delete("/api/admin/pipelines/{setup_id}")
async def delete_pipeline(
    setup_id: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Tear down every provisioned resource (reverse order) then delete metadata.

    Delegates to ``teardown_service.teardown_setup``, which walks the ledger in
    descending ``teardown_order`` and, after the per-resource deletes, drops the
    ``pipeline_setup`` row — whose ``ON DELETE CASCADE`` FKs remove the
    deployments and ledger rows.
    """
    _require_admin(request)
    db = _require_db(state)

    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")

    # Build the resource registry. Catalog/schema entries are preservation-only:
    # namespace cleanup is never part of a single pipeline delete.
    try:
        deleters = resource_deleters.make_all_deleters(
            warehouse_id=setup.get("warehouse_id"),
        )
    except Exception as exc:
        logger.exception("deleter registry unavailable for setup %s", setup_id)
        raise HTTPException(
            status_code=503,
            detail=f"pipeline teardown is unavailable: {exc}",
        ) from exc

    result = await teardown_service.teardown_setup(db, setup_id, deleters)
    if not result.get("success") or not result.get("deleted_setup"):
        raise HTTPException(status_code=502, detail=result)
    state.invalidate_industry_runtime_cache()
    return result
