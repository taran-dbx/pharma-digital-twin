"""Admin per-mode pipeline deploy / lifecycle endpoints (spec §2.6, Phase 5b).

Thin HTTP surface over ``iot_twin.services.deployment.deployment_service``:

    POST /api/admin/pipelines/{setup_id}/deploy/{mode}   deploy the mode's job(s)
    POST /api/admin/pipelines/{setup_id}/{mode}/start     trigger a run
    POST /api/admin/pipelines/{setup_id}/{mode}/stop      cancel active runs
    GET  /api/admin/pipelines/{setup_id}/{mode}/status    latest run state
    POST /api/admin/pipelines/{setup_id}/genie/{mode}     Genie space for an
         already-deployed pipeline mode (no job/stream changes)

The consumer/producer notebook sources are the P5a-generated artifacts
(``client/src/lib/notebook-template.ts`` / ``data-producer-notebook.ts``),
passed in the request body.  Deploy is hard-gated on validation
(``deployment_service.assert_gate_passed``); the gate can be explicitly
overridden by the caller with ``gate_override=true``.

All endpoints are admin-gated (same ``app_permissions.is_admin`` check as the
rest of the admin surface).  Additive — this router is registered in
``routes/__init__.py`` (``create_api_router``); ``app.py`` is untouched.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions, cdf_reconciliation_service
from iot_twin.services.deployment import deployment_repo, deployment_service
from iot_twin.services.genie import genie_provisioning

logger = logging.getLogger("iot_digital_twin.routes.pipeline_deploy")

router = APIRouter()


def _require_admin(request: Request) -> None:
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")


def _require_db(state: AppState) -> Any:
    db = getattr(state, "app_backend_db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="app-backend metadata database is unavailable")
    return db


def _validate_mode(mode: str) -> None:
    if mode not in deployment_service.VALID_MODES:
        raise HTTPException(
            status_code=422,
            detail=f"unknown mode '{mode}' (expected one of {deployment_service.VALID_MODES})",
        )


def _invalidate_industry_runtime_cache(state: AppState) -> None:
    state.invalidate_industry_runtime_cache()


async def _resolve_job_id(db: Any, setup_id: str, mode: str) -> str:
    deployment = await deployment_repo.get_deployment_by_mode(db, setup_id, mode)
    job_id = deployment.get("job_id") if deployment else None
    if not job_id:
        raise HTTPException(
            status_code=404,
            detail=f"no deployed job for setup {setup_id} mode '{mode}'",
        )
    return job_id


@router.post("/api/admin/pipelines/{setup_id}/deploy/{mode}")
async def deploy_pipeline_endpoint(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Deploy the per-mode consumer/producer job(s) for a validated setup."""
    _require_admin(request)
    _validate_mode(mode)
    db = _require_db(state)

    try:
        body = await request.json()
    except Exception:
        body = {}

    try:
        result = await deployment_service.deploy_pipeline(
            db,
            setup_id,
            mode,
            consumer_notebook=body.get("consumer_notebook", ""),
            producer_notebook=body.get("producer_notebook", ""),
            node_type=body.get("node_type"),
            num_workers=body.get("num_workers"),
            single_user_name=body.get("single_user_name"),
            base_parameters=body.get("base_parameters"),
            gate_override=bool(body.get("gate_override", False)),
        )
    except deployment_service.DeployGateError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": str(exc), "outstanding": exc.outstanding},
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except cdf_reconciliation_service.RealtimeCdfError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except deployment_service.RuntimeOAuthCredentialError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _invalidate_industry_runtime_cache(state)
    return result


@router.post("/api/admin/pipelines/{setup_id}/genie/{mode}")
async def provision_genie_space_endpoint(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Create-or-update the Genie space of an EXISTING deployed pipeline mode.

    ``setup_id`` may be the ``pipeline_setup.id`` or the mode's
    ``pipeline_deployment.id``.  Touches only the Genie space + its ledger row.
    """
    _require_admin(request)
    _validate_mode(mode)
    db = _require_db(state)
    try:
        return await genie_provisioning.provision_existing_pipeline_space(db, setup_id, mode)
    except genie_provisioning.GenieProvisionError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/api/admin/pipelines/{setup_id}/{mode}/start")
async def start_pipeline_endpoint(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Trigger a run of the mode's deployed job."""
    _require_admin(request)
    _validate_mode(mode)
    db = _require_db(state)
    try:
        result = await deployment_service.start_deployment(db, setup_id, mode)
    except cdf_reconciliation_service.RealtimeCdfError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except deployment_service.RuntimeOAuthCredentialError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _invalidate_industry_runtime_cache(state)
    return result


@router.post("/api/admin/pipelines/{setup_id}/realtime/reconcile-cdf")
async def reconcile_realtime_cdf_endpoint(
    setup_id: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Reconcile an existing realtime deployment's exact CDF binding."""
    _require_admin(request)
    db = _require_db(state)
    try:
        return await cdf_reconciliation_service.reconcile_realtime_cdf(db, setup_id)
    except cdf_reconciliation_service.RealtimeCdfError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/admin/pipelines/{setup_id}/{mode}/stop")
async def stop_pipeline_endpoint(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Cancel active runs of the mode's deployed job."""
    _require_admin(request)
    _validate_mode(mode)
    db = _require_db(state)
    job_id = await _resolve_job_id(db, setup_id, mode)
    result = deployment_service.stop_pipeline(job_id, continuous=mode != "zerobus")
    _invalidate_industry_runtime_cache(state)
    return result


@router.get("/api/admin/pipelines/{setup_id}/{mode}/status")
async def pipeline_status_endpoint(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Return the latest run state of the mode's deployed job."""
    _require_admin(request)
    _validate_mode(mode)
    db = _require_db(state)
    job_id = await _resolve_job_id(db, setup_id, mode)
    return deployment_service.pipeline_status(job_id)
