"""Admin validation-flow API (plan Phase 4; spec §1.10–1.13).

Drives the persisted validation state machine
(``services/validation/validation_service.py`` + ``pipeline_setup.validation_state``) over
admin-gated HTTP endpoints:

    POST /api/admin/industries/{industry}/validate/start        start/restart the flow
    GET  /api/admin/industries/{industry}/validate/state        poll live step state
    POST /api/admin/industries/{industry}/validate/retry/{step} retry a failed step + downstream
    POST /api/admin/industries/{industry}/validate/cdf-enabled  release the cdf_manual park
    GET  /api/admin/industries/{industry}/validate/can-deploy   hard deploy-gate verdict

A full run provisions an interactive cluster and runs bounded streaming tests —
minutes of work — so the mutating endpoints persist the pre-run state
**synchronously**, kick the driver off in the **background**, and return the
current state immediately.  The frontend polls ``.../validate/state`` for live
progress (Phase 6).

Admin-gated via ``app_permissions.is_admin`` and backed by the additive
``state.app_backend_db`` pool (503 when unavailable), exactly like
``routes/industries_admin.py``.  Additive only — registers its own router.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions
from iot_twin.services.validation import validation_service
from iot_twin.services.validation.validation_service import IncompleteSetupError, ValidationError

logger = logging.getLogger("iot_digital_twin.routes.validation_admin")

router = APIRouter()

# Strong refs to background driver tasks so they are not garbage-collected.
_BG_TASKS: set[asyncio.Task] = set()


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------
def _require_admin(request: Request) -> None:
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")


def _require_db(state: AppState) -> Any:
    db = getattr(state, "app_backend_db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="app-backend metadata database is unavailable")
    return db


def _run_in_background(db: Any, setup: dict) -> None:
    """Schedule the state-machine driver for ``setup`` on the running loop."""

    async def _drive() -> None:
        try:
            await validation_service.run_machine(db, setup)
        except Exception as exc:  # pragma: no cover - defensive (logged only)
            logger.warning(
                "background validation run for %s failed: %s",
                setup.get("industry"),
                exc,
            )

    task = asyncio.create_task(_drive())
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


# ---------------------------------------------------------------------------
# POST .../validate/start
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/validate/start")
async def validate_start(industry: str, request: Request, state: AppState = Depends(get_app_state)):
    """Initialize a fresh validation state and run the flow in the background."""
    _require_admin(request)
    db = _require_db(state)
    try:
        setup = await validation_service.prepare_start(db, industry)
    except IncompleteSetupError as exc:
        # Draft missing required inputs — a clear 400, never a 500, so the button
        # is safe to press on an incomplete draft.
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _run_in_background(db, setup)
    return {"industry": industry, "validation_state": setup["validation_state"]}


# ---------------------------------------------------------------------------
# GET .../validate/state
# ---------------------------------------------------------------------------
@router.get("/api/admin/industries/{industry}/validate/state")
async def validate_state(industry: str, request: Request, state: AppState = Depends(get_app_state)):
    """Return the persisted validation state (poll for live progress)."""
    _require_admin(request)
    db = _require_db(state)
    vstate = await validation_service.get_state(db, industry)
    if vstate is None:
        # Distinguish "no setup" from "not started" for a clear client message.
        from iot_twin.services.pipelines import pipeline_setup_repo

        if await pipeline_setup_repo.get_setup_by_industry(db, industry) is None:
            raise HTTPException(
                status_code=404, detail=f"no pipeline setup for industry '{industry}'"
            )
    return {"industry": industry, "validation_state": vstate}


# ---------------------------------------------------------------------------
# POST .../validate/retry/{step}
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/validate/retry/{step}")
async def validate_retry(
    industry: str,
    step: str,
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Reset a failed step + its downstream, then re-run from there (§1.12).

    ``step`` is the flat state key (e.g. ``realtime:cdf_verify`` or
    ``topics_created``).
    """
    _require_admin(request)
    db = _require_db(state)
    try:
        setup = await validation_service.prepare_retry(db, industry, step)
    except ValidationError as exc:
        detail = str(exc)
        code = 404 if "no pipeline setup" in detail else 400
        raise HTTPException(status_code=code, detail=detail) from exc
    _run_in_background(db, setup)
    return {"industry": industry, "retried": step, "validation_state": setup["validation_state"]}


# ---------------------------------------------------------------------------
# POST .../validate/cdf-enabled
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/validate/cdf-enabled")
async def validate_cdf_enabled(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Admin confirms managed CDF sync is enabled — release the park and continue.

    Optional JSON body ``{"mode": "realtime"}`` (defaults to ``realtime``).
    """
    _require_admin(request)
    db = _require_db(state)
    try:
        body = await request.json()
    except Exception:
        body = {}
    mode = body.get("mode", "realtime")
    try:
        setup = await validation_service.prepare_cdf_enabled(db, industry, mode)
    except ValidationError as exc:
        detail = str(exc)
        code = 404 if "no pipeline setup" in detail else 400
        raise HTTPException(status_code=code, detail=detail) from exc
    _run_in_background(db, setup)
    return {"industry": industry, "mode": mode, "validation_state": setup["validation_state"]}


# ---------------------------------------------------------------------------
# GET .../validate/can-deploy
# ---------------------------------------------------------------------------
@router.get("/api/admin/industries/{industry}/validate/can-deploy")
async def validate_can_deploy(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Return the **per-mode** deploy-gate verdict, aligned with ``deploy_pipeline``.

    Response carries an enabled-set aggregate (``can_deploy`` / ``data_purged`` /
    ``outstanding``) plus a ``modes`` map of per-mode verdicts so the frontend can
    enable Deploy per lane.
    """
    _require_admin(request)
    db = _require_db(state)
    try:
        gate = await validation_service.can_deploy(db, industry)
    except ValidationError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"industry": industry, **gate}
