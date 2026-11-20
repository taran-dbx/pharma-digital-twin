"""AI action plan endpoints (grounded, audited, advisory).

* ``POST /api/alerts/{alert_id}/action-plan`` — generate and log a plan (admin,
  like the other alert writes). Admission-controlled (429 + Retry-After): a
  concurrency cap, one generation per alert, a per-user cooldown. Returns the
  latest plan (200, ``reused: true``) when its grounding is unchanged, unless
  ``force: true``.
* ``GET  /api/alerts/{alert_id}/action-plan`` — prior plans for the alert.
* ``GET  /api/genai/audit/{plan_id}`` — the full grounding trace of one plan.

Nothing here changes an alert or a response: applying a plan means signing a
decision through ``POST /api/alerts/{alert_id}/responses`` with
``action_plan_id``, which records who applied it and when.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions
from iot_twin.services.alerts.alert_engine import engine_for
from iot_twin.services.alerts.alert_responses import ResponseStoreUnavailable, response_store_for
from iot_twin.services.genai.action_plan import (
    ActionPlanModelError,
    PlanRequest,
    dbsql_runner,
    generate_action_plan,
    prepare,
    readings_query,
    reusable_plan,
)
from iot_twin.services.genai.audit_store import AuditStoreUnavailable, audit_store_for
from iot_twin.services.genai.audit_sync import mark_audit_dirty
from iot_twin.services.genai.grounding import GroundingSources
from iot_twin.services.genai.limits import RateLimited, limiter_for
from iot_twin.services.genai.status import status_payload
from iot_twin.services.industry.industry_templates import MODES

router = APIRouter()

_BODY_KEYS = frozenset({"mode", "force"})
_ERROR_STATUS = {"timeout": 504, "endpoint": 502}


def _backend_db(state: AppState) -> Any | None:
    return state.app_backend_db if state.app_backend_available else None


def require_uuid(value: str, name: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise HTTPException(status_code=422, detail=f"{name} must be a UUID") from exc


async def _parse_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw.strip():
        return {}
    try:
        body = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="body must be JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="body must be a JSON object")
    unknown = sorted(set(body) - _BODY_KEYS)
    if unknown:
        raise HTTPException(status_code=422, detail=f"unknown field(s): {', '.join(unknown)}")
    mode = body.get("mode")
    if mode is not None and mode not in MODES:
        raise HTTPException(status_code=422, detail=f"mode must be one of: {', '.join(MODES)}")
    if not isinstance(body.get("force", False), bool):
        raise HTTPException(status_code=422, detail="force must be true or false")
    return body


async def _sources(state: AppState, alert: Any, mode: str | None, user: str) -> GroundingSources:
    db = _backend_db(state)
    now = datetime.now(UTC)
    responses = await response_store_for(state).list_for(alert, db=db)
    others = await engine_for(state).list_alerts(industry=alert.industry, db=db, limit=50)
    sql = readings_query(alert, mode, now)
    runner = None
    if sql is not None:
        runner = await dbsql_runner(state.db, state.db_available, user)
    return GroundingSources(
        db=db, responses=responses, other_alerts=others, readings_sql=sql, readings_runner=runner
    )


@router.post("/api/alerts/{alert_id}/action-plan")
async def create_action_plan(
    request: Request,
    alert_id: str,
    state: AppState = Depends(get_app_state),
    user_email: str = Depends(require_identity),
):
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")
    alert_id = require_uuid(alert_id, "alert_id")
    body = await _parse_body(request)
    db = _backend_db(state)
    alert = await engine_for(state).get_alert(alert_id, db=db)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    try:
        async with limiter_for(state).admit(user_email, alert_id):
            return await _generate(state, alert, body, user_email, db)
    except RateLimited as exc:
        return JSONResponse(
            status_code=429,
            content={"detail": str(exc)},
            headers={"Retry-After": str(exc.retry_after_s)},
        )


async def _generate(state: AppState, alert: Any, body: dict[str, Any], user: str, db: Any):
    store = audit_store_for(state)
    request = PlanRequest(alert=alert, requested_by=user)
    try:
        sources = await _sources(state, alert, body.get("mode"), user)
        prepared = await prepare(request, sources)
        if not body.get("force"):
            reused = await reusable_plan(store, prepared, alert.id, db=db)
            if reused is not None:
                return JSONResponse(status_code=200, content={**reused.summary(), "reused": True})
        record = await generate_action_plan(request, sources, store, db=db, prepared=prepared)
    except (ResponseStoreUnavailable, AuditStoreUnavailable) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ActionPlanModelError as exc:
        mark_audit_dirty(state)
        return JSONResponse(
            status_code=_ERROR_STATUS.get(exc.kind, 502),
            content={"detail": f"AI endpoint did not answer: {exc}", "plan": exc.record.summary()},
        )
    mark_audit_dirty(state)
    return JSONResponse(status_code=201, content={**record.summary(), "reused": False})


@router.get("/api/alerts/{alert_id}/action-plan")
async def list_action_plans(
    alert_id: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    alert_id = require_uuid(alert_id, "alert_id")
    db = _backend_db(state)
    alert = await engine_for(state).get_alert(alert_id, db=db)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    try:
        plans = await audit_store_for(state).list_for_alert(alert_id, db=db)
    except AuditStoreUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"alert_id": alert_id, "plans": [plan.summary() for plan in plans]}


@router.get("/api/genai/status")
async def genai_status(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    """Pseudonym key source, append-only enforcement and call limits (admin only)."""
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")
    return await status_payload(state, _backend_db(state))


@router.get("/api/genai/audit/{plan_id}")
async def get_action_plan_trace(
    plan_id: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    plan_id = require_uuid(plan_id, "plan_id")
    try:
        record = await audit_store_for(state).get(plan_id, db=_backend_db(state))
    except AuditStoreUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if record is None:
        raise HTTPException(status_code=404, detail="Action plan not found")
    return record.trace()
