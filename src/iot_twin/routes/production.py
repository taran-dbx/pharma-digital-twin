"""Cell production state: read, stop and resume."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions
from iot_twin.services.alerts.alert_sse import enqueue_alert_events
from iot_twin.services.production_control import (
    CELL_ASSETS,
    PRODUCTION_STATES,
    ProductionControlError,
    production_control_for,
    production_event,
)

router = APIRouter()


def _backend_db(state: AppState):
    if state.app_backend_available:
        return state.app_backend_db
    return None


def _require_cell(industry: str) -> None:
    if industry not in CELL_ASSETS:
        raise HTTPException(status_code=404, detail=f"{industry} has no controllable cell")


@router.get("/api/industries/{industry}/production")
async def get_production(
    industry: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    _require_cell(industry)
    return await production_control_for(state).current(industry, db=_backend_db(state))


@router.get("/api/industries/{industry}/production/events")
async def list_production_events(
    industry: str,
    hours: float = Query(2.0, gt=0, le=48),
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    _require_cell(industry)
    since = datetime.now(UTC) - timedelta(hours=hours)
    changes = await production_control_for(state).history(industry, since, db=_backend_db(state))
    return {"industry": industry, "events": [c.to_dict() for c in changes]}


@router.post("/api/industries/{industry}/production")
async def set_production(
    request: Request,
    industry: str,
    state: AppState = Depends(get_app_state),
    user_email: str = Depends(require_identity),
):
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")
    _require_cell(industry)
    body: dict[str, Any] = await request.json()
    target = str(body.get("state") or "")
    if target not in PRODUCTION_STATES:
        raise HTTPException(
            status_code=422, detail=f"state must be one of: {', '.join(PRODUCTION_STATES)}"
        )
    try:
        change, changed = await production_control_for(state).set_state(
            industry,
            target,
            actor=user_email,
            reason=str(body["reason"]) if body.get("reason") else None,
            alert_id=str(body["alert_id"]) if body.get("alert_id") else None,
            db=_backend_db(state),
        )
    except ProductionControlError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if changed:
        enqueue_alert_events(state, [production_event(change)])
    return {**change, "changed": changed}
