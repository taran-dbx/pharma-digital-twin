"""Alert list, detail, acknowledge, and evaluate endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions
from iot_twin.services.alerts.alert_engine import engine_for, evaluate_and_emit
from iot_twin.services.alerts.alert_history import alert_episodes
from iot_twin.services.alerts.alert_responses import (
    RESPONSE_STATIONS,
    response_event,
    response_store_for,
)
from iot_twin.services.alerts.alert_sse import build_lifecycle_payload, enqueue_alert_events
from iot_twin.services.production_control import (
    CELL_ASSETS,
    ProductionControlError,
    production_control_for,
    production_event,
)

router = APIRouter()

# decision -> production stop reason recorded when the decision halts the cell
STOPPING_ACTIONS: dict[str, str] = {"send_to_inspect": "inspection"}


def _backend_db(state: AppState):
    if state.app_backend_available:
        return state.app_backend_db
    return None


@router.get("/api/alerts")
async def list_alerts(
    status: str | None = Query(None),
    industry: str | None = Query(None),
    severity: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    engine = engine_for(state)
    alerts = await engine.list_alerts(
        status=status,
        industry=industry,
        severity=severity,
        db=_backend_db(state),
        limit=limit,
        offset=offset,
    )
    return {
        "alerts": [item.to_dict() for item in alerts],
        "limit": limit,
        "offset": offset,
    }


@router.get("/api/alerts/history")
async def alert_history(
    industry: str = Query(...),
    hours: float = Query(1.0, gt=0, le=24),
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    db = _backend_db(state)
    since = datetime.now(UTC) - timedelta(hours=hours)
    episodes = await alert_episodes(engine_for(state), industry, since, db=db)
    production = []
    if industry in CELL_ASSETS:
        changes = await production_control_for(state).history(industry, since, db=db)
        production = [c.to_dict() for c in changes]
    return {
        "industry": industry,
        "since": since.isoformat(),
        "episodes": episodes,
        "production": production,
    }


@router.get("/api/alerts/{alert_id}")
async def get_alert(
    alert_id: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    engine = engine_for(state)
    alert = await engine.get_alert(alert_id, db=_backend_db(state))
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert.to_dict()


@router.post("/api/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    request: Request,
    alert_id: str,
    state: AppState = Depends(get_app_state),
    user_email: str = Depends(require_identity),
):
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")
    engine = engine_for(state)
    existing = await engine.get_alert(alert_id, db=_backend_db(state))
    if existing is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    transition = await engine.acknowledge(alert_id, user=user_email, db=_backend_db(state))
    if transition is None:
        raise HTTPException(status_code=409, detail="Alert cannot be acknowledged")
    payload = build_lifecycle_payload(transition.event_type, transition.alert)
    enqueue_alert_events(state, [payload])
    syncer = getattr(state, "alert_snapshot_synchronizer", None)
    if syncer is not None:
        syncer.mark_dirty()
    return payload


@router.get("/api/alerts/{alert_id}/responses")
async def list_alert_responses(
    alert_id: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    db = _backend_db(state)
    alert = await engine_for(state).get_alert(alert_id, db=db)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    responses = await response_store_for(state).list_for(alert, db=db)
    return {
        "alert_id": alert.id,
        "episode_opened_at": alert.opened_at.isoformat() if alert.opened_at else None,
        "responses": [item.to_dict() for item in responses],
    }


@router.post("/api/alerts/{alert_id}/responses")
async def record_alert_response(
    request: Request,
    alert_id: str,
    state: AppState = Depends(get_app_state),
    user_email: str = Depends(require_identity),
):
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")
    body: dict[str, Any] = await request.json()
    action = str(body.get("action") or "")
    if action not in RESPONSE_STATIONS:
        raise HTTPException(
            status_code=422,
            detail=f"action must be one of: {', '.join(RESPONSE_STATIONS)}",
        )
    db = _backend_db(state)
    alert = await engine_for(state).get_alert(alert_id, db=db)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    response, created = await response_store_for(state).record(
        alert, action, recorded_by=user_email, db=db
    )
    payload = response_event(alert, response)
    events = [payload] if created else []
    result: dict[str, Any] = {**payload, "created": created}
    if created and action in STOPPING_ACTIONS and alert.industry in CELL_ASSETS:
        try:
            change, changed = await production_control_for(state).set_state(
                alert.industry,
                "stopped",
                actor=user_email,
                reason=STOPPING_ACTIONS[action],
                alert_id=alert.id,
                db=db,
            )
            result["production"] = change
            if changed:
                events.append(production_event(change))
        except ProductionControlError as exc:
            result["production_error"] = str(exc)
    enqueue_alert_events(state, events)
    return result


@router.post("/api/alerts/evaluate")
async def evaluate_alerts(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")
    body: dict[str, Any] = await request.json()
    readings = body.get("readings") or []
    if isinstance(readings, list) and len(readings) > 500:
        readings = readings[:500]
    industry = body.get("industry")
    pipeline_id = body.get("pipeline_id")
    transitions = await evaluate_and_emit(
        state,
        readings,
        industry=str(industry) if industry else None,
        pipeline_id=str(pipeline_id) if pipeline_id else None,
    )
    payloads = [
        build_lifecycle_payload(item.event_type, item.alert, item.extra) for item in transitions
    ]
    return JSONResponse(
        content={
            "transitions": payloads,
            "alerts": [item.alert.to_dict() for item in transitions],
        }
    )
