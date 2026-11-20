"""Protected intelligence catalog sync for Genie reference tables."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions, intelligence_catalog_service
from iot_twin.services.alerts.alert_engine import engine_for
from iot_twin.services.alerts.alert_snapshot_sync import AlertSnapshotSynchronizer

router = APIRouter()


def _require_admin(request: Request) -> None:
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")


def _contract(state: AppState) -> dict:
    return intelligence_catalog_service.alert_snapshot_contract(
        getattr(state, "alert_snapshot_synchronizer", None)
    )


@router.get("/api/admin/intelligence/status")
async def intelligence_status(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    _require_admin(request)
    return {"contract": _contract(state)}


@router.post("/api/admin/intelligence/sync")
async def sync_intelligence_catalog(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    _require_admin(request)
    engine = engine_for(state)
    db = state.app_backend_db if state.app_backend_available else None
    alerts = await engine.list_alerts(db=db)
    payload = [item.to_dict() for item in alerts]
    syncer = getattr(state, "alert_snapshot_synchronizer", None)
    if syncer is None:
        syncer = AlertSnapshotSynchronizer(state)
        state.alert_snapshot_synchronizer = syncer
    result = await syncer.run_full_sync(alerts=payload)
    return {
        "provision": result["provision"],
        "sync": result["sync"],
        "contract": _contract(state),
    }
