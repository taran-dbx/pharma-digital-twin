"""User identity and settings endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state

router = APIRouter()


@router.get("/api/me")
async def get_me(user_email: str = Depends(get_current_user)):
    return {"email": user_email}


@router.get("/api/settings")
async def get_user_settings(
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    row = await state.db.fetchrow(
        "SELECT settings FROM user_settings WHERE user_email = $1", user_email
    )
    if row:
        return json.loads(row["settings"]) if isinstance(row["settings"], str) else row["settings"]
    return {}


@router.put("/api/settings")
async def save_user_settings(
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

    await state.db.execute(
        """
        INSERT INTO user_settings (user_email, settings, updated_at)
        VALUES ($1, $2::jsonb, NOW())
        ON CONFLICT (user_email) DO UPDATE
        SET settings = $2::jsonb, updated_at = NOW()
        """,
        user_email,
        json.dumps(body),
    )

    # Mirror the warehouse selection into the APP-GLOBAL config row so DBSQL
    # (which queries as the app service principal) resolves it for every
    # caller, independent of per-user / anonymous identity.
    warehouse = body.get("warehouse") or {}
    if warehouse.get("warehouse_id"):
        from iot_twin.services.dbsql_service import APP_GLOBAL_SETTINGS_KEY

        await state.db.execute(
            """
            INSERT INTO user_settings (user_email, settings, updated_at)
            VALUES ($1, $2::jsonb, NOW())
            ON CONFLICT (user_email) DO UPDATE
            SET settings = COALESCE(user_settings.settings, '{}'::jsonb) || $2::jsonb,
                updated_at = NOW()
            """,
            APP_GLOBAL_SETTINGS_KEY,
            json.dumps({"warehouse": warehouse}),
        )

    return body
