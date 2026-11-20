"""Genie copilot HTTP API — start, follow up, and poll without blocking."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.domain.catalog import to_record_industry
from iot_twin.services import genie_service
from iot_twin.services.alerts.alert_engine import engine_for

logger = logging.getLogger("iot_digital_twin.routes.genie")

router = APIRouter()


def _http_for(exc: Exception) -> HTTPException:
    if isinstance(exc, genie_service.GenieDisabledError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, genie_service.GenieValidationError):
        return HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, asyncio.TimeoutError):
        return HTTPException(status_code=504, detail="Genie request timed out")
    logger.exception("Genie request failed")
    return HTTPException(status_code=500, detail="Genie request failed")


async def _alert_record(state: AppState, alert_id: str | None) -> dict[str, Any] | None:
    if not alert_id:
        return None
    engine = engine_for(state)
    db = state.app_backend_db if state.app_backend_available else None
    alert = await engine.get_alert(alert_id, db=db)
    if alert is None:
        raise genie_service.GenieValidationError("unknown alert")
    return alert.to_dict()


def _assert_alert_industry(alert: dict[str, Any] | None, industry: str) -> None:
    if not alert:
        return
    alert_industry = to_record_industry(str(alert.get("industry") or ""))
    requested = to_record_industry(industry)
    if alert_industry and requested and alert_industry != requested:
        raise genie_service.GenieValidationError("alert industry mismatch")


def _remember(state: AppState, conversation_id: str, owner: str, industry: str) -> None:
    state.genie_conversations[conversation_id] = {
        "owner": owner,
        "industry": to_record_industry(industry),
    }


def _require_conversation(
    state: AppState, conversation_id: str, owner: str, industry: str | None = None
) -> dict[str, str]:
    record = state.genie_conversations.get(conversation_id)
    if record is None or record.get("setup_id"):
        # Pipeline-scoped conversations belong to routes.genie_pipelines.
        raise HTTPException(status_code=404, detail="unknown conversation")
    if record.get("owner") != owner:
        raise HTTPException(status_code=404, detail="unknown conversation")
    if industry is not None and record.get("industry") != to_record_industry(industry):
        raise HTTPException(status_code=404, detail="unknown conversation")
    return record


async def _pack_for(
    state: AppState, parsed: genie_service.GenieRequest, user: str = ""
) -> dict[str, Any]:
    alert = await _alert_record(state, parsed.alert_id)
    _assert_alert_industry(alert, parsed.industry)
    pack = genie_service.build_context_pack(
        industry=parsed.industry,
        mode=parsed.mode,
        asset_id=parsed.asset_id,
        component_id=parsed.component_id,
        alert=alert,
    )
    sensor_names = [sensor["name"] for sensor in pack["catalog"]["sensors"]]
    pack["telemetry_summary"] = await genie_service.bounded_telemetry_summary(
        parsed.industry,
        parsed.mode,
        sensor_names,
        db=state.app_backend_db,
        db_available=state.app_backend_available,
        user_email=user,
    )
    return pack


async def _sdk_call(state: AppState, func: Any, *args: Any, **kwargs: Any) -> Any:
    async with state.genie_sdk_semaphore:
        return await asyncio.wait_for(
            asyncio.to_thread(func, *args, **kwargs),
            timeout=genie_service.SDK_TIMEOUT_S,
        )


async def _require_conversations_authorized(state: AppState) -> None:
    genie_service.require_enabled()
    await _sdk_call(state, genie_service.require_table_allowlist)


@router.get("/api/genie/status")
async def genie_status(
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    return await _sdk_call(state, genie_service.status_payload)


@router.post("/api/genie/conversations")
async def start_conversation(
    request: Request,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        await _require_conversations_authorized(state)
        body: dict[str, Any] = await request.json()
        parsed = genie_service.validate_request(body)
        pack = await _pack_for(state, parsed, user)
        result = await _sdk_call(state, genie_service.start_conversation, parsed.question, pack)
        _remember(state, result["conversation_id"], user, parsed.industry)
        return result
    except (genie_service.GenieDisabledError, genie_service.GenieValidationError) as exc:
        raise _http_for(exc) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_for(exc) from exc


@router.post("/api/genie/conversations/{conversation_id}/messages")
async def add_message(
    conversation_id: str,
    request: Request,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        await _require_conversations_authorized(state)
        body: dict[str, Any] = await request.json()
        parsed = genie_service.validate_request(body)
        _require_conversation(state, conversation_id, user, parsed.industry)
        pack = await _pack_for(state, parsed, user)
        return await _sdk_call(
            state,
            genie_service.create_message,
            conversation_id,
            parsed.question,
            pack,
        )
    except (genie_service.GenieDisabledError, genie_service.GenieValidationError) as exc:
        raise _http_for(exc) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_for(exc) from exc


@router.get("/api/genie/conversations/{conversation_id}/messages/{message_id}")
async def poll_message(
    conversation_id: str,
    message_id: str,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        await _require_conversations_authorized(state)
        _require_conversation(state, conversation_id, user)
        return await _sdk_call(state, genie_service.get_message, conversation_id, message_id)
    except (genie_service.GenieDisabledError, genie_service.GenieValidationError) as exc:
        raise _http_for(exc) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_for(exc) from exc
