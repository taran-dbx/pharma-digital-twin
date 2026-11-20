"""WebSocket and Server-Sent Events endpoints."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Depends, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.domain.catalog import to_record_industry
from iot_twin.services.alerts.alert_engine import engine_for
from iot_twin.services.alerts.alert_snapshot_sync import mark_snapshot_dirty
from iot_twin.services.alerts.alert_sse import build_lifecycle_payload, enqueue_alert_events

logger = logging.getLogger("iot_digital_twin.routes.websocket")
router = APIRouter()


# ---------------------------------------------------------------------------
# SSE endpoint (preferred — works through Databricks App proxy)
# ---------------------------------------------------------------------------


@router.get("/api/events")
async def sse_events(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    """Server-Sent Events endpoint for real-time sensor data."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    industry = (request.query_params.get("industry") or "").strip()
    if industry:
        queue.filter_industry = to_record_industry(industry)
    state.sse_clients.append(queue)
    logger.info("SSE client connected (%d total)", len(state.sse_clients))

    async def event_stream():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {json.dumps(data, default=str)}\n\n"
                except TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            try:
                state.sse_clients.remove(queue)
            except ValueError:
                pass
            logger.info("SSE client disconnected (%d remaining)", len(state.sse_clients))

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# WebSocket endpoint (fallback — blocked by Databricks App proxy)
# ---------------------------------------------------------------------------


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, state: AppState = Depends(get_app_state)):
    await ws.accept()
    state.connected_clients.append(ws)
    logger.info("WebSocket client connected (%d total)", len(state.connected_clients))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        try:
            state.connected_clients.remove(ws)
        except ValueError:
            pass
        logger.info("WebSocket client disconnected (%d remaining)", len(state.connected_clients))


# ---------------------------------------------------------------------------
# SSS threshold-alert ingest endpoint
# ---------------------------------------------------------------------------


@router.post("/api/sss/alerts")
async def sss_alerts_ingest(request: Request, user_email: str = Depends(require_identity)):
    """Accept legacy SSS threshold alerts and emit full lifecycle events."""
    state: AppState = request.app.state.app_state
    body = await request.json()
    alerts = body.get("alerts", [])
    if isinstance(alerts, list) and len(alerts) > 200:
        alerts = alerts[:200]

    if not alerts:
        return JSONResponse(content={"status": "ok", "count": 0})

    accepted: list[dict] = []
    for raw in alerts:
        if not isinstance(raw, dict):
            continue
        if not str(raw.get("industry") or "").strip():
            logger.warning("sss_alerts_ingest: rejecting alert without industry")
            continue
        accepted.append(raw)
    if not accepted:
        return JSONResponse(content={"status": "ok", "count": 0})

    engine = engine_for(state)
    db = state.app_backend_db if state.app_backend_available else None
    transitions = await engine.ingest_threshold_alerts(accepted, db=db)
    payloads = [
        build_lifecycle_payload(item.event_type, item.alert, item.extra) for item in transitions
    ]
    enqueue_alert_events(state, payloads)
    if transitions:
        mark_snapshot_dirty(state)

    logger.info(
        "sss_alerts_ingest: %d alerts received, %d lifecycle events dispatched to %d clients (user=%s)",
        len(alerts),
        len(payloads),
        len(state.sse_clients),
        user_email,
    )
    return JSONResponse(content={"status": "ok", "count": len(alerts)})
