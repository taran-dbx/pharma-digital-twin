"""Simulator control API — status and restart."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, Request

from iot_twin.core.state import AppState, get_app_state
from iot_twin.services.simulator_service import run_simulator

logger = logging.getLogger("iot_digital_twin.routes.simulator")
router = APIRouter()


@router.get("/api/simulator/status")
async def simulator_status(state: AppState = Depends(get_app_state)):
    """Return the currently active industry and whether the simulator is running."""
    return {
        "industries": state.industries.split(","),
        "running": state.simulator_task is not None and not state.simulator_task.done(),
    }


@router.post("/api/simulator/restart")
async def restart_simulator(request: Request, state: AppState = Depends(get_app_state)):
    """Cancel the running simulator and start a new one with the given industries."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    new_industries = body.get("industries", ["pharmaceutical"])
    state.industries = ",".join(new_industries)

    new_rate = body.get("update_rate_hz")
    if new_rate is not None:
        rate = max(0.1, min(float(new_rate), 60.0))
        state.update_rate_hz = rate

    logger.info(
        "Simulator restart requested — industries=%s, update_rate_hz=%s",
        state.industries,
        state.update_rate_hz,
    )

    if state.simulator_task and not state.simulator_task.done():
        state.simulator_task.cancel()
        try:
            await state.simulator_task
        except (asyncio.CancelledError, Exception):
            pass
        await asyncio.sleep(0.5)

    state.sources["websocket"].total_records = 0
    state.sources["websocket"].status = "initializing"

    state.simulator_task = asyncio.create_task(run_simulator(state))
    return {"industries": new_industries, "status": "restarting"}
