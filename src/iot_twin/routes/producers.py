"""Per-pipeline data producer control endpoints + legacy Kafka compat."""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.state import AppState, get_app_state
from iot_twin.models.producer_state import new_producer_state
from iot_twin.services.producer_service import (
    poll_producer_state,
    start_data_producer,
    stop_data_producer,
)

logger = logging.getLogger("iot_digital_twin.routes.producers")

router = APIRouter()


@router.get("/api/pipelines/{pipeline_id}/producer/status")
async def producer_status_endpoint(pipeline_id: str, state: AppState = Depends(get_app_state)):
    """Return the current producer state for a pipeline, polling the run if active."""
    ps = state.producer_states.get(pipeline_id, new_producer_state())
    return poll_producer_state(ps)


@router.post("/api/pipelines/{pipeline_id}/producer/start")
async def producer_start_endpoint(
    pipeline_id: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Submit a serverless data producer notebook for a pipeline."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    source_type = body.get("source_type", "")
    destination = body.get("destination", "")
    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "notebook content is required"})
    if not source_type:
        return JSONResponse(status_code=422, content={"error": "source_type is required"})

    # Derive metrics table FQN from pipeline's UC catalog
    base_parameters: dict[str, str] = {}
    if state.db_available and state.db:
        try:
            row = await state.db.fetchrow(
                "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id
            )
            if row:
                resources = (
                    json.loads(row["resources"] or "{}")
                    if isinstance(row["resources"], str)
                    else (row["resources"] or {})
                )
                uc_catalog = resources.get("uc_catalog")
                if uc_catalog:
                    # Metrics are per-mode now (streaming_metrics_<mode>); the legacy
                    # per-pipeline producer is realtime-shaped, so point at that table.
                    base_parameters["metrics_table"] = (
                        f"{uc_catalog}.metrics.streaming_metrics_realtime"
                    )
        except Exception as exc:
            logger.warning("Could not resolve metrics table for pipeline %s: %s", pipeline_id, exc)

    result = await start_data_producer(
        state.producer_states,
        pipeline_id,
        notebook_content,
        source_type,
        destination,
        base_parameters,
    )
    status_code = 200 if result.get("running") or result.get("state") == "STARTING" else 500
    return JSONResponse(status_code=status_code, content=result)


@router.post("/api/pipelines/{pipeline_id}/producer/stop")
async def producer_stop_endpoint(pipeline_id: str, state: AppState = Depends(get_app_state)):
    """Cancel the running data producer for a pipeline."""
    result = await stop_data_producer(state.producer_states, pipeline_id)
    return result


# Backward-compat aliases for old /api/kafka-producer/* endpoints
@router.get("/api/kafka-producer/status")
async def kafka_producer_status_compat(state: AppState = Depends(get_app_state)):
    """Legacy endpoint — returns the first active producer or IDLE."""
    for ps in state.producer_states.values():
        if ps.get("running") or ps.get("state") not in ("IDLE", None):
            return poll_producer_state(ps)
    return new_producer_state()


@router.post("/api/kafka-producer/start")
async def kafka_producer_start_compat(request: Request, state: AppState = Depends(get_app_state)):
    """Legacy endpoint — delegates to per-pipeline producer."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    pipeline_id = body.get("pipeline_id", "")
    if not pipeline_id:
        return JSONResponse(status_code=422, content={"error": "pipeline_id is required"})

    notebook_content = body.get("notebook", "")
    topic = body.get("topic", "")
    result = await start_data_producer(
        state.producer_states, pipeline_id, notebook_content, "kafka", topic
    )
    status_code = 200 if result.get("running") or result.get("state") == "STARTING" else 500
    return JSONResponse(status_code=status_code, content=result)


@router.post("/api/kafka-producer/stop")
async def kafka_producer_stop_compat(request: Request, state: AppState = Depends(get_app_state)):
    """Legacy endpoint — stops the producer for the given pipeline_id."""
    try:
        body = await request.json()
        pipeline_id = body.get("pipeline_id", "")
    except Exception:
        pipeline_id = ""
    if pipeline_id:
        return await stop_data_producer(state.producer_states, pipeline_id)
    for pid in list(state.producer_states):
        if state.producer_states[pid].get("running"):
            return await stop_data_producer(state.producer_states, pid)
    return new_producer_state()
