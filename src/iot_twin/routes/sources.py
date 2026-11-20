"""Source metrics and config endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.state import AppState, get_app_state

router = APIRouter()


@router.get("/api/sources")
async def get_sources(state: AppState = Depends(get_app_state)):
    """Return status and latency metrics for every data source."""
    out = {}
    for name, s in state.sources.items():
        d = s.to_dict()
        if hasattr(s, "_lb_counters"):
            d["_debug_lb_counters"] = s._lb_counters
        out[name] = d
    return out


@router.put("/api/sources/{name}/config")
async def update_source_config(
    name: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Mutate a source's SinkConfig at runtime."""
    if name not in state.sources:
        return JSONResponse(status_code=404, content={"error": f"Unknown source: {name}"})

    src = state.sources[name]
    if src.sink_config is None:
        return JSONResponse(
            status_code=400,
            content={"error": f"Source '{name}' has no configurable sink"},
        )

    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    cfg = src.sink_config

    MUTABLE_FIELDS = {
        "rate_hz": float,
        "batch_size": int,
        "max_buffer_size": int,
        "backpressure": str,
        "retry_count": int,
        "retry_delay_s": float,
    }

    applied: dict[str, Any] = {}
    for field, cast_fn in MUTABLE_FIELDS.items():
        if field in body:
            try:
                setattr(cfg, field, cast_fn(body[field]))
                applied[field] = getattr(cfg, field)
            except (ValueError, TypeError) as exc:
                return JSONResponse(
                    status_code=422,
                    content={"error": f"Bad value for {field}: {exc}"},
                )

    return {
        "source": name,
        "applied": applied,
        "config": {
            "rate_hz": cfg.rate_hz,
            "batch_size": cfg.batch_size,
            "max_buffer_size": cfg.max_buffer_size,
            "backpressure": cfg.backpressure,
            "retry_count": cfg.retry_count,
            "retry_delay_s": cfg.retry_delay_s,
        },
    }
