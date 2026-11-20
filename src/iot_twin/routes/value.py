"""Executive value and operations summaries for one pipeline's cell.

``GET /api/value/summary`` — the funding executive's view: the annual savings
projection from (overridable) assumptions plus the actuals recorded over the
window. ``GET /api/ops/summary`` — the operations owner's view: detection and
response times, NCRs and CMM inspections triggered, projected RFT, MTTR and the
recent events.

Both answer with the envelope ``{"success", "data", "error", "meta"}``; the
``error`` field is a string so the client's ``ApiError`` shows it as-is, and
``meta.details`` lists per-field problems on a 422. ``pipeline_id`` is
required: every dashboard is per-pipeline and every query filters on it.
Identity is enforced like the alert routes these read from.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.routes.sensor_data import _is_industry_deployment
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.value.episodes import (
    INDUSTRY_PIPELINE_PREFIX,
    EpisodeWindow,
    ScopeError,
    fetch_episode_window,
    parse_window,
    pipeline_keys_for,
    validate_pipeline_id,
)
from iot_twin.services.value.ops_metrics import ops_summary
from iot_twin.services.value.value_model import (
    FIELDS,
    AssumptionError,
    actuals,
    assumption_fields,
    default_assumptions,
    project,
    resolve_assumptions,
)

logger = logging.getLogger(__name__)
router = APIRouter()

SCOPE_PARAMS: frozenset[str] = frozenset({"industry", "pipeline_id", "window"})


def ok(data: Any, meta: dict[str, Any]) -> JSONResponse:
    return JSONResponse({"success": True, "data": data, "error": None, "meta": meta})


def fail(status: int, message: str, details: list[dict[str, str]] | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "success": False,
            "data": None,
            "error": message,
            "meta": {"details": details or []},
        },
    )


def _detail(field: str, message: str) -> list[dict[str, str]]:
    return [{"field": field, "message": message}]


class _Invalid(Exception):
    def __init__(self, response: JSONResponse) -> None:
        super().__init__("invalid request")
        self.response = response


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def _backend_db(state: AppState) -> Any | None:
    return state.app_backend_db if state.app_backend_available else None


async def _scope(
    request: Request, state: AppState, allowed: frozenset[str]
) -> tuple[str, str, datetime, datetime, tuple[str, ...]]:
    params = request.query_params
    unknown = sorted(set(params.keys()) - allowed)
    if unknown:
        raise _Invalid(
            fail(
                422,
                f"unknown query parameter: {', '.join(unknown)}",
                [{"field": name, "message": "is not a recognised parameter"} for name in unknown],
            )
        )
    industry = (params.get("industry") or "").strip().lower()
    if industry not in CURATED_INDUSTRIES:
        raise _Invalid(fail(422, "industry is invalid", _detail("industry", "is invalid")))
    try:
        pipeline_id = validate_pipeline_id(params.get("pipeline_id"))
        span = parse_window(params.get("window"))
    except ScopeError as exc:
        field = "window" if "window" in str(exc) else "pipeline_id"
        raise _Invalid(fail(422, str(exc), _detail(field, str(exc)))) from exc
    alias = f"{INDUSTRY_PIPELINE_PREFIX}{industry}"
    if pipeline_id.startswith(INDUSTRY_PIPELINE_PREFIX) and pipeline_id != alias:
        message = "pipeline_id belongs to another industry"
        raise _Invalid(fail(422, message, _detail("pipeline_id", message)))
    realtime_bound = _is_uuid(pipeline_id) and await _is_industry_deployment(
        state, pipeline_id, industry, "realtime"
    )
    end = datetime.now(UTC)
    return (
        industry,
        pipeline_id,
        end - span,
        end,
        pipeline_keys_for(pipeline_id, industry, realtime_bound),
    )


async def _window(
    state: AppState, keys: tuple[str, ...], industry: str, start: datetime, end: datetime
) -> EpisodeWindow:
    return await fetch_episode_window(_backend_db(state), keys, industry, start, end)


def _meta(window: EpisodeWindow, pipeline_id: str, window_param: str | None) -> dict[str, Any]:
    return {**window.meta(), "pipeline_id": pipeline_id, "window": window_param or "30d"}


@router.get("/api/value/summary")
async def value_summary(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    try:
        industry, pipeline_id, start, end, keys = await _scope(
            request, state, SCOPE_PARAMS | frozenset(FIELDS)
        )
        overrides = {k: v for k, v in request.query_params.items() if k in FIELDS}
        assumptions = resolve_assumptions(overrides)
    except _Invalid as exc:
        return exc.response
    except AssumptionError as exc:
        return fail(422, f"invalid assumptions: {exc}", exc.errors)
    try:
        window = await _window(state, keys, industry, start, end)
    except Exception:
        logger.exception("value summary query failed for pipeline %s", pipeline_id)
        return fail(503, "value data is unavailable right now")
    data = {
        "assumptions": assumptions.to_dict(),
        "defaults": default_assumptions().to_dict(),
        "overridden": sorted(overrides),
        "fields": assumption_fields(),
        "projection": project(assumptions),
        "actuals": actuals(window.episodes, assumptions),
    }
    return ok(data, _meta(window, pipeline_id, request.query_params.get("window")))


@router.get("/api/ops/summary")
async def operations_summary(
    request: Request,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    try:
        industry, pipeline_id, start, end, keys = await _scope(request, state, SCOPE_PARAMS)
    except _Invalid as exc:
        return exc.response
    try:
        window = await _window(state, keys, industry, start, end)
    except Exception:
        logger.exception("ops summary query failed for pipeline %s", pipeline_id)
        return fail(503, "operations data is unavailable right now")
    data = ops_summary(window.episodes, window.production_stops)
    return ok(data, _meta(window, pipeline_id, request.query_params.get("window")))
