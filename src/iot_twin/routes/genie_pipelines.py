"""Per-pipeline Genie HTTP API (all industries) (see docs/genie.md).

``/api/genie/pipelines/{setup_id}/{mode}/...`` — status, starters, start,
follow-up, poll and attachment query results for the Genie space provisioned
for one ``(setup_id, mode)``.  Same guarantees as the static copilot routes:
``require_identity`` auth, process-local conversation ownership (fail closed
after restart), SDK calls off the event loop under a semaphore + timeout, and
the trusted/untrusted prompt wrapper.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from databricks.sdk.errors import NotFound
from fastapi import APIRouter, Depends, HTTPException, Request

from iot_twin.core.auth import require_identity
from iot_twin.core.state import AppState, get_app_state
from iot_twin.routes.genie import _http_for, _sdk_call
from iot_twin.services import genie_service
from iot_twin.services.genie import genie_space_service

logger = logging.getLogger("iot_digital_twin.routes.genie_pipelines")

router = APIRouter()

_PREFIX = "/api/genie/pipelines/{setup_id}/{mode}"
_ALLOWED_BODY_KEYS = frozenset({"question"})


@dataclass(frozen=True)
class PipelineScope:
    setup_id: str
    mode: str
    industry: str
    space_id: str | None

    @property
    def expected_tables(self) -> list[str]:
        return list(genie_space_service.expected_tables(self.industry, self.mode))


# ---------------------------------------------------------------------------
# Scope resolution
# ---------------------------------------------------------------------------
def _require_mode(mode: str) -> str:
    if mode not in genie_space_service.PIPELINE_MODES:
        raise HTTPException(status_code=422, detail="unknown mode")
    return mode


def _require_db(state: AppState) -> Any:
    if not state.app_backend_available or state.app_backend_db is None:
        raise HTTPException(status_code=503, detail="app backend database is unavailable")
    return state.app_backend_db


async def _resolve_scope(state: AppState, pipeline_id: str, mode: str) -> PipelineScope:
    _require_mode(mode)
    db = _require_db(state)
    try:
        setup = await genie_space_service.resolve_pipeline_setup(db, pipeline_id, mode)
    except genie_space_service.PipelineNotFound as exc:
        raise HTTPException(status_code=404, detail="unknown pipeline") from exc
    setup_id = str(setup.get("id"))
    industry = str(setup.get("industry") or "").strip().lower()
    space_id = None
    if genie_space_service.industry_supported(industry):
        space_id = await genie_space_service.find_space_id(db, setup_id, mode)
    return PipelineScope(setup_id=setup_id, mode=mode, industry=industry, space_id=space_id)


async def _require_space(state: AppState, setup_id: str, mode: str) -> PipelineScope:
    scope = await _resolve_scope(state, setup_id, mode)
    if not genie_space_service.industry_supported(scope.industry):
        raise HTTPException(status_code=404, detail="unknown pipeline")
    if not scope.space_id:
        raise genie_service.GenieDisabledError(
            "No Genie space is provisioned for this pipeline yet. Redeploy it to create one."
        )
    return scope


# ---------------------------------------------------------------------------
# Request validation + conversation ownership
# ---------------------------------------------------------------------------
async def _question_from(request: Request) -> str:
    try:
        body = await request.json()
    except Exception as exc:
        raise genie_service.GenieValidationError("request body must be JSON") from exc
    if not isinstance(body, dict):
        raise genie_service.GenieValidationError("request body must be an object")
    extra = set(body) - _ALLOWED_BODY_KEYS
    if extra:
        raise genie_service.GenieValidationError(f"unexpected fields: {', '.join(sorted(extra))}")
    question = str(body.get("question") or "").strip()
    if not question:
        raise genie_service.GenieValidationError("question is required")
    if len(question) > genie_service.MAX_QUESTION_CHARS:
        raise genie_service.GenieValidationError("question exceeds maximum length")
    return question


def _remember(state: AppState, conversation_id: str, owner: str, scope: PipelineScope) -> None:
    state.genie_conversations[conversation_id] = {
        "owner": owner,
        "industry": scope.industry,
        "setup_id": scope.setup_id,
        "mode": scope.mode,
        "space_id": str(scope.space_id),
    }


def _require_conversation(
    state: AppState, conversation_id: str, owner: str, scope: PipelineScope
) -> None:
    record = state.genie_conversations.get(conversation_id)
    if (
        record is None
        or record.get("owner") != owner
        or record.get("setup_id") != scope.setup_id
        or record.get("mode") != scope.mode
        or record.get("space_id") != str(scope.space_id)
    ):
        raise HTTPException(status_code=404, detail="unknown conversation")


async def _pack_for(state: AppState, scope: PipelineScope, user: str) -> dict[str, Any]:
    pack = genie_service.build_context_pack(
        industry=scope.industry,
        mode=scope.mode,
        allowed_tables=scope.expected_tables,
    )
    sensor_names = [sensor["name"] for sensor in pack["catalog"]["sensors"]]
    summary = await genie_service.bounded_telemetry_summary(
        scope.industry,
        scope.mode,
        sensor_names,
        db=state.app_backend_db,
        db_available=state.app_backend_available,
        user_email=user,
        pipeline_scope=True,
    )
    return {**pack, "telemetry_summary": summary}


def _raise_http(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, NotFound):
        raise HTTPException(status_code=404, detail="Genie resource not found") from exc
    raise _http_for(exc) from exc


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.get(f"{_PREFIX}/status")
async def pipeline_genie_status(
    setup_id: str,
    mode: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    scope = await _resolve_scope(state, setup_id, mode)
    base = {"industry": scope.industry, "supported": True}
    if not genie_space_service.industry_supported(scope.industry):
        return {
            **base,
            "supported": False,
            "available": False,
            "space_id": None,
            "reason": f"Per-pipeline Genie is not available for industry {scope.industry!r}.",
        }
    if not scope.space_id:
        return {
            **base,
            "available": False,
            "space_id": None,
            "reason": "No Genie space is provisioned for this pipeline yet. "
            "Redeploy the pipeline to create one.",
        }
    try:
        allowlist = await _sdk_call(
            state,
            genie_service.cached_allowlist_status,
            None,
            scope.space_id,
            scope.expected_tables,
        )
    except Exception as exc:
        logger.warning("Genie allowlist check failed for space %s: %s", scope.space_id, exc)
        return {
            **base,
            "available": False,
            "space_id": scope.space_id,
            "reason": "Genie space could not be verified. Try again shortly.",
        }
    if not allowlist.get("authorization_enforced"):
        return {
            **base,
            "available": False,
            "space_id": scope.space_id,
            "reason": str(allowlist.get("requirement") or "Genie table allowlist mismatch"),
        }
    return {**base, "available": True, "space_id": scope.space_id}


@router.get(f"{_PREFIX}/starters")
async def pipeline_genie_starters(
    setup_id: str,
    mode: str,
    state: AppState = Depends(get_app_state),
    _user: str = Depends(require_identity),
):
    scope = await _resolve_scope(state, setup_id, mode)
    if not genie_space_service.industry_supported(scope.industry):
        raise HTTPException(status_code=404, detail="unknown pipeline")
    return {"questions": list(genie_space_service.starter_questions(scope.industry, scope.mode))}


@router.post(f"{_PREFIX}/conversations")
async def pipeline_genie_start(
    setup_id: str,
    mode: str,
    request: Request,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        scope = await _require_space(state, setup_id, mode)
        question = await _question_from(request)
        pack = await _pack_for(state, scope, user)
        result = await _sdk_call(
            state,
            genie_service.start_conversation,
            question,
            pack,
            space_id=scope.space_id,
            expected_tables=scope.expected_tables,
        )
        _remember(state, result["conversation_id"], user, scope)
        return result
    except Exception as exc:
        _raise_http(exc)


@router.post(f"{_PREFIX}/conversations/{{conversation_id}}/messages")
async def pipeline_genie_follow_up(
    setup_id: str,
    mode: str,
    conversation_id: str,
    request: Request,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        scope = await _require_space(state, setup_id, mode)
        _require_conversation(state, conversation_id, user, scope)
        question = await _question_from(request)
        pack = await _pack_for(state, scope, user)
        return await _sdk_call(
            state,
            genie_service.create_message,
            conversation_id,
            question,
            pack,
            space_id=scope.space_id,
            expected_tables=scope.expected_tables,
        )
    except Exception as exc:
        _raise_http(exc)


@router.get(f"{_PREFIX}/conversations/{{conversation_id}}/messages/{{message_id}}")
async def pipeline_genie_poll(
    setup_id: str,
    mode: str,
    conversation_id: str,
    message_id: str,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        scope = await _require_space(state, setup_id, mode)
        _require_conversation(state, conversation_id, user, scope)
        return await _sdk_call(
            state,
            genie_service.get_message,
            conversation_id,
            message_id,
            space_id=scope.space_id,
            expected_tables=scope.expected_tables,
        )
    except Exception as exc:
        _raise_http(exc)


@router.get(
    f"{_PREFIX}/conversations/{{conversation_id}}/messages/{{message_id}}"
    "/attachments/{attachment_id}/result"
)
async def pipeline_genie_attachment_result(
    setup_id: str,
    mode: str,
    conversation_id: str,
    message_id: str,
    attachment_id: str,
    state: AppState = Depends(get_app_state),
    user: str = Depends(require_identity),
):
    try:
        scope = await _require_space(state, setup_id, mode)
        _require_conversation(state, conversation_id, user, scope)
        return await _sdk_call(
            state,
            genie_service.get_attachment_query_result,
            conversation_id,
            message_id,
            attachment_id,
            space_id=scope.space_id,
            expected_tables=scope.expected_tables,
        )
    except Exception as exc:
        _raise_http(exc)
