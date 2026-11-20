"""Grounded, audited AI action plan for one alert episode.

Flow for ``POST /api/alerts/{alert_id}/action-plan``:

1. :func:`build_sources` + :func:`grounding.assemble_grounding` — collect the
   alert row, its ``alert_event`` history and ``alert_response`` rows, the
   readings window (DBSQL), lots/serials at risk, the validated limit and OQ/PQ
   refs, maintenance history and the model risk score (when present). Every
   row gets a citation id.
2. :func:`prompt.render_messages` — system prompt restricted to the grounding
   set; rows passed as UNTRUSTED data.
3. :func:`fm_client.query_chat` — Foundation Model endpoint (``GENAI_ENDPOINT``,
   default ``databricks-claude-sonnet-5-5``) with timeout and retry.
4. :func:`plan_schema.parse_plan` + :func:`plan_schema.check_grounding` —
   schema validation, then citations/numbers/identifiers against the rows.
5. :class:`audit_store.GenaiAuditStore` — one audit row per attempt (success,
   rejected or endpoint error), mirrored to UC for Genie.

The plan is ADVISORY: nothing here writes ``alert`` or ``alert_response``. A
person applies it by signing decisions through the existing response workflow.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from iot_twin.services.alerts.alert_engine import AlertRecord
from iot_twin.services.genai import fm_client
from iot_twin.services.genai.audit_store import AuditRecord, GenaiAuditStore
from iot_twin.services.genai.grounding import (
    GroundingSet,
    GroundingSources,
    ReadingsRunner,
    assemble_grounding,
    reading_window,
    readings_sql,
    sensor_name_for,
)
from iot_twin.services.genai.plan_schema import (
    CHECKER_VERSION,
    GroundingCheck,
    check_grounding,
    failed_check,
    parse_plan,
)
from iot_twin.services.genai.prompt import RenderedPrompt, render_messages
from iot_twin.services.genai.tracing import trace_span

logger = logging.getLogger(__name__)

MAX_RAW_TEXT = 20000


class ActionPlanModelError(RuntimeError):
    """The endpoint failed; the attempt is still audited as ``record``."""

    def __init__(self, message: str, *, kind: str, record: AuditRecord) -> None:
        super().__init__(message)
        self.kind = kind
        self.record = record


@dataclass(frozen=True)
class PlanRequest:
    alert: AlertRecord
    requested_by: str
    endpoint: str | None = None
    client: Any | None = None
    timeout_s: float | None = None
    retries: int | None = None


def readings_query(alert: AlertRecord, mode: str | None, now: datetime) -> str | None:
    """The DBSQL readings-window query for ``alert`` in pipeline ``mode`` (None if n/a)."""
    from iot_twin.services.genie.genie_space_content import telemetry_row_filter
    from iot_twin.services.genie.genie_space_service import telemetry_table
    from iot_twin.services.uc_ident import sql_fqn

    sensor = sensor_name_for(alert)
    if not mode or not sensor or not alert.industry:
        return None
    try:
        fqn = telemetry_table(alert.industry, mode)
    except (RuntimeError, ValueError, KeyError):
        return None
    start, end = reading_window(alert, now)
    return readings_sql(
        sql_fqn(*fqn.split(".")), sensor, start, end, telemetry_row_filter(mode, alert.industry)
    )


async def dbsql_runner(db: Any, db_available: bool, user_email: str) -> ReadingsRunner | None:
    """A readings runner on the app's SQL warehouse, or None when none is configured."""
    from iot_twin.services.dbsql_service import execute_dbsql, resolve_warehouse_id

    warehouse = await resolve_warehouse_id(db, db_available, user_email)
    if not warehouse:
        return None

    async def run(sql: str) -> list[dict[str, Any]]:
        result = await execute_dbsql(sql, warehouse)
        rows = result.get("data") if isinstance(result, dict) else None
        return [dict(row) for row in rows or []]

    return run


def _base_record(
    request: PlanRequest,
    grounding: GroundingSet,
    prompt: RenderedPrompt,
    endpoint: str,
    requested_at: datetime,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "alert_id": request.alert.id,
        "industry": request.alert.industry,
        "requested_by": request.requested_by,
        "requested_at": requested_at,
        "endpoint": endpoint,
        "prompt_template_version": prompt.template_version,
        "prompt_hash": prompt.prompt_hash,
        "grounding_hash": grounding.digest(),
        "grounding_rows": grounding.rows(),
    }


def _evaluate(text: str, grounding: GroundingSet) -> tuple[dict[str, Any], GroundingCheck, str]:
    parsed = parse_plan(text)
    if parsed.plan is None:
        response = {
            "raw_text": text[:MAX_RAW_TEXT],
            "plan": None,
            "schema_errors": list(parsed.errors),
            "checker_version": CHECKER_VERSION,
        }
        return response, failed_check(parsed.errors), "rejected"
    check = check_grounding(parsed.plan, grounding)
    response = {
        "raw_text": text[:MAX_RAW_TEXT],
        "plan": parsed.plan.model_dump(),
        "schema_errors": [],
        "grounding_gaps": list(grounding.gaps),
        "checker_version": CHECKER_VERSION,
    }
    return response, check, "generated"


@dataclass(frozen=True)
class PreparedPlan:
    """Grounding and prompt for one request, before any model call."""

    grounding: GroundingSet
    prompt: RenderedPrompt
    endpoint: str
    requested_at: datetime


async def prepare(
    request: PlanRequest, sources: GroundingSources, now: datetime | None = None
) -> PreparedPlan:
    requested_at = now or datetime.now(UTC)
    grounding = await assemble_grounding(request.alert, sources, requested_at)
    return PreparedPlan(
        grounding=grounding,
        prompt=render_messages(request.alert.id, grounding),
        endpoint=request.endpoint or fm_client.endpoint_name(),
        requested_at=requested_at,
    )


async def reusable_plan(
    store: GenaiAuditStore, prepared: PreparedPlan, alert_id: str, db: Any | None = None
) -> AuditRecord | None:
    """The latest plan for the alert when nothing it depends on has changed.

    Same grounding rows (hash), template, endpoint and grounding-checker
    version, and it was generated and
    passed the check — re-asking the model would only spend tokens.
    """
    latest = next(iter(await store.list_for_alert(alert_id, db=db, limit=1)), None)
    if latest is None:
        return None
    same = (
        latest.grounding_hash == prepared.grounding.digest()
        and latest.prompt_template_version == prepared.prompt.template_version
        and latest.endpoint == prepared.endpoint
        and latest.response.get("checker_version") == CHECKER_VERSION
    )
    passed = latest.status == "generated" and latest.grounding_check == "pass"
    return latest if same and passed else None


async def generate_action_plan(
    request: PlanRequest,
    sources: GroundingSources,
    store: GenaiAuditStore,
    *,
    db: Any | None = None,
    now: datetime | None = None,
    prepared: PreparedPlan | None = None,
) -> AuditRecord:
    """Assemble grounding, call the model, validate and audit. Never mutates alerts."""
    ready = prepared or await prepare(request, sources, now)
    grounding, prompt, endpoint = ready.grounding, ready.prompt, ready.endpoint
    requested_at = ready.requested_at
    base = _base_record(request, grounding, prompt, endpoint, requested_at)
    inputs = {
        "alert_id": request.alert.id,
        "plan_id": base["id"],
        "prompt_hash": prompt.prompt_hash,
        "citation_ids": sorted(grounding.ids),
    }
    started = time.monotonic()
    with trace_span("action_plan", inputs) as record_outputs:
        try:
            reply = await fm_client.query_chat(
                prompt.messages,
                endpoint=endpoint,
                client=request.client,
                timeout_s=request.timeout_s,
                retries=request.retries,
            )
        except fm_client.ModelCallError as exc:
            latency = int((time.monotonic() - started) * 1000)
            entry = AuditRecord(
                **base,
                model=None,
                model_version=None,
                response={"plan": None, "attempts": exc.attempts},
                grounding_check="fail",
                grounding_reasons=[f"model call failed ({exc.kind}): {exc}"],
                status="error",
                error=str(exc)[:1000],
                latency_ms=latency,
            )
            await store.record(entry, db=db)
            record_outputs({"status": "error", "error": str(exc)})
            raise ActionPlanModelError(str(exc), kind=exc.kind, record=entry) from exc
        latency = int((time.monotonic() - started) * 1000)
        response, check, status = _evaluate(reply.text, grounding)
        served, version = await fm_client.describe_endpoint(endpoint, request.client)
        entry = AuditRecord(
            **base,
            model=reply.model or served or endpoint,
            model_version=reply.served_model_name or version,
            response={**response, "attempts": reply.attempts},
            grounding_check=check.status,
            grounding_reasons=list(check.reasons),
            status=status,
            latency_ms=latency,
            prompt_tokens=reply.usage.get("prompt_tokens"),
            completion_tokens=reply.usage.get("completion_tokens"),
            total_tokens=reply.usage.get("total_tokens"),
        )
        await store.record(entry, db=db)
        record_outputs(
            {
                "status": status,
                "grounding_check": check.status,
                "latency_ms": latency,
                **reply.usage,
            }
        )
    logger.info(
        "action plan %s for alert %s: %s / grounding %s (%d ms)",
        entry.id,
        entry.alert_id,
        status,
        check.status,
        latency,
    )
    return entry
