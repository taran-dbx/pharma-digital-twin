"""Applying an AI action plan through the signed response workflow.

A person signs a decision with ``action_plan_id``. Two rules hold on the server,
whatever the UI does:

* **Only grounded plans apply by default.** The plan must be ``generated`` with
  ``grounding_check == "pass"``. Anything else needs an explicit
  ``apply_override_reason``, which is recorded in the audit row next to who
  applied it.
* **The decision and the plan link commit together.** With Lakebase, the
  signed response and the ``applied_*`` update run in one transaction: either
  both are saved or neither is (→ 503). In memory they are sequential.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from iot_twin.services.alerts.alert_engine import AlertRecord
from iot_twin.services.alerts.alert_responses import (
    AlertResponse,
    AlertResponseStore,
    ResponseAudit,
)
from iot_twin.services.genai.audit_store import AuditRecord, GenaiAuditStore

MAX_OVERRIDE_CHARS = 280


class PlanLinkError(ValueError):
    """The ``action_plan_id`` cannot be applied to this decision (→ 422)."""


@dataclass(frozen=True)
class PlanLink:
    plan_id: str
    override_reason: str | None


@dataclass(frozen=True)
class SignedWithPlan:
    response: AlertResponse
    created: bool
    applied: AuditRecord | None


def _override(body: Mapping[str, Any]) -> str | None:
    raw = body.get("apply_override_reason")
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise PlanLinkError("apply_override_reason must be non-empty text")
    reason = raw.strip()
    if len(reason) > MAX_OVERRIDE_CHARS:
        raise PlanLinkError(
            f"apply_override_reason must be at most {MAX_OVERRIDE_CHARS} characters"
        )
    return reason


async def resolve_link(
    store: GenaiAuditStore, alert_id: str, body: Mapping[str, Any], db: Any | None
) -> PlanLink | None:
    """Validate ``action_plan_id`` (+ optional override) for a decision on ``alert_id``."""
    raw = body.get("action_plan_id")
    if raw in (None, ""):
        if body.get("apply_override_reason") is not None:
            raise PlanLinkError("apply_override_reason needs an action_plan_id")
        return None
    try:
        plan_id = str(uuid.UUID(str(raw)))
    except ValueError as exc:
        raise PlanLinkError("action_plan_id must be a UUID") from exc
    override = _override(body)
    plan = await store.get(plan_id, db=db)
    if plan is None or plan.alert_id != alert_id:
        raise PlanLinkError("action_plan_id is not a plan for this alert")
    grounded = plan.status == "generated" and plan.grounding_check == "pass"
    if not grounded and override is None:
        raise PlanLinkError(
            f"action plan {plan_id[:8]} did not pass the grounding check "
            f"(status {plan.status}, check {plan.grounding_check}); "
            "give apply_override_reason to apply it anyway"
        )
    return PlanLink(plan_id=plan_id, override_reason=None if grounded else override)


async def _sign_and_link(
    responses: AlertResponseStore,
    audits: GenaiAuditStore,
    alert: AlertRecord,
    action: str,
    user: str,
    audit: ResponseAudit,
    link: PlanLink,
    db: Any | None,
) -> SignedWithPlan:
    response, created = await responses.record(alert, action, recorded_by=user, db=db, audit=audit)
    applied = None
    if created:
        applied = await audits.mark_applied(
            link.plan_id,
            applied_by=user,
            response_id=response.id,
            override_reason=link.override_reason,
            db=db,
        )
    return SignedWithPlan(response=response, created=created, applied=applied)


async def sign_with_plan(
    responses: AlertResponseStore,
    audits: GenaiAuditStore,
    alert: AlertRecord,
    action: str,
    *,
    user: str,
    audit: ResponseAudit,
    link: PlanLink,
    db: Any | None,
) -> SignedWithPlan:
    """Record the signed decision and mark the plan applied, atomically with Lakebase.

    Raises ``ResponseStoreUnavailable`` / ``AuditStoreUnavailable`` when either
    write fails; with Lakebase the transaction is then rolled back, so nothing
    is half-saved.
    """
    acquire = getattr(db, "acquire", None)
    if db is None or acquire is None:
        return await _sign_and_link(responses, audits, alert, action, user, audit, link, db)
    async with acquire() as conn, conn.transaction():
        return await _sign_and_link(responses, audits, alert, action, user, audit, link, conn)
