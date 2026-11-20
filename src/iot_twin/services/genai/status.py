"""Boot checks and the ``GET /api/genai/status`` payload for the AI action plan.

Reports what an operator needs to trust the audit trail: where the pseudonym
key comes from (``secret`` / ``lakebase`` / ``redacted``), whether the
append-only trigger is installed and enabled, and the call limits in force.
"""

from __future__ import annotations

import logging
from typing import Any

from iot_twin.models.app_schema import GENAI_AUDIT_TRIGGER
from iot_twin.services.genai import fm_client, redact
from iot_twin.services.genai.limits import limiter_for

logger = logging.getLogger(__name__)

_TRIGGERS_SQL = """
SELECT tgname, tgenabled FROM pg_trigger
WHERE tgrelid = to_regclass('genai_action_plan_audit') AND tgname = ANY($1::text[])
"""
_TRIGGER_NAMES = [GENAI_AUDIT_TRIGGER, f"{GENAI_AUDIT_TRIGGER}_truncate"]


def _flag(value: Any) -> str:
    """``pg_trigger.tgenabled`` is a ``"char"``; asyncpg returns it as bytes."""
    return value.decode() if isinstance(value, bytes | bytearray) else str(value)


async def append_only_status(db: Any | None) -> dict[str, Any]:
    """``{"enforced": bool | None, ...}``; None when there is no Lakebase to check."""
    if db is None:
        return {"enforced": None, "detail": "no Lakebase (in-memory audit)"}
    try:
        rows = await db.fetch(_TRIGGERS_SQL, _TRIGGER_NAMES)
    except Exception as exc:  # noqa: BLE001 - status only
        return {"enforced": None, "detail": f"trigger check failed: {exc}"}
    enabled = {str(r["tgname"]) for r in rows if _flag(r["tgenabled"]) != "D"}
    missing = [name for name in _TRIGGER_NAMES if name not in enabled]
    return {
        "enforced": not missing,
        "detail": "" if not missing else f"missing or disabled: {', '.join(missing)}",
    }


async def init_genai_privacy(db: Any | None) -> None:
    """Resolve the pseudonym key and warn when the audit is not append-only."""
    try:
        await redact.init_key(db)
    except Exception as exc:  # noqa: BLE001 - never block boot
        logger.warning("pseudonym key init failed: %s", exc)
    append = await append_only_status(db)
    if append["enforced"] is False:
        logger.warning("genai_action_plan_audit is NOT append-only: %s", append["detail"])


async def status_payload(state: Any, db: Any | None) -> dict[str, Any]:
    limits = fm_client.call_limits()
    limiter = limiter_for(state)
    append = await append_only_status(db)
    return {
        "endpoint": fm_client.endpoint_name(),
        "pseudonym_key": redact.key_status(),
        "append_only_enforced": append["enforced"],
        "append_only": append,
        "limits": {
            "timeout_s": limits.timeout_s,
            "attempts": limits.attempts,
            "request_budget_s": limits.budget_s,
            "max_concurrent": limiter.max_concurrent,
            "user_cooldown_s": limiter.cooldown_s,
        },
        "advisory": True,
    }
