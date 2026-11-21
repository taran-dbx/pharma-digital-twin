"""Set-based LakeTS INSERT triggers for the real-time ChronoTable.

LakeTS 0.1.0 runs its last-value-cache and CDF-shadow triggers ``FOR EACH ROW``.
On the live pipeline branch that costs ~0.7 ms of Postgres time per inserted
row, which capped one Kafka-partition writer near 1,300 rows/s server-side and
the real-time consumer at ~950 rows/s end to end (docs/evidence/2026-11-21).
:func:`ensure_statement_insert_triggers` installs statement-level replacements
(``assets/lakets_insert_triggers.sql``) and swaps them in atomically; it is
idempotent and safe to run on every deploy.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from iot_twin.core.paths import LAKETS_INSERT_TRIGGERS_SQL_PATH

logger = logging.getLogger("pharma_twin.services.lakets.insert_triggers")

SWAP_SQL = "SELECT lakets.use_statement_insert_triggers($1, $2)"

# Trigger names the swap installs (statement-level, transition table ``_lakets_new_rows``).
SYNC_INSERT_TRIGGER = "trg_lakets_sync_insert"
LVC_INSERT_TRIGGER = "trg_lakets_lvc_insert"


def insert_triggers_sql() -> str:
    """Return the idempotent function definitions (no trigger changes)."""
    return LAKETS_INSERT_TRIGGERS_SQL_PATH.read_text(encoding="utf-8")


def _decode(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (str, bytes)):
        decoded = json.loads(raw)
        return decoded if isinstance(decoded, dict) else {}
    return {}


async def ensure_statement_insert_triggers(
    conn: Any,
    *,
    table: str = "sensor_readings",
    schema: str = "public",
) -> dict:
    """Install and apply the statement-level INSERT triggers on ``schema.table``.

    ``conn`` is a write connection owned by the ChronoTable owner (asyncpg-style
    ``execute``/``fetchval``). Never raises: a failure is returned as
    ``{"status": "failed", "error": ...}`` so a deploy is not blocked by a
    performance upgrade; the health check flags remaining per-row triggers.
    """
    resource = f"LakeTS statement INSERT triggers on {schema}.{table}"
    try:
        await conn.execute(insert_triggers_sql())
        summary = _decode(await conn.fetchval(SWAP_SQL, table, schema))
    except Exception as exc:
        logger.warning("Statement INSERT trigger swap failed for %s.%s: %s", schema, table, exc)
        return {"resource": resource, "status": "failed", "error": str(exc)[:300]}

    leftover = list(summary.get("row_insert_triggers") or [])
    if leftover:
        return {
            "resource": resource,
            "status": "failed",
            "error": f"per-row INSERT triggers remain: {', '.join(leftover)}",
            "summary": summary,
        }
    return {"resource": resource, "status": "ok", "summary": summary}
