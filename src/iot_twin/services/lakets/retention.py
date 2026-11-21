"""Hot-tier retention and CDF-shadow pruning for the real-time ChronoTable.

Before 2026-11-21 neither tier had retention: the hot tier held 240 chunks (7.3 GB) and
the CDF shadow 6.6 GB after one day, growing ~1.7 GB/h together at 2000 Hz.

- Hot tier: LakeTS's own fail-closed pair. ``add_tiering_policy`` installs per-chunk
  write-LSN tracking and the Tiering job flags a chunk ``tiered`` only once the
  sync's committed LSN is past the chunk's last write; ``add_tiered_retention_policy``
  lets the Retention job drop it after ``drop_after`` under the same gate, so a stalled
  sync (frozen committed LSN) never loses data.
- Shadow: ``lakets.prune_cdf_shadow()`` (``assets/lakets_cdf_prune.sql``) deletes only
  rows whose inserting transaction finished before a WAL mark the sync has committed.

:func:`ensure_retention` is idempotent and never raises. It is OFF unless
``LAKETS_RETENTION_ENABLED=true``: while off it changes nothing (no policy, no prune
function), so the Tiering job's prune step is a no-op too. Enabling it is a separate,
reviewed decision (data-loss review of the shadow deletes, 2026-11-21).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from iot_twin.core.paths import LAKETS_CDF_PRUNE_SQL_PATH

logger = logging.getLogger("pharma_twin.services.lakets.retention")

# Hot tier: chunks become tier-eligible after TIER_AFTER and are dropped after
# DROP_AFTER (both only once durable in Delta). The dashboards and the health check
# read at most the last 24 h of the hot tier. Override per workspace via env.
DEFAULT_TIER_AFTER = "1 hour"
DEFAULT_DROP_AFTER = "24 hours"
RETENTION_ENABLED_ENV = "LAKETS_RETENTION_ENABLED"
TIER_AFTER_ENV = "LAKETS_HOT_TIER_AFTER"
DROP_AFTER_ENV = "LAKETS_HOT_DROP_AFTER"

# Shadow rows are kept this long even when durable (reconciliation / replays).
SHADOW_KEEP = "2 hours"
SHADOW_PRUNE_BATCH = 50_000

_POLICY_EXISTS_SQL = (
    "SELECT EXISTS (SELECT 1 FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON c.id = p.chronotable_id "
    "WHERE c.schema_name = $1 AND c.table_name = $2 AND p.policy_type = $3)"
)
_ADD_TIERING_SQL = "SELECT lakets.add_tiering_policy($1, $2::text::interval, $3)"
_ADD_TIERED_RETENTION_SQL = (
    "SELECT lakets.add_tiered_retention_policy($1, $2::text::interval, $3::text::interval, $4)"
)


def prune_sql() -> str:
    """Idempotent DDL for the marks table and ``lakets.prune_cdf_shadow``."""
    return LAKETS_CDF_PRUNE_SQL_PATH.read_text(encoding="utf-8")


def retention_enabled() -> bool:
    return os.getenv(RETENTION_ENABLED_ENV, "").strip().lower() in {"1", "true", "yes"}


def hot_tier_window() -> tuple[str, str]:
    return (
        os.getenv(TIER_AFTER_ENV) or DEFAULT_TIER_AFTER,
        os.getenv(DROP_AFTER_ENV) or DEFAULT_DROP_AFTER,
    )


async def ensure_retention(
    conn: Any,
    *,
    table: str = "sensor_readings",
    schema: str = "public",
) -> dict:
    """Install shadow pruning and the hot-tier tiering + tiered-retention policies."""
    resource = f"LakeTS retention on {schema}.{table}"
    if not retention_enabled():
        return {
            "resource": resource,
            "status": "disabled",
            "applied": [],
            "detail": f"set {RETENTION_ENABLED_ENV}=true to install retention and shadow pruning",
        }
    tier_after, drop_after = hot_tier_window()
    applied: list[str] = []
    try:
        await conn.execute(prune_sql())
        if not await conn.fetchval(_POLICY_EXISTS_SQL, schema, table, "tiering"):
            await conn.fetchval(_ADD_TIERING_SQL, table, tier_after, schema)
            applied.append("tiering")
        if not await conn.fetchval(_POLICY_EXISTS_SQL, schema, table, "tiered_retention"):
            await conn.fetchval(_ADD_TIERED_RETENTION_SQL, table, tier_after, drop_after, schema)
            applied.append("tiered_retention")
    except Exception as exc:
        logger.warning("Retention setup failed for %s.%s: %s", schema, table, exc)
        return {"resource": resource, "status": "failed", "error": str(exc)[:300]}
    return {
        "resource": resource,
        "status": "ok",
        "applied": applied,
        "tier_after": tier_after,
        "drop_after": drop_after,
        "shadow_keep": SHADOW_KEEP,
    }
