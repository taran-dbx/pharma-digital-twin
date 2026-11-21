"""Opt-in OPTIMIZE of this deployment's Lakehouse Sync history table (stall remediation).

Evidence (docs/evidence/2026-11-21/12_lakehouse_sync_escalation.md): the managed
Lakehouse Sync writer stopped three times, each right after a checkpoint-interval
version (1000, 1800, 2000) while the history table held ~770-1,000 uncompacted files
(one per 15 s commit). Two stalls ended seconds after a predictive-optimization
OPTIMIZE committed; that OPTIMIZE ran for 10 s, 2.5 h into stall 1, so it did not block
the writer. The link is a correlation, not a proven cause.

Why it is safe to offer:
- OPTIMIZE rewrites files only (no data change); wal2delta's appends are blind and do
  not conflict with compaction. Both predictive OPTIMIZEs ran concurrently with the writer.
- The app service principal owns the history table (MODIFY is enough).
Why it is opt-in: the effect on a stall is correlational, and it spends warehouse time.

Guards: enabled only with ``CDF_HISTORY_OPTIMIZE_ENABLED=true``; the target is derived
(never passed in) from a curated industry and the configured catalog/schema, and the
schema must carry this deployment's resource prefix (``pharma_twin``...), so no other
app's tables can be touched.
"""

from __future__ import annotations

import os
import re
from typing import Any

from iot_twin.core.config import get_resource_prefix_ident
from iot_twin.services import cdf_reconciliation_service as cdf
from iot_twin.services import dbsql_service
from iot_twin.services.industry import industry_catalog_service

OPTIMIZE_ENV = "CDF_HISTORY_OPTIMIZE_ENABLED"
OPTIMIZE_TIMEOUT = "50s"
_UC_IDENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class CompactionRefused(RuntimeError):
    """The remediation is disabled or the target failed a guard."""


def enabled() -> bool:
    return os.getenv(OPTIMIZE_ENV, "").strip().lower() in {"1", "true", "yes"}


def _quote(identifier: str) -> str:
    if not _UC_IDENT_RE.match(identifier or ""):
        raise CompactionRefused(f"refusing unsafe identifier {identifier!r}")
    return f"`{identifier}`"


def optimize_statement(
    industry: str, *, catalog: str | None = None, schema: str | None = None
) -> str:
    """Return ``OPTIMIZE `<catalog>`.`<schema>`.`lb__shadow_<industry>_..._history```."""
    catalog = industry_catalog_service.uc_catalog_name() if catalog is None else catalog
    schema = industry_catalog_service.uc_schema_name() if schema is None else schema
    prefix = get_resource_prefix_ident()
    if not schema or not (schema == prefix or schema.startswith(f"{prefix}_")):
        raise CompactionRefused(
            f"schema {schema!r} is not this deployment's ({prefix}*) schema; refusing to OPTIMIZE"
        )
    try:
        table = cdf.delta_table_name(industry)  # curated industries only
    except ValueError as exc:
        raise CompactionRefused(str(exc)) from exc
    return f"OPTIMIZE {_quote(catalog)}.{_quote(schema)}.{_quote(table)}"


async def optimize_history_table(industry: str, warehouse_id: str) -> dict:
    """Run the guarded OPTIMIZE. Raises :class:`CompactionRefused` when not allowed."""
    if not enabled():
        raise CompactionRefused(
            f"history OPTIMIZE is disabled; set {OPTIMIZE_ENV}=true to allow it (opt-in)"
        )
    if not warehouse_id:
        raise CompactionRefused("no SQL warehouse configured")
    statement = optimize_statement(industry)
    result: Any = await dbsql_service.execute_dbsql(statement, warehouse_id, OPTIMIZE_TIMEOUT)
    rows = (result or {}).get("data") or []
    return {"statement": statement, "status": "ok", "metrics": rows[0] if rows else {}}
