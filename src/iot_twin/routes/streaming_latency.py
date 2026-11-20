"""Latency + throughput KPI endpoints for the per-mode dashboards.

Two industry-scoped endpoints feed the dashboard's latency/throughput tiles:

  * ``GET /api/latency/streaming?industry=&mode=realtime|microbatch`` reads the
    latest rows of the **shared** ``{industry}_metrics`` managed Delta table
    (``{UC_CATALOG}.{UC_SCHEMA}.{industry}_metrics`` — see
    ``iot_twin.services.industry.industry_catalog_service.industry_table``) filtered by
    ``mode``, surfacing the e2e / processing / queuing p50+p99 percentiles,
    input/processed rows-per-sec, trigger interval, latest batch id/timestamp,
    and a short ascending series for a sparkline.  Both tiles (latency KPIs)
    and charts (``/api/pipeline/{id}/streaming-metrics``) now read this same
    table.
  * ``GET /api/latency/zerobus?industry=`` derives ingest KPIs for the zerobus
    target (``{UC_CATALOG}.{UC_SCHEMA}.{industry}_zerobus``) from the system
    table ``system.lakeflow.zerobus_ingest``: commit freshness, throughput,
    committed bytes and any recent ingest error messages.  Zerobus is not a
    Spark stream, so there are no p50/p99 percentiles.

Both endpoints are defensive: a catalog/schema/table that hasn't been
provisioned yet, a window with no rows, or an inaccessible system table
degrades to a 200 response with a nulled/zeroed payload and a human-readable
``reason`` — never a 500. Warehouse resolution + DBSQL execution reuse the same
helpers as ``routes/sensor_data.py`` (``resolve_warehouse_id`` /
``execute_dbsql``), so DBSQL always runs as the app service principal.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state
from iot_twin.models import metrics_schema  # noqa: F401 — kept for backward compat; new code uses _icat
from iot_twin.routes.sensor_data import _is_industry_deployment
from iot_twin.services.dbsql_service import (
    execute_dbsql,
    resolve_warehouse_id,
    validate_catalog,
)
from iot_twin.services.industry import industry_catalog_service as _icat
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.uc_ident import sql_fqn

logger = logging.getLogger("iot_digital_twin.routes.streaming_latency")
router = APIRouter()


async def _is_bound_pipeline(
    state: AppState,
    pipeline_id: str,
    industry: str,
    mode: str,
    user_email: str,
) -> bool:
    """Authorize industry KPI reads against deployment or legacy ownership."""
    industry = industry.strip().lower()
    if industry not in CURATED_INDUSTRIES:
        return False
    if await _is_industry_deployment(state, pipeline_id, industry, mode):
        return True
    if not state.db_available or state.db is None:
        return False
    try:
        row = await state.db.fetchrow(
            """
            SELECT 1
            FROM pipelines
            WHERE id = $1::uuid
              AND industry = $2
              AND user_email = $3
              AND status != 'deleted'
            """,
            pipeline_id,
            industry,
            user_email,
        )
    except Exception:
        return False
    return row is not None


def _is_missing_table_error(exc: Exception) -> bool:
    """True when a DBSQL query failed because the target table/schema/catalog
    doesn't exist yet — "no data yet", not a server fault."""
    msg = str(exc).lower()
    return (
        "table_or_view_not_found" in msg
        or "table or view not found" in msg
        or "schema_not_found" in msg
        or "catalog_not_found" in msg
        or ("does not exist" in msg and ("table" in msg or "view" in msg or "schema" in msg))
    )


def _num(value: Any) -> float | None:
    """Coerce a DBSQL scalar to ``float`` (or ``None`` when null/blank/bad)."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    """Coerce a DBSQL scalar to ``int`` (or ``None`` when null/blank/bad)."""
    n = _num(value)
    return int(n) if n is not None else None


def _first_non_null(rows: list[dict[str, Any]], column: str) -> Any:
    """Return the first non-null value of ``column`` scanning newest-first rows.

    The metrics table can hold nulls in a given latency column for the most
    recent batch (e.g. a trigger with no input), so the latest *non-null* value
    is a truer "current" reading than blindly taking row 0.
    """
    for row in rows:
        val = row.get(column)
        if val is not None and val != "":
            return val
    return None


# ---------------------------------------------------------------------------
# Streaming latency (realtime / microbatch) — per-mode metrics Delta table
# ---------------------------------------------------------------------------
def _empty_streaming_payload(
    table: str,
    industry: str,
    mode: str,
    reason: str,
) -> dict[str, Any]:
    """Nulled streaming-latency payload (all KPI keys present) with a reason."""
    return {
        "table": table,
        "mode": mode,
        "scope": {"industry": industry, "mode": mode},
        "has_data": False,
        "last_batch_id": None,
        "last_timestamp": None,
        "num_input_rows": None,
        "input_rps": None,
        "processed_rps": None,
        "trigger_ms": None,
        "e2e_p50": None,
        "e2e_p99": None,
        "processing_p50": None,
        "processing_p99": None,
        "queuing_p50": None,
        "queuing_p99": None,
        "rows_per_sec": None,
        "processing_latency_p50_ms": None,
        "processing_latency_p99_ms": None,
        "spark_e2e_latency_p99_ms": None,
        "throughput_basis": "spark_processed_rows_per_second",
        "e2e_basis": "spark_source_available_to_trigger_complete",
        "series": [],
        "reason": reason,
    }


@router.get("/api/latency/streaming")
async def get_streaming_latency(
    industry: str = Query(..., description="Industry id — resolves iot_<industry>"),
    mode: str = Query(..., description="Streaming mode: realtime or microbatch"),
    pipeline_id: str = Query(..., description="Bound pipeline deployment id"),
    minutes: int = Query(
        60, ge=1, le=10080, description="Lookback window (minutes) for the sparkline series"
    ),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Latest latency + throughput KPIs from the shared ``{industry}_metrics`` table.

    Reads the latest rows of the managed per-industry metrics Delta table
    (``{UC_CATALOG}.{UC_SCHEMA}.{industry}_metrics``) filtered by ``mode``,
    surfacing the latest non-null e2e/processing/queuing p50+p99, input/processed
    rps, trigger interval, num_input_rows and the latest batch id/timestamp, plus
    a short ascending series for a sparkline.  Both this tile path AND the charts
    path (``/api/pipeline/{id}/streaming-metrics``) read the same table.

    Missing table / no rows → 200 with a nulled payload (never 500).
    """
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in metrics_schema.ALL_METRIC_MODES:
        return JSONResponse(
            status_code=400,
            content={"error": f"mode must be one of {metrics_schema.ALL_METRIC_MODES}"},
        )
    industry = industry.strip().lower()
    if industry not in CURATED_INDUSTRIES:
        return JSONResponse(status_code=400, content={"error": "Invalid industry"})
    if not await _is_bound_pipeline(state, pipeline_id, industry, normalized_mode, user_email):
        return JSONResponse(status_code=404, content={"error": "Industry deployment not found"})

    try:
        table = _icat.industry_table(industry, "metrics")
    except (ValueError, RuntimeError) as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid industry: {exc}"})
    if not validate_catalog(table.split(".")[0]):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    sql_table = sql_fqn(*table.split("."))
    sql = f"""
        SELECT batch_id, timestamp, num_input_rows, input_rps, processed_rps,
               trigger_ms, e2e_p50, e2e_p99, processing_p50, processing_p99,
               queuing_p50, queuing_p99
        FROM {sql_table}
        WHERE mode = '{normalized_mode}'
          AND timestamp >= TIMESTAMPADD(MINUTE, -{int(minutes)}, CURRENT_TIMESTAMP())
        ORDER BY timestamp DESC, batch_id DESC
        LIMIT 60
    """

    try:
        result = await execute_dbsql(sql, warehouse_id)
    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.info(
                "streaming latency: %s not provisioned/populated yet — returning empty",
                table,
            )
            return _empty_streaming_payload(
                table,
                industry,
                normalized_mode,
                "Metrics table not provisioned yet",
            )
        # Never 500 — degrade gracefully so the dashboard tile shows "—".
        logger.warning("streaming latency query failed for %s: %s", table, exc)
        return _empty_streaming_payload(
            table,
            industry,
            normalized_mode,
            f"Metrics query failed: {type(exc).__name__}: {str(exc)[:200]}",
        )

    rows = result.get("data") or []
    if not rows:
        return _empty_streaming_payload(
            table,
            industry,
            normalized_mode,
            "No metrics rows in the requested window",
        )

    latest = rows[0]
    # Ascending series (oldest → newest) for the sparkline.
    series = [
        {
            "batch_id": _int(r.get("batch_id")),
            "timestamp": r.get("timestamp"),
            "e2e_p50": _num(r.get("e2e_p50")),
            "e2e_p99": _num(r.get("e2e_p99")),
            "processing_p50": _num(r.get("processing_p50")),
            "processing_p99": _num(r.get("processing_p99")),
            "input_rps": _num(r.get("input_rps")),
            "processed_rps": _num(r.get("processed_rps")),
            "trigger_ms": _num(r.get("trigger_ms")),
        }
        for r in reversed(rows)
    ]

    return {
        "table": table,
        "mode": normalized_mode,
        "scope": {"industry": industry, "mode": normalized_mode},
        "has_data": True,
        "last_batch_id": _int(latest.get("batch_id")),
        "last_timestamp": latest.get("timestamp"),
        "num_input_rows": _int(_first_non_null(rows, "num_input_rows")),
        "input_rps": _num(_first_non_null(rows, "input_rps")),
        "processed_rps": _num(_first_non_null(rows, "processed_rps")),
        "trigger_ms": _num(_first_non_null(rows, "trigger_ms")),
        "e2e_p50": _num(_first_non_null(rows, "e2e_p50")),
        "e2e_p99": _num(_first_non_null(rows, "e2e_p99")),
        "processing_p50": _num(_first_non_null(rows, "processing_p50")),
        "processing_p99": _num(_first_non_null(rows, "processing_p99")),
        "queuing_p50": _num(_first_non_null(rows, "queuing_p50")),
        "queuing_p99": _num(_first_non_null(rows, "queuing_p99")),
        # Explicit aliases for the four summary tiles. These values come from
        # this industry+mode metrics table, not the process-global /api/sources
        # counters (which measure app HTTP activity rather than Spark work).
        "rows_per_sec": _num(_first_non_null(rows, "processed_rps")),
        "processing_latency_p50_ms": _num(_first_non_null(rows, "processing_p50")),
        "processing_latency_p99_ms": _num(_first_non_null(rows, "processing_p99")),
        "spark_e2e_latency_p99_ms": _num(_first_non_null(rows, "e2e_p99")),
        "throughput_basis": "spark_processed_rows_per_second",
        "e2e_basis": "spark_source_available_to_trigger_complete",
        "series": series,
    }


# ---------------------------------------------------------------------------
# Zerobus latency/health — system.lakeflow.zerobus_ingest
# ---------------------------------------------------------------------------
def _empty_zerobus_payload(table_name: str, reason: str) -> dict[str, Any]:
    """Nulled/zeroed zerobus-latency payload with a human-readable reason."""
    return {
        "table_name": table_name,
        "has_data": False,
        "last_commit_time": None,
        "seconds_since_commit": None,
        "committed_records_total": 0,
        "committed_bytes_total": 0,
        "records_per_min": 0.0,
        "recent_error_count": 0,
        "recent_errors": [],
        "reason": reason,
    }


@router.get("/api/latency/zerobus")
async def get_zerobus_latency(
    industry: str = Query(..., description="Industry id — resolves iot_<industry>"),
    pipeline_id: str = Query(..., description="Bound pipeline deployment id"),
    minutes: int = Query(60, ge=1, le=10080, description="Lookback window in minutes"),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Zerobus commit-freshness/health KPIs for ``{industry}_zerobus``.

    Resolves the target table as ``{UC_CATALOG}.{UC_SCHEMA}.{industry}_zerobus``
    via ``industry_table(industry, "zerobus")``.  Derives commit freshness,
    throughput (committed_records_total / records_per_min / committed_bytes_total),
    a recent error count and up to 3 recent error messages from
    ``system.lakeflow.zerobus_ingest``.  Inaccessible system table or an empty
    window → 200 with a zeroed payload (never 500).
    """
    industry = industry.strip().lower()
    if industry not in CURATED_INDUSTRIES:
        return JSONResponse(status_code=400, content={"error": "Invalid industry"})
    if not await _is_bound_pipeline(state, pipeline_id, industry, "zerobus", user_email):
        return JSONResponse(status_code=404, content={"error": "Industry deployment not found"})
    try:
        target_table = _icat.industry_table(industry, "zerobus")
    except (ValueError, RuntimeError) as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid industry: {exc}"})
    if not validate_catalog(target_table.split(".")[0]):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    # ``target_table`` comes from our own catalog/config, not arbitrary user
    # input, but escape single quotes anyway before it goes into the literal.
    safe_target = target_table.replace("'", "''")
    window = int(minutes)

    summary_sql = f"""
        SELECT MAX(commit_time) AS last_commit_time,
               COALESCE(SUM(committed_records), 0) AS committed_records_total,
               COALESCE(SUM(committed_bytes), 0) AS committed_bytes_total,
               COALESCE(SUM(size(COALESCE(errors, array()))), 0) AS recent_error_count,
               (unix_timestamp(now()) - unix_timestamp(MAX(commit_time))) AS seconds_since_commit
        FROM system.lakeflow.zerobus_ingest
        WHERE table_name = '{safe_target}'
          AND commit_time >= now() - INTERVAL {window} MINUTES
    """
    errors_sql = f"""
        SELECT e.error_message AS error_message
        FROM system.lakeflow.zerobus_ingest
        LATERAL VIEW explode(errors) t AS e
        WHERE table_name = '{safe_target}'
          AND commit_time >= now() - INTERVAL {window} MINUTES
          AND e.error_message IS NOT NULL
        ORDER BY e.timestamp DESC
        LIMIT 3
    """

    try:
        summary_result = await execute_dbsql(summary_sql, warehouse_id)
        errors_result = await execute_dbsql(errors_sql, warehouse_id)
    except Exception as exc:
        logger.warning("Zerobus latency query failed for %s: %s", target_table, exc)
        return _empty_zerobus_payload(
            target_table,
            f"Zerobus system tables not accessible: {type(exc).__name__}: {str(exc)[:200]}",
        )

    summary_rows = summary_result.get("data") or []
    if not summary_rows:
        return _empty_zerobus_payload(
            target_table, "No Zerobus ingest rows for this table in the requested window"
        )

    summary = summary_rows[0]
    last_commit_time = summary.get("last_commit_time")
    committed_records_total = _int(summary.get("committed_records_total")) or 0
    committed_bytes_total = _int(summary.get("committed_bytes_total")) or 0
    recent_error_count = _int(summary.get("recent_error_count")) or 0
    seconds_since_commit = _num(summary.get("seconds_since_commit"))
    records_per_min = round(committed_records_total / window, 2) if window else 0.0

    recent_errors = [
        str(r.get("error_message"))
        for r in (errors_result.get("data") or [])
        if r.get("error_message") is not None
    ][:3]

    return {
        "table_name": target_table,
        "has_data": last_commit_time is not None or committed_records_total > 0,
        "last_commit_time": last_commit_time,
        "seconds_since_commit": (
            round(seconds_since_commit, 1) if seconds_since_commit is not None else None
        ),
        "committed_records_total": committed_records_total,
        "committed_bytes_total": committed_bytes_total,
        "records_per_min": records_per_min,
        "recent_error_count": recent_error_count,
        "recent_errors": recent_errors,
    }
