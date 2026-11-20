"""Sensor data query endpoints: ingest, timeseries, sensors, data, aggregates, latest, streaming-metrics, dbsql-timeseries."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state
from iot_twin.services import app_permissions, cdf_reconciliation_service
from iot_twin.services.alerts.alert_engine import evaluate_and_emit
from iot_twin.services.dbsql_service import (
    execute_dbsql,
    get_pipeline_catalog,
    resolve_warehouse_id,
    validate_catalog,
)
from iot_twin.services.industry import industry_catalog_service, industry_service
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.pipelines.pipeline_db import (
    PipelineLakebaseHandle,
    get_industry_lakebase,
    get_pipeline_lakebase,
)
from iot_twin.services.simulator_service import broadcast
from iot_twin.services.uc_ident import sql_fqn

logger = logging.getLogger("iot_digital_twin.routes.sensor_data")
router = APIRouter()

# Bound Lakebase dashboard probes so hung industry branches fail closed
# instead of waiting on the reverse-proxy upstream timeout.
_LAKEBASE_PROBE_TIMEOUT_SECONDS = 8.0
_LAKEBASE_RESOLVE_TIMEOUT_SECONDS = 25.0
_LAKEBASE_ANALYTICS_SENSOR_LIMIT = 6
_CHUNK_TABLE_RE = re.compile(r"^(?:public\.)?(sensor_readings_\d{8}_\d{6})$")


async def _probe_wait(coro, *, timeout: float = _LAKEBASE_PROBE_TIMEOUT_SECONDS):
    """Await ``coro`` with a deadline and fully reap cancellation."""
    return await asyncio.wait_for(coro, timeout=timeout)


def _chunk_table_names(rows: list[Any] | None) -> list[str]:
    """Return validated ChronoTable child names from ``_chunk_metadata`` rows."""
    tables: list[str] = []
    for row in rows or []:
        raw = ""
        if hasattr(row, "get"):
            raw = str(row.get("chunk_name") or "")
        match = _CHUNK_TABLE_RE.match(raw)
        if match:
            tables.append(match.group(1))
    return tables


def _readings_relation(tables: list[str], columns: str = "event_time, sensor_name, value") -> str:
    """SQL relation that reads recent chunks, or the parent table as a fallback."""
    if not tables:
        return "sensor_readings"
    unions = " UNION ALL ".join(f"SELECT {columns} FROM public.{table}" for table in tables)
    return f"({unions})"


async def _recent_chunk_tables(
    lb: PipelineLakebaseHandle,
    minutes: int,
) -> list[str]:
    """Child partitions overlapping the lookback window.

    Parent ``sensor_readings`` COUNT/aggregates do not prune under load and
    trip the dashboard probe timeout even while inserts are flowing.
    """
    try:
        rows = await lb.fetch(
            """
            SELECT chunk_name
            FROM lakets._chunk_metadata
            WHERE status = 'active'
              AND range_end > NOW() - make_interval(mins => $1)
              AND range_start < NOW() + interval '1 minute'
            ORDER BY range_start
            LIMIT 8
            """,
            minutes,
        )
    except Exception as exc:
        if _is_undefined_relation(exc):
            return []
        raise
    return _chunk_table_names(rows)


def _is_undefined_relation(exc: Exception) -> bool:
    """True when a query hit a Postgres 'undefined table' (SQLSTATE 42P01).

    A rollup/aggregate table that was never provisioned or populated means "no
    data yet", not a server fault — callers degrade to an empty result instead
    of a 500.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if sqlstate == "42P01":
        return True
    msg = str(exc).lower()
    return "does not exist" in msg and "relation" in msg


async def _resolve_lakebase(
    state: AppState, pipeline_id: str, industry: str | None
) -> PipelineLakebaseHandle | None:
    """Resolve the Lakebase handle to query: industry-first when ``industry`` is
    passed (deterministic ``iot-pipelines`` branch, no legacy ``pipelines`` row
    required), else the legacy per-pipeline ``sink_config`` lookup."""
    if industry:
        industry = industry.strip().lower()
        if industry not in CURATED_INDUSTRIES:
            raise HTTPException(status_code=404, detail="Realtime industry deployment not found")
        kind = await _authorize_mode_pipeline(
            state, pipeline_id, industry, "realtime", user_email=""
        )
        if kind == "deployment":
            return await get_industry_lakebase(state, industry)
        if kind == "legacy":
            return await get_pipeline_lakebase(state, pipeline_id)
        raise HTTPException(status_code=404, detail="Realtime industry deployment not found")
    return await get_pipeline_lakebase(state, pipeline_id)


@router.post("/api/sss/ingest")
async def sss_ingest(request: Request, state: AppState = Depends(get_app_state)):
    """Receive aggregated sensor data from the SSS Databricks Job.

    When the request carries an ``X-Pipeline-Id`` header, also INSERT the rows
    into that pipeline's Lakebase ``sensor_readings`` table via the app's
    per-pipeline OAuth pool (``get_pipeline_lakebase``). This shifts the
    write responsibility from Spark executors (which suffer from OAuth token
    expiry on long-lived connections) to the app, which already handles token
    rotation cleanly.
    """
    try:
        data = await request.json()
        if not isinstance(data, list):
            data = [data]
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    state.sources["sss"].status = "active"
    state.sources["sss"].error = None
    t0 = time.time()

    e2e_ms = None
    ts_fields = [
        d.get("window_end") or d.get("timestamp")
        for d in data
        if d.get("window_end") or d.get("timestamp")
    ]
    if ts_fields:
        import datetime as _dt

        now = time.time()
        epoch_vals = []
        for ts in ts_fields:
            if isinstance(ts, (int, float)):
                epoch_vals.append(float(ts))
            elif isinstance(ts, str):
                try:
                    epoch_vals.append(
                        _dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                    )
                except Exception:
                    pass
        if epoch_vals:
            avg_ts = sum(epoch_vals) / len(epoch_vals)
            e2e_ms = (now - avg_ts) * 1000

    # If the consumer identifies a pipeline, insert rows into its Lakebase.
    pipeline_id = request.headers.get("X-Pipeline-Id") or request.headers.get("x-pipeline-id")
    # Industry-first REALTIME pipelines send ``X-Pipeline-Id="industry:<industry>"``.
    # The deployed consumer's RealtimeDualWriter writes sensor_readings DIRECTLY to
    # the Lakebase branch (LakebaseWriter, per-open OAuth mint + auto-reconnect) AND
    # POSTs here purely for the SSE broadcast — so this handler must NOT insert again
    # for industry ids (that would double every row). Only legacy per-pipeline ids
    # persist below, via the app's OAuth-refreshing pool.
    industry_id: str | None = None
    if pipeline_id and pipeline_id.startswith("industry:"):
        industry_id = pipeline_id.split(":", 1)[1] or None
    lakebase_inserted = 0
    lakebase_error: str | None = None
    lb_resolved = False

    # Diagnostic: stamp every call's outcome on the sources object so we can
    # see at a glance (via /api/sources) what headers the consumer sent and
    # whether the INSERT path executed.

    src = state.sources["sss"]
    # Attach counters on first use
    if not hasattr(src, "_lb_counters"):
        src._lb_counters = {
            "with_header": 0,
            "no_header": 0,
            "inserted": 0,
            "errors": 0,
            "last_keys": None,
            "last_headers": None,
        }
    headers_snapshot = {
        k: v
        for k, v in request.headers.items()
        if k.lower() in ("x-pipeline-id", "content-type", "user-agent", "host")
    }
    src._lb_counters["last_headers"] = headers_snapshot
    if data:
        src._lb_counters["last_keys"] = list(data[0].keys() if isinstance(data[0], dict) else [])

    if not pipeline_id:
        src._lb_counters["no_header"] += 1
        msg = f"X-Pipeline-Id missing — skipped {len(data)} rows (last_keys={src._lb_counters['last_keys']})"
        src.error = msg
    else:
        src._lb_counters["with_header"] += 1
    # Legacy per-pipeline ids persist to Lakebase here. Industry-first pipelines
    # are BROADCAST-ONLY: the realtime consumer's RealtimeDualWriter already
    # writes sensor_readings directly (LakebaseWriter), so inserting again here
    # would double every row (see project_pipeline_dataflow_wiring). We still
    # broadcast the rows over SSE below for the live dashboard.
    if pipeline_id and not industry_id:
        try:
            lb = await get_pipeline_lakebase(state, pipeline_id)
            lb_resolved = lb is not None
            if lb is None:
                lakebase_error = "get_pipeline_lakebase returned None"
                logger.error("sss_ingest: get_pipeline_lakebase(%s) returned None", pipeline_id)
                state.sources["sss"].error = lakebase_error
            if lb is not None:
                import datetime as _dt

                # Use ingestion time as event_time so the dashboard's "last 5 min"
                # window shows live data, not timestamps from whatever point in
                # the Kafka backlog the consumer is currently chewing through.
                # The original producer timestamp is still on the record via
                # the broadcasted row field but we don't persist it separately.
                now_utc = _dt.datetime.now(_dt.UTC)
                src._lb_counters["last_event_time"] = now_utc.isoformat()
                rows: list[tuple] = []
                for d in data:
                    event_time = now_utc
                    rows.append(
                        (
                            event_time,
                            d.get("sensor_name"),
                            d.get("industry"),
                            float(d.get("value")) if d.get("value") is not None else None,
                            d.get("unit"),
                            d.get("sensor_type"),
                            bool(d.get("fault_active"))
                            if d.get("fault_active") is not None
                            else False,
                            "sss",
                        )
                    )
                # asyncpg: use executemany-style INSERT for batch performance
                in_txn_count = await lb.executemany(
                    """
                    INSERT INTO sensor_readings
                      (event_time, sensor_name, industry, value, unit,
                       sensor_type, fault_active, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    rows,
                )
                lakebase_inserted = len(rows)
                src._lb_counters["inserted"] += lakebase_inserted
                # executemany now returns COUNT(*) AS SEEN INSIDE THE TRANSACTION
                # post-INSERT. If that is > 0 but a fresh SELECT shows 0, the
                # commit is the problem. If in_txn_count is 0 or -1, INSERT is
                # not taking effect at all inside the transaction itself.
                src._lb_counters["in_txn_count"] = in_txn_count
                # Separate pool-acquired fetchval AFTER commit, to see if
                # another connection sees the newly-committed rows.
                try:
                    current_count = await lb.fetchval(
                        "SELECT COUNT(*) FROM sensor_readings WHERE source = 'sss' AND event_time > NOW() - INTERVAL '5 minutes'"
                    )
                    src._lb_counters["last_post_verify_count_5m"] = current_count
                except Exception as _e:
                    src._lb_counters["last_post_verify_count_5m"] = f"err: {_e}"
        except Exception as exc:
            lakebase_error = f"{type(exc).__name__}: {str(exc)[:200]}"
            logger.error(
                "sss_ingest Lakebase insert failed for pipeline %s (lb_resolved=%s): %s",
                pipeline_id,
                lb_resolved,
                exc,
                exc_info=True,
            )
            state.sources["sss"].error = lakebase_error

    await broadcast(
        state,
        {
            "type": "sensor_update",
            "source": "sss",
            "sensors": data,
            "write_latency_ms": 0,
            "e2e_latency_ms": round(e2e_ms, 1) if e2e_ms is not None else 0,
        },
    )

    try:
        reading_industry = industry_id
        if not reading_industry and data and isinstance(data[0], dict):
            reading_industry = data[0].get("industry")
        await evaluate_and_emit(
            state,
            data,
            industry=reading_industry,
            pipeline_id=pipeline_id,
        )
    except Exception:
        logger.exception("sss_ingest: alert evaluation failed")

    write_ms = (time.time() - t0) * 1000
    state.sources["sss"].record_write(len(data), write_ms, e2e_ms)

    return {
        "status": "ok",
        "records": len(data),
        "lakebase_inserted": lakebase_inserted,
        **({"lakebase_error": lakebase_error} if lakebase_error else {}),
    }


@router.get("/api/sss/aggregates")
async def sss_aggregates(
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return recent windowed aggregates from the sensor_aggregates table."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        rows = await state.db.fetch(
            """
            SELECT window_start, window_end, sensor_name, industry,
                   avg_value, min_value, max_value, record_count,
                   unit, sensor_type
            FROM sensor_aggregates
            WHERE window_start > NOW() - make_interval(mins => $1)
            ORDER BY window_start DESC
            LIMIT 500
            """,
            minutes,
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        # The sensor_aggregates rollup table may not be provisioned/populated
        # yet — that is "no aggregates", not a server error.
        if _is_undefined_relation(exc):
            logger.info("sss_aggregates: sensor_aggregates not provisioned yet — returning empty")
            return []
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/timeseries")
async def get_timeseries(
    minutes: int = Query(5, ge=1, le=1440, description="Lookback window in minutes"),
    source: str | None = Query(None, description="Filter by source"),
    sensor_type: str | None = Query(None, description="Filter by sensor type"),
    industry: str | None = Query(None, description="Filter by industry"),
    state: AppState = Depends(get_app_state),
):
    """Unified time-series endpoint with auto-resolution."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    try:
        use_raw = minutes <= 10 or source is not None

        if use_raw and minutes <= 10:
            resolution = "raw"
            query = """
                SELECT event_time AS time, sensor_name, sensor_type, industry,
                       value, unit, fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params: list[Any] = [minutes]
            if source:
                query += f" AND source = ${len(params) + 1}"
                params.append(source)
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            if industry:
                query += f" AND industry = ${len(params) + 1}"
                params.append(industry)
            query += " ORDER BY event_time ASC LIMIT 5000"
            rows = await state.db.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": r["value"],
                    "unit": r.get("unit"),
                    "fault_active": r.get("fault_active", False),
                }
                for r in rows
            ]

        elif use_raw and source is not None:
            resolution = "raw_agg"
            # date_trunc takes unit NAMES ('minute', 'hour'), not intervals.
            # For 5-min buckets we use arithmetic since date_trunc can't do it.
            if minutes <= 60:
                bucket_expr = "date_trunc('minute', event_time)"
            else:
                bucket_expr = (
                    "to_timestamp(floor(extract(epoch from event_time) / 300) * 300) "
                    "AT TIME ZONE 'UTC'"
                )
            query = f"""
                SELECT {bucket_expr} AS time,
                       sensor_name, sensor_type, industry,
                       AVG(value) AS value,
                       MIN(value) AS min_value,
                       MAX(value) AS max_value,
                       COUNT(*) AS sample_count
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
                  AND source = $2
            """
            params = [minutes, source]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            if industry:
                query += f" AND industry = ${len(params) + 1}"
                params.append(industry)
            query += """
                GROUP BY time, sensor_name, sensor_type, industry
                ORDER BY time ASC
                LIMIT 5000
            """
            rows = await state.db.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": float(r["value"]),
                    "min_value": float(r["min_value"]),
                    "max_value": float(r["max_value"]),
                    "sample_count": r["sample_count"],
                }
                for r in rows
            ]

        else:
            # Aggregate from raw sensor_readings using date_trunc.
            # Use a whitelist for the interval literal to prevent injection.
            # date_trunc takes unit NAMES ('minute', 'hour'), not intervals.
            # For 5-min buckets we use arithmetic since date_trunc can't do it.
            if minutes <= 60:
                bucket_expr = "date_trunc('minute', event_time)"
                resolution = "1m"
            else:
                bucket_expr = (
                    "to_timestamp(floor(extract(epoch from event_time) / 300) * 300) "
                    "AT TIME ZONE 'UTC'"
                )
                resolution = "5m"
            query = f"""
                SELECT {bucket_expr} AS time,
                       sensor_name, sensor_type, industry,
                       AVG(value) AS value,
                       MIN(value) AS min_value,
                       MAX(value) AS max_value,
                       COUNT(*) AS sample_count,
                       BOOL_OR(COALESCE(fault_active, false)) AS fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params = [minutes]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            if industry:
                query += f" AND industry = ${len(params) + 1}"
                params.append(industry)
            query += """
                GROUP BY time, sensor_name, sensor_type, industry
                ORDER BY time ASC
                LIMIT 5000
            """
            rows = await state.db.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": float(r["value"]),
                    "min_value": float(r["min_value"]),
                    "max_value": float(r["max_value"]),
                    "sample_count": r["sample_count"],
                    "fault_active": r.get("fault_active", False),
                }
                for r in rows
            ]

        return {"resolution": resolution, "minutes": minutes, "count": len(data), "rows": data}

    except Exception as exc:
        logger.warning("Timeseries query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/sensors")
async def list_sensors(state: AppState = Depends(get_app_state)):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        """
        SELECT DISTINCT ON (sensor_name)
            sensor_name, industry, sensor_type, unit, value,
            event_time, fault_active
        FROM sensor_readings
        ORDER BY sensor_name, event_time DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/api/data")
async def get_data(
    sensor: str = Query(..., description="Sensor name"),
    minutes: int = Query(5, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        """
        SELECT event_time, value, fault_active
        FROM sensor_readings
        WHERE sensor_name = $1
          AND event_time > NOW() - make_interval(mins => $2)
        ORDER BY event_time ASC
        """,
        sensor,
        minutes,
    )
    return [dict(r) for r in rows]


@router.get("/api/aggregates")
async def get_aggregates(
    interval: str = Query("1m", description="Aggregate interval: 1m or 5m"),
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    ALLOWED_VIEWS = {"1m": "sensor_1m", "5m": "sensor_5m"}
    if interval not in ALLOWED_VIEWS:
        return JSONResponse(status_code=422, content={"error": "interval must be '1m' or '5m'"})
    view = ALLOWED_VIEWS[interval]
    try:
        rows = await state.db.fetch(
            f"""
            SELECT *
            FROM {view}
            WHERE bucket > NOW() - make_interval(mins => $1)
            ORDER BY bucket DESC
            """,
            minutes,
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": f"Aggregate query failed: {exc}"})


@router.get("/api/latest")
async def get_latest(state: AppState = Depends(get_app_state)):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        """
        SELECT DISTINCT ON (sensor_name)
            sensor_name, industry, sensor_type, unit, value,
            event_time, fault_active, source
        FROM sensor_readings
        ORDER BY sensor_name, event_time DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/api/pipelines/{pipeline_id}/stats")
async def get_pipeline_stats_scoped(
    pipeline_id: str,
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return throughput and freshness stats for a specific pipeline's Lakebase."""
    lb = await get_pipeline_lakebase(state, pipeline_id)
    if lb is None:
        return JSONResponse(status_code=503, content={"error": "Pipeline Lakebase not resolvable"})
    try:
        row = await lb.fetchrow(
            """
            SELECT COUNT(*) AS total_records,
                   COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) / 60, 1) AS records_per_min,
                   MAX(event_time) AS latest_event,
                   MIN(event_time) AS oldest_event,
                   EXTRACT(EPOCH FROM (NOW() - MAX(event_time))) AS freshness_sec
            FROM sensor_readings
            WHERE event_time > NOW() - make_interval(mins => $1)
            """,
            minutes,
        )
        if not row:
            return {
                "total_records": 0,
                "records_per_min": 0,
                "latest_event": None,
                "oldest_event": None,
                "freshness_sec": None,
            }
        return {
            "total_records": row["total_records"],
            "records_per_min": round(float(row["records_per_min"] or 0), 1),
            "latest_event": str(row["latest_event"]) if row["latest_event"] else None,
            "oldest_event": str(row["oldest_event"]) if row["oldest_event"] else None,
            "freshness_sec": round(float(row["freshness_sec"] or 0), 1)
            if row["freshness_sec"]
            else None,
        }
    except Exception as exc:
        logger.warning("Pipeline %s stats query failed: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/pipelines/{pipeline_id}/timeseries")
async def get_pipeline_timeseries(
    pipeline_id: str,
    minutes: int = Query(5, ge=1, le=1440, description="Lookback window in minutes"),
    sensor_type: str | None = Query(None, description="Filter by sensor type"),
    state: AppState = Depends(get_app_state),
):
    """Pipeline-scoped timeseries — queries the pipeline's own Lakebase."""
    lb = await get_pipeline_lakebase(state, pipeline_id)
    if lb is None:
        return JSONResponse(status_code=503, content={"error": "Pipeline Lakebase not resolvable"})

    try:
        use_raw = minutes <= 10
        if use_raw:
            resolution = "raw"
            query = """
                SELECT event_time AS time, sensor_name, sensor_type, industry,
                       value, unit, fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params: list[Any] = [minutes]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            query += " ORDER BY event_time ASC LIMIT 5000"
            rows = await lb.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": r["value"],
                    "unit": r.get("unit"),
                    "fault_active": r.get("fault_active", False),
                }
                for r in rows
            ]
        else:
            # date_trunc takes unit NAMES ('minute', 'hour'), not intervals.
            # For 5-min buckets we use arithmetic since date_trunc can't do it.
            if minutes <= 60:
                bucket_expr = "date_trunc('minute', event_time)"
                resolution = "1m"
            else:
                bucket_expr = (
                    "to_timestamp(floor(extract(epoch from event_time) / 300) * 300) "
                    "AT TIME ZONE 'UTC'"
                )
                resolution = "5m"
            query = f"""
                SELECT {bucket_expr} AS time,
                       sensor_name, sensor_type, industry,
                       AVG(value) AS value,
                       MIN(value) AS min_value,
                       MAX(value) AS max_value,
                       COUNT(*) AS sample_count,
                       BOOL_OR(COALESCE(fault_active, false)) AS fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params = [minutes]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            query += """
                GROUP BY time, sensor_name, sensor_type, industry
                ORDER BY time ASC
                LIMIT 5000
            """
            rows = await lb.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": float(r["value"]),
                    "min_value": float(r["min_value"]),
                    "max_value": float(r["max_value"]),
                    "sample_count": r["sample_count"],
                    "fault_active": r.get("fault_active", False),
                }
                for r in rows
            ]

        return {"resolution": resolution, "minutes": minutes, "count": len(data), "rows": data}

    except Exception as exc:
        logger.warning("Pipeline %s timeseries query failed: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


# Deprecated alias — kept for backward compat with the workspace-wide
# /api/pipeline-stats caller.  New code should use /api/pipelines/:id/stats.
@router.get("/api/pipeline-stats")
async def get_pipeline_stats(
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    industry: str | None = Query(None, description="Filter by industry"),
    state: AppState = Depends(get_app_state),
):
    """Legacy workspace-scoped stats against app backend Lakebase (no pipeline filter)."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        query = """
            SELECT COUNT(*) AS total_records,
                   COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) / 60, 1) AS records_per_min,
                   MAX(event_time) AS latest_event,
                   MIN(event_time) AS oldest_event,
                   EXTRACT(EPOCH FROM (NOW() - MAX(event_time))) AS freshness_sec
            FROM sensor_readings
            WHERE event_time > NOW() - make_interval(mins => $1)
        """
        params: list[Any] = [minutes]
        if industry:
            query += " AND industry = $2"
            params.append(industry)

        row = await state.db.fetchrow(query, *params)
        if not row:
            return {
                "total_records": 0,
                "records_per_min": 0,
                "latest_event": None,
                "oldest_event": None,
                "freshness_sec": None,
            }
        return {
            "total_records": row["total_records"],
            "records_per_min": round(float(row["records_per_min"] or 0), 1),
            "latest_event": str(row["latest_event"]) if row["latest_event"] else None,
            "oldest_event": str(row["oldest_event"]) if row["oldest_event"] else None,
            "freshness_sec": round(float(row["freshness_sec"] or 0), 1)
            if row["freshness_sec"]
            else None,
        }
    except Exception as exc:
        logger.warning("Pipeline stats query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/pipelines/{pipeline_id}/lakebase-perf")
async def get_lakebase_perf(
    pipeline_id: str,
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    industry: str | None = Query(
        None,
        description="Industry id — resolves the deterministic iot-pipelines "
        "branch directly (for industry-first dashboards with no legacy "
        "`pipelines` row).",
    ),
    state: AppState = Depends(get_app_state),
):
    """Return insertion-time write performance for a pipeline's Lakebase.

    Throughput (``rows_per_sec`` / ``rows_per_min`` / ``series``)
    -----------------------------------------------------------
    The app stamps ``event_time = NOW()`` at ingest (see ``sss_ingest``), so
    for this table ``event_time`` *is* the Lakebase insertion time. Rows are
    bucketed by ``event_time`` per-minute (``date_trunc('minute', ...)``) over
    the lookback window. Therefore each bucket counts rows *inserted* in that
    minute (not rows produced by upstream sensors in that minute):

      * ``rows_per_sec`` = total rows in window / (minutes * 60)
      * ``rows_per_min`` = total rows in window / minutes
      * ``series[i].rows_per_sec`` = that minute-bucket's row count / 60

    Ingest lag (``p50_insert_lag_ms`` / ``p95_insert_lag_ms``)
    ---------------------------------------------------------
    The ``sensor_readings`` table has no producer-originated timestamp column,
    so a true event->insert lag cannot be computed in SQL. Instead the lag is
    surfaced from the ``sss`` source's rolling end-to-end latency window, which
    is measured at insert time in ``sss_ingest`` as
    ``e2e_ms = (now() - avg(producer event-time)) * 1000``. So the lag here is
    the **producer event-time -> Lakebase insert** latency:

      * ``p50_insert_lag_ms`` <- ``avg_e2e_latency_ms`` (rolling mean)
      * ``p95_insert_lag_ms`` <- ``p95_e2e_latency_ms`` (rolling p95)
      * ``lag_sample_count``  <- number of samples in the rolling window
      * ``lag_basis`` / ``lag_definition`` describe the above for the UI

    When the rolling window holds 0 samples (no recent ingests) p50/p95 are
    returned as ``null`` and ``lag_sample_count`` is 0, so the UI can render
    "—" rather than a misleading ``0``. When no lb is resolvable we return
    graceful zero throughput and null lag.
    """
    lag_basis = "producer_event_to_lakebase_insert"
    lag_definition = "Ingest lag = now() - producer event-time, measured at Lakebase insert"

    empty_perf_payload = {
        "rows_per_sec": 0,
        "rows_per_min": 0,
        "p50_insert_lag_ms": None,
        "p95_insert_lag_ms": None,
        "lag_basis": lag_basis,
        "lag_definition": lag_definition,
        "lag_sample_count": 0,
        "series": [],
    }

    lb = None
    try:
        lb = await _probe_wait(
            _resolve_lakebase(state, pipeline_id, industry),
            timeout=_LAKEBASE_RESOLVE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "Pipeline %s lakebase-perf resolve timed out after %.0fs",
            pipeline_id,
            _LAKEBASE_RESOLVE_TIMEOUT_SECONDS,
        )
        return empty_perf_payload
    if lb is None:
        return empty_perf_payload

    try:

        async def _load_perf_buckets():
            tables = await _recent_chunk_tables(lb, minutes)
            relation = _readings_relation(tables, columns="event_time")
            return await lb.fetch(
                f"""
            SELECT date_trunc('minute', readings.event_time) AS bucket,
                   COUNT(*) AS cnt
            FROM {relation} readings
            WHERE readings.event_time > NOW() - make_interval(mins => $1)
              AND readings.event_time < NOW() + interval '1 minute'
            GROUP BY bucket
            ORDER BY bucket ASC
            LIMIT 5000
            """,
                minutes,
            )

        rows = await _probe_wait(
            _load_perf_buckets(),
            timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "Pipeline %s lakebase-perf query timed out after %.0fs",
            pipeline_id,
            _LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )
        return empty_perf_payload
    except Exception as exc:
        if industry and _is_undefined_relation(exc):
            # Realtime not deployed yet for this industry branch — no table,
            # not a server fault.
            logger.info(
                "Lakebase perf: sensor_readings not provisioned yet for industry %s",
                industry,
            )
            return empty_perf_payload
        logger.warning("Pipeline %s lakebase-perf query failed: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    total = sum(int(r["cnt"] or 0) for r in rows)
    window_sec = max(minutes * 60, 1)
    rows_per_sec = total / window_sec
    rows_per_min = round(total / minutes) if minutes else 0

    # Each per-minute bucket holds `cnt` rows over 60 seconds.
    series = [
        {
            "t": str(r["bucket"]) if r["bucket"] is not None else None,
            "rows_per_sec": round(int(r["cnt"] or 0) / 60.0, 3),
        }
        for r in rows
    ]

    # Industry-first consumers write directly to Lakebase, bypassing sss_ingest;
    # its process-global latency window therefore cannot describe this pipeline.
    if industry:
        lag_sample_count = 0
        p50_lag = None
        p95_lag = None
        lag_basis = "unavailable_for_direct_lakebase_writer"
        lag_definition = "Direct Lakebase writes do not currently persist producer timestamps"
    else:
        src = state.sources.get("sss")
        lag_sample_count = len(src.e2e_latency_samples) if src is not None else 0
        if lag_sample_count > 0:
            p50_lag = round(src.avg_e2e_latency_ms, 1)
            p95_lag = round(src.p95_e2e_latency_ms, 1)
        else:
            p50_lag = None
            p95_lag = None

    return {
        "rows_per_sec": round(rows_per_sec, 3),
        "rows_per_min": rows_per_min,
        "p50_insert_lag_ms": p50_lag,
        "p95_insert_lag_ms": p95_lag,
        "lag_basis": lag_basis,
        "lag_definition": lag_definition,
        "lag_sample_count": lag_sample_count,
        "series": series,
    }


async def _probe_lakets_available(
    lb: PipelineLakebaseHandle | None,
) -> tuple[bool, str | None]:
    """Probe ``lakets._version`` on the pipeline's Lakebase.

    On success: ``(True, None)``.
    On failure: ``(False, <human-readable reason>)`` so the caller can surface
    a specific message to the client (e.g. permissions vs missing schema).
    """
    if lb is None:
        return False, "Pipeline Lakebase not resolvable"
    try:
        version = await lb.fetchval(
            "SELECT version FROM lakets._version ORDER BY installed_at DESC LIMIT 1"
        )
        if not version:
            return False, "LakeTS not installed (lakets._version empty)"
        return True, None
    except Exception as exc:
        reason = f"LakeTS probe failed: {type(exc).__name__}: {exc}"
        logger.warning("lakets._version probe failed: %s", exc)
        return False, reason


@router.get("/api/pipelines/{pipeline_id}/lakets/gapfill")
async def get_lakets_gapfill(
    pipeline_id: str,
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    industry: str | None = Query(
        None,
        description="Industry id — resolves the deterministic iot-pipelines "
        "branch directly (for industry-first dashboards with no legacy "
        "`pipelines` row).",
    ),
    state: AppState = Depends(get_app_state),
):
    """Return a bounded 1-minute spine with last-observation carry-forward."""
    lb = await _resolve_lakebase(state, pipeline_id, industry)
    installed, reason = await _probe_lakets_available(lb)
    if not installed or lb is None:
        return {"rows": [], "reason": reason or "LakeTS not installed"}

    try:

        async def _load_gapfill():
            tables = await _recent_chunk_tables(lb, minutes)
            relation = _readings_relation(tables)
            return await lb.fetch(
                f"""
            WITH bounds AS (
                SELECT date_trunc('minute', now())
                           - make_interval(mins => $1 - 1) AS window_start,
                       date_trunc('minute', now()) AS window_end
            ),
            active_sensors AS (
                SELECT readings.sensor_name
                FROM {relation} readings
                CROSS JOIN bounds
                WHERE readings.event_time >= bounds.window_start
                  AND readings.event_time < bounds.window_end + interval '1 minute'
                GROUP BY readings.sensor_name
                ORDER BY MAX(readings.event_time) DESC
                LIMIT {_LAKEBASE_ANALYTICS_SENSOR_LIMIT}
            ),
            buckets AS (
                SELECT generate_series(
                           bounds.window_start,
                           bounds.window_end,
                           interval '1 minute'
                       ) AS bucket
                FROM bounds
            ),
            base AS (
                SELECT lakets.time_bucket('1 minute'::interval, event_time) AS bucket,
                       sensor_name,
                       AVG(value) AS avg_value
                FROM {relation} readings
                CROSS JOIN bounds
                WHERE event_time >= bounds.window_start
                  AND event_time < bounds.window_end + interval '1 minute'
                  AND sensor_name IN (SELECT sensor_name FROM active_sensors)
                GROUP BY bucket, sensor_name
            ),
            spine AS (
                SELECT buckets.bucket, active_sensors.sensor_name
                FROM buckets
                CROSS JOIN active_sensors
            )
            SELECT spine.bucket,
                   spine.sensor_name,
                   COALESCE(current_bucket.avg_value, prior_bucket.avg_value) AS value,
                   (
                       current_bucket.avg_value IS NULL
                       AND prior_bucket.avg_value IS NOT NULL
                   ) AS gapfilled
            FROM spine
            LEFT JOIN base current_bucket
              ON current_bucket.bucket = spine.bucket
             AND current_bucket.sensor_name = spine.sensor_name
            LEFT JOIN LATERAL (
                SELECT previous.avg_value
                FROM base previous
                WHERE previous.sensor_name = spine.sensor_name
                  AND previous.bucket < spine.bucket
                  AND previous.avg_value IS NOT NULL
                ORDER BY previous.bucket DESC
                LIMIT 1
            ) prior_bucket ON current_bucket.avg_value IS NULL
            ORDER BY spine.bucket ASC, spine.sensor_name
            LIMIT 5000
            """,
                minutes,
            )

        rows = await _probe_wait(
            _load_gapfill(),
            timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "LakeTS gapfill query timed out after %.0fs for pipeline %s",
            _LAKEBASE_PROBE_TIMEOUT_SECONDS,
            pipeline_id,
        )
        return {"rows": [], "reason": "Lakebase query timed out"}
    except Exception as exc:
        if industry and _is_undefined_relation(exc):
            return {"rows": [], "reason": "sensor_readings not provisioned yet"}
        logger.warning("LakeTS gapfill query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    data = [
        {
            "bucket": str(r["bucket"]) if r["bucket"] is not None else None,
            "sensor_name": r["sensor_name"],
            "value": float(r["value"]) if r["value"] is not None else None,
            "gapfilled": bool(r.get("gapfilled", False)),
        }
        for r in rows
    ]
    return {
        "rows": data,
        "resolution": "1 min",
        "minutes": minutes,
        "gapfilled_count": sum(1 for row in data if row["gapfilled"]),
    }


@router.get("/api/pipelines/{pipeline_id}/lakets/rate")
async def get_lakets_rate(
    pipeline_id: str,
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    industry: str | None = Query(
        None,
        description="Industry id — resolves the deterministic iot-pipelines "
        "branch directly (for industry-first dashboards with no legacy "
        "`pipelines` row).",
    ),
    state: AppState = Depends(get_app_state),
):
    """Return per-minute value rate-of-change per sensor via ``lakets.rate``.

    Buckets raw readings with ``lakets.time_bucket``, then invokes
    ``lakets.rate`` with the current bucket value and a ``LAG`` of the previous
    bucket value / timestamp to compute per-second change.
    """
    lb = await _resolve_lakebase(state, pipeline_id, industry)
    installed, reason = await _probe_lakets_available(lb)
    if not installed or lb is None:
        return {"rows": [], "reason": reason or "LakeTS not installed"}

    try:

        async def _load_rate():
            tables = await _recent_chunk_tables(lb, minutes)
            relation = _readings_relation(tables)
            return await lb.fetch(
                f"""
            WITH ranked AS (
                SELECT sensor_name,
                       MAX(event_time) AS last_seen
                FROM {relation} readings
                WHERE event_time > NOW() - make_interval(mins => $1)
                GROUP BY sensor_name
                ORDER BY last_seen DESC
                LIMIT {_LAKEBASE_ANALYTICS_SENSOR_LIMIT}
            ),
            base AS (
                SELECT lakets.time_bucket('1 minute'::interval, event_time) AS bucket,
                       sensor_name,
                       AVG(value) AS avg_value
                FROM {relation} readings
                WHERE event_time > NOW() - make_interval(mins => $1)
                  AND sensor_name IN (SELECT sensor_name FROM ranked)
                GROUP BY bucket, sensor_name
            )
            SELECT bucket,
                   sensor_name,
                   lakets.rate(
                       avg_value,
                       LAG(avg_value) OVER (
                           PARTITION BY sensor_name ORDER BY bucket
                       ),
                       bucket,
                       LAG(bucket) OVER (
                           PARTITION BY sensor_name ORDER BY bucket
                       ),
                       FALSE
                   ) AS rate
            FROM base
            ORDER BY bucket ASC
            LIMIT 5000
            """,
                minutes,
            )

        rows = await _probe_wait(
            _load_rate(),
            timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "LakeTS rate query timed out after %.0fs for pipeline %s",
            _LAKEBASE_PROBE_TIMEOUT_SECONDS,
            pipeline_id,
        )
        return {"rows": [], "reason": "Lakebase query timed out"}
    except Exception as exc:
        if industry and _is_undefined_relation(exc):
            return {"rows": [], "reason": "sensor_readings not provisioned yet"}
        logger.warning("LakeTS rate query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    data = [
        {
            "bucket": str(r["bucket"]) if r["bucket"] is not None else None,
            "sensor_name": r["sensor_name"],
            "rate": float(r["rate"]) if r["rate"] is not None else None,
        }
        for r in rows
    ]
    return {"rows": data, "resolution": "1 min", "minutes": minutes}


@router.get("/api/pipelines/{pipeline_id}/lakets/histogram")
async def get_lakets_histogram(
    pipeline_id: str,
    sensor: str = Query(..., description="Sensor name to compute the distribution for"),
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    buckets: int = Query(20, ge=2, le=200, description="Number of histogram buckets"),
    industry: str | None = Query(
        None,
        description="Industry id — resolves the deterministic iot-pipelines "
        "branch directly (for industry-first dashboards with no legacy "
        "`pipelines` row).",
    ),
    state: AppState = Depends(get_app_state),
):
    """Return a value-distribution histogram for a given sensor via LakeTS.

    Uses ``lakets.histogram(value, min, max, num_buckets)`` to assign each
    reading to a bucket index, then aggregates counts + emits each bucket's
    lower/upper numeric bounds.
    """
    lb = await _resolve_lakebase(state, pipeline_id, industry)
    installed, reason = await _probe_lakets_available(lb)
    if not installed or lb is None:
        return {"rows": [], "reason": reason or "LakeTS not installed"}

    try:

        async def _load_histogram():
            tables = await _recent_chunk_tables(lb, minutes)
            relation = _readings_relation(tables)
            return await lb.fetch(
                f"""
            WITH bounds AS (
                SELECT MIN(value) AS min_v, MAX(value) AS max_v
                FROM {relation} readings
                WHERE sensor_name = $1
                  AND event_time > NOW() - make_interval(mins => $2)
            ),
            bucketed AS (
                SELECT lakets.histogram(
                           r.value,
                           b.min_v,
                           b.max_v,
                           $3::int
                       ) AS bucket_idx,
                       b.min_v,
                       b.max_v
                FROM {relation} r
                CROSS JOIN bounds b
                WHERE r.sensor_name = $1
                  AND r.event_time > NOW() - make_interval(mins => $2)
                  AND b.min_v IS NOT NULL
                  AND b.max_v IS NOT NULL
            )
            SELECT bucket_idx,
                   MIN(min_v) AS min_v,
                   MIN(max_v) AS max_v,
                   COUNT(*) AS cnt
            FROM bucketed
            WHERE bucket_idx IS NOT NULL
            GROUP BY bucket_idx
            ORDER BY bucket_idx ASC
            """,
                sensor,
                minutes,
                buckets,
            )

        rows = await _probe_wait(
            _load_histogram(),
            timeout=_LAKEBASE_PROBE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "LakeTS histogram query timed out after %.0fs for pipeline %s",
            _LAKEBASE_PROBE_TIMEOUT_SECONDS,
            pipeline_id,
        )
        return {"rows": [], "reason": "Lakebase query timed out"}
    except Exception as exc:
        if industry and _is_undefined_relation(exc):
            return {"rows": [], "reason": "sensor_readings not provisioned yet"}
        logger.warning("LakeTS histogram query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    data: list[dict[str, Any]] = []
    for r in rows:
        min_v = float(r["min_v"]) if r["min_v"] is not None else 0.0
        max_v = float(r["max_v"]) if r["max_v"] is not None else 0.0
        width = (max_v - min_v) / buckets if buckets else 0.0
        bucket_idx = int(r["bucket_idx"])
        lower = min_v + width * bucket_idx
        upper = min_v + width * (bucket_idx + 1)
        data.append(
            {
                "bucket_idx": bucket_idx,
                "lower": round(lower, 4),
                "upper": round(upper, 4),
                "count": int(r["cnt"] or 0),
            }
        )

    return {"rows": data, "resolution": "1 min", "minutes": minutes}


@router.get("/api/pipeline/{pipeline_id}/streaming-metrics")
async def get_streaming_metrics(
    pipeline_id: str,
    request: Request,
    industry: str | None = Query(
        None,
        description="Industry id — resolves the one-schema prefixed metrics table "
        "``{UC_CATALOG}.{UC_SCHEMA}.{industry}_metrics`` directly "
        "(required for industry-first pipelines that have no legacy ``pipelines`` row).",
    ),
    mode: str | None = Query(
        None, description="Streaming mode: realtime or microbatch (filters the metrics table)"
    ),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query the shared ``{industry}_metrics`` table from the one-schema layout via DBSQL.

    For industry-first pipelines ``industry`` is required (no legacy
    ``pipelines`` row exists). When both ``industry`` and ``mode`` are
    supplied the metrics table is filtered by ``mode``. The response uses the
    typed COLUMNAR schema (``metrics_schema.METRICS_FIELD_MAP`` + leading
    ``mode`` column) — the legacy JSON-``progress``-field path is replaced.

    Tiles path (``/api/latency/streaming``) and this charts path now read the
    same ``{industry}_metrics`` table.
    """
    is_admin = app_permissions.is_admin(request)
    normalized_mode = (mode or "").strip().lower() or None
    if normalized_mode and normalized_mode not in _DBSQL_MODES:
        return JSONResponse(
            status_code=400,
            content={"error": (f"invalid mode {mode!r}; must be one of {sorted(_DBSQL_MODES)}")},
        )

    use_one_schema = False
    if industry:
        try:
            industry = industry.strip().lower()
            kind = await _authorize_mode_pipeline(
                state,
                pipeline_id,
                industry,
                normalized_mode,
                user_email,
                is_admin=is_admin,
            )
            if kind is None:
                return JSONResponse(
                    status_code=404,
                    content={"error": "Industry deployment not found"},
                )
            if kind == "deployment":
                use_one_schema = True
                metrics_table = industry_catalog_service.industry_table(industry, "metrics")
                catalog_part = metrics_table.split(".")[0]
                if not validate_catalog(catalog_part):
                    return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
            else:
                catalog = await get_pipeline_catalog(
                    state.db,
                    state.db_available,
                    pipeline_id,
                    user_email,
                    is_admin=is_admin,
                )
                if not catalog:
                    return JSONResponse(
                        status_code=404,
                        content={"error": "Pipeline not found or no UC catalog"},
                    )
                if not validate_catalog(catalog):
                    return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
                metrics_table = f"{catalog}.metrics.streaming_metrics"
        except (ValueError, RuntimeError) as exc:
            return JSONResponse(status_code=400, content={"error": f"Invalid industry: {exc}"})
    else:
        # Legacy path: resolve catalog from the pipeline row.
        catalog = await get_pipeline_catalog(
            state.db,
            state.db_available,
            pipeline_id,
            user_email,
            is_admin=is_admin,
        )
        if not catalog:
            return JSONResponse(
                status_code=404, content={"error": "Pipeline not found or no UC catalog"}
            )
        if not validate_catalog(catalog):
            return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
        # Legacy path uses the old per-catalog metrics table (JSON progress column).
        # This is kept for backward compat; industry-first pipelines should supply
        # the ``industry`` param to get the new columnar one-schema table.
        metrics_table = f"{catalog}.metrics.streaming_metrics"

    sql_metrics_table = sql_fqn(*metrics_table.split("."))

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    try:
        if use_one_schema:
            # One-schema columnar table: typed columns, leading ``mode`` column.
            mode_filter = f"WHERE mode = '{normalized_mode}'" if normalized_mode else ""
            summary_sql = f"""
                SELECT COUNT(*) AS total_rows,
                       MAX(timestamp) AS last_write,
                       MAX(batch_id) AS last_batch,
                       AVG(input_rps) AS avg_input_rps,
                       AVG(processed_rps) AS avg_processed_rps,
                       AVG(trigger_ms) AS avg_batch_duration_ms,
                       SUM(num_input_rows) AS total_input_rows,
                       AVG(e2e_p50) AS avg_e2e_p50_ms,
                       AVG(e2e_p99) AS avg_e2e_p99_ms,
                       AVG(processing_p50) AS avg_processing_p50_ms,
                       AVG(processing_p99) AS avg_processing_p99_ms,
                       AVG(queuing_p50) AS avg_queuing_p50_ms,
                       AVG(queuing_p99) AS avg_queuing_p99_ms
                FROM {sql_metrics_table}
                {mode_filter}
            """
            rows_sql = f"""
                SELECT query_id, query_name, batch_id, timestamp,
                       num_input_rows,
                       input_rps AS input_rows_per_second,
                       processed_rps AS processed_rows_per_second,
                       trigger_ms AS batch_duration_ms,
                       NULL AS add_batch_ms,
                       NULL AS latest_offset_ms,
                       NULL AS wal_commit_ms,
                       NULL AS num_output_rows,
                       e2e_p50 AS e2e_p50_ms,
                       e2e_p99 AS e2e_p99_ms,
                       processing_p50 AS processing_p50_ms,
                       queuing_p50 AS queuing_p50_ms,
                       NULL AS observed_metrics,
                       NULL AS progress
                FROM {sql_metrics_table}
                {mode_filter}
                ORDER BY batch_id DESC
                LIMIT 50
            """
        else:
            # Legacy path: JSON progress column.
            summary_sql = f"""
                SELECT COUNT(*) AS total_rows,
                       MAX(timestamp) AS last_write,
                       MAX(batch_id) AS last_batch,
                       AVG(progress:inputRowsPerSecond::DOUBLE) AS avg_input_rps,
                       AVG(progress:processedRowsPerSecond::DOUBLE) AS avg_processed_rps,
                       AVG(progress:durationMs.triggerExecution::LONG) AS avg_batch_duration_ms,
                       SUM(progress:numInputRows::LONG) AS total_input_rows,
                       AVG(progress:e2eLatencyMs.p50::DOUBLE) AS avg_e2e_p50_ms,
                       AVG(progress:e2eLatencyMs.p99::DOUBLE) AS avg_e2e_p99_ms,
                       AVG(progress:processingLatencyMs.p50::DOUBLE) AS avg_processing_p50_ms,
                       AVG(progress:processingLatencyMs.p99::DOUBLE) AS avg_processing_p99_ms,
                       AVG(progress:sourceQueuingLatencyMs.p50::DOUBLE) AS avg_queuing_p50_ms,
                       AVG(progress:sourceQueuingLatencyMs.p99::DOUBLE) AS avg_queuing_p99_ms
                FROM {sql_metrics_table}
            """
            rows_sql = f"""
                SELECT query_id, query_name, batch_id, timestamp,
                       progress:numInputRows::LONG AS num_input_rows,
                       progress:inputRowsPerSecond::DOUBLE AS input_rows_per_second,
                       progress:processedRowsPerSecond::DOUBLE AS processed_rows_per_second,
                       progress:durationMs.triggerExecution::LONG AS batch_duration_ms,
                       progress:durationMs.addBatch::LONG AS add_batch_ms,
                       progress:durationMs.latestOffset::LONG AS latest_offset_ms,
                       progress:durationMs.walCommit::LONG AS wal_commit_ms,
                       progress:sink.numOutputRows::LONG AS num_output_rows,
                       progress:e2eLatencyMs.p50::DOUBLE AS e2e_p50_ms,
                       progress:e2eLatencyMs.p99::DOUBLE AS e2e_p99_ms,
                       progress:processingLatencyMs.p50::DOUBLE AS processing_p50_ms,
                       progress:sourceQueuingLatencyMs.p50::DOUBLE AS queuing_p50_ms,
                       progress:observedMetrics AS observed_metrics,
                       progress
                FROM {sql_metrics_table}
                ORDER BY batch_id DESC
                LIMIT 50
            """

        summary_result = await execute_dbsql(summary_sql, warehouse_id)
        rows_result = await execute_dbsql(rows_sql, warehouse_id)

        summary = summary_result["data"][0] if summary_result["data"] else {}
        return {
            "table": metrics_table,
            "summary": {
                "total_rows": int(summary.get("total_rows", 0) or 0),
                "last_write": summary.get("last_write"),
                "last_batch": int(summary.get("last_batch", 0) or 0),
                "avg_input_rps": round(float(summary.get("avg_input_rps", 0) or 0), 2),
                "avg_processed_rps": round(float(summary.get("avg_processed_rps", 0) or 0), 2),
                "avg_batch_duration_ms": round(
                    float(summary.get("avg_batch_duration_ms", 0) or 0), 1
                ),
                "total_input_rows": int(summary.get("total_input_rows", 0) or 0),
                "avg_e2e_p50_ms": round(float(summary.get("avg_e2e_p50_ms", 0) or 0), 1),
                "avg_e2e_p99_ms": round(float(summary.get("avg_e2e_p99_ms", 0) or 0), 1),
                "avg_processing_p50_ms": round(
                    float(summary.get("avg_processing_p50_ms", 0) or 0), 1
                ),
                "avg_processing_p99_ms": round(
                    float(summary.get("avg_processing_p99_ms", 0) or 0), 1
                ),
                "avg_queuing_p50_ms": round(float(summary.get("avg_queuing_p50_ms", 0) or 0), 1),
                "avg_queuing_p99_ms": round(float(summary.get("avg_queuing_p99_ms", 0) or 0), 1),
            },
            "rows": rows_result["data"],
        }

    except Exception as exc:
        logger.warning("Streaming metrics query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


async def _resolve_zerobus_target(
    state: AppState,
    pipeline_id: str,
    user_email: str,
    catalog: str,
) -> str:
    """Resolve the fully-qualified target Delta table for a Zerobus pipeline.

    Uses ``source_config.zerobus.table_name`` when present, otherwise defaults
    to ``<uc_catalog>.raw.iot_sensor_data``. The system tables store the
    fully-qualified name, so the returned value is used verbatim in the filter.
    """
    default_target = f"{catalog}.raw.iot_sensor_data"
    if not state.db_available or not state.db:
        return default_target
    try:
        row = await state.db.fetchrow(
            "SELECT source_config FROM pipelines WHERE id = $1::uuid AND user_email = $2",
            pipeline_id,
            user_email,
        )
        if not row or row["source_config"] is None:
            return default_target
        sc = row["source_config"]
        if isinstance(sc, str):
            sc = json.loads(sc or "{}")
        if not isinstance(sc, dict):
            return default_target
        zb = sc.get("zerobus")
        if isinstance(zb, dict):
            table_name = zb.get("table_name")
            if isinstance(table_name, str) and table_name.strip():
                return table_name.strip()
    except Exception as exc:
        logger.warning("Failed to resolve zerobus target for pipeline %s: %s", pipeline_id, exc)
    return default_target


def _graceful_zerobus_metrics(table_name: str, reason: str) -> dict[str, Any]:
    """Return a zeroed-out metrics payload with a human-readable ``reason``."""
    return {
        "table_name": table_name,
        "total_records": 0,
        "total_bytes": 0,
        "records_per_sec": 0,
        "records_per_min": 0,
        "last_commit_time": None,
        "stream_status": "none",
        "error_count": 0,
        "series": [],
        "recent_errors": [],
        "latest_readings": [],
        "reason": reason,
    }


async def _is_industry_deployment(
    state: AppState, deployment_id: str, industry: str, mode: str | None
) -> bool:
    """Validate an industry-first dashboard identity against app metadata."""
    industry = industry.strip().lower()
    if industry not in CURATED_INDUSTRIES:
        return False
    db = getattr(state, "app_backend_db", None)
    if db is None:
        return False
    try:
        if mode is None:
            row = await db.fetchrow(
                """
                SELECT 1
                FROM pipeline_deployment pd
                JOIN pipeline_setup ps ON ps.id = pd.setup_id
                WHERE pd.id = $1::uuid
                  AND ps.industry = $2
                  AND pd.job_id IS NOT NULL
                  AND pd.status NOT IN ('pending', 'deploying', 'failed')
                """,
                deployment_id,
                industry,
            )
        else:
            row = await db.fetchrow(
                """
            SELECT 1
            FROM pipeline_deployment pd
            JOIN pipeline_setup ps ON ps.id = pd.setup_id
            WHERE pd.id = $1::uuid
              AND ps.industry = $2
              AND pd.mode = $3
              AND pd.job_id IS NOT NULL
              AND pd.status NOT IN ('pending', 'deploying', 'failed')
            """,
                deployment_id,
                industry,
                mode,
            )
    except Exception as exc:
        logger.warning("Failed to validate industry deployment %s: %s", deployment_id, exc)
        return False
    return row is not None


async def _is_legacy_pipeline(
    state: AppState,
    pipeline_id: str,
    industry: str,
    mode: str | None,
    user_email: str,
    *,
    is_admin: bool = False,
) -> bool:
    """Validate a legacy ``pipelines`` row for the requested industry/mode."""
    industry = industry.strip().lower()
    if industry not in CURATED_INDUSTRIES:
        return False
    db = getattr(state, "db", None)
    if db is None or not getattr(state, "db_available", False):
        return False
    try:
        row = await db.fetchrow(
            """
            SELECT source_config, user_email
            FROM pipelines
            WHERE id = $1::uuid
              AND industry = $2
              AND status != 'deleted'
            """,
            pipeline_id,
            industry,
        )
    except Exception as exc:
        logger.warning("Failed to validate legacy pipeline %s: %s", pipeline_id, exc)
        return False
    if row is None:
        return False
    owner = str(row.get("user_email") or "")
    if not is_admin and user_email and owner and owner != user_email:
        return False
    if mode:
        inferred = industry_service._infer_mode(dict(row))
        if inferred != mode:
            return False
    return True


async def _authorize_mode_pipeline(
    state: AppState,
    pipeline_id: str,
    industry: str,
    mode: str | None,
    user_email: str,
    *,
    is_admin: bool = False,
) -> str | None:
    """Return ``deployment`` or ``legacy`` when the id is valid for industry/mode."""
    industry = (industry or "").strip().lower()
    if industry not in CURATED_INDUSTRIES:
        return None
    exact_mode = (mode or "").strip().lower() or None
    if await _is_industry_deployment(state, pipeline_id, industry, exact_mode):
        return "deployment"
    if await _is_legacy_pipeline(
        state, pipeline_id, industry, exact_mode, user_email, is_admin=is_admin
    ):
        return "legacy"
    return None


@router.get("/api/pipelines/{pipeline_id}/zerobus-metrics")
async def get_zerobus_metrics(
    pipeline_id: str,
    minutes: int = Query(60, ge=1, le=10080, description="Lookback window in minutes"),
    industry: str | None = Query(
        None,
        description="Industry id for an industry-first deployment without a legacy pipeline row",
    ),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Return Zerobus ingest metrics for a pipeline's target Delta table.

    Sourced from the Databricks system tables ``system.lakeflow.zerobus_ingest``
    and ``system.lakeflow.zerobus_stream`` via DBSQL. When those tables are not
    accessible (permissions / Beta not enabled) or hold no rows for the target,
    a graceful zeroed payload with ``stream_status:"none"`` and a ``reason`` is
    returned.
    """
    if industry:
        kind = await _authorize_mode_pipeline(state, pipeline_id, industry, "zerobus", user_email)
        if kind is None:
            return JSONResponse(
                status_code=404,
                content={"error": "Industry deployment not found"},
            )
        if kind == "legacy":
            catalog = await get_pipeline_catalog(
                state.db, state.db_available, pipeline_id, user_email
            )
            if not catalog:
                return JSONResponse(
                    status_code=404,
                    content={"error": "Pipeline not found or no UC catalog"},
                )
            target_table = await _resolve_zerobus_target(state, pipeline_id, user_email, catalog)
        else:
            try:
                target_table = industry_catalog_service.industry_table(industry, "zerobus")
            except (ValueError, RuntimeError) as exc:
                return JSONResponse(status_code=400, content={"error": f"Invalid industry: {exc}"})
            catalog = target_table.split(".", 1)[0]
    else:
        catalog = await get_pipeline_catalog(state.db, state.db_available, pipeline_id, user_email)
        if not catalog:
            return JSONResponse(
                status_code=404,
                content={"error": "Pipeline not found or no UC catalog"},
            )
        target_table = await _resolve_zerobus_target(state, pipeline_id, user_email, catalog)
    if not validate_catalog(catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    # The target table name is a literal in the WHERE clause. Escape single
    # quotes to avoid breaking the SQL string (the value comes from our own
    # config / catalog, not arbitrary user input, but be defensive anyway).
    safe_target = target_table.replace("'", "''")

    try:
        summary_sql = f"""
            SELECT COALESCE(SUM(committed_records), 0) AS total_records,
                   COALESCE(SUM(committed_bytes), 0) AS total_bytes,
                   MAX(commit_time) AS last_commit_time,
                   COALESCE(SUM(size(COALESCE(errors, array()))), 0) AS error_count,
                   COALESCE(SUM(CASE
                       WHEN commit_time >= date_trunc('minute', now()) - INTERVAL 1 MINUTE
                        AND commit_time < date_trunc('minute', now())
                       THEN committed_records END), 0) AS current_records_per_min
            FROM system.lakeflow.zerobus_ingest
            WHERE table_name = '{safe_target}'
              AND commit_time >= now() - INTERVAL {int(minutes)} MINUTES
        """
        series_sql = f"""
            SELECT date_trunc('minute', commit_time) AS bucket,
                   COALESCE(SUM(committed_records), 0) AS records
            FROM system.lakeflow.zerobus_ingest
            WHERE table_name = '{safe_target}'
              AND commit_time >= now() - INTERVAL {int(minutes)} MINUTES
              AND commit_time < date_trunc('minute', now())
            GROUP BY bucket
            ORDER BY bucket ASC
            LIMIT 5000
        """
        stream_sql = f"""
            SELECT opened_time, closed_time, errors
            FROM system.lakeflow.zerobus_stream
            WHERE table_name = '{safe_target}'
            ORDER BY COALESCE(closed_time, opened_time, event_time) DESC
            LIMIT 1
        """

        summary_result = await execute_dbsql(summary_sql, warehouse_id)
        series_result = await execute_dbsql(series_sql, warehouse_id)
        stream_result = await execute_dbsql(stream_sql, warehouse_id)
    except Exception as exc:
        logger.warning("Zerobus metrics query failed for pipeline %s: %s", pipeline_id, exc)
        return _graceful_zerobus_metrics(
            target_table,
            f"Zerobus system tables not accessible: {type(exc).__name__}: {str(exc)[:200]}",
        )

    latest_result: dict[str, Any] = {"data": []}
    if industry:
        try:
            sql_target = sql_fqn(*target_table.split("."))
            latest_sql = f"""
                SELECT sensor_name, sensor_type, value, unit, fault_active, event_time
                FROM (
                    SELECT sensor_name, sensor_type, value, unit, fault_active, event_time,
                           ROW_NUMBER() OVER (PARTITION BY sensor_name ORDER BY event_time DESC) AS reading_rank
                    FROM {sql_target}
                    WHERE event_time >= now() - INTERVAL {int(minutes)} MINUTES
                ) ranked
                WHERE reading_rank = 1
            """
            latest_result = await execute_dbsql(latest_sql, warehouse_id)
        except Exception as exc:
            logger.warning(
                "Zerobus latest readings query failed for pipeline %s: %s",
                pipeline_id,
                exc,
            )
            latest_result = {"data": []}

    summary_rows = summary_result.get("data") or []
    if not summary_rows:
        return _graceful_zerobus_metrics(
            target_table,
            "No Zerobus ingest rows for this table in the requested window",
        )

    summary = summary_rows[0]
    total_records = int(summary.get("total_records", 0) or 0)
    total_bytes = int(summary.get("total_bytes", 0) or 0)
    last_commit_time = summary.get("last_commit_time")
    error_count = int(summary.get("error_count", 0) or 0)

    series = [
        {
            "t": r.get("bucket"),
            "records_per_min": float(r.get("records", 0) or 0),
        }
        for r in (series_result.get("data") or [])
    ]
    records_per_min = float(summary.get("current_records_per_min") or 0)
    records_per_sec = records_per_min / 60

    # Stream status from the latest stream row's open/close times.
    stream_rows = stream_result.get("data") or []
    stream_status = "none"
    recent_errors: list[str] = []
    if stream_rows:
        latest = stream_rows[0]
        opened = latest.get("opened_time")
        closed = latest.get("closed_time")
        if opened and not closed:
            stream_status = "open"
        elif closed:
            stream_status = "closed"
        errors = latest.get("errors")
        if isinstance(errors, (list, tuple)):
            recent_errors = [str(e) for e in errors if e is not None][:5]

    return {
        "table_name": target_table,
        "total_records": total_records,
        "total_bytes": total_bytes,
        "records_per_sec": round(records_per_sec, 4),
        "records_per_min": round(records_per_min, 2),
        "last_commit_time": last_commit_time,
        "stream_status": stream_status,
        "error_count": error_count,
        "series": series,
        "recent_errors": recent_errors,
        "latest_readings": list(latest_result.get("data") or []),
    }


# Valid streaming modes for the DBSQL cold-tier timeseries endpoint.
# Industry-first (``industry`` query param):
#   * realtime  — CDF-owned Lakebase→Delta destination
#     ``lb__shadow_<industry>_sensor_readings_history``
#   * microbatch / zerobus — direct-write industry-prefixed tables via
#     ``industry_catalog_service.industry_table(industry, mode)``
#     (``{UC_CATALOG}.{UC_SCHEMA}.{industry}_{mode}``)
# Legacy (no ``industry`` param) keeps the per-catalog ``_LEGACY_MODE_TABLES``
# paths so non-industry-first callers still work.
_DBSQL_MODES: frozenset[str] = frozenset({"realtime", "microbatch", "zerobus"})
_LEGACY_CDF_SYNCED_TABLE = "lb__shadow_sensor_readings_history"

# Legacy per-catalog schema/table map — used ONLY when the ``industry`` param
# is absent (backward compat with non-industry-first pipelines).  New callers
# should always supply ``industry`` to get the one-schema prefixed table.
_LEGACY_MODE_TABLES: dict[str, str] = {
    "realtime": f"realtime.{_LEGACY_CDF_SYNCED_TABLE}",
    "microbatch": "microbatch.sensor_data",
    "zerobus": "zerobus.iot_sensor_data",
}


def _is_missing_table_error(exc: Exception) -> bool:
    """True when a DBSQL query failed because the target table doesn't exist yet.

    A per-mode table that hasn't been created/populated yet (pipeline never
    deployed, or the mode's Delta table hasn't landed its first batch) means
    "no data yet", not a server fault — the caller degrades to an empty
    series instead of a 500.
    """
    msg = str(exc).lower()
    return (
        "table_or_view_not_found" in msg
        or "table or view not found" in msg
        or "schema_not_found" in msg
        or "catalog_not_found" in msg
        or ("does not exist" in msg and ("table" in msg or "view" in msg or "schema" in msg))
    )


async def _resolve_dbsql_mode(
    state: AppState,
    pipeline_id: str,
    mode: str | None,
) -> str:
    """Resolve the streaming mode (realtime/microbatch/zerobus) for a dashboard query.

    Prefers the explicit ``mode`` query param (validated against the known
    set). Falls back to inferring it from the pipeline's
    ``source_type``/``source_config`` via
    ``iot_twin.services.industry.industry_service._infer_mode`` (the same inference the
    curated-industry catalog uses to reconcile pre-convention pipeline rows).
    Defaults to ``microbatch`` — the mode with no Lakebase/CDF prerequisite —
    when nothing can be determined.
    """
    normalized = (mode or "").strip().lower()
    if normalized in _DBSQL_MODES:
        return normalized

    if state.db_available and state.db:
        try:
            row = await state.db.fetchrow(
                "SELECT source_type, source_config FROM pipelines WHERE id = $1::uuid",
                pipeline_id,
            )
        except Exception as exc:
            logger.warning(
                "dbsql-timeseries: mode inference lookup failed for pipeline %s: %s",
                pipeline_id,
                exc,
            )
            row = None
        if row:
            inferred = industry_service._infer_mode(dict(row))
            if inferred in _DBSQL_MODES:
                return inferred
    return "microbatch"


@router.get("/api/pipeline/{pipeline_id}/dbsql-timeseries")
async def get_dbsql_timeseries(
    pipeline_id: str,
    request: Request,
    minutes: int = Query(1440, ge=1, le=10080, description="Lookback window in minutes"),
    mode: str | None = Query(
        None,
        description="Streaming mode: realtime (CDF-owned cold table), "
        "microbatch, or zerobus (industry-prefixed direct-write tables)",
    ),
    industry: str | None = Query(
        None,
        description="Industry id for industry-first dashboards (no legacy "
        "`pipelines` row). Realtime resolves the CDF-owned "
        "`lb__shadow_<industry>_sensor_readings_history` table; microbatch and "
        "zerobus resolve `{UC_CATALOG}.{UC_SCHEMA}.{industry}_{mode}`.",
    ),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query time-series sensor data from the pipeline's UC catalog via DBSQL.

    Table resolution:
    - When ``industry`` is supplied and ``mode=realtime``: the CDF-owned
      Lakebase→Delta destination
      ``{UC_CATALOG}.{UC_SCHEMA}.lb__shadow_<industry>_sensor_readings_history``.
      Managed CDF does not write the industry-prefixed ``{industry}_realtime``
      table used by direct Delta pipelines.
    - When ``industry`` is supplied and ``mode`` is microbatch or zerobus:
      the one-schema direct-write table via ``industry_table(industry, mode)``
      (``{UC_CATALOG}.{UC_SCHEMA}.{industry}_{mode}``).
    - When ``industry`` is absent: falls back to the legacy per-pipeline catalog
      lookup (``{iot_<industry>}.{schema}.{table}`` from ``_LEGACY_MODE_TABLES``),
      kept for non-industry-first pipelines that still have a ``pipelines`` row.

    ``minutes`` bounds only the chart rows. For industry-first realtime, chart
    aggregation is scoped to the requested industry and value-bearing CDF
    images (insert/update postimage). ``latest_event_at`` reports cold-tier
    freshness from DBSQL without performing an expensive all-time row count.
    """
    is_admin = app_permissions.is_admin(request)
    resolved_mode = await _resolve_dbsql_mode(state, pipeline_id, mode)

    if industry:
        industry = industry.strip().lower()
        if industry not in CURATED_INDUSTRIES:
            return JSONResponse(
                status_code=400,
                content={"error": f"Unknown industry: {industry}"},
            )
        kind = await _authorize_mode_pipeline(
            state,
            pipeline_id,
            industry,
            resolved_mode,
            user_email,
            is_admin=is_admin,
        )
        if kind is None:
            return JSONResponse(
                status_code=404,
                content={"error": "Industry deployment not found"},
            )
        if kind == "legacy":
            catalog = await get_pipeline_catalog(
                state.db,
                state.db_available,
                pipeline_id,
                user_email,
                is_admin=is_admin,
            )
            if not catalog:
                return JSONResponse(
                    status_code=404, content={"error": "Pipeline not found or no UC catalog"}
                )
            if not validate_catalog(catalog):
                return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
            table = f"{catalog}.{_LEGACY_MODE_TABLES[resolved_mode]}"
        else:
            try:
                if resolved_mode == "realtime":
                    table = cdf_reconciliation_service.delta_table_fqn(
                        industry,
                        catalog=industry_catalog_service.uc_catalog_name(),
                        schema=industry_catalog_service.uc_schema_name(),
                    )
                else:
                    table = industry_catalog_service.industry_table(industry, resolved_mode)
            except (ValueError, RuntimeError) as exc:
                return JSONResponse(status_code=400, content={"error": f"Invalid industry: {exc}"})
            catalog_part = table.split(".")[0]
            if not validate_catalog(catalog_part):
                return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
    else:
        catalog = await get_pipeline_catalog(
            state.db,
            state.db_available,
            pipeline_id,
            user_email,
            is_admin=is_admin,
        )
        if not catalog:
            return JSONResponse(
                status_code=404, content={"error": "Pipeline not found or no UC catalog"}
            )
        if not validate_catalog(catalog):
            return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
        table = f"{catalog}.{_LEGACY_MODE_TABLES[resolved_mode]}"

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    # Choose aggregation interval based on lookback range
    interval = "MINUTE" if minutes <= 60 else "HOUR"

    empty_payload = {
        "source": "dbsql",
        "mode": resolved_mode,
        "table": table,
        "resolution": interval.lower(),
        "minutes": minutes,
        "count": 0,
        "latest_event_at": None,
        "rows": [],
    }

    sql_table = sql_fqn(*table.split("."))

    try:
        chart_filters = [f"event_time >= TIMESTAMPADD(MINUTE, -{minutes}, CURRENT_TIMESTAMP())"]
        if industry and resolved_mode == "realtime":
            safe_industry = industry.replace("'", "''")
            chart_filters.extend(
                [
                    f"industry = '{safe_industry}'",
                    "_pg_change_type IN ('insert', 'update_postimage')",
                ]
            )
        chart_where = "\n              AND ".join(chart_filters)
        sql = f"""
            SELECT DATE_TRUNC('{interval}', event_time) AS time,
                   sensor_name, sensor_type, industry,
                   AVG(value) AS value,
                   MIN(value) AS min_value,
                   MAX(value) AS max_value,
                   COUNT(*) AS sample_count
            FROM {sql_table}
            WHERE {chart_where}
            GROUP BY 1, 2, 3, 4
            ORDER BY time ASC
            LIMIT 5000
        """
        result = await execute_dbsql(sql, warehouse_id)
        # Cold-tier freshness for the Delta tile. MAX(event_time) can use Delta
        # column statistics and avoids an all-time COUNT/SUM scan.
        latest_event_at = None
        try:
            latest_filters: list[str] = []
            if industry and resolved_mode == "realtime":
                safe_industry = industry.replace("'", "''")
                latest_filters = [
                    f"industry = '{safe_industry}'",
                    "_pg_change_type IN ('insert', 'update_postimage')",
                ]
            elif industry:
                safe_industry = industry.replace("'", "''")
                latest_filters = [f"industry = '{safe_industry}'"]

            latest_where = (
                "\n            WHERE " + "\n              AND ".join(latest_filters)
                if latest_filters
                else ""
            )
            latest_sql = f"""
                SELECT MAX(event_time) AS latest_event_at
                FROM {sql_table}{latest_where}
            """
            latest = await execute_dbsql(latest_sql, warehouse_id)
            if latest["data"]:
                latest_event_at = latest["data"][0].get("latest_event_at")
        except Exception as exc:  # never fail the timeseries on freshness
            logger.warning(
                "DBSQL latest-event query failed for pipeline %s table %s: %s",
                pipeline_id,
                table,
                exc,
            )
            latest_event_at = None
        return {
            **empty_payload,
            "count": result["row_count"],
            "latest_event_at": latest_event_at,
            "rows": result["data"],
        }

    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.info(
                "DBSQL timeseries: %s not provisioned/populated yet for pipeline %s — returning empty",
                table,
                pipeline_id,
            )
            return empty_payload
        logger.warning("DBSQL timeseries query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
