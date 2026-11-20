"""IoT Simulator background task and WebSocket broadcast."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import traceback
from typing import Any

from fastapi import WebSocket

from iot_twin.core.state import AppState
from iot_twin.domain.catalog import map_simulator_industries, to_record_industry

logger = logging.getLogger("iot_digital_twin.simulator")


async def broadcast(state: AppState, data: dict[str, Any]) -> None:
    """Send a JSON payload to all connected WebSocket and SSE clients."""
    has_ws = bool(state.connected_clients)
    has_sse = bool(state.sse_clients)

    if not has_ws and not has_sse:
        return

    # --- WebSocket broadcast ---
    if has_ws:
        payload = json.dumps(data, default=str)
        stale: list[WebSocket] = []
        for ws in state.connected_clients:
            try:
                await ws.send_text(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            try:
                state.connected_clients.remove(ws)
            except ValueError:
                pass

    # --- SSE broadcast ---
    for queue in state.sse_clients[:]:
        try:
            queue.put_nowait(data)
        except asyncio.QueueFull:
            pass  # drop if client is slow


def _records_to_dicts(records: Any, source_tag: str) -> list[dict]:
    """Convert SensorRecord list to dicts, keeping only the latest per sensor."""
    latest: dict[str, dict] = {}
    for r in records:
        latest[r.sensor_name] = {
            "sensor_name": r.sensor_name,
            "industry": to_record_industry(r.industry),
            "value": round(r.value, 3),
            "unit": r.unit,
            "sensor_type": r.sensor_type,
            "timestamp": r.timestamp,
            "fault_active": r.fault_active,
            "source": source_tag,
        }
    return list(latest.values())


async def run_simulator(state: AppState) -> None:
    """Main simulator loop — sets up sinks and runs the IoT simulator."""
    await asyncio.sleep(2)

    try:
        from iot_simulator import Simulator

        sim = Simulator(
            industries=map_simulator_industries(state.industries.split(",")),
            update_rate_hz=state.update_rate_hz,
        )
        logger.info("Simulator created — industries=%s", state.industries)
    except Exception as exc:
        logger.error("Failed to create Simulator: %s\n%s", exc, traceback.format_exc())
        return

    sources = state.sources

    # --- Zerobus sink ---
    zerobus_sink = None
    zerobus_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT")
    zerobus_table = os.getenv("ZEROBUS_TABLE_NAME")
    if zerobus_endpoint and zerobus_table:
        try:
            from iot_simulator.sinks.zerobus import ZerobusSink

            zb_host = os.getenv("DATABRICKS_HOST", "")
            if zb_host and not zb_host.startswith("http"):
                zb_host = f"https://{zb_host}"

            # Resolve Zerobus client secret: env var first, then UC secrets via SDK
            zerobus_client_secret = os.getenv("ZEROBUS_CLIENT_SECRET")
            if not zerobus_client_secret:
                zb_scope = os.getenv("ZEROBUS_SECRET_SCOPE")
                zb_key = os.getenv("ZEROBUS_SECRET_KEY")
                if zb_scope and zb_key:
                    import base64

                    from databricks.sdk import WorkspaceClient

                    w = WorkspaceClient()
                    resp = w.secrets.get_secret(scope=zb_scope, key=zb_key)
                    zerobus_client_secret = base64.b64decode(resp.value).decode()
                    logger.info(
                        "Zerobus client secret loaded from scope=%s key=%s", zb_scope, zb_key
                    )

            zerobus_sink = ZerobusSink(
                server_endpoint=zerobus_endpoint,
                table_name=zerobus_table,
                client_id=os.getenv("ZEROBUS_CLIENT_ID"),
                client_secret=zerobus_client_secret,
                workspace_url=zb_host,
                rate_hz=1.0,
                batch_size=10,
            )
            await zerobus_sink.connect()
            sim.add_sink(zerobus_sink)
            sources["zerobus"].sink_config = zerobus_sink.sink_config
            sources["zerobus"]._sink_runner = sim._runners[-1]
            sources["zerobus"].status = "active"
            logger.info(
                "ZerobusSink enabled — endpoint=%s table=%s", zerobus_endpoint, zerobus_table
            )
        except Exception as exc:
            sources["zerobus"].status = "error"
            sources["zerobus"].error = str(exc)[:500]
            logger.warning("ZerobusSink failed: %s\n%s", exc, traceback.format_exc())
            zerobus_sink = None
    else:
        sources["zerobus"].status = "inactive"
        sources["zerobus"].error = "ZEROBUS_SERVER_ENDPOINT or ZEROBUS_TABLE_NAME not set"
        logger.info("ZerobusSink disabled — env vars not set")

    # --- SSS ---
    sources["sss"].status = "waiting"
    sources["sss"].error = "Waiting for SSS job to connect via /api/sss/ingest"

    # --- Latency-tracking wrapper for Zerobus sink (with reconnect) ---
    if zerobus_sink is not None:
        original_zerobus_write = zerobus_sink.write
        _zb_reconnect_lock = asyncio.Lock()

        async def _zerobus_reconnect() -> None:
            try:
                await zerobus_sink.close()
            except Exception:
                pass
            await zerobus_sink.connect()
            logger.info("ZerobusSink reconnected successfully")

        async def tracked_zerobus_write(records):
            t0 = time.time()
            try:
                await original_zerobus_write(records)
            except Exception as first_exc:
                if (
                    "stream is closed" in str(first_exc).lower()
                    or "before it's opened" in str(first_exc).lower()
                ):
                    logger.warning("Zerobus stream dead — attempting reconnect: %s", first_exc)
                    async with _zb_reconnect_lock:
                        try:
                            await _zerobus_reconnect()
                            await original_zerobus_write(records)
                        except Exception as retry_exc:
                            sources["zerobus"].status = "error"
                            sources["zerobus"].error = str(retry_exc)[:200]
                            logger.warning("Zerobus reconnect+retry failed: %s", retry_exc)
                            return
                else:
                    sources["zerobus"].status = "error"
                    sources["zerobus"].error = str(first_exc)[:200]
                    return

            now = time.time()
            write_ms = (now - t0) * 1000
            avg_ts = sum(r.timestamp for r in records) / len(records)
            e2e_ms = (now - avg_ts) * 1000
            sources["zerobus"].status = "active"
            sources["zerobus"].error = None
            sources["zerobus"].record_write(len(records), write_ms, e2e_ms)
            sensor_dicts = _records_to_dicts(records, "zerobus")
            if sensor_dicts:
                await broadcast(
                    state,
                    {
                        "type": "sensor_update",
                        "source": "zerobus",
                        "sensors": sensor_dicts,
                        "write_latency_ms": round(write_ms, 1),
                        "e2e_latency_ms": round(e2e_ms, 1),
                    },
                )

        zerobus_sink.write = tracked_zerobus_write  # type: ignore[assignment]

    # NOTE: the legacy in-app WebSocket CallbackSink used to generate synthetic
    # sensor data and push it over SSE, which made the dashboard look alive even
    # when no Spark job was running.  Per-pipeline architecture: only a pipeline's
    # deployed SSS job (via /api/sss/ingest) should feed SSE.  The callback sink
    # was removed along with LakebaseSink; see project_lakebase_two_roles memory.

    # --- Periodic source-status broadcast ---
    async def broadcast_source_status():
        while True:
            await asyncio.sleep(3)
            try:
                await broadcast(
                    state,
                    {
                        "type": "source_status",
                        "sources": {name: s.to_dict() for name, s in sources.items()},
                    },
                )
            except Exception:
                pass

    status_task = asyncio.create_task(broadcast_source_status())

    logger.info("Starting IoT simulator loop — industries=%s", state.industries)
    try:
        await sim.run_async()
    except asyncio.CancelledError:
        logger.info("Simulator cancelled")
    except Exception as exc:
        logger.error("Simulator crashed: %s\n%s", exc, traceback.format_exc())
    finally:
        status_task.cancel()
