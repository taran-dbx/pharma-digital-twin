"""SQL Warehouse management endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("iot_digital_twin.routes.warehouses")

router = APIRouter(tags=["warehouses"])


@router.get("/api/warehouses")
async def list_warehouses():
    """List SQL warehouses available in the workspace."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        warehouses = []
        for wh in w.warehouses.list():
            warehouses.append(
                {
                    "id": wh.id,
                    "name": wh.name,
                    "cluster_size": wh.cluster_size,
                    "state": str(wh.state.value) if wh.state else "UNKNOWN",
                    "serverless": bool(wh.enable_serverless_compute),
                    "auto_stop_mins": wh.auto_stop_mins or 0,
                }
            )
        return warehouses
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)[:300]})


@router.post("/api/warehouses")
async def create_warehouse(request: Request):
    """Create a new serverless SQL warehouse."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    name = body.get("name", "iot-digital-twin")
    cluster_size = body.get("cluster_size", "2X-Small")
    auto_stop_mins = body.get("auto_stop_mins", 10)

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

        w = WorkspaceClient()
        waiter = w.warehouses.create(
            name=name,
            cluster_size=cluster_size,
            enable_serverless_compute=True,
            auto_stop_mins=auto_stop_mins,
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            max_num_clusters=1,
            min_num_clusters=1,
        )
        result = waiter.result()
        return {
            "id": result.id,
            "name": result.name,
            "cluster_size": result.cluster_size,
            "state": str(result.state.value) if result.state else "UNKNOWN",
            "serverless": True,
            "auto_stop_mins": result.auto_stop_mins or 0,
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)[:300]})
