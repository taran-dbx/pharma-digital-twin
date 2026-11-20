"""One-click Lakebase + LakeTS pipeline setup endpoint.

``POST /api/setup/pipeline-lakebase`` validates the request body and delegates
to ``iot_twin.services.pipelines.pipeline_setup_service.setup_pipeline_lakebase``, which chains the
streaming-SP, Lakebase project/branch/role/table, rollup, and ops-job
provisioning steps.  Each step is non-fatal; the orchestrator surfaces per-step
status in its ``steps`` list.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.state import AppState, get_app_state
from iot_twin.core.workspace_config import get_secret_scope
from iot_twin.services.pipelines.pipeline_setup_service import setup_pipeline_lakebase

logger = logging.getLogger("iot_digital_twin.routes.setup")

router = APIRouter(tags=["setup"])


@router.post("/api/setup/pipeline-lakebase")
async def post_setup_pipeline_lakebase(
    request: Request,
    state: AppState = Depends(get_app_state),
):
    """Run the one-click Lakebase + LakeTS setup for a pipeline."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"error": "Body must be a JSON object"})

    pipeline_id = (body.get("pipeline_id") or "").strip()
    if not pipeline_id:
        return JSONResponse(status_code=400, content={"error": "pipeline_id is required"})

    create_new_project = bool(body.get("create_new_project"))
    lb_project_id = (body.get("lb_project_id") or "").strip()
    if not lb_project_id:
        return JSONResponse(status_code=400, content={"error": "lb_project_id is required"})

    rollups = body.get("rollups") or []
    if not isinstance(rollups, list):
        return JSONResponse(status_code=400, content={"error": "rollups must be a list"})
    for rollup in rollups:
        if not isinstance(rollup, dict) or not rollup.get("name") or not rollup.get("bucket"):
            return JSONResponse(
                status_code=400,
                content={"error": "each rollup requires 'name' and 'bucket'"},
            )

    tiering = body.get("tiering")
    if tiering is not None and (
        not isinstance(tiering, dict) or not tiering.get("hot") or not tiering.get("drop")
    ):
        return JSONResponse(
            status_code=400,
            content={"error": "tiering requires 'hot' and 'drop'"},
        )

    params = {
        "pipeline_id": pipeline_id,
        "create_new_project": create_new_project,
        "lb_project_id": lb_project_id,
        "lb_branch_id": (body.get("lb_branch_id") or "production").strip(),
        "lb_database": (body.get("lb_database") or "databricks_postgres").strip(),
        "table_name": (body.get("table_name") or "sensor_readings").strip(),
        "secret_scope": (body.get("secret_scope") or get_secret_scope()).strip(),
        "industry": body.get("industry"),
        "tiering": tiering,
        "retention": body.get("retention"),
        "rollups": rollups,
    }

    try:
        result = await setup_pipeline_lakebase(state, params)
    except Exception as exc:  # pragma: no cover - orchestrator is non-raising
        logger.exception("pipeline-lakebase setup raised unexpectedly")
        return JSONResponse(status_code=500, content={"error": str(exc)[:300]})

    return result
