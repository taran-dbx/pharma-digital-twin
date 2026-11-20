"""Job control endpoints — status, stop, restart."""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse

logger = logging.getLogger("iot_digital_twin.routes.jobs")

router = APIRouter(tags=["jobs"])


@router.get("/api/job/{job_id}/status")
async def get_job_status(job_id: int):
    """Return the current lifecycle state of the latest run for a job.

    When the job has multiple tasks, includes a ``tasks`` dict with
    per-task state so the frontend can show granular status.
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        def _extract_task_states(run) -> dict[str, dict] | None:
            """Build per-task status from a run's task list."""
            task_runs = getattr(run, "tasks", None)
            if not task_runs or len(task_runs) <= 1:
                return None
            result: dict[str, dict] = {}
            for tr in task_runs:
                ts = tr.state
                result[tr.task_key] = {
                    "state": ts.life_cycle_state.value if ts and ts.life_cycle_state else "UNKNOWN",
                    "run_id": tr.run_id,
                    "state_message": ts.state_message if ts else "",
                }
            return result

        runs = list(w.jobs.list_runs(job_id=job_id, active_only=True))
        if runs:
            latest = runs[0]
            state = latest.state
            resp = {
                "state": state.life_cycle_state.value
                if state and state.life_cycle_state
                else "UNKNOWN",
                "run_id": latest.run_id,
                "state_message": state.state_message if state else "",
            }
            tasks = _extract_task_states(latest)
            if tasks:
                resp["tasks"] = tasks
            return resp

        all_runs = list(w.jobs.list_runs(job_id=job_id, limit=1))
        if all_runs:
            latest = all_runs[0]
            state = latest.state
            lcs = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
            rs = state.result_state.value if state and state.result_state else None
            resp = {
                "state": lcs,
                "result_state": rs,
                "run_id": latest.run_id,
                "state_message": state.state_message if state else "",
            }
            tasks = _extract_task_states(latest)
            if tasks:
                resp["tasks"] = tasks
            return resp

        return {"state": "NO_RUNS", "run_id": None, "state_message": "No runs found"}

    except Exception as exc:
        logger.error("Job status check failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/job/{job_id}/stop")
async def stop_job(job_id: int):
    """Cancel all active runs and pause the continuous schedule."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus

        w = WorkspaceClient()
        w.jobs.cancel_all_runs(job_id=job_id)
        w.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                continuous=Continuous(pause_status=PauseStatus.PAUSED),
            ),
        )
        return {"stopped": True}

    except Exception as exc:
        logger.error("Job stop failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/job/{job_id}/restart")
async def restart_job(job_id: int):
    """Unpause the continuous schedule and trigger a new run."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus

        w = WorkspaceClient()
        w.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                continuous=Continuous(pause_status=PauseStatus.UNPAUSED),
            ),
        )
        return {"restarted": True}

    except Exception as exc:
        logger.error("Job restart failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
