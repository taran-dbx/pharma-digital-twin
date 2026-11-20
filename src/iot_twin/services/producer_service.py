"""Per-pipeline data producer management — start, stop, poll."""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any

from iot_twin.models.producer_state import new_producer_state, source_label

logger = logging.getLogger("iot_digital_twin.services.producer")


async def start_data_producer(
    producer_states: dict[str, dict[str, Any]],
    pipeline_id: str,
    notebook_content: str,
    source_type: str,
    destination: str,
    base_parameters: dict[str, str] | None = None,
) -> dict:
    """Upload a producer notebook and submit a serverless run for a pipeline."""
    await stop_data_producer(producer_states, pipeline_id)

    state = new_producer_state()
    producer_states[pipeline_id] = state

    if not notebook_content:
        state["error"] = "notebook content is required"
        return state

    try:
        import base64

        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.compute import Environment
        from databricks.sdk.service.jobs import JobEnvironment, NotebookTask, Source, SubmitTask
        from databricks.sdk.service.workspace import ImportFormat, Language

        w = WorkspaceClient()
        me = w.current_user.me()
        username = me.user_name or "unknown"
        ts = int(time.time())
        short_id = pipeline_id[:8]
        notebook_path = (
            f"/Workspace/Users/{username}/.iot-digital-twin/data_producer_{short_id}_{ts}"
        )
        parent_folder = notebook_path.rsplit("/", 1)[0]

        try:
            w.workspace.mkdirs(parent_folder)
        except Exception:
            pass

        w.workspace.import_(
            path=notebook_path,
            content=base64.b64encode(notebook_content.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded data producer notebook to %s", notebook_path)

        waiter = w.jobs.submit(
            run_name=f"iot-producer-{source_type}-{short_id}-{ts}",
            tasks=[
                SubmitTask(
                    task_key="data_producer",
                    notebook_task=NotebookTask(
                        notebook_path=notebook_path,
                        source=Source.WORKSPACE,
                        base_parameters=base_parameters or {},
                    ),
                    environment_key="default",
                )
            ],
            environments=[
                JobEnvironment(
                    environment_key="default",
                    spec=Environment(client="1"),
                )
            ],
        )
        run_id = waiter.response.run_id
        logger.info(
            "Submitted serverless %s producer run_id=%s for pipeline %s",
            source_type,
            run_id,
            short_id,
        )

        state.update(
            {
                "running": True,
                "source_type": source_type,
                "source_label": source_label(source_type),
                "destination": destination,
                "run_id": run_id,
                "state": "STARTING",
                "error": None,
            }
        )

    except Exception as exc:
        state.update(
            {
                "running": False,
                "source_type": source_type,
                "source_label": source_label(source_type),
                "state": "FAILED",
                "error": str(exc)[:500],
            }
        )
        logger.error("Data producer start failed: %s\n%s", exc, traceback.format_exc())

    return state


async def stop_data_producer(
    producer_states: dict[str, dict[str, Any]],
    pipeline_id: str,
) -> dict:
    """Cancel the serverless producer run for a pipeline and reset state."""
    state = producer_states.get(pipeline_id, new_producer_state())
    run_id = state.get("run_id")
    if run_id:
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            w.jobs.cancel_run(run_id=run_id)
            logger.info("Data producer run %s cancelled for pipeline %s", run_id, pipeline_id[:8])
        except Exception as exc:
            logger.warning("Error cancelling producer run %s: %s", run_id, exc)

    fresh = new_producer_state()
    producer_states[pipeline_id] = fresh
    return fresh


def poll_producer_state(state: dict) -> dict:
    """Poll Databricks for the current run status and update state in place."""
    run_id = state.get("run_id")
    if not run_id or state.get("state") in ("IDLE", "FAILED"):
        return state
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        run = w.jobs.get_run(run_id=run_id)
        lcs = (
            run.state.life_cycle_state.value
            if run.state and run.state.life_cycle_state
            else "UNKNOWN"
        )

        if lcs == "RUNNING":
            state["state"] = "RUNNING"
            state["running"] = True
            state["error"] = None
        elif lcs in ("PENDING", "QUEUED", "BLOCKED"):
            state["state"] = "STARTING"
            state["running"] = True
        elif lcs in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            rs = run.state.result_state.value if run.state and run.state.result_state else None
            state["running"] = False
            state["state"] = "FAILED" if rs != "SUCCESS" else "IDLE"
            state["error"] = (
                run.state.state_message[:500] if run.state and run.state.state_message else None
            )
        elif lcs == "TERMINATING":
            state["state"] = "STOPPING"
    except Exception as exc:
        logger.warning("Failed to poll producer run %s: %s", run_id, exc)
    return state
