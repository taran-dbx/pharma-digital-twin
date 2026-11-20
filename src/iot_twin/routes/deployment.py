"""Deployment endpoints — connectivity test, cluster validate, deploy-job."""

from __future__ import annotations

import base64
import json
import logging
import os
import time
import traceback
from datetime import timedelta
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.auth import get_current_user
from iot_twin.core.state import AppState, get_app_state
from iot_twin.services.databricks_sdk import detect_cloud
from iot_twin.services.pipelines.pipeline_service import track_resource
from iot_twin.services.runtime_wheel_service import (
    WHEEL_FILENAME,
    RuntimeWheelError,
    augment_runtime_wheel,
    bundled_wheel_path,
)

# LakeTS operational jobs (partition manager, rollup, retention). Owned by a
# sibling refactor in iot_twin.services.lakets.lakets_deploy_service; tolerate its absence so
# this module still imports during the interim. Tests patch this symbol via
# ``iot_twin.routes.deployment.deploy_lakets_ops_jobs`` so the patch target exists either way.
try:
    from iot_twin.services.lakets.lakets_deploy_service import deploy_lakets_ops_jobs  # type: ignore
except ImportError:  # pragma: no cover — transitional fallback

    def deploy_lakets_ops_jobs(*_args: Any, **_kwargs: Any) -> list[dict]:  # type: ignore[no-redef]
        raise RuntimeError(
            "deploy_lakets_ops_jobs is not yet available in iot_twin.services.lakets.lakets_deploy_service"
        )


logger = logging.getLogger("iot_digital_twin.routes.deployment")

router = APIRouter(tags=["deployment"])


@router.post("/api/test-connectivity/upload-wheel")
async def upload_wheel(user_email: str = Depends(get_current_user)):
    """Upload the bundled iot-data-simulator wheel to the Databricks workspace."""
    try:
        local_wheel = bundled_wheel_path()
        if not local_wheel.exists():
            return JSONResponse(
                status_code=404,
                content={
                    "error": f"Wheel not found at {local_wheel}. Ensure lib/{WHEEL_FILENAME} is packaged with the app.",
                },
            )

        w = WorkspaceClient()
        me = w.current_user.me()
        username = me.user_name or "unknown"
        ws_dir = f"/Workspace/Users/{username}/.iot-digital-twin/lib"
        ws_path = f"{ws_dir}/{WHEEL_FILENAME}"

        try:
            w.workspace.mkdirs(ws_dir)
        except Exception:
            pass

        raw = augment_runtime_wheel(local_wheel)
        w.workspace.import_(
            path=ws_path,
            content=base64.b64encode(raw).decode(),
            format=ImportFormat.AUTO,
            overwrite=True,
        )
        logger.info("Uploaded wheel to %s (%d bytes)", ws_path, len(raw))
        return {"wheel_path": ws_path}

    except RuntimeWheelError as exc:
        logger.error("Wheel upload rejected: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=400, content={"error": "Wheel upload failed"})
    except Exception as exc:
        logger.error("Wheel upload failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": "Wheel upload failed"})


@router.post("/api/test-connectivity/submit")
async def submit_connectivity_test(request: Request, user_email: str = Depends(get_current_user)):
    """Upload a test notebook and submit a one-time serverless run."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "'notebook' content is required"})

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
        notebook_path = f"/Workspace/Users/{username}/.iot-digital-twin/connectivity_test_{ts}"
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
        logger.info("Uploaded connectivity test notebook to %s", notebook_path)

        waiter = w.jobs.submit(
            run_name=f"iot-connectivity-test-{ts}",
            tasks=[
                SubmitTask(
                    task_key="connectivity_test",
                    notebook_task=NotebookTask(
                        notebook_path=notebook_path,
                        source=Source.WORKSPACE,
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
        logger.info("Submitted serverless connectivity test run_id=%s", run_id)

        return {"run_id": run_id, "notebook_path": notebook_path}

    except Exception as exc:
        logger.error("Connectivity test submit failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/test-connectivity/{run_id}/status")
async def get_connectivity_test_status(run_id: int, user_email: str = Depends(get_current_user)):
    """Poll a serverless connectivity test run for its current state and result."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        run = w.jobs.get_run(run_id=run_id)

        life_cycle = (
            run.state.life_cycle_state.value
            if run.state and run.state.life_cycle_state
            else "UNKNOWN"
        )
        result_state = (
            run.state.result_state.value if run.state and run.state.result_state else None
        )
        state_message = run.state.state_message if run.state else ""

        if life_cycle in ("PENDING", "QUEUED", "RUNNING", "BLOCKED"):
            return {"state": life_cycle, "state_message": state_message}

        if life_cycle in ("TERMINATING",):
            return {"state": "RUNNING", "state_message": "Finishing up..."}

        if life_cycle == "TERMINATED" and result_state == "SUCCESS":
            nb_result = None

            # For multi-task runs, get output from the individual task run
            task_run_id = None
            if run.tasks:
                for task in run.tasks:
                    if task.run_id:
                        task_run_id = task.run_id
                        break

            try:
                output = w.jobs.get_run_output(run_id=task_run_id or run_id)
                if output.notebook_output and output.notebook_output.result:
                    try:
                        nb_result = json.loads(output.notebook_output.result)
                    except (json.JSONDecodeError, TypeError):
                        nb_result = {"raw": output.notebook_output.result}
            except Exception as out_err:
                logger.warning("Could not get run output: %s", out_err)
                nb_result = {
                    "success": True,
                    "message": "Run completed but output could not be retrieved.",
                }

            # Clean up the temp notebook
            if run.tasks and run.tasks[0].notebook_task:
                try:
                    w.workspace.delete(run.tasks[0].notebook_task.notebook_path)
                except Exception:
                    pass

            return {"state": "COMPLETED", "result": nb_result}

        error_msg = state_message
        if run.state and run.state.state_message:
            error_msg = run.state.state_message

        return {"state": "FAILED", "error": error_msg, "result_state": result_state}

    except Exception as exc:
        logger.error("Connectivity test status check failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/cluster/{cluster_id}/validate")
async def validate_on_cluster(
    cluster_id: str, request: Request, user_email: str = Depends(get_current_user)
):
    """Upload a notebook and execute it on the given cluster for validation.

    The notebook is written to a temporary workspace path, then executed
    via the command execution API.  The response includes stdout/stderr
    so the caller can inspect results.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "'notebook' content is required"})

    try:
        import base64

        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat, Language

        w = WorkspaceClient()

        me = w.current_user.me()
        username = me.user_name or "unknown"
        notebook_path = f"/Workspace/Users/{username}/.iot-digital-twin/validate_{int(time.time())}"

        parent_folder = notebook_path.rsplit("/", 1)[0]
        try:
            w.workspace.mkdirs(parent_folder)
        except Exception:
            pass

        # For validation, replace the entire try/awaitTermination/except block
        # with a time-bounded check so the command execution API returns
        # instead of blocking forever.
        _await_block = (
            "try:\n"
            '    print("[SSS] Streaming query started \u2014 awaiting termination \u2026")\n'
            "    query.awaitTermination()\n"
            "except Exception as e:\n"
            '    print(f"[SSS] Streaming query failed: {e}")\n'
            "    import traceback\n"
            "    traceback.print_exc()\n"
            "    raise"
        )
        _validation_block = (
            "import time as _t\n"
            "# Give Kafka admin client a longer window to drain on stop — when\n"
            "# brokers are unreachable (e.g. placeholder/demo values), the default\n"
            "# 15 s stop timeout surfaces as a Py4J TimeoutException and breaks\n"
            "# validation even though the stream started successfully.\n"
            'spark.conf.set("spark.sql.streaming.stopTimeout", "60000")\n'
            "_deadline = _t.time() + 20\n"
            "_batch_seen = False\n"
            "while _t.time() < _deadline:\n"
            "    for _q in spark.streams.active:\n"
            '        if _q.lastProgress and _q.lastProgress.get("numInputRows", 0) > 0:\n'
            "            _batch_seen = True\n"
            "    if _batch_seen:\n"
            "        break\n"
            "    _t.sleep(2)\n"
            "for _q in spark.streams.active:\n"
            '    _status = "active" if _q.isActive else "DEAD"\n'
            '    _rows = _q.lastProgress.get("numInputRows", 0) if _q.lastProgress else 0\n'
            '    print(f"Stream {_q.name}: {_status}, rows_processed={_rows}")\n'
            "    if not _q.isActive:\n"
            '        raise RuntimeError(f"Stream {_q.name} died: {_q.exception()}")\n'
            "    # Stop failures are ignored — we care that the stream *started*,\n"
            "    # not that it can cleanly drain an unreachable source.\n"
            "    try:\n"
            "        _q.stop()\n"
            "    except Exception as _stop_exc:\n"
            '        print(f"Stream {_q.name} did not stop cleanly (ignored): {type(_stop_exc).__name__}")\n'
            "if not spark.streams.active and not _batch_seen:\n"
            '    print("Warning: No streaming queries found after timeout")\n'
            'print("Validation OK \\u2014 streaming queries started and processed data successfully")'
        )
        validation_content = notebook_content.replace(_await_block, _validation_block)

        w.workspace.import_(
            path=notebook_path,
            content=base64.b64encode(validation_content.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded validation notebook to %s", notebook_path)

        from databricks.sdk.service.compute import Language as CmdLanguage

        ctx = w.command_execution.create(
            cluster_id=cluster_id,
            language=CmdLanguage.PYTHON,
        ).result()
        context_id = ctx.id

        # Kafka/Kinesis setup + 20 s batch wait inside the validation block
        # regularly runs past 60 s.  Allow a generous 5 min default and
        # accept a caller-provided ``validation_timeout_sec`` for tuning.
        validation_timeout_sec = int(body.get("validation_timeout_sec", 300))
        result = w.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context_id,
            language=CmdLanguage.PYTHON,
            command=validation_content,
        ).result(timeout=timedelta(seconds=validation_timeout_sec))

        status = result.status.value if result.status else "UNKNOWN"
        output = ""
        error = None

        if result.results:
            output = getattr(result.results, "data", "") or ""
            if getattr(result.results, "cause", None):
                error = result.results.cause

        if error:
            logger.error("validate_on_cluster error:\n%s", error)
        logger.info(
            "validate_on_cluster status=%s has_error=%s output_len=%s notebook=%s",
            status,
            error is not None,
            len(str(output)),
            notebook_path,
        )

        try:
            w.command_execution.destroy(cluster_id=cluster_id, context_id=context_id)
        except Exception:
            pass

        success = status == "Finished" and error is None
        return {
            "success": success,
            "status": status,
            "output": str(output),
            "error": error,
            "notebook_path": notebook_path,
        }

    except Exception as exc:
        logger.error("Validation failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


async def _maybe_deploy_lakets_ops_jobs(
    state: AppState,
    body: dict,
    pipeline_id: str | None,
    w: Any,
) -> dict | None:
    """Deploy LakeTS operational jobs when LakeTS is enabled for the pipeline.

    LakeTS is considered enabled when the pipeline's ``sink_config`` carries a
    tiering block OR its ``source_config`` carries an ``lb_project_id`` — the
    same signal the provisioning flow uses. Returns a result dict (status +
    jobs/error) or ``None`` when LakeTS is not enabled. Always non-raising.
    """
    if not pipeline_id:
        return None

    source_config: dict = {}
    sink_config: dict = {}
    if state.db_available and state.db:
        try:
            row = await state.db.fetchrow(
                "SELECT source_config, sink_config FROM pipelines WHERE id = $1::uuid",
                pipeline_id,
            )
            if row:
                source_config = _coerce_json_dict(row["source_config"])
                sink_config = _coerce_json_dict(row["sink_config"])
        except Exception as exc:
            logger.warning("deploy_job: failed to load pipeline %s config: %s", pipeline_id, exc)
            return None

    tiering = sink_config.get("tiering") if isinstance(sink_config, dict) else None
    lb_project_id = (
        source_config.get("lb_project_id") if isinstance(source_config, dict) else None
    ) or body.get("project_id")
    lakets_enabled = bool(tiering) or bool(lb_project_id)
    if not lakets_enabled:
        return None

    if not lb_project_id:
        logger.warning(
            "deploy_job: LakeTS enabled for pipeline %s but no lb_project_id resolvable",
            pipeline_id,
        )
        return {"status": "skipped", "error": "No lb_project_id resolvable"}

    branch_id = (
        (source_config.get("lb_branch_id") if isinstance(source_config, dict) else None)
        or body.get("branch_id")
        or "production"
    )
    database = (
        (source_config.get("lb_database") if isinstance(source_config, dict) else None)
        or body.get("database")
        or "databricks_postgres"
    )

    try:
        results = deploy_lakets_ops_jobs(
            w,
            lb_project_id,
            branch_id,
            database=database,
        )
        return {"status": "deployed", "jobs": results}
    except Exception as exc:
        logger.exception("deploy_job: deploy_lakets_ops_jobs failed for %s", pipeline_id)
        return {"status": "failed", "error": str(exc)}


def _coerce_json_dict(raw: Any) -> dict:
    """Return ``raw`` as a dict, parsing JSON strings; ``{}`` on any failure."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


@router.post("/api/cluster/deploy-job")
async def deploy_job(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Create a Databricks job with a job cluster and optionally start it.

    The notebook is uploaded to the workspace, then a job is created with
    a ``new_cluster`` spec so it runs on its own ephemeral job cluster
    (not the all-purpose cluster used for validation).
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    producer_notebook_content = body.get("producer_notebook")
    cluster_config = body.get("cluster_config", {})

    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "'notebook' content is required"})
    if not cluster_config.get("spark_version") or not cluster_config.get("worker_node_type"):
        return JSONResponse(
            status_code=422,
            content={
                "error": "'cluster_config' must include 'spark_version' and 'worker_node_type'"
            },
        )

    try:
        import base64

        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode
        from databricks.sdk.service.jobs import (
            Continuous,
            JobSettings,
            NotebookTask,
            PauseStatus,
        )
        from databricks.sdk.service.jobs import (
            Task as JobTaskSettings,
        )

        w = WorkspaceClient()

        me = w.current_user.me()
        username = me.user_name or "unknown"
        notebook_path = (
            f"/Workspace/Users/{username}/.iot-digital-twin/sss_pipeline_{int(time.time())}"
        )

        from databricks.sdk.service.workspace import ImportFormat, Language

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
        logger.info("Uploaded job notebook to %s", notebook_path)

        # Upload producer notebook if provided
        producer_notebook_path = None
        if producer_notebook_content:
            producer_notebook_path = (
                f"/Workspace/Users/{username}/.iot-digital-twin/data_producer_{int(time.time())}"
            )
            w.workspace.import_(
                path=producer_notebook_path,
                content=base64.b64encode(producer_notebook_content.encode()).decode(),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )
            logger.info("Uploaded producer notebook to %s", producer_notebook_path)

        spark_version = cluster_config["spark_version"]
        worker_node_type = cluster_config["worker_node_type"]
        num_workers = int(cluster_config.get("num_workers", 2))
        spark_conf = cluster_config.get("spark_conf", {})
        spark_conf["spark.databricks.streaming.realTimeMode.enabled"] = "true"

        from databricks.sdk.service.compute import AwsAttributes, AzureAttributes, EbsVolumeType

        cloud = detect_cloud()
        no_spot_kwargs: dict[str, Any] = {}
        if cloud == "aws":
            no_spot_kwargs["aws_attributes"] = AwsAttributes(
                first_on_demand=num_workers + 1,
                ebs_volume_count=1,
                ebs_volume_size=100,
                ebs_volume_type=EbsVolumeType.GENERAL_PURPOSE_SSD,
            )
        elif cloud == "azure":
            no_spot_kwargs["azure_attributes"] = AzureAttributes(first_on_demand=num_workers + 1)

        tasks = [
            JobTaskSettings(
                task_key="sss_consumer",
                new_cluster=ClusterSpec(
                    spark_version=spark_version,
                    node_type_id=worker_node_type,
                    driver_node_type_id=worker_node_type,
                    num_workers=num_workers,
                    spark_conf=spark_conf,
                    data_security_mode=DataSecurityMode.SINGLE_USER,
                    single_user_name=username,
                    **no_spot_kwargs,
                ),
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                ),
            ),
        ]

        environments = None
        if producer_notebook_path:
            tasks.append(
                JobTaskSettings(
                    task_key="data_producer",
                    new_cluster=ClusterSpec(
                        spark_version=spark_version,
                        node_type_id=worker_node_type,
                        driver_node_type_id=worker_node_type,
                        num_workers=0,
                        spark_conf={
                            "spark.databricks.cluster.profile": "singleNode",
                            "spark.master": "local[*]",
                        },
                        data_security_mode=DataSecurityMode.SINGLE_USER,
                        single_user_name=username,
                        custom_tags={"ResourceClass": "SingleNode"},
                        **no_spot_kwargs,
                    ),
                    notebook_task=NotebookTask(
                        notebook_path=producer_notebook_path,
                    ),
                ),
            )

        # Check for existing job_id — reuse instead of creating a new one
        existing_job_id = body.get("job_id")
        pipeline_id = body.get("pipeline_id")

        # Also check the pipeline's stored resources if no job_id in request
        if not existing_job_id and pipeline_id and state.db_available and state.db:
            try:
                import json as _json

                row = await state.db.fetchrow(
                    "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id
                )
                if row:
                    res = (
                        _json.loads(row["resources"])
                        if isinstance(row["resources"], str)
                        else (row["resources"] or {})
                    )
                    existing_job_id = res.get("job_id")
            except Exception:
                pass

        host = os.getenv("DATABRICKS_HOST", "").rstrip("/")

        if existing_job_id:
            # Update the existing job with new notebook and config
            jid = int(existing_job_id)
            try:
                w.jobs.cancel_all_runs(job_id=jid)
            except Exception as cancel_err:
                logger.warning("Could not cancel runs for job %s: %s", jid, cancel_err)

            w.jobs.reset(
                job_id=jid,
                new_settings=JobSettings(
                    name=f"IoT-SSS-Pipeline-{jid}",
                    continuous=Continuous(pause_status=PauseStatus.UNPAUSED),
                    tasks=tasks,
                    environments=environments,
                ),
            )
            job_id = jid
            job_url = f"{host}/#job/{job_id}" if host else ""
            logger.info("Job updated (reused): id=%s, notebook=%s", job_id, notebook_path)
        else:
            # Create a brand new job
            job = w.jobs.create(
                name=f"IoT-SSS-Pipeline-{int(time.time())}",
                continuous=Continuous(pause_status=PauseStatus.UNPAUSED),
                tasks=tasks,
                environments=environments,
            )
            job_id = job.job_id
            job_url = f"{host}/#job/{job_id}" if host else ""
            logger.info("Job created (new): id=%s, notebook=%s", job_id, notebook_path)

        if pipeline_id:
            await track_resource(state.db, state.db_available, pipeline_id, "job_id", str(job_id))
            await track_resource(
                state.db, state.db_available, pipeline_id, "notebook_path", notebook_path
            )

        response: dict[str, Any] = {
            "job_id": job_id,
            "job_url": job_url,
            "notebook_path": notebook_path,
        }

        # If LakeTS is enabled for this pipeline, deploy the operational jobs
        # (partition manager, rollup, retention) against its Lakebase. Non-fatal:
        # a failure here is reported but does not fail the consumer-job deploy.
        lakets_jobs_result = await _maybe_deploy_lakets_ops_jobs(state, body, pipeline_id, w)
        if lakets_jobs_result is not None:
            response["lakets_jobs"] = lakets_jobs_result

        return response

    except Exception as exc:
        logger.error("Job deployment failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})
