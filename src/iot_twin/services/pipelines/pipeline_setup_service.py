"""One-click Lakebase + LakeTS pipeline-setup orchestrator.

``setup_pipeline_lakebase`` chains the existing provisioning helpers into a
single idempotent flow, then deploys the LakeTS operational jobs.  Every step
is NON-FATAL: a failure is captured in the returned ``steps`` list and the
chain continues.  Nothing here raises for an individual step failure.

Step order (mirrors the product spec):
    a. ensure_streaming_sp        — secret scope + SP + OAuth secret + app grant
    b. provision_lakebase_project_and_branch (only when create_new_project)
    c. provision_lakebase_role    — register the SP as an OAuth Postgres role
    d. provision_lakebase_table   — table DDL + install LakeTS + chronotable +
                                    tier policy + enable_sync (when tiering set)
    e. lakets_service.create_rollup (per requested rollup)
    f. deploy_lakets_ops_jobs     — partition-manager + rollup + retention + tiering

The LakeTS surface (``lakets_service.create_rollup`` /
``deploy_lakets_ops_jobs``) is owned by a sibling module; imports are guarded
so this module always imports even if that surface is still in flight.
"""

from __future__ import annotations

import logging
from typing import Any

from iot_twin.core.workspace_config import get_secret_scope
from iot_twin.services.lakets import lakets_service
from iot_twin.services.provisioning_service import (
    ensure_streaming_sp,
    provision_lakebase_project_and_branch,
    provision_lakebase_role,
    provision_lakebase_table,
)

# ``deploy_lakets_ops_jobs`` is owned by iot_twin.services.lakets.lakets_deploy_service (sibling
# agent).  Guard the import so this module loads even if that surface is mid
# flight; the deploy step then records a clear "unavailable" failure.
try:  # pragma: no cover - import guard
    from iot_twin.services.lakets.lakets_deploy_service import deploy_lakets_ops_jobs
except Exception as _exc:  # pragma: no cover - import guard
    deploy_lakets_ops_jobs = None  # type: ignore[assignment]
    _DEPLOY_IMPORT_ERROR = str(_exc)
else:
    _DEPLOY_IMPORT_ERROR = ""

logger = logging.getLogger("iot_digital_twin.services.pipeline_setup")


def _ok_step(step: str, results: Any) -> dict:
    return {"step": step, "status": "ok", "results": results}


def _failed_step(step: str, exc: Exception | str) -> dict:
    return {"step": step, "status": "failed", "error": str(exc)[:300]}


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _check_project_health(w: Any, project_id: str, branch_id: str) -> str | None:
    """Return a human-readable error if the Lakebase project is unusable.

    Catches the "zombie project" failure mode: a corrupted control-plane record
    where ``get_project`` succeeds but ``list_endpoints`` raises "Project not
    found" (the project's branch/endpoint graph is gone and the name cannot be
    healed or even deleted). Without this, table/role/jobs steps may tick on
    timing variance while ``create_rollup`` fails with a confusing
    "Project ... not found" — see the iot-twin-lakebase incident.

    Returns ``None`` when the project's endpoints are reachable.
    """
    parent = f"projects/{project_id}/branches/{branch_id}"
    try:
        endpoints = list(w.postgres.list_endpoints(parent=parent))
    except Exception as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            return (
                f"Lakebase project '{project_id}' is in a broken/unusable state "
                f"(endpoints not reachable: {msg[:120]}). This name cannot be "
                f"recovered — choose a DIFFERENT project name and run setup again."
            )
        return f"Could not list endpoints for {parent}: {msg[:160]}"
    if not endpoints:
        return (
            f"Lakebase project '{project_id}' has no endpoints on branch "
            f"'{branch_id}'. Wait for provisioning to finish, or choose a "
            f"different project name and run setup again."
        )
    return None


async def _open_pipeline_connection(w: Any, project_id: str, branch_id: str, database: str) -> Any:
    """Open an asyncpg connection to the pipeline's Lakebase endpoint.

    Resolves host + OAuth token the same way ``provision_lakebase_table`` does,
    so rollups run against the per-pipeline Lakebase rather than the app
    backend's shared one.  Raises on failure — the caller wraps it.
    """
    import asyncpg

    parent = f"projects/{project_id}/branches/{branch_id}"
    endpoints = list(w.postgres.list_endpoints(parent=parent))
    if not endpoints:
        raise RuntimeError(f"No endpoints for {parent}")

    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    pg_host = endpoint.status.hosts.host
    pg_user = w.current_user.me().user_name
    cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)

    return await asyncpg.connect(
        host=pg_host,
        database=database,
        user=pg_user,
        password=cred.token,
        port=5432,
        ssl="require",
    )


async def setup_pipeline_lakebase(state: Any, params: dict) -> dict:
    """Orchestrate the full Lakebase + LakeTS setup for one pipeline.

    Args:
        state: the application ``AppState`` (kept for symmetry / future use —
            the per-pipeline Lakebase connection is resolved fresh here).
        params: validated request body (see ``routes/setup.py``).

    Returns:
        ``{"steps": [...], "lb_project_id": str, "lb_branch_id": str,
           "table_name": str, "client_id": str | None}``.
    """
    steps: list[dict] = []

    pipeline_id = params.get("pipeline_id")
    create_new_project = bool(params.get("create_new_project"))
    lb_project_id = (params.get("lb_project_id") or "").strip()
    lb_branch_id = (params.get("lb_branch_id") or "production").strip()
    lb_database = (params.get("lb_database") or "databricks_postgres").strip()
    table_name = (params.get("table_name") or "sensor_readings").strip()
    secret_scope = (params.get("secret_scope") or get_secret_scope()).strip()
    tiering = params.get("tiering")
    # ``params["retention"]`` (the UI drop-after interval) is intentionally not
    # read here: no plain-retention LakeTS policy is wired up yet. Hot-store
    # retention is enforced by the tiering policy's ``drop`` interval.
    rollups = params.get("rollups") or []

    w = _new_ws()
    client_id: str | None = None

    # ── (a) Streaming SP + scope + OAuth secret + app grant ────────────────
    try:
        sp_result = await ensure_streaming_sp(w, secret_scope)
        client_id = sp_result.get("client_id")
        steps.append(_ok_step("ensure_streaming_sp", sp_result.get("results", [])))
    except Exception as exc:  # pragma: no cover - ensure_streaming_sp is non-raising
        logger.warning("ensure_streaming_sp failed (non-fatal): %s", exc)
        steps.append(_failed_step("ensure_streaming_sp", exc))

    # ── (b) Create the Lakebase project + branch (only when requested) ─────
    if create_new_project:
        try:
            proj_results = await provision_lakebase_project_and_branch(lb_project_id, lb_branch_id)
            steps.append(_ok_step("provision_lakebase_project_and_branch", proj_results))
        except Exception as exc:
            logger.warning("provision_lakebase_project_and_branch failed (non-fatal): %s", exc)
            steps.append(_failed_step("provision_lakebase_project_and_branch", exc))

    # ── Pre-flight: confirm the Lakebase project's endpoints are reachable ──
    # A broken/zombie project (get_project succeeds but list_endpoints 404s)
    # would otherwise let role/table/jobs tick on timing variance while
    # create_rollup fails with a confusing "Project not found". Fail fast here
    # with one clear, actionable message and skip the per-project steps.
    health_error = _check_project_health(w, lb_project_id, lb_branch_id)
    if health_error:
        logger.warning("Lakebase project health check failed: %s", health_error)
        steps.append(_failed_step("lakebase_project_health", health_error))
        return {
            "success": False,
            "failed_steps": ["lakebase_project_health"],
            "steps": steps,
            "pipeline_id": pipeline_id,
            "lb_project_id": lb_project_id,
            "lb_branch_id": lb_branch_id,
            "table_name": table_name,
            "client_id": client_id,
        }

    # ── (c) Register the SP as an OAuth Postgres role ──────────────────────
    try:
        role_results = await provision_lakebase_role(
            sp_client_id=client_id,
            project_id=lb_project_id,
            branch_id=lb_branch_id,
            database=lb_database,
            sink_tables=(table_name,),
        )
        steps.append(_ok_step("provision_lakebase_role", role_results))
    except Exception as exc:
        logger.warning("provision_lakebase_role failed (non-fatal): %s", exc)
        steps.append(_failed_step("provision_lakebase_role", exc))

    # ── (d) Table DDL + install LakeTS + chronotable + tier + enable_sync ──
    try:
        table_lb: dict[str, Any] = {
            "mode": "autoscale",
            "project_id": lb_project_id,
            "branch_id": lb_branch_id,
            "database": lb_database,
            "pg_table": table_name,
        }
        if tiering:
            table_lb["tiering"] = tiering
        table_result = await provision_lakebase_table(table_lb)
        steps.append(_ok_step("provision_lakebase_table", [table_result]))
    except Exception as exc:
        logger.warning("provision_lakebase_table failed (non-fatal): %s", exc)
        steps.append(_failed_step("provision_lakebase_table", exc))

    # ── (e) Create rollups (one LakeTS call per requested rollup) ──────────
    if rollups:
        conn = None
        try:
            conn = await _open_pipeline_connection(w, lb_project_id, lb_branch_id, lb_database)
        except Exception as exc:
            logger.warning("rollup connection failed (non-fatal): %s", exc)
            steps.append(_failed_step("create_rollup", exc))
            conn = None

        if conn is not None:
            try:
                for rollup in rollups:
                    name = rollup.get("name")
                    try:
                        rollup_result = await lakets_service.create_rollup(
                            conn,
                            table=table_name,
                            name=name,
                            bucket=rollup.get("bucket"),
                            aggregates=rollup.get("aggregates"),
                            time_col=(
                                rollup.get("time_column")
                                or (tiering or {}).get("time_column")
                                or "event_time"
                            ),
                            value_column=(
                                rollup.get("value_column")
                                or (tiering or {}).get("value_column")
                                or "value"
                            ),
                        )
                        steps.append(
                            {
                                "step": "create_rollup",
                                "status": rollup_result.get("status", "ok"),
                                "results": rollup_result,
                            }
                        )
                    except Exception as exc:
                        logger.warning("create_rollup '%s' failed (non-fatal): %s", name, exc)
                        steps.append(_failed_step("create_rollup", exc))
            finally:
                try:
                    await conn.close()
                except Exception:  # pragma: no cover - defensive
                    pass

    # ── (f) Deploy the LakeTS operational jobs ─────────────────────────────
    if deploy_lakets_ops_jobs is None:
        steps.append(
            _failed_step(
                "deploy_lakets_ops_jobs",
                f"deploy_lakets_ops_jobs unavailable: {_DEPLOY_IMPORT_ERROR}",
            )
        )
    else:
        try:
            job_results = deploy_lakets_ops_jobs(
                w,
                lb_project_id,
                lb_branch_id,
                lb_database,
                partition_manager=True,
                rollup=True,
                retention=True,
                tiering=True,
            )
            steps.append(_ok_step("deploy_lakets_ops_jobs", job_results))
        except Exception as exc:
            logger.warning("deploy_lakets_ops_jobs failed (non-fatal): %s", exc)
            steps.append(_failed_step("deploy_lakets_ops_jobs", exc))

    # The chain is non-fatal per step; report overall success so the UI doesn't
    # render an all-green run as "failed". A step is a failure only when its
    # status is failed/error — "ok"/"created"/"already_exists"/"skipped" all pass.
    _FAILED = {"failed", "error"}
    failed_steps = [s["step"] for s in steps if s.get("status") in _FAILED]
    success = not failed_steps

    return {
        "success": success,
        "failed_steps": failed_steps,
        "steps": steps,
        "pipeline_id": pipeline_id,
        "lb_project_id": lb_project_id,
        "lb_branch_id": lb_branch_id,
        "table_name": table_name,
        "client_id": client_id,
    }
