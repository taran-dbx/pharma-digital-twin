"""Industry catalog status resolution.

Joins the curated industry/mode templates (``iot_twin.services.industry.industry_templates``)
against real pipeline rows in the Lakebase ``pipelines`` table to report, per
curated industry, which of its three streaming modes are
provisioned/running. This is the data the gallery + setup screens consume.

Naming is fully deterministic — a mode's pipeline/catalog/topic name is
derived from ``(industry, mode)`` alone (see ``pipeline_name``,
``catalog_name``, ``topic_name``), so a pipeline row can be joined back to
its template slot with no extra lookup table.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
from typing import Any

from iot_twin.core.state import AppState
from iot_twin.core.workspace_config import get_secret_scope
from iot_twin.services.deployment import deployment_service
from iot_twin.services.industry.industry_templates import (
    CURATED_INDUSTRIES,
    MODES,
    get_template,
    mode_config,
)


def _resolve_app_sp(state: Any) -> dict:
    """Resolve the SINGLE app service principal client id — no minting.

    Task 3 removed the per-pipeline / shared streaming SP: the app SP
    (``DATABRICKS_CLIENT_ID``) is the one identity used for every auth path,
    so ``start_mode`` only needs to resolve it, never create it.
    """
    cid = os.getenv("DATABRICKS_CLIENT_ID", "")
    return {"client_id": cid or None}


logger = logging.getLogger("iot_digital_twin.services.industry_service")

# Pipeline lifecycle values (row "status", falling back to "state") that map
# to a "running" / "starting" mode status. Anything else — including no
# matching pipeline row at all — reports as "idle".
_RUNNING_STATES = {"running"}
_STARTING_STATES = {"starting", "pending", "queued", "provisioning", "deploying", "blocked"}


def pipeline_name(industry: str, mode: str) -> str:
    """Deterministic pipeline name for a curated industry's streaming mode."""
    return f"iot-{industry}-{mode}"


def catalog_name(industry: str) -> str:
    """Deterministic Unity Catalog name for a curated industry."""
    return f"iot_{industry}"


def topic_name(industry: str, mode: str) -> str:
    """Deterministic Kafka topic name for a curated industry's streaming mode."""
    return f"iot.{industry}.{mode}"


async def _list_pipelines(state: AppState) -> list[dict[str, Any]]:
    """Return non-deleted pipeline rows (name/status/industry/source info).

    Thin wrapper around the same ``pipelines`` table access
    ``iot_twin.routes.pipelines.list_pipelines`` uses (``state.db.fetch`` against the
    Lakebase-backed ``pipelines`` table, excluding ``status = 'deleted'``).
    The gallery/setup catalog is not user-scoped — it needs the lifecycle of
    every curated-industry pipeline, not just one user's — so this omits
    that route's ``user_email`` filter.

    ``industry``, ``source_type`` and ``source_config`` are pulled alongside
    ``name``/``status`` so the reconciliation pass in ``get_catalog`` can
    recognize pipelines that predate the deterministic ``iot-<industry>-
    <mode>`` naming convention (see ``_infer_mode``).
    """
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return []
    try:
        rows = await state.db.fetch(
            "SELECT id, name, status, industry, source_type, source_config "
            "FROM pipelines WHERE status != 'deleted'"
        )
    except Exception as exc:
        logger.warning("Failed to list pipelines for industry catalog: %s", exc)
        return []
    return [dict(row) for row in rows]


async def _list_industry_first_deployments(
    state: AppState,
) -> dict[tuple[str, str], dict[str, str | None]]:
    """Return ``{(industry, mode): {deployment_id, setup_id}}`` for admin deploys.

    Industry-first deployments intentionally do not create legacy ``pipelines``
    rows. Their ``pipeline_deployment.id`` is therefore the stable per-mode
    identity used to unlock industry-aware dashboard endpoints; ``setup_id``
    (``pipeline_setup.id``) addresses per-pipeline Genie routes.
    """
    db = getattr(state, "app_backend_db", None) if state else None
    if db is None:
        return {}
    try:
        rows = await db.fetch(
            """
            SELECT pd.id AS deployment_id, ps.id AS setup_id, ps.industry, pd.mode
            FROM pipeline_deployment pd
            JOIN pipeline_setup ps ON ps.id = pd.setup_id
            WHERE pd.job_id IS NOT NULL
              AND pd.status NOT IN ('pending', 'deploying', 'failed')
            """
        )
    except Exception as exc:
        logger.warning("Failed to list industry-first deployments for catalog: %s", exc)
        return {}
    return {
        (str(row["industry"]), str(row["mode"])): {
            "deployment_id": str(row["deployment_id"]),
            "setup_id": str(row["setup_id"]) if row.get("setup_id") else None,
        }
        for row in rows
        if row.get("deployment_id") and row.get("industry") and row.get("mode")
    }


def _coerce_json_dict(raw: Any) -> dict:
    """Return ``raw`` as a dict, parsing JSON strings; ``{}`` on any failure."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


# Sink identifiers that indicate a Lakebase/LakeTS destination — used by
# ``_infer_mode`` to tell "realtime" (SSS -> Lakebase) apart from
# "microbatch" (SSS -> Delta) for pre-convention pipeline rows.
_LAKEBASE_SINK_MARKERS = ("lakebase", "lb_", "lakets")


def _infer_mode(row: dict[str, Any]) -> str | None:
    """Best-effort streaming-mode inference for a pipeline row.

    Existing pipelines deployed before the deterministic ``iot-<industry>-
    <mode>`` naming convention (e.g. one literally named ``"Demo"``) can't be
    joined back to a curated mode by name. This infers the mode from
    ``source_type``/``source_config`` instead, so ``get_catalog``'s
    reconciliation pass can still report it as running.

    - ``source_type == "zerobus"`` -> ``"zerobus"`` (no Spark job, writes
      straight to Delta — see ``mode_config("zerobus")``).
    - ``source_type == "sss"`` (Spark Structured Streaming): a
      Lakebase/LakeTS sink (``rtm_sink == "lakebase"``, or any
      ``lb_*``/``lakets`` marker in ``source_config``) -> ``"realtime"``;
      a Delta/micro-batch sink -> ``"microbatch"``; otherwise defaults to
      ``"realtime"`` (SSS's most common target for this app).
    - Anything else (unknown/legacy ``source_type``, or none at all) ->
      ``None`` — no mode can be inferred, so the reconciliation pass skips
      the row.
    """
    source_type = str(row.get("source_type") or "").lower()
    if source_type == "zerobus":
        return "zerobus"
    if source_type != "sss":
        return None

    source_config = _coerce_json_dict(row.get("source_config"))
    rtm_sink = str(source_config.get("rtm_sink") or "").lower()
    if rtm_sink == "lakebase":
        return "realtime"
    if rtm_sink in ("delta", "microbatch"):
        return "microbatch"

    config_blob = json.dumps(source_config).lower()
    if any(marker in config_blob for marker in _LAKEBASE_SINK_MARKERS):
        return "realtime"

    return "realtime"


def _mode_status(row: dict[str, Any] | None) -> str:
    """Map a pipeline row's lifecycle state to a mode status; absent -> idle."""
    if row is None:
        return "idle"
    state_value = str(row.get("status") or row.get("state") or "").lower()
    if state_value in _RUNNING_STATES:
        return "running"
    if state_value in _STARTING_STATES:
        return "starting"
    return "idle"


def is_mode_active(row: dict[str, Any] | None) -> bool:
    """Is this pipeline row effectively live (should block a config change)?

    Consistent with the catalog's displayed status: a row is "active" if
    ``_mode_status`` reports running/starting OR its status is one of the
    reconciled-deployed states (``step2_complete``/``running``) that the
    gallery surfaces as "running" for legacy pre-convention pipelines (see
    ``_RECONCILE_DEPLOYED_STATES``). The config endpoint uses this so it
    can't silently mutate a pipeline the UI shows as running.
    """
    if row is None:
        return False
    if _mode_status(row) in ("running", "starting"):
        return True
    return str(row.get("status") or row.get("state") or "").lower() in _RECONCILE_DEPLOYED_STATES


# Pipeline row statuses treated as "deployed/active" by the reconciliation
# pass below — a legacy row need not be literally ``"running"`` to count,
# since ``_STOPPED_STATUS`` (``"step2_complete"``) is also written for
# pipelines that were provisioned and deployed outside the new deterministic
# naming flow. ``draft``/``deleted`` rows never reconcile as running.
#
# Known limitation: ``step2_complete`` is reused by ``stop_mode`` (see its
# docstring) to mean "stopped" for pipelines created through the new
# deterministic-name flow. A pre-convention pipeline that some future flow
# stops by writing this same status would still reconcile as "running" here
# — there is no separate status value today to distinguish "legacy deployed"
# from "new-flow stopped". Acceptable for this reconciliation slice.
_RECONCILE_DEPLOYED_STATES = {"step2_complete", "running"}


def _all_deterministic_names() -> set[str]:
    """Every curated ``(industry, mode)`` slot's deterministic pipeline name."""
    return {pipeline_name(industry, mode) for industry in CURATED_INDUSTRIES for mode in MODES}


async def resolve_pipeline_for_mode(
    state: AppState,
    industry: str,
    mode: str,
    *,
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Resolve the pipeline row (if any) provisioned for a curated ``(industry,
    mode)`` slot — the single source of matching truth ``get_catalog`` (the
    gallery/setup status view) and the stopped-only config endpoint
    (``routes/industries.py``) both rely on, so the two never drift apart.

    Two passes, mirroring ``get_catalog``'s original two-pass algorithm:
    1. Exact deterministic-name join (``iot-<industry>-<mode>``) — handles
       every pipeline created through the new industry-catalog flow
       (``start_mode``/``stop_mode``).
    2. Reconciliation — a pipeline deployed before that naming convention
       existed (e.g. one literally named ``"Demo"``) can't be found by name,
       so any row not already claimed by *some* curated slot's exact name
       (``_all_deterministic_names``), whose ``industry`` matches and whose
       inferred mode (``_infer_mode``) matches, and whose status counts as
       deployed (``_RECONCILE_DEPLOYED_STATES``), is returned instead.

    ``rows`` lets ``get_catalog`` pass its already-fetched pipeline rows in
    so building the full catalog doesn't re-query per ``(industry, mode)``
    pair; callers resolving a single pair (e.g. the config endpoint) omit it
    and this fetches fresh via ``_list_pipelines``.
    """
    if rows is None:
        rows = await _list_pipelines(state)

    name = pipeline_name(industry, mode)
    for row in rows:
        if row.get("name") == name:
            return row

    reserved_names = _all_deterministic_names()
    for row in rows:
        if row.get("name") in reserved_names:
            continue  # already claimed by another curated slot's exact match
        if row.get("industry") != industry:
            continue
        status_value = str(row.get("status") or "").lower()
        if status_value not in _RECONCILE_DEPLOYED_STATES:
            continue  # draft / anything else — not deployed, skip
        if _infer_mode(row) != mode:
            continue
        return row

    return None


async def update_pipeline_config(
    state: AppState,
    pipeline_id: str,
    patch: dict[str, Any],
) -> dict[str, Any]:
    """Merge ``patch`` into a pipeline row's ``source_config`` JSONB, by id.

    Reuses the exact jsonb-merge technique ``routes/pipelines.py::update_
    pipeline`` already uses for jsonb columns (``COALESCE(source_config,
    '{}'::jsonb) || $2::jsonb``) rather than a new hand-written UPDATE — just
    scoped to a single column and addressed by pipeline id. Returns the
    row's merged ``source_config`` dict (``{}`` if there's no DB or no
    matching row).
    """
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return {}
    row = await state.db.fetchrow(
        "UPDATE pipelines SET source_config = COALESCE(source_config, '{}'::jsonb) || $2::jsonb, "
        "updated_at = NOW() WHERE id = $1::uuid RETURNING source_config",
        pipeline_id,
        json.dumps(patch),
    )
    if not row:
        return {}
    return _coerce_json_dict(row["source_config"])


async def get_catalog(state: AppState) -> list[dict[str, Any]]:
    """Return one gallery entry per curated industry, with per-mode status.

    Each entry: ``{id, name, accent, icon, twin_type, tag, modes, running_count}``
    where ``modes`` is ``[{id, name, to, desc, status, pipeline_id,
    deployment_id, setup_id}]`` and
    ``status`` is one of ``"idle" | "starting" | "running"``.

    Per-mode matching is delegated to ``resolve_pipeline_for_mode`` (shared
    with the config endpoint, so the two never drift apart). A row found via
    the exact deterministic-name join reports its own lifecycle status
    (``_mode_status``); a row found only via the reconciliation pass is
    promoted to ``"running"`` for display regardless of its literal status
    value (see ``resolve_pipeline_for_mode``'s docstring for why a legacy row
    counts as deployed at all).
    """
    rows = await _list_pipelines(state)
    industry_first_ids = await _list_industry_first_deployments(state)

    catalog: list[dict[str, Any]] = []
    for industry in CURATED_INDUSTRIES:
        template = get_template(industry)
        if template is None:
            continue

        modes: list[dict[str, Any]] = []
        running_count = 0
        for mode in MODES:
            cfg = mode_config(mode)
            first_ids = industry_first_ids.get((industry, mode)) or {}
            row = await resolve_pipeline_for_mode(state, industry, mode, rows=rows)
            if row is None:
                status = "idle"
            elif row.get("name") == pipeline_name(industry, mode):
                status = _mode_status(row)
            else:
                status = "running"  # reconciled legacy row
            if status == "running":
                running_count += 1
            modes.append(
                {
                    "id": mode,
                    "name": cfg["name"],
                    "to": cfg["to"],
                    "desc": cfg["desc"],
                    "status": status,
                    # Real legacy pipeline-row id, retained only for API
                    # compatibility and data resolution. The UI no longer
                    # navigates to legacy per-pipeline pages.
                    "pipeline_id": str(row["id"]) if row and row.get("id") else None,
                    # Industry-first deliberately has no legacy ``pipelines``
                    # row. Keep its deployment identity separate so config
                    # links never send a deployment UUID to legacy CRUD.
                    "deployment_id": first_ids.get("deployment_id"),
                    # pipeline_setup.id — addresses /api/genie/pipelines/{setup_id}/{mode}.
                    "setup_id": first_ids.get("setup_id"),
                }
            )

        catalog.append(
            {
                "id": template.id,
                "name": template.name,
                "accent": template.accent,
                "icon": template.icon,
                "twin_type": template.twin_type,
                "tag": template.tag,
                "lede": template.lede,
                "kpis": template.kpis,
                "sensors": template.sensors,
                "modes": modes,
                "running_count": running_count,
            }
        )

    return catalog


# ---------------------------------------------------------------------------
# Start / stop a mode's pipeline
# ---------------------------------------------------------------------------

# Curated-industry pipelines are app-wide (see ``get_catalog``'s docstring) —
# they are not created through the per-user wizard
# (``routes/pipelines.py::create_pipeline``), so there is no end-user
# ``user_email`` to attribute the row to. ``pipelines.user_email`` is
# NOT NULL (see ``core/lakebase.py::ensure_pipelines_table``), so a fixed system
# owner is used for every curated-industry row.
_INDUSTRY_PIPELINE_OWNER = "industries@iot-digital-twin.system"

# Status ``stop_mode`` writes. Reuses the exact value
# ``routes/pipelines.py::reset_pipeline`` already writes for "provisioned but
# not currently running" — deliberately NOT in ``_RUNNING_STATES`` or
# ``_STARTING_STATES`` above, so ``_mode_status`` reports it as "idle",
# which is what the gallery/setup screens should show once a mode is stopped.
# Distinct from the legacy "step2_complete" (which means "deployed" and is
# treated as active by the reconciliation pass): a pipeline stopped through
# the new industry flow gets its own unambiguous status so its config stays
# editable and it reads as idle in the catalog.
_STOPPED_STATUS = "stopped"


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call ``fn`` and await its result if ``fn`` returned an awaitable.

    The provisioning seams below (``_provision_catalog_schema``,
    ``_provision_lakebase_branch``, ``_deploy_and_start``) and
    ``_resolve_app_sp`` are seams (async or sync) in
    production, but ``tests/test_industry_service_start.py`` (per the task
    brief) monkeypatches them with plain synchronous lambdas that return a
    value directly instead of a coroutine. This lets ``start_mode`` /
    ``stop_mode`` call every seam the same way regardless of which shape the
    caller installed.
    """
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


def _parse_resources(row: dict[str, Any] | None) -> dict[str, Any]:
    """Best-effort JSON-decode a pipeline row's ``resources`` column."""
    if not row:
        return {}
    resources = row.get("resources")
    if isinstance(resources, str):
        try:
            return json.loads(resources) or {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return resources or {}


async def _ensure_pipeline_row(
    state: AppState,
    name: str,
    industry: str,
    status: str,
) -> str | None:
    """Ensure a pipeline row named ``name`` exists, set to ``status``.

    Idempotent lookup-by-name — deterministic naming means no other lookup
    key is needed. Returns the row's id, or ``None`` when no DB is
    configured; the caller (``start_mode``) degrades gracefully in that
    case, same as every other DB-touching helper in this module.
    """
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return None
    try:
        row = await state.db.fetchrow(
            "SELECT id FROM pipelines WHERE name = $1 AND status != 'deleted'",
            name,
        )
        if row is None:
            row = await state.db.fetchrow(
                """
                INSERT INTO pipelines (user_email, name, industry, status)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                _INDUSTRY_PIPELINE_OWNER,
                name,
                industry,
                status,
            )
        else:
            await state.db.execute(
                "UPDATE pipelines SET status = $2, updated_at = NOW() WHERE id = $1",
                row["id"],
                status,
            )
    except Exception as exc:
        logger.warning("Failed to ensure pipeline row for %s: %s", name, exc)
        return None
    return str(row["id"]) if row else None


async def _set_pipeline_status(state: AppState, name: str, status: str) -> None:
    """Write ``status`` onto the pipeline row named ``name`` (non-fatal)."""
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return
    try:
        await state.db.execute(
            "UPDATE pipelines SET status = $2, updated_at = NOW() WHERE name = $1",
            name,
            status,
        )
    except Exception as exc:
        logger.warning("Failed to set status for pipeline %s: %s", name, exc)


async def _get_job_id(state: AppState, pipeline_id: str | None) -> str | None:
    """Resolve the tracked ``job_id`` (if any) for a pipeline row by id."""
    if (
        not pipeline_id
        or not state
        or not getattr(state, "db_available", False)
        or not getattr(state, "db", None)
    ):
        return None
    try:
        row = await state.db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid",
            pipeline_id,
        )
    except Exception as exc:
        logger.warning("Failed to look up job_id for pipeline %s: %s", pipeline_id, exc)
        return None
    return _parse_resources(row).get("job_id")


async def _get_pipeline_id_and_job(
    state: AppState,
    name: str,
) -> tuple[str | None, str | None]:
    """Resolve ``(pipeline_id, job_id)`` for the mode's pipeline row by name."""
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return None, None
    try:
        row = await state.db.fetchrow(
            "SELECT id, resources FROM pipelines WHERE name = $1 AND status != 'deleted'",
            name,
        )
    except Exception as exc:
        logger.warning("Failed to look up pipeline %s: %s", name, exc)
        return None, None
    if not row:
        return None, None
    return str(row["id"]), _parse_resources(row).get("job_id")


async def _provision_catalog_schema(industry: str, mode: str) -> list[dict]:
    """Idempotently create catalog ``iot_<industry>`` with schema ``<mode>``.

    Thin wrapper around ``provisioning_service.provision_uc_catalog`` — the
    real UC catalog/schema provisioning already used by the settings page
    and the pipeline wizard. Passes the exact catalog name (no ``{name}``
    templating) and a single schema (the mode name), so no new provisioning
    logic is introduced here.
    """
    from iot_twin.services.provisioning_service import provision_uc_catalog

    return await provision_uc_catalog(
        {
            "catalog_pattern": catalog_name(industry),
            "schemas": [mode],
        }
    )


async def _provision_lakebase_branch(
    state: AppState,
    industry: str,
    mode: str,
    pipeline_id: str | None,
) -> dict:
    """Provision the mode's Lakebase branch + LakeTS (realtime only).

    Thin wrapper around the existing one-click orchestrator
    ``pipeline_setup_service.setup_pipeline_lakebase`` — reuses its full
    Lakebase project/branch/role/table/rollup/ops-job chain rather than
    reimplementing any of it. Each mode gets its own dedicated Lakebase
    project, named after the mode's own deterministic pipeline name, so
    ``create_new_project=True`` is always safe to pass — idempotent, since
    ``provision_lakebase_project_and_branch`` no-ops on an already-existing
    project/branch.
    """
    from iot_twin.services.pipelines.pipeline_setup_service import setup_pipeline_lakebase

    params = {
        "pipeline_id": pipeline_id,
        "create_new_project": True,
        "lb_project_id": pipeline_name(industry, mode),
        "lb_branch_id": "production",
        "lb_database": "databricks_postgres",
        "table_name": "sensor_readings",
        "secret_scope": os.getenv("ZEROBUS_SECRET_SCOPE") or get_secret_scope(),
        "industry": industry,
    }
    return await setup_pipeline_lakebase(state, params)


async def _deploy_and_start(
    state: AppState,
    industry: str,
    mode: str,
    pipeline_id: str | None,
) -> dict:
    """Deploy (idempotently) and start the mode's streaming job.

    Zerobus writes straight to Delta via the SDK — there is no Spark job for
    that mode (``mode_config("zerobus")["uses_kafka"] is False``), so once
    provisioning above finishes the mode is already live:
    returns ``{"status": "running"}`` directly.

    Realtime/microbatch run as a ``Continuous`` Databricks Job — the same
    trigger type ``routes/deployment.py::deploy_job`` creates, which
    self-starts on create/unpause (no separate ``run_now`` call needed).
    Creating that job from nothing requires the SSS consumer notebook's
    *content*, which is generated client-side
    (``client/src/lib/notebook-template.ts``) and supplied through
    ``POST /api/cluster/deploy-job`` — outside this backend-only service's
    scope (wiring that route is a later task). So:
      - if a job is already deployed for this pipeline
        (``resources.job_id``, set by that same deploy route on a previous
        run), (re)start it idempotently by unpausing it — exactly like
        ``routes/jobs.py::restart_job``;
      - otherwise, report ``"starting"`` truthfully (no job has actually
        been created/started yet) rather than fabricating a job this
        service has no real notebook content to give.
    """
    if not mode_config(mode).get("uses_kafka"):
        # Zerobus writes straight to Delta via a *serverless, run-once producer
        # job* (``build_zerobus_task``) that only the deploy flow can create —
        # it needs the client-generated producer notebook this backend-only
        # service can't produce. So:
        #   - if that job already exists, trigger a run so data actually lands
        #     (serverless run-once → ``run_now``, not the continuous unpause
        #     path below);
        #   - otherwise report ``"starting"`` truthfully — NOT a fabricated
        #     ``"running"`` (the old behaviour, which lied and left the mode
        #     stuck showing "not deployed" because no job ever backed it).
        zerobus_job_id = await _get_job_id(state, pipeline_id)
        if not zerobus_job_id:
            return {
                "status": "starting",
                "note": (
                    "No zerobus job deployed yet — deploy it from the setup "
                    "flow first (the serverless producer job needs the "
                    "client-generated producer notebook)."
                ),
            }
        from databricks.sdk import WorkspaceClient

        w = getattr(state, "workspace_client", None) or WorkspaceClient()
        try:
            await deployment_service.sync_runtime_oauth_secret(w)
            run = w.jobs.run_now(job_id=int(zerobus_job_id))
            return {
                "status": "running",
                "job_id": zerobus_job_id,
                "run_id": getattr(run, "run_id", None),
            }
        except Exception as exc:
            logger.warning(
                "Failed to run zerobus job %s for %s: %s",
                zerobus_job_id,
                pipeline_name(industry, mode),
                exc,
            )
            return {"status": "starting", "job_id": zerobus_job_id, "error": str(exc)[:200]}

    job_id = await _get_job_id(state, pipeline_id)
    if not job_id:
        return {
            "status": "starting",
            "note": (
                "No job deployed yet for this pipeline — job creation needs "
                "the SSS consumer notebook content (generated client-side) "
                "and is wired by the deploy route, not this service."
            ),
        }

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus

    w = getattr(state, "workspace_client", None) or WorkspaceClient()
    try:
        await deployment_service.sync_runtime_oauth_secret(w)
        w.jobs.update(
            job_id=int(job_id),
            new_settings=JobSettings(continuous=Continuous(pause_status=PauseStatus.UNPAUSED)),
        )
    except Exception as exc:
        logger.warning(
            "Failed to start job %s for %s: %s",
            job_id,
            pipeline_name(industry, mode),
            exc,
        )
        return {"status": "starting", "job_id": job_id, "error": str(exc)[:200]}
    return {"status": "running", "job_id": job_id}


def _stop_job(job_id: str, state: AppState | None = None) -> dict:
    """Cancel active runs and pause the continuous job (keeps every asset).

    Mirrors ``routes/jobs.py::stop_job`` exactly — the canonical
    "stop but keep resources" pair with ``_deploy_and_start``'s unpause path.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus

    w = getattr(state, "workspace_client", None) or WorkspaceClient()
    # PAUSE FIRST, then cancel: a Continuous job's scheduler immediately starts a
    # fresh run when the active one is cancelled, so cancelling before pausing
    # races (the run comes back before the pause lands, leaving it RUNNING).
    # Pausing stops the scheduler; the subsequent cancel is then durable.
    paused_ok = False
    try:
        w.jobs.update(
            job_id=int(job_id),
            new_settings=JobSettings(continuous=Continuous(pause_status=PauseStatus.PAUSED)),
        )
        paused_ok = True
    except Exception as exc:
        logger.warning("pause failed for job %s: %s", job_id, exc)
    try:
        w.jobs.cancel_all_runs(job_id=int(job_id))
    except Exception as exc:  # non-fatal — the pause above already stops future runs
        logger.warning("cancel_all_runs failed for job %s (non-fatal): %s", job_id, exc)
    if paused_ok:
        return {"status": "stopped", "job_id": job_id}
    return {"status": "failed", "job_id": job_id, "error": "pause failed"}


async def start_mode(state: AppState, industry: str, mode: str) -> dict:
    """Provision (idempotently) and start one curated industry's mode.

    Owns the pipeline row's status transitions so the gallery/setup screens
    reflect reality: writes ``'starting'`` before any provisioning begins,
    and ``'running'`` once the job is confirmed started (per
    ``_deploy_and_start``'s result). Order: catalog+schema -> shared SP ->
    (realtime only) Lakebase branch+LakeTS -> topic (streaming modes) ->
    deploy+start job.
    """
    name = pipeline_name(industry, mode)
    cfg = mode_config(mode)
    steps: list[dict[str, Any]] = []

    pipeline_id = await _ensure_pipeline_row(state, name, industry, status="starting")

    catalog_result = await _call_maybe_async(_provision_catalog_schema, industry, mode)
    steps.append({"step": "catalog_schema", "result": catalog_result})

    sp_result = await _call_maybe_async(_resolve_app_sp, state)
    steps.append({"step": "app_sp", "result": sp_result})

    if cfg.get("uses_lakebase"):
        lakebase_result = await _call_maybe_async(
            _provision_lakebase_branch,
            state,
            industry,
            mode,
            pipeline_id,
        )
        steps.append({"step": "lakebase_branch", "result": lakebase_result})

    if cfg.get("uses_kafka"):
        steps.append({"step": "topic", "topic": topic_name(industry, mode)})

    deploy_result = await _call_maybe_async(_deploy_and_start, state, industry, mode, pipeline_id)
    steps.append({"step": "deploy_and_start", "result": deploy_result})

    if isinstance(deploy_result, dict) and deploy_result.get("status") == "running":
        final_status = "running"
        await _set_pipeline_status(state, name, "running")
    else:
        # The start attempt did NOT bring up a running job — either nothing is
        # deployed for this mode yet (e.g. zerobus with no serverless producer
        # job), or the unpause/run call failed. ``start_mode`` is synchronous,
        # so by the time we're here the mode is definitively not running; don't
        # strand the row at ``"starting"`` (that would render a perpetual
        # "provisioning…" spinner on the card that never resolves). Persist a
        # not-running status so the card reflects reality — the real per-mode
        # ``ModeView`` (``draft`` when there's no job) then drives it to the
        # honest "Set up & deploy →" / idle affordance.
        await _set_pipeline_status(state, name, _STOPPED_STATUS)
        final_status = _mode_status({"status": _STOPPED_STATUS})

    return {"pipeline": name, "status": final_status, "steps": steps}


async def stop_mode(state: AppState, industry: str, mode: str) -> dict:
    """Stop the mode's job, keeping all provisioned assets (branch kept forever).

    Sets the pipeline row's status to ``_STOPPED_STATUS``
    (``"step2_complete"``) — see that constant's docstring for why this maps
    to "idle" (via ``_mode_status``) rather than "running"/"starting".
    """
    name = pipeline_name(industry, mode)
    steps: list[dict[str, Any]] = []

    _pipeline_id, job_id = await _get_pipeline_id_and_job(state, name)

    if job_id:
        stop_result = _stop_job(job_id, state)
        steps.append({"step": "stop_job", "result": stop_result})
    else:
        steps.append(
            {"step": "stop_job", "result": {"status": "skipped", "note": "no job deployed"}}
        )

    await _set_pipeline_status(state, name, _STOPPED_STATUS)

    return {
        "pipeline": name,
        "status": _mode_status({"status": _STOPPED_STATUS}),
        "steps": steps,
    }
