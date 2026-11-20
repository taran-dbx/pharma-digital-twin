"""Pure derivation of the admin pipeline API contract (status / health / startability).

The admin surface (``GET /api/admin/pipelines`` and the per-pipeline health
endpoint) must report *real* per-mode state — never a fabricated "idle/running"
or a hardcoded count.  This module contains **only** side-effect-free logic that
turns the persisted rows (``pipeline_setup`` + its ``pipeline_deployment`` rows +
``validation_state``) plus a *job-running lookup* (the caller resolves live job
state via the Jobs API) into the frontend contract:

    {
      "setup_id", "industry",
      "status",   # draft | starting | running | degraded | stopped
      "health",   # draft | healthy | unhealthy  (pipeline rollup)
      "modes": [{"mode", "status", "health", "startable"}, ...],
      "startable", "ready",
      "validation_passed",  # every selected mode's validation gate is open
    }

Derivation rules (spec §1.10, §1.12):

* **status** (per mode) — ``draft`` is the DEFAULT when there is no real data:
    - ``draft``    — the mode is not deployed (no deployment row / no ``job_id``).
    - ``running``  — the mode is deployed AND its Jobs-API run is RUNNING.
    - ``stopped``  — the mode is deployed but not running.
* **health** (per mode) — ``healthy | unhealthy | draft``:
    - ``draft``     — not deployed.
    - ``healthy``   — deployed AND the mode's validation gate is open AND (for
      realtime) managed CDF is confirmed.
    - ``unhealthy`` — deployed but the gate/CDF is incomplete (this is the case
      for every current realtime pipeline, since CDF is a manual step nobody has
      done yet).
* **startable** (per mode) — ``True`` ONLY when the mode is deployed AND its
  validation gate is open AND (for realtime) CDF is confirmed.  End users only
  see startable pipelines; admins see all.

Keeping this pure makes the whole contract exhaustively unit-testable with the
Jobs API / repos mocked, and guarantees a partial / empty row degrades to
``draft`` rather than raising.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from iot_twin.services.validation import validation_state_machine as sm

# --- Per-mode RUNTIME status -------------------------------------------------
# ``draft``    — the mode is not deployed (no job).
# ``starting`` — deployed and the run is still PENDING/STARTING.
# ``running``  — deployed, run RUNNING, and all tasks are healthy. Sink
#                freshness is reported separately as ``flowing``; a delayed or
#                unavailable probe must not leave a healthy job "starting".
# ``degraded`` — deployed, the run is up but a task crashed (e.g. the producer
#                died, so data isn't actually flowing). RUNNING alone hides this.
# ``stopped``  — deployed but not actively running (paused/idle).
STATUS_DRAFT = "draft"
STATUS_STARTING = "starting"
STATUS_RUNNING = "running"
STATUS_DEGRADED = "degraded"
STATUS_STOPPED = "stopped"

# --- Health ------------------------------------------------------------------
HEALTH_DRAFT = "draft"
HEALTH_HEALTHY = "healthy"
HEALTH_UNHEALTHY = "unhealthy"
# ``unknown`` is the live-check rollup verdict when nothing failed but nothing
# affirmatively passed either (e.g. every Tier-1 check was ``skip``ped).
HEALTH_UNKNOWN = "unknown"

# --- Tier-1 (API/DB) live health-check verdicts ------------------------------
# The two-tier health service (``services/health_checks.py``) emits one
# ``{name, tier, status, detail}`` per check.  These are the shared vocabularies
# so the pure rollup below and the checks never drift.
CHECK_PASS, CHECK_FAIL, CHECK_SKIP = ("pass", "fail", "skip")
TIER_API = "api"  # runs from the app backend, no interactive cluster
TIER_E2E = "e2e"  # opt-in interactive harness (spins the cluster)

# ``pipeline_deployment.status`` values that (together with a ``job_id``) mean the
# mode's job was actually created in the workspace.
_NOT_DEPLOYED_STATUSES = frozenset({"pending", "deploying", "failed"})


def is_deployed(deployment: Mapping | None) -> bool:
    """True when a mode's job actually exists (has a ``job_id`` and is past deploy).

    A ``job_id`` is written only after the workspace Job is created, so its
    presence is the authoritative "deployed" signal; a row still ``pending`` /
    ``deploying`` / ``failed`` is not deployed even if it somehow carries an id.
    """
    if not deployment:
        return False
    if not deployment.get("job_id"):
        return False
    return deployment.get("status") not in _NOT_DEPLOYED_STATUSES


def cdf_confirmed(setup: Mapping | None, deployment: Mapping | None, mode: str) -> bool:
    """True when managed CDF is confirmed for ``mode`` (only realtime needs it).

    Non-realtime modes never require CDF.  For realtime, CDF is confirmed when
    the validation ``realtime:cdf_verify`` step passed OR the deployment row's
    ``cdf_enabled`` flag is set.  Since CDF is a **manual** admin step that is not
    done for current pipelines, this returns ``False`` for them.
    """
    if mode != "realtime":
        return True
    vstate = (setup or {}).get("validation_state") or {}
    if sm._key_status(vstate, sm.step_key("realtime", "cdf_verify")) == sm.PASSED:
        return True
    return bool(deployment and deployment.get("cdf_enabled"))


def gate_open(setup: Mapping | None, mode: str) -> bool:
    """True when the mode's validation deploy-gate is open (all required steps passed).

    Delegates to the single source of truth in ``validation_state_machine``.
    An unknown mode or empty state degrades to ``False`` (not open).
    """
    vstate = (setup or {}).get("validation_state") or {}
    try:
        return sm.is_mode_gate_open(vstate, mode)
    except ValueError:
        return False


def _runtime(job_state: Any) -> tuple[bool, bool, bool, bool, str | None]:
    """Normalize runtime into ``(running, degraded, starting, restartable, detail)``.

    Accepts either a plain ``bool`` (back-compat: ``True`` == running) or the
    richer ``{running, degraded, starting, detail}`` mapping the Jobs-API lookup
    now returns. ``starting`` marks a run whose coarse life-cycle is
    PENDING/STARTING (not yet RUNNING).
    """
    if isinstance(job_state, Mapping):
        return (
            bool(job_state.get("running")),
            bool(job_state.get("degraded")),
            bool(job_state.get("starting")),
            bool(job_state.get("restartable")),
            job_state.get("detail"),
        )
    return bool(job_state), False, False, False, None


def mode_status(deployment: Mapping | None, job_state: Any, flowing: bool = False) -> str:
    """Return ``draft`` / ``starting`` / ``running`` / ``degraded`` / ``stopped``.

    ``draft`` (not deployed) is the default:

    * a crashed task → ``degraded`` (checked first; RUNNING alone hides it);
    * a run still coming up (PENDING/STARTING) → ``starting``;
    * a healthy RUNNING run → ``running``;
    * deployed but the run is down (paused/idle) → ``stopped``.

    ``flowing`` remains in the signature for the response's independent sink
    freshness signal. It deliberately does not gate the job lifecycle status:
    freshness probes can lag or fail even after Databricks reports the job and
    all of its tasks as RUNNING.
    """
    if not is_deployed(deployment):
        return STATUS_DRAFT
    running, degraded, starting, _restartable, _detail = _runtime(job_state)
    if degraded:
        return STATUS_DEGRADED
    if starting:
        return STATUS_STARTING
    if running:
        return STATUS_RUNNING
    return STATUS_STOPPED


def mode_startable(setup: Mapping | None, deployment: Mapping | None, mode: str) -> bool:
    """True only when the mode is deployed, its gate is open, and CDF is confirmed."""
    return (
        is_deployed(deployment)
        and gate_open(setup, mode)
        and cdf_confirmed(setup, deployment, mode)
    )


def mode_health(setup: Mapping | None, deployment: Mapping | None, mode: str) -> str:
    """Return ``draft`` / ``healthy`` / ``unhealthy`` for a mode."""
    if not is_deployed(deployment):
        return HEALTH_DRAFT
    if gate_open(setup, mode) and cdf_confirmed(setup, deployment, mode):
        return HEALTH_HEALTHY
    return HEALTH_UNHEALTHY


def build_mode_view(
    setup: Mapping | None,
    deployment: Mapping | None,
    mode: str,
    job_state: Any,
    flowing: bool = False,
) -> dict:
    """Return the per-mode contract for the admin pipeline view.

    ``{mode, status, health, startable, runtime_detail, tasks, deployed, flowing}``.
    ``flowing`` is the resolved data-freshness signal (recent rows in the mode's
    sink), computed in the route layer and threaded in so this module stays pure.
    ``runtime_detail`` names why a mode is ``degraded`` (e.g. the crashed task);
    ``tasks`` carries the per-task live states for the admin pipeline view.
    """
    _running, degraded, _starting, restartable, detail = _runtime(job_state)
    tasks = (
        list(job_state.get("tasks"))
        if isinstance(job_state, Mapping) and job_state.get("tasks")
        else []
    )
    deployed = is_deployed(deployment)
    status = mode_status(deployment, job_state, flowing)
    deploy_health = mode_health(setup, deployment, mode)
    health = HEALTH_UNHEALTHY if degraded or restartable else deploy_health
    return {
        "mode": mode,
        "status": status,
        "health": health,
        "startable": mode_startable(setup, deployment, mode),
        "runtime_detail": detail,
        "restartable": restartable,
        "tasks": tasks,
        "deployed": deployed,
        "flowing": bool(flowing) if deployed else False,
    }


# --- Rollups -----------------------------------------------------------------
def rollup_status(mode_views: list[dict]) -> str:
    """Return the strongest actual runtime state across selected modes.

    Undeployed (draft) and intentionally stopped modes do not degrade a healthy
    running lane. ``degraded`` is reserved for a deployed mode whose runtime is
    explicitly degraded.
    """
    statuses = {m["status"] for m in mode_views}
    if STATUS_DEGRADED in statuses:
        return STATUS_DEGRADED
    if STATUS_RUNNING in statuses:
        return STATUS_RUNNING
    if STATUS_STARTING in statuses:
        return STATUS_STARTING
    if STATUS_STOPPED in statuses:
        return STATUS_STOPPED
    return STATUS_DRAFT


def rollup_health(mode_views: list[dict]) -> str:
    """Ignore undeployed drafts; report unhealthy only for an unhealthy mode."""
    healths = {m["health"] for m in mode_views}
    if HEALTH_UNHEALTHY in healths:
        return HEALTH_UNHEALTHY
    if HEALTH_HEALTHY in healths:
        return HEALTH_HEALTHY
    return HEALTH_DRAFT


def _ordered_modes(setup: Mapping) -> list[str]:
    """Return the modes to report, ordered by :data:`sm.MODE_ORDER`.

    The setup's current ``enabled_modes`` is authoritative. A stale deployment
    row for a removed mode must not make the pipeline unhealthy or appear in
    the active-mode UI while asynchronous job cleanup finishes.
    """
    known = set(sm.MODE_ORDER)
    enabled = {m for m in setup.get("enabled_modes") or [] if m in known}
    return [m for m in sm.MODE_ORDER if m in enabled]


def build_pipeline_view(
    setup: Mapping,
    deployments: Iterable[Mapping],
    job_running: Mapping[str, Any] | None = None,
    flowing: Mapping[str, bool] | None = None,
) -> dict:
    """Return the full pipeline contract object for one setup.

    ``job_running`` maps ``job_id`` (as ``str``) → the Jobs-API runtime, either a
    plain ``bool`` (RUNNING) or the richer
    ``{running, degraded, starting, detail, tasks}`` mapping.  ``flowing`` maps
    ``mode`` → whether fresh rows are landing in that mode's sink; the caller
    resolves both via the Jobs API / a freshness probe and passes them in so this
    stays pure. A missing freshness entry reports ``flowing=False`` but does not
    override a healthy RUNNING job's lifecycle status.
    ``validation_passed`` intentionally covers every selected mode, including
    undeployed drafts, because it represents completion of the full setup rather
    than runtime health of the currently deployed subset.
    """
    deployments = list(deployments or [])
    job_running = job_running or {}
    flowing = flowing or {}
    dep_by_mode = {d.get("mode"): d for d in deployments}

    mode_views: list[dict] = []
    for mode in _ordered_modes(setup):
        dep = dep_by_mode.get(mode)
        job_id = dep.get("job_id") if dep else None
        job_state = job_running.get(str(job_id)) if job_id else None
        mode_views.append(build_mode_view(setup, dep, mode, job_state, bool(flowing.get(mode))))

    startable = any(m["startable"] for m in mode_views)
    validation_passed = bool(mode_views) and all(
        gate_open(setup, mode_view["mode"]) for mode_view in mode_views
    )
    return {
        "setup_id": setup.get("id"),
        "industry": setup.get("industry"),
        "status": rollup_status(mode_views),
        "health": rollup_health(mode_views),
        "modes": mode_views,
        "startable": startable,
        "ready": startable,
        "validation_passed": validation_passed,
    }


def rollup_api_health(checks: Iterable[Mapping]) -> str:
    """Roll up the Tier-1 (``tier="api"``) check results into a health verdict.

    Only ``api`` checks count toward the pipeline's live health — the opt-in
    ``e2e`` interactive test (§1.16) never gates it.  ``unhealthy`` if any Tier-1
    check ``fail``ed; ``healthy`` if at least one ``pass``ed and none failed;
    ``unknown`` when everything was ``skip``ped (e.g. no warehouse configured).
    """
    api = [c for c in (checks or []) if c.get("tier") == TIER_API]
    statuses = {c.get("status") for c in api}
    if CHECK_FAIL in statuses:
        return HEALTH_UNHEALTHY
    if CHECK_PASS in statuses:
        return HEALTH_HEALTHY
    return HEALTH_UNKNOWN


def collect_job_ids(deployments: Iterable[Mapping]) -> list[str]:
    """Return the distinct ``job_id`` strings across deployed modes (for a Jobs-API sweep)."""
    ids: list[str] = []
    for dep in deployments or []:
        if is_deployed(dep):
            jid = str(dep.get("job_id"))
            if jid not in ids:
                ids.append(jid)
    return ids
