"""Pure, persisted validation state machine (spec §1.10, §1.12; plan Phase 4).

This module contains **only** side-effect-free logic that operates on a plain
``dict`` — the value stored in ``pipeline_setup.validation_state`` (JSONB).  It
knows nothing about clusters, Kafka, or the database; the effectful step
executors live in ``services/validation/validation_steps.py`` and the driver that ties
them together lives in ``services/validation/validation_service.py``.  Keeping the state
machine pure makes the transition / retry-from-failure / resume / deploy-gate
logic exhaustively unit-testable with no mocks.

State shape::

    {
      "version": 1,
      "enabled_modes": ["realtime", "microbatch", "zerobus"],
      "sequence": ["provision_cluster", ..., "realtime:write_test", ..., "done"],
      "steps": {"<key>": {"status", "error", "updated_at"}, ...},
      "cluster_id": "0705-...",           # set by provision_cluster
      "run_ids": {"<key>": "run-123"},    # per-step bounded-run ids
    }

Step flow (spec §1.12)::

    provision_cluster → source_connectivity → topics_created → schema_conformance
      ├─ realtime:  partitions_precreate → write_test → cdf_manual(waiting_admin)
      │             → cdf_verify → read_back → dbsql_read → metrics_check → cleanup
      ├─ microbatch: write_test → dbsql_read → metrics_check → cleanup
      └─ zerobus:   create_table → auth_probe → produce_test → dbsql_read → cleanup
    → show_final_notebook → done

Common steps are stored under their bare name (``"provision_cluster"``); per-mode
steps under a ``"<mode>:<step>"`` key (``"realtime:write_test"``) so the same
step name in different lanes stays distinct.

All mutating helpers return a **new** dict (deep-copied) — the input is never
mutated, per the immutability rule.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable

__all__ = [
    "PENDING",
    "RUNNING",
    "PASSED",
    "FAILED",
    "WAITING_ADMIN",
    "STEP_STATUSES",
    "COMMON_STEPS",
    "MODE_STEPS",
    "MODE_ORDER",
    "TERMINAL_STEPS",
    "WAITING_ADMIN_STEPS",
    "CLEANUP_STEP",
    "SEP",
    "step_key",
    "bare_step",
    "mode_of",
    "is_terminal",
    "build_sequence",
    "initial_state",
    "reconcile_modes",
    "get_step",
    "set_step",
    "mark_running",
    "next_runnable_step",
    "retry_from",
    "mark_cdf_enabled",
    "reset_legacy_cdf_park",
    "current_step",
    "derive_phase",
    "PROVISION_CLUSTER_STEP",
    "PROVISIONING_CLUSTER_STATES",
    "PHASE_PROVISIONING",
    "PHASE_VALIDATING",
    "PHASE_WAITING_ADMIN",
    "PHASE_PASSED",
    "PHASE_FAILED",
    "validation_step_keys",
    "outstanding_steps",
    "is_gate_open",
    "data_purged",
    "gate_status",
    "mode_required_keys",
    "mode_outstanding_steps",
    "is_mode_gate_open",
    "mode_data_purged",
    "mode_gate_status",
    "multi_mode_gate_status",
]

# --- Step statuses ---------------------------------------------------------
PENDING = "pending"
RUNNING = "running"
PASSED = "passed"
FAILED = "failed"
WAITING_ADMIN = "waiting_admin"

STEP_STATUSES = (PENDING, RUNNING, PASSED, FAILED, WAITING_ADMIN)

# --- Step catalogue --------------------------------------------------------
COMMON_STEPS: tuple[str, ...] = (
    "provision_cluster",
    "source_connectivity",
    "topics_created",
    "schema_conformance",
)

MODE_STEPS: dict[str, tuple[str, ...]] = {
    "realtime": (
        "partitions_precreate",
        "write_test",
        "cdf_manual",
        "cdf_verify",
        "read_back",
        "dbsql_read",
        "metrics_check",
        "cleanup",
    ),
    "microbatch": (
        "write_test",
        "dbsql_read",
        "metrics_check",
        "cleanup",
    ),
    "zerobus": (
        "create_table",
        "auth_probe",
        "produce_test",
        "dbsql_read",
        "cleanup",
    ),
}

# Deterministic ordering of the per-mode lanes in the flat sequence.
MODE_ORDER: tuple[str, ...] = ("realtime", "microbatch", "zerobus")

# Terminal steps after every lane — not counted toward the deploy gate.
TERMINAL_STEPS: tuple[str, ...] = ("show_final_notebook", "done")

# No current step requires a manual admin action. ``WAITING_ADMIN`` remains a
# valid persisted status so older validation states can be migrated safely.
WAITING_ADMIN_STEPS: frozenset[str] = frozenset()

# The data-purge step (its success proves test data was cleaned up — §1.13).
CLEANUP_STEP = "cleanup"

# The first common step — provisions the interactive validation cluster.  Used
# to derive the ``provisioning_cluster`` UI phase.
PROVISION_CLUSTER_STEP = "provision_cluster"

# Databricks Clusters-API states that mean the cluster is still coming up (not
# yet usable) — the UI treats these as the ``provisioning_cluster`` phase.
PROVISIONING_CLUSTER_STATES: frozenset[str] = frozenset({"PENDING", "RESTARTING", "RESIZING"})

# Coarse UI phases surfaced by :func:`derive_phase` (spec §1.12; used by the
# ``validate/state`` poll so a running validation never "looks frozen").
PHASE_PROVISIONING = "provisioning_cluster"
PHASE_VALIDATING = "validating"
PHASE_WAITING_ADMIN = "waiting_admin"
PHASE_PASSED = "passed"
PHASE_FAILED = "failed"

SEP = ":"


# --- Key helpers -----------------------------------------------------------
def step_key(mode: str | None, step: str) -> str:
    """Return the flat state key for ``step`` in ``mode`` (``None`` = common)."""
    return f"{mode}{SEP}{step}" if mode else step


def bare_step(key: str) -> str:
    """Return the bare step name for a flat key (strips any ``<mode>:`` prefix)."""
    return key.split(SEP, 1)[1] if SEP in key else key


def mode_of(key: str) -> str | None:
    """Return the mode for a flat key, or ``None`` for a common/terminal step."""
    return key.split(SEP, 1)[0] if SEP in key else None


def is_terminal(key: str) -> bool:
    """True when ``key`` is a terminal (non-gated) step."""
    return key in TERMINAL_STEPS


# --- Construction ----------------------------------------------------------
def _blank_step() -> dict:
    return {"status": PENDING, "error": None, "updated_at": None}


def build_sequence(enabled_modes: Iterable[str]) -> list[str]:
    """Return the ordered flat step sequence for the enabled modes.

    Common steps (``provision_cluster``, ``source_connectivity``,
    ``topics_created``, ``schema_conformance``) are Kafka / interactive-cluster
    steps — they are included **only** when ``realtime`` or ``microbatch`` is
    enabled.  A zerobus-only setup skips them entirely (zerobus is serverless
    and uses no interactive cluster).

    Lane steps follow in :data:`MODE_ORDER` order; terminal steps are last.
    Unknown / disabled modes are silently ignored.
    """
    enabled = set(enabled_modes or ())
    needs_cluster = bool(enabled & {"realtime", "microbatch"})
    seq: list[str] = list(COMMON_STEPS) if needs_cluster else []
    for mode in MODE_ORDER:
        if mode in enabled:
            seq += [step_key(mode, s) for s in MODE_STEPS[mode]]
    seq += list(TERMINAL_STEPS)
    return seq


def initial_state(enabled_modes: Iterable[str]) -> dict:
    """Return a fresh validation state with every step ``pending``."""
    seq = build_sequence(enabled_modes)
    return {
        "version": 1,
        "enabled_modes": [m for m in MODE_ORDER if m in set(enabled_modes or ())],
        "sequence": seq,
        "steps": {key: _blank_step() for key in seq},
        "cluster_id": None,
        "run_ids": {},
    }


def reconcile_modes(state: dict, enabled_modes: Iterable[str]) -> dict:
    """Rebuild a state for a changed mode selection without re-running retained lanes.

    Existing records are preserved for common steps and mode lanes that remain
    enabled. Newly introduced steps start pending, removed lanes disappear, and
    terminal presentation steps restart so the updated configuration gets a
    fresh final review. This is the incremental add/remove-mode path; an
    explicit re-run with an unchanged selection still uses :func:`initial_state`.
    """
    reconciled = initial_state(enabled_modes)
    previous_steps = state.get("steps") or {}
    for key in reconciled["sequence"]:
        if key in TERMINAL_STEPS:
            continue
        if key in previous_steps:
            reconciled["steps"][key] = copy.deepcopy(previous_steps[key])

    if any(mode in reconciled["enabled_modes"] for mode in ("realtime", "microbatch")):
        reconciled["cluster_id"] = state.get("cluster_id")

    previous_run_ids = state.get("run_ids") or {}
    reconciled["run_ids"] = {
        key: run_id for key, run_id in previous_run_ids.items() if key in reconciled["steps"]
    }
    return reconciled


# --- Accessors -------------------------------------------------------------
def get_step(state: dict, key: str) -> dict:
    """Return the step record for ``key`` (raises ``KeyError`` if absent)."""
    return state["steps"][key]


# --- Transitions (all return a new state) ----------------------------------
def set_step(
    state: dict,
    key: str,
    status: str,
    *,
    error: str | None = None,
    updated_at: str | None = None,
) -> dict:
    """Return a copy of ``state`` with ``key`` set to ``status`` (+ error/ts)."""
    if key not in state.get("steps", {}):
        raise KeyError(f"unknown step: {key!r}")
    if status not in STEP_STATUSES:
        raise ValueError(f"invalid status: {status!r}")
    new = copy.deepcopy(state)
    new["steps"][key] = {"status": status, "error": error, "updated_at": updated_at}
    return new


def mark_running(state: dict, key: str, *, updated_at: str | None = None) -> dict:
    """Return a copy with ``key`` marked ``running``."""
    return set_step(state, key, RUNNING, updated_at=updated_at)


def next_runnable_step(state: dict) -> str | None:
    """Return the next step to execute, or ``None`` when blocked/complete.

    Walks the sequence: skips ``passed`` steps; returns the first
    ``pending`` / ``failed`` / ``running`` step (running = interrupted, safe to
    re-run on resume).

    Lane-aware parking (honours the documented per-mode deploy design — a lane
    parked on a manual action must not block *independent* lanes): when a lane
    step is ``waiting_admin`` that whole lane is skipped (its downstream steps
    depend on the parked step) while sibling lanes keep advancing.  So a
    realtime lane parked at ``cdf_manual`` no longer stops the micro-batch /
    zerobus lanes from running to green.  Common (mode-less) steps gate every
    lane and are therefore returned before any lane step.  Terminal steps run
    only once nothing is parked (a parked lane means the run is not complete).
    """
    parked_lanes: set[str | None] = set()
    terminal_candidate: str | None = None
    for key in state["sequence"]:
        status = state["steps"][key]["status"]
        if status == PASSED:
            continue
        if is_terminal(key):
            if terminal_candidate is None:
                terminal_candidate = key
            continue
        mode = mode_of(key)
        if status == WAITING_ADMIN:
            # Park this lane; skip its remaining (downstream) steps but keep
            # scanning sibling lanes.
            parked_lanes.add(mode)
            continue
        if mode in parked_lanes:
            # A downstream step of an already-parked lane — cannot run yet.
            continue
        return key
    # Every non-terminal step is passed or parked.  Only advance to a terminal
    # step when no lane is parked (an open park means the run isn't finished).
    if terminal_candidate is not None and not parked_lanes:
        return terminal_candidate
    return None


def retry_from(state: dict, key: str) -> dict:
    """Reset ``key`` and every downstream step to ``pending`` (retry-from-failure).

    Upstream ``passed`` steps are left untouched, so a retry re-runs only the
    failed step and its dependents (spec §1.12).
    """
    seq = state["sequence"]
    if key not in seq:
        raise KeyError(f"unknown step: {key!r}")
    idx = seq.index(key)
    new = copy.deepcopy(state)
    for downstream in seq[idx:]:
        new["steps"][downstream] = _blank_step()
    return new


def mark_cdf_enabled(state: dict, mode: str = "realtime") -> dict:
    """Mark the ``<mode>:cdf_manual`` step ``passed`` (admin clicked "enabled").

    Used by the ``/validate/cdf-enabled`` endpoint to release the
    ``waiting_admin`` park so the driver can run ``cdf_verify`` downstream.
    """
    key = step_key(mode, "cdf_manual")
    if key not in state.get("steps", {}):
        raise KeyError(f"no cdf_manual step for mode {mode!r}")
    return set_step(state, key, PASSED)


def reset_legacy_cdf_park(state: dict) -> dict:
    """Make a persisted pre-automation CDF park runnable through the API."""
    key = step_key("realtime", "cdf_manual")
    record = (state.get("steps") or {}).get(key)
    if not record or record.get("status") != WAITING_ADMIN:
        return state
    return set_step(state, key, PENDING)


# --- UI progress derivation (spec §1.12) -----------------------------------
def current_step(state: dict) -> str | None:
    """Return the step the run is *currently on*, or ``None`` when complete.

    Walks the sequence and returns the first step that is not yet ``passed`` —
    the ``running`` step during a live run, the ``waiting_admin`` step when
    parked on a manual action, the ``failed`` step when halted, or the next
    ``pending`` step when idle between runs.  Unlike :func:`next_runnable_step`
    this also surfaces a ``waiting_admin`` park (which is *not* runnable) so the
    UI can highlight what it is waiting for.  Returns ``None`` when every step
    has passed.
    """
    steps = state.get("steps", {})
    for key in state.get("sequence", []):
        if (steps.get(key) or {}).get("status", PENDING) != PASSED:
            return key
    return None


def derive_phase(state: dict, *, cluster_state: str | None = None) -> str:
    """Return a coarse UI phase for the whole run.

    One of :data:`PHASE_PROVISIONING` / :data:`PHASE_VALIDATING` /
    :data:`PHASE_WAITING_ADMIN` / :data:`PHASE_PASSED` / :data:`PHASE_FAILED`,
    derived from the per-step statuses, the current step, and the live
    ``cluster_state`` (from the Clusters API).  Precedence: a failed step wins
    (``failed``); then an open deploy gate (``passed``); then a manual park
    (``waiting_admin``); then cluster bring-up (``provisioning_cluster``) — while
    the run is on ``provision_cluster`` or the cluster is still coming up; else
    ``validating``.
    """
    steps = state.get("steps", {})
    if any((steps.get(k) or {}).get("status") == FAILED for k in validation_step_keys(state)):
        return PHASE_FAILED
    if is_gate_open(state):
        return PHASE_PASSED

    cur = current_step(state)
    cur_status = (steps.get(cur) or {}).get("status") if cur else None
    if cur_status == WAITING_ADMIN:
        return PHASE_WAITING_ADMIN
    if cur is not None and bare_step(cur) == PROVISION_CLUSTER_STEP:
        return PHASE_PROVISIONING
    if cluster_state in PROVISIONING_CLUSTER_STATES:
        return PHASE_PROVISIONING
    return PHASE_VALIDATING


# --- Deploy gate (spec §1.12) ----------------------------------------------
def validation_step_keys(state: dict) -> list[str]:
    """Return every non-terminal step key (the gate-relevant steps)."""
    return [k for k in state["sequence"] if not is_terminal(k)]


def outstanding_steps(state: dict) -> list[str]:
    """Return non-terminal steps that are not yet ``passed``."""
    return [k for k in validation_step_keys(state) if state["steps"][k]["status"] != PASSED]


def is_gate_open(state: dict) -> bool:
    """True when every non-terminal step is ``passed`` (hard deploy gate).

    Because every mode lane ends in a ``cleanup`` step, an open gate implies
    test data has been purged as well.
    """
    return not outstanding_steps(state)


def data_purged(state: dict) -> bool:
    """True when every mode's ``cleanup`` step has ``passed`` (test data purged)."""
    cleanup_keys = [k for k in state["sequence"] if bare_step(k) == CLEANUP_STEP]
    return all(state["steps"][k]["status"] == PASSED for k in cleanup_keys)


def gate_status(state: dict) -> dict:
    """Return the deploy-gate verdict + what is still outstanding."""
    return {
        "can_deploy": is_gate_open(state),
        "data_purged": data_purged(state),
        "outstanding": outstanding_steps(state),
    }


# --- Per-mode deploy gate (spec §1.10, §1.12) ------------------------------
# Deploy is *per-mode*: an admin can deploy the micro-batch job the moment the
# micro-batch lane is green, even if the realtime lane is still running.  This
# is the single source of truth for the per-mode gate — the ``deployment_gate``
# module and the ``/validate/can-deploy`` route both delegate here so the two
# no longer disagree (previously one read a flat ``mb_``/``rt_``/``zb_`` state
# while the other read the global nested gate).
def _key_status(state: dict, key: str) -> str:
    """Return the status of a flat/nested step ``key`` ('pending' if absent).

    Tolerates a missing ``steps`` map and both the nested record shape
    (``{"status": ...}``) and a bare status string, so a partial / empty state
    reports every required step as outstanding rather than raising.
    """
    rec = (state or {}).get("steps")
    value = rec.get(key) if isinstance(rec, dict) else (state or {}).get(key)
    if isinstance(value, dict):
        return value.get("status", PENDING)
    return value or PENDING


def mode_required_keys(mode: str) -> list[str]:
    """Return the flat step keys a ``mode`` must pass to open its deploy gate.

    The common Kafka-cluster steps (:data:`COMMON_STEPS`) plus that mode's lane
    steps — **except** zerobus, which is non-Kafka / serverless and therefore
    requires only its own lane (spec §1.10).
    """
    if mode not in MODE_STEPS:
        raise ValueError(f"unknown mode: {mode!r}")
    common: list[str] = [] if mode == "zerobus" else list(COMMON_STEPS)
    lane = [step_key(mode, s) for s in MODE_STEPS[mode]]
    return common + lane


def mode_outstanding_steps(state: dict, mode: str) -> list[str]:
    """Return the required keys for ``mode`` that are not yet ``passed``."""
    return [k for k in mode_required_keys(mode) if _key_status(state, k) != PASSED]


def is_mode_gate_open(state: dict, mode: str) -> bool:
    """True when every required step for ``mode`` is ``passed`` (per-mode gate)."""
    return not mode_outstanding_steps(state, mode)


def mode_data_purged(state: dict, mode: str) -> bool:
    """True when the mode's ``cleanup`` step passed (its test data was purged)."""
    return _key_status(state, step_key(mode, CLEANUP_STEP)) == PASSED


def mode_gate_status(state: dict, mode: str) -> dict:
    """Return the per-mode deploy-gate verdict + what is still outstanding."""
    return {
        "can_deploy": is_mode_gate_open(state, mode),
        "data_purged": mode_data_purged(state, mode),
        "outstanding": mode_outstanding_steps(state, mode),
    }


def multi_mode_gate_status(state: dict, modes: Iterable[str]) -> dict:
    """Return per-mode gate verdicts for ``modes`` plus an enabled-set aggregate.

    ``modes`` (the setup's enabled modes) is normalised to :data:`MODE_ORDER`.
    The top-level ``can_deploy`` / ``data_purged`` / ``outstanding`` summarise the
    whole set (all enabled modes open, all purged, de-duplicated union of
    outstanding keys) so existing global consumers keep working, while ``modes``
    carries the per-mode verdicts the frontend uses to enable Deploy per lane.
    """
    ordered = [m for m in MODE_ORDER if m in set(modes or ())]
    per_mode = {m: mode_gate_status(state, m) for m in ordered}
    outstanding: list[str] = []
    for m in ordered:
        for k in per_mode[m]["outstanding"]:
            if k not in outstanding:
                outstanding.append(k)
    return {
        "can_deploy": bool(ordered) and all(g["can_deploy"] for g in per_mode.values()),
        "data_purged": bool(ordered) and all(g["data_purged"] for g in per_mode.values()),
        "outstanding": outstanding,
        "modes": per_mode,
    }
