"""Unit tests for the validation driver (plan Phase 4).

Exercises the driver end-to-end with **fake executors** and an in-memory repo —
no cluster, no Kafka, no real DB — covering: full run to an open deploy gate,
halt-on-failure + retry-from-failure, the ``cdf_manual`` waiting_admin park +
release, resume after a crash, and top-level data merge (``cluster_id``).
"""

from __future__ import annotations

import copy

import pytest

from iot_twin.services.validation import validation_service as vs
from iot_twin.services.validation import validation_state_machine as sm

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# In-memory repo double
# ---------------------------------------------------------------------------
class FakeRepo:
    """Minimal stand-in for ``pipeline_setup_repo`` backed by one dict."""

    def __init__(self, setup: dict) -> None:
        self.setup = setup
        self.update_calls = 0

    async def get_setup_by_industry(self, db, industry):
        if self.setup.get("industry") != industry:
            return None
        return copy.deepcopy(self.setup)

    async def update_setup(self, db, setup_id, **fields):
        self.update_calls += 1
        self.setup.update(fields)
        return copy.deepcopy(self.setup)


@pytest.fixture()
def repo(monkeypatch):
    """Patch the service's repo with an in-memory double; return it."""
    setup = {
        "id": "setup-1",
        "industry": "pharmaceutical",
        "enabled_modes": ["microbatch"],
        "validation_state": {},
        "warehouse_id": "wh-1",
        "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9098",
    }
    fake = FakeRepo(setup)
    monkeypatch.setattr(vs.pipeline_setup_repo, "get_setup_by_industry", fake.get_setup_by_industry)
    monkeypatch.setattr(vs.pipeline_setup_repo, "update_setup", fake.update_setup)
    return fake


# ---------------------------------------------------------------------------
# Fake executors
# ---------------------------------------------------------------------------
def _pass(**data):
    async def _fn(ctx):
        return vs.StepResult(sm.PASSED, data=data)

    return _fn


def _fail(msg):
    async def _fn(ctx):
        return vs.StepResult(sm.FAILED, error=msg)

    return _fn


async def _wait_admin(ctx):
    return vs.StepResult(sm.WAITING_ADMIN)


def _all_pass_executors():
    # Bare-name keyed, mirroring DEFAULT_EXECUTORS.
    names = [
        "provision_cluster",
        "source_connectivity",
        "topics_created",
        "schema_conformance",
        "partitions_precreate",
        "write_test",
        "cdf_manual",
        "cdf_verify",
        "read_back",
        "dbsql_read",
        "metrics_check",
        "cleanup",
        "create_table",
        "auth_probe",
        "produce_test",
    ]
    return {n: _pass() for n in names}


# ---------------------------------------------------------------------------
# Full run → open gate
# ---------------------------------------------------------------------------
async def test_full_run_opens_gate(repo):
    execs = _all_pass_executors()
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert sm.is_gate_open(state) is True
    # Persisted state matches.
    assert sm.is_gate_open(repo.setup["validation_state"]) is True
    # Terminal steps ran too (no-op passes since not in exec map → default pass).
    assert state["steps"]["done"]["status"] == sm.PASSED


async def test_start_missing_setup_raises(repo):
    with pytest.raises(vs.ValidationError):
        await vs.start_validation(None, "unknown-industry", executors={})


# ---------------------------------------------------------------------------
# Halt on failure + retry from failure point
# ---------------------------------------------------------------------------
async def test_run_halts_on_failure(repo):
    execs = _all_pass_executors()
    execs["source_connectivity"] = _fail("dns down")
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert state["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert state["steps"]["source_connectivity"]["error"] == "dns down"
    # Downstream never ran.
    assert state["steps"]["topics_created"]["status"] == sm.PENDING
    assert sm.is_gate_open(state) is False


async def test_run_records_timeout_type_when_message_empty_c7238(repo):
    execs = _all_pass_executors()

    async def boom(_ctx):
        raise TimeoutError()

    execs["source_connectivity"] = boom
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert state["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert state["steps"]["source_connectivity"]["error"] == "TimeoutError"


async def test_retry_from_failure_resumes(repo):
    execs = _all_pass_executors()
    execs["source_connectivity"] = _fail("dns down")
    await vs.start_validation(None, "pharmaceutical", executors=execs)

    # Fix the backend cause → executor now passes; retry that step.
    execs["source_connectivity"] = _pass()
    state = await vs.retry_step(None, "pharmaceutical", "source_connectivity", executors=execs)
    assert sm.is_gate_open(state) is True


async def test_retry_unknown_step_raises(repo):
    await vs.start_validation(None, "pharmaceutical", executors=_all_pass_executors())
    with pytest.raises(vs.ValidationError):
        await vs.retry_step(None, "pharmaceutical", "realtime:write_test", executors={})


# ---------------------------------------------------------------------------
# cdf_manual park + release (realtime lane)
# ---------------------------------------------------------------------------
@pytest.fixture()
def rt_repo(monkeypatch):
    setup = {
        "id": "setup-rt",
        "industry": "pharmaceutical",
        "enabled_modes": ["realtime"],
        "validation_state": {},
        "warehouse_id": "wh-1",
        "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9098",
    }
    fake = FakeRepo(setup)
    monkeypatch.setattr(vs.pipeline_setup_repo, "get_setup_by_industry", fake.get_setup_by_industry)
    monkeypatch.setattr(vs.pipeline_setup_repo, "update_setup", fake.update_setup)
    return fake


async def test_realtime_parks_on_cdf_manual(rt_repo):
    execs = _all_pass_executors()
    execs["cdf_manual"] = _wait_admin
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert state["steps"]["realtime:cdf_manual"]["status"] == sm.WAITING_ADMIN
    # Parked — cdf_verify and downstream have not run.
    assert state["steps"]["realtime:cdf_verify"]["status"] == sm.PENDING
    assert sm.next_runnable_step(state) is None


async def test_cdf_enabled_releases_and_completes(rt_repo):
    execs = _all_pass_executors()
    execs["cdf_manual"] = _wait_admin
    await vs.start_validation(None, "pharmaceutical", executors=execs)

    # Admin enables managed sync → release the park (cdf_manual now passes).
    state = await vs.set_cdf_enabled(None, "pharmaceutical", executors=execs)
    assert state["steps"]["realtime:cdf_manual"]["status"] == sm.PASSED
    assert sm.is_gate_open(state) is True


# ---------------------------------------------------------------------------
# Resume after interruption
# ---------------------------------------------------------------------------
async def test_resume_continues_from_persisted_state(repo):
    execs = _all_pass_executors()
    execs["topics_created"] = _fail("kafka perms")
    await vs.start_validation(None, "pharmaceutical", executors=execs)
    assert repo.setup["validation_state"]["steps"]["topics_created"]["status"] == sm.FAILED

    # Simulate a reload: resume rehydrates the persisted state. Without a retry
    # the failed step is the next runnable, so a fixed executor lets it proceed.
    execs["topics_created"] = _pass()
    state = await vs.resume_validation(None, "pharmaceutical", executors=execs)
    assert sm.is_gate_open(state) is True


# ---------------------------------------------------------------------------
# cluster_id / run_id merge
# ---------------------------------------------------------------------------
async def test_cluster_id_merged_to_top_level(repo):
    execs = _all_pass_executors()
    execs["provision_cluster"] = _pass(cluster_id="0705-abc")

    async def _rec_run(ctx):
        return vs.StepResult(sm.PASSED, data={"run_id": "run-xyz"})

    execs["write_test"] = _rec_run
    ws = _FakeWs(_FakeClusters(state_value="RUNNING"))
    state = await vs.start_validation(None, "pharmaceutical", executors=execs, ws=ws)

    assert state["cluster_id"] == "0705-abc"
    assert state["run_ids"]["microbatch:write_test"] == "run-xyz"


# ---------------------------------------------------------------------------
# can_deploy gate
# ---------------------------------------------------------------------------
async def test_can_deploy_reflects_gate(repo):
    execs = _all_pass_executors()
    execs["metrics_check"] = _fail("no metrics")
    await vs.start_validation(None, "pharmaceutical", executors=execs)

    gate = await vs.can_deploy(None, "pharmaceutical")
    assert gate["can_deploy"] is False
    assert "microbatch:metrics_check" in gate["outstanding"]
    # Per-mode verdicts are exposed so the frontend can gate Deploy per lane.
    assert gate["modes"]["microbatch"]["can_deploy"] is False
    assert "microbatch:metrics_check" in gate["modes"]["microbatch"]["outstanding"]

    # A never-started setup still reports a closed gate with full outstanding list.
    repo.setup["validation_state"] = {}
    gate2 = await vs.can_deploy(None, "pharmaceutical")
    assert gate2["can_deploy"] is False
    assert "provision_cluster" in gate2["outstanding"]


# ---------------------------------------------------------------------------
# get_state enrichment — cluster_state / current_step / phase / updated_at
# (spec §1.12 — live progress for the validate/state poll)
# ---------------------------------------------------------------------------
class _FakeClusterState:
    """Stand-in for the SDK ``ClusterState`` enum (has a ``.value``)."""

    def __init__(self, value):
        self.value = value


class _FakeClusterInfo:
    def __init__(self, state):
        self.state = state


class _FakeClusters:
    """Records the cluster_id asked for; returns a canned state (or raises)."""

    def __init__(self, *, state_value=None, raises=False):
        self._state_value = state_value
        self._raises = raises
        self.asked = None

    def get(self, cluster_id=None):
        self.asked = cluster_id
        if self._raises:
            raise RuntimeError("cluster gone")
        return _FakeClusterInfo(_FakeClusterState(self._state_value))


class _FakeWs:
    def __init__(self, clusters):
        self.clusters = clusters


def _started_setup(**extra):
    """A setup whose validation_state has a real (mid-run) sequence."""
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(
        state, "provision_cluster", sm.PASSED, updated_at="2026-07-06T00:00:00+00:00"
    )
    state = sm.set_step(
        state, "source_connectivity", sm.RUNNING, updated_at="2026-07-06T00:01:00+00:00"
    )
    state["cluster_id"] = "0705-abc"
    return {
        "id": "setup-1",
        "industry": "pharmaceutical",
        "enabled_modes": ["microbatch"],
        "validation_state": state,
        **extra,
    }


def _repo_with(monkeypatch, setup):
    fake = FakeRepo(setup)
    monkeypatch.setattr(vs.pipeline_setup_repo, "get_setup_by_industry", fake.get_setup_by_industry)
    monkeypatch.setattr(vs.pipeline_setup_repo, "update_setup", fake.update_setup)
    return fake


async def test_get_state_surfaces_cluster_state(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    clusters = _FakeClusters(state_value="RUNNING")
    out = await vs.get_state(None, "pharmaceutical", ws=_FakeWs(clusters))
    assert clusters.asked == "0705-abc"
    assert out["cluster_state"] == "RUNNING"
    assert out["cluster_id"] == "0705-abc"


async def test_get_state_current_step_and_phase(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(
        None, "pharmaceutical", ws=_FakeWs(_FakeClusters(state_value="RUNNING"))
    )
    # source_connectivity is the running (first non-passed) step.
    assert out["current_step"] == "source_connectivity"
    # Cluster RUNNING + past provision_cluster → validating.
    assert out["phase"] == "validating"


async def test_get_state_phase_provisioning_when_cluster_pending(monkeypatch):
    setup = _started_setup()
    # Roll back to the provision_cluster step still running.
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.RUNNING)
    state["cluster_id"] = "0705-abc"
    setup["validation_state"] = state
    _repo_with(monkeypatch, setup)
    out = await vs.get_state(
        None, "pharmaceutical", ws=_FakeWs(_FakeClusters(state_value="PENDING"))
    )
    assert out["phase"] == "provisioning_cluster"
    assert out["cluster_state"] == "PENDING"


async def test_get_state_preserves_per_step_updated_at(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(
        None, "pharmaceutical", ws=_FakeWs(_FakeClusters(state_value="RUNNING"))
    )
    steps = out["steps"]
    assert steps["provision_cluster"]["updated_at"] == "2026-07-06T00:00:00+00:00"
    assert steps["source_connectivity"]["updated_at"] == "2026-07-06T00:01:00+00:00"
    # Existing fields kept intact (backwards-compatible additive shape).
    assert out["sequence"] == sm.build_sequence(["microbatch"])
    assert "run_ids" in out


async def test_get_state_null_cluster_state_when_no_cluster(monkeypatch):
    setup = _started_setup()
    setup["validation_state"]["cluster_id"] = None
    _repo_with(monkeypatch, setup)
    clusters = _FakeClusters(state_value="RUNNING")
    out = await vs.get_state(None, "pharmaceutical", ws=_FakeWs(clusters))
    assert out["cluster_state"] is None
    assert out["cluster_id"] is None
    assert clusters.asked is None  # no lookup attempted


async def test_get_state_unknown_when_lookup_fails(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(None, "pharmaceutical", ws=_FakeWs(_FakeClusters(raises=True)))
    assert out["cluster_state"] == "UNKNOWN"


async def test_get_state_default_workspace_client_lookup_is_graceful(monkeypatch):
    # No ws injected → default client path; a failing client yields UNKNOWN,
    # never errors the endpoint.
    _repo_with(monkeypatch, _started_setup())
    monkeypatch.setattr(vs, "_workspace_client", lambda: _FakeWs(_FakeClusters(raises=True)))
    out = await vs.get_state(None, "pharmaceutical")
    assert out["cluster_state"] == "UNKNOWN"


async def test_get_state_none_when_no_state(monkeypatch):
    _repo_with(
        monkeypatch,
        {
            "id": "s",
            "industry": "pharmaceutical",
            "enabled_modes": ["microbatch"],
            "validation_state": {},
        },
    )
    assert await vs.get_state(None, "pharmaceutical") is None


async def test_get_state_none_when_no_setup(monkeypatch):
    _repo_with(monkeypatch, {"id": "s", "industry": "other", "validation_state": {}})
    assert await vs.get_state(None, "pharmaceutical") is None


async def test_get_state_fails_running_step_when_cluster_terminated_c7239(monkeypatch):
    """Poll must settle a stuck run when the interactive cluster is gone [C7239][defect]."""
    repo = _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(
        None,
        "pharmaceutical",
        ws=_FakeWs(_FakeClusters(state_value="TERMINATED")),
    )
    assert out["cluster_state"] == "TERMINATED"
    assert out["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert "TERMINATED" in (out["steps"]["source_connectivity"]["error"] or "")
    assert repo.setup["validation_state"]["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert out["phase"] == "failed"


async def test_get_state_preserves_original_failure_after_autoterminate_c7239(monkeypatch):
    """A real step error must not be replaced by cluster-gone after idle autoterminate [C7239][regression]."""
    setup = _started_setup()
    setup["validation_state"] = sm.set_step(
        setup["validation_state"],
        "source_connectivity",
        sm.FAILED,
        error="dns down",
        updated_at="2026-07-06T00:01:00+00:00",
    )
    repo = _repo_with(monkeypatch, setup)
    out = await vs.get_state(
        None,
        "pharmaceutical",
        ws=_FakeWs(_FakeClusters(state_value="TERMINATED")),
    )
    assert out["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert out["steps"]["source_connectivity"]["error"] == "dns down"
    assert repo.update_calls == 0


async def test_get_state_fills_empty_failure_when_cluster_terminated_c7239(monkeypatch):
    """Blank TimeoutError() reasons get a cluster-gone message on poll [C7239][defect]."""
    setup = _started_setup()
    setup["validation_state"] = sm.set_step(
        setup["validation_state"],
        "source_connectivity",
        sm.FAILED,
        error="",
        updated_at="2026-07-06T00:01:00+00:00",
    )
    repo = _repo_with(monkeypatch, setup)
    out = await vs.get_state(
        None,
        "pharmaceutical",
        ws=_FakeWs(_FakeClusters(state_value="TERMINATED")),
    )
    assert "TERMINATED" in (out["steps"]["source_connectivity"]["error"] or "")
    assert repo.setup["validation_state"]["steps"]["source_connectivity"]["error"]
    assert repo.update_calls >= 1


async def test_get_state_leaves_passed_run_when_cluster_autoterminate_c7239(monkeypatch):
    """90-min autoterminate after a green run must not rewrite passed steps [C7239][regression]."""
    state = sm.initial_state(["microbatch"])
    for key in sm.validation_step_keys(state):
        state = sm.set_step(state, key, sm.PASSED)
    state["cluster_id"] = "0705-abc"
    setup = _started_setup()
    setup["validation_state"] = state
    repo = _repo_with(monkeypatch, setup)
    out = await vs.get_state(
        None,
        "pharmaceutical",
        ws=_FakeWs(_FakeClusters(state_value="TERMINATED")),
    )
    assert out["cluster_state"] == "TERMINATED"
    assert sm.is_gate_open(out) is True
    assert repo.setup["validation_state"]["steps"]["source_connectivity"]["status"] == sm.PASSED


async def test_run_machine_does_not_advance_on_terminated_cluster_c7239(repo):
    """Driver must not keep executing Lakebase steps after autoterminate [C7239][defect]."""
    state = sm.initial_state(["realtime"])
    for key in sm.validation_step_keys(state):
        if key in ("realtime:read_back", "realtime:dbsql_read"):
            continue
        state = sm.set_step(state, key, sm.PASSED)
    state = sm.mark_running(state, "realtime:read_back")
    state["cluster_id"] = "1002-082401-3jzzquj4"
    repo.setup["enabled_modes"] = ["realtime"]
    repo.setup["validation_state"] = state

    ran = []

    async def track(name):
        async def _fn(_ctx):
            ran.append(name)
            return vs.StepResult(sm.PASSED)

        return _fn

    execs = _all_pass_executors()
    execs["read_back"] = await track("read_back")
    execs["dbsql_read"] = await track("dbsql_read")
    ws = _FakeWs(_FakeClusters(state_value="TERMINATED"))
    out = await vs.run_machine(None, repo.setup, executors=execs, ws=ws)
    assert ran == []
    assert out["steps"]["realtime:read_back"]["status"] == sm.FAILED
    assert "TERMINATED" in (out["steps"]["realtime:read_back"]["error"] or "")
    assert out["steps"]["realtime:dbsql_read"]["status"] == sm.PENDING


async def test_run_machine_still_provisions_when_prior_cluster_is_dead_c7239(repo):
    """A fresh run may re-provision even if a stale cluster_id is TERMINATED [C7239][smoke]."""
    execs = _all_pass_executors()
    execs["provision_cluster"] = _pass(cluster_id="new-cluster")
    repo.setup["validation_state"] = {
        **sm.initial_state(["microbatch"]),
        "cluster_id": "stale-dead",
    }
    ws = _FakeWs(_FakeClusters(state_value="TERMINATED"))
    state = await vs.start_validation(None, "pharmaceutical", executors=execs, ws=ws)
    # After provision_cluster the fake ws still reports TERMINATED, so the
    # driver must halt rather than pretend later steps can use the dead cluster.
    assert state["steps"]["provision_cluster"]["status"] == sm.PASSED
    assert state["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert "TERMINATED" in (state["steps"]["source_connectivity"]["error"] or "")


# ---------------------------------------------------------------------------
# prepare_start readiness guard (spec §1.10 — never 500; 4xx on incomplete draft)
# ---------------------------------------------------------------------------
def _ready_setup(**extra):
    """A setup that has every input an interactive validation run needs."""
    base = {
        "id": "setup-ready",
        "industry": "automotive",
        "enabled_modes": ["realtime", "microbatch", "zerobus"],
        "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9098",
        "warehouse_id": "wh-1",
        "validation_state": {},
    }
    base.update(extra)
    return base


async def test_incomplete_setup_error_is_validation_error():
    # The route relies on the subclass relationship (catch 400 before 404).
    assert issubclass(vs.IncompleteSetupError, vs.ValidationError)


async def test_prepare_start_valid_returns_initial_state(monkeypatch):
    fake = _repo_with(monkeypatch, _ready_setup())
    out = await vs.prepare_start(None, "automotive")
    state = out["validation_state"]
    # A fresh, fully-pending state was built and persisted synchronously.
    assert state["sequence"] == sm.build_sequence(["realtime", "microbatch", "zerobus"])
    assert state["steps"]["provision_cluster"]["status"] == sm.PENDING
    assert fake.setup["validation_state"]["sequence"]  # persisted


async def test_prepare_start_only_adds_new_mode_when_enabled_modes_changed(monkeypatch):
    previous = sm.initial_state(["realtime", "microbatch"])
    for key in previous["sequence"]:
        previous = sm.set_step(previous, key, sm.PASSED)
    _repo_with(
        monkeypatch,
        _ready_setup(
            enabled_modes=["realtime", "microbatch", "zerobus"],
            validation_state=previous,
        ),
    )

    out = await vs.prepare_start(None, "automotive")
    state = out["validation_state"]

    assert state["steps"]["realtime:write_test"]["status"] == sm.PASSED
    assert state["steps"]["microbatch:write_test"]["status"] == sm.PASSED
    assert state["steps"]["zerobus:create_table"]["status"] == sm.PENDING


async def test_prepare_start_same_modes_is_explicit_full_rerun(monkeypatch):
    previous = sm.initial_state(["zerobus"])
    for key in previous["sequence"]:
        previous = sm.set_step(previous, key, sm.PASSED)
    _repo_with(
        monkeypatch,
        _ready_setup(enabled_modes=["zerobus"], validation_state=previous),
    )

    out = await vs.prepare_start(None, "automotive")

    assert all(
        record["status"] == sm.PENDING for record in out["validation_state"]["steps"].values()
    )


async def test_prepare_start_resumes_reconciled_state_with_new_pending_lane(monkeypatch):
    previous = sm.initial_state(["realtime"])
    for key in previous["sequence"]:
        previous = sm.set_step(previous, key, sm.PASSED)
    reconciled = sm.reconcile_modes(previous, ["realtime", "zerobus"])
    _repo_with(
        monkeypatch,
        _ready_setup(
            enabled_modes=["realtime", "zerobus"],
            validation_state=reconciled,
        ),
    )

    out = await vs.prepare_start(None, "automotive")

    assert out["validation_state"]["steps"]["realtime:write_test"]["status"] == sm.PASSED
    assert out["validation_state"]["steps"]["zerobus:create_table"]["status"] == sm.PENDING


def _manufacturing_stuck_state(cluster_id="1013-114619-s4mcy68g"):
    state = sm.initial_state(["realtime"])
    for key in sm.validation_step_keys(state):
        if key in (
            "realtime:read_back",
            "realtime:dbsql_read",
            "realtime:metrics_check",
            "realtime:cleanup",
        ):
            continue
        state = sm.set_step(state, key, sm.PASSED, updated_at="2026-10-13T11:55:00+00:00")
    state = sm.set_step(state, "realtime:read_back", sm.FAILED, error="no validation row")
    state["cluster_id"] = cluster_id
    return state


async def test_prepare_start_reprovisions_when_cluster_terminated_c7240(monkeypatch):
    """Re-run after autoterminate must not resume at read_back on a dead cluster [C7240][defect]."""
    fake = _repo_with(
        monkeypatch,
        _ready_setup(enabled_modes=["realtime"], validation_state=_manufacturing_stuck_state()),
    )
    out = await vs.prepare_start(
        None, "automotive", ws=_FakeWs(_FakeClusters(state_value="TERMINATED"))
    )
    state = out["validation_state"]
    assert state["steps"]["provision_cluster"]["status"] == sm.PENDING
    assert state["steps"]["realtime:write_test"]["status"] == sm.PENDING
    assert state["steps"]["realtime:read_back"]["status"] == sm.PENDING
    assert sm.next_runnable_step(state) == "provision_cluster"
    assert fake.setup["validation_state"]["steps"]["provision_cluster"]["status"] == sm.PENDING


async def test_prepare_start_resumes_failed_step_when_cluster_running_c7240(monkeypatch):
    """A live cluster keeps the resume-from-failure behaviour [C7240][regression]."""
    _repo_with(
        monkeypatch,
        _ready_setup(enabled_modes=["realtime"], validation_state=_manufacturing_stuck_state()),
    )
    out = await vs.prepare_start(
        None, "automotive", ws=_FakeWs(_FakeClusters(state_value="RUNNING"))
    )
    state = out["validation_state"]
    assert state["steps"]["provision_cluster"]["status"] == sm.PASSED
    assert state["steps"]["realtime:write_test"]["status"] == sm.PASSED
    assert sm.next_runnable_step(state) == "realtime:read_back"


async def test_prepare_start_keeps_green_lane_when_reprovisioning_c7240(monkeypatch):
    """Dead cluster resets only the unfinished lane; a fully green lane stays green [C7240][regression]."""
    previous = sm.initial_state(["realtime", "microbatch"])
    for key in sm.validation_step_keys(previous):
        previous = sm.set_step(previous, key, sm.PASSED)
    previous = sm.set_step(previous, "realtime:read_back", sm.FAILED, error="boom")
    previous["cluster_id"] = "dead"
    _repo_with(
        monkeypatch,
        _ready_setup(enabled_modes=["realtime", "microbatch"], validation_state=previous),
    )
    out = await vs.prepare_start(
        None, "automotive", ws=_FakeWs(_FakeClusters(state_value="TERMINATED"))
    )
    state = out["validation_state"]
    assert state["steps"]["provision_cluster"]["status"] == sm.PENDING
    assert state["steps"]["microbatch:write_test"]["status"] == sm.PASSED
    assert state["steps"]["realtime:write_test"]["status"] == sm.PENDING


async def test_run_machine_halts_retried_failed_step_on_dead_cluster_c7240(repo):
    """Driver must not re-execute a failed step against a dead cluster [C7240][defect]."""
    repo.setup["enabled_modes"] = ["realtime"]
    repo.setup["validation_state"] = _manufacturing_stuck_state()
    ran = []

    async def read_back(_ctx):
        ran.append("read_back")
        return vs.StepResult(sm.FAILED, error="no validation row")

    execs = _all_pass_executors()
    execs["read_back"] = read_back
    out = await vs.run_machine(
        None,
        repo.setup,
        executors=execs,
        ws=_FakeWs(_FakeClusters(state_value="TERMINATED")),
    )
    assert ran == []
    assert "TERMINATED" in out["steps"]["realtime:read_back"]["error"]


async def test_prepare_start_incomplete_missing_warehouse_raises(monkeypatch):
    _repo_with(monkeypatch, _ready_setup(warehouse_id=None))
    with pytest.raises(vs.IncompleteSetupError) as ei:
        await vs.prepare_start(None, "automotive")
    assert "warehouse_id" in str(ei.value)
    assert "complete the source config first" in str(ei.value)


async def test_prepare_start_incomplete_missing_bootstrap_raises(monkeypatch):
    _repo_with(monkeypatch, _ready_setup(bootstrap_servers="   "))
    with pytest.raises(vs.IncompleteSetupError) as ei:
        await vs.prepare_start(None, "automotive")
    assert "bootstrap_servers" in str(ei.value)


async def test_prepare_start_incomplete_no_enabled_modes_raises(monkeypatch):
    _repo_with(monkeypatch, _ready_setup(enabled_modes=[]))
    with pytest.raises(vs.IncompleteSetupError) as ei:
        await vs.prepare_start(None, "automotive")
    assert "enabled_modes" in str(ei.value)


async def test_prepare_start_zerobus_only_needs_no_bootstrap(monkeypatch):
    # Zerobus is serverless (no Kafka), so a missing broker list is fine.
    _repo_with(monkeypatch, _ready_setup(enabled_modes=["zerobus"], bootstrap_servers=None))
    out = await vs.prepare_start(None, "automotive")
    assert out["validation_state"]["sequence"] == sm.build_sequence(["zerobus"])


async def test_prepare_start_missing_setup_raises_plain_validation_error(monkeypatch):
    # No setup at all → base ValidationError (route maps to 404), NOT incomplete.
    _repo_with(monkeypatch, {"id": "s", "industry": "other", "validation_state": {}})
    with pytest.raises(vs.ValidationError) as ei:
        await vs.prepare_start(None, "automotive")
    assert not isinstance(ei.value, vs.IncompleteSetupError)
