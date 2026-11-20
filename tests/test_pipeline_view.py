"""Unit tests for services/pipelines/pipeline_view.py — the admin pipeline contract derivation.

Pure logic, no I/O: the Jobs-API result is passed in as a ``job_running`` map so
these tests fully cover the status / health / startability rules with no mocks.
"""

from __future__ import annotations

from iot_twin.services.pipelines import pipeline_view as pv
from iot_twin.services.validation import validation_state_machine as sm


# --- helpers -----------------------------------------------------------------
def _all_passed_state(mode: str) -> dict:
    """A validation_state with every required step for ``mode`` marked passed."""
    keys = sm.mode_required_keys(mode)
    return {"steps": {k: {"status": sm.PASSED} for k in keys}}


def _deployed(mode: str = "realtime", **extra) -> dict:
    d = {"mode": mode, "status": "deployed", "job_id": "123"}
    d.update(extra)
    return d


# --- is_deployed -------------------------------------------------------------
class TestIsDeployed:
    def test_none_is_not_deployed(self):
        assert pv.is_deployed(None) is False

    def test_no_job_id_is_not_deployed(self):
        assert pv.is_deployed({"mode": "realtime", "status": "deployed"}) is False

    def test_pending_status_is_not_deployed(self):
        assert pv.is_deployed({"job_id": "1", "status": "pending"}) is False

    def test_deployed_with_job_id(self):
        assert pv.is_deployed({"job_id": "1", "status": "deployed"}) is True


# --- status ------------------------------------------------------------------
class TestModeStatus:
    def test_draft_is_default_when_no_deployment(self):
        assert pv.mode_status(None, True) == pv.STATUS_DRAFT

    def test_draft_when_not_deployed(self):
        assert pv.mode_status({"job_id": None}, True) == pv.STATUS_DRAFT

    def test_running_when_deployed_running_and_flowing_c7264(self):
        """[C7264][regression] Running jobs retain their positive flow signal."""
        # RUNNING job + data flowing → running, with freshness also reported.
        assert pv.mode_status(_deployed(), True, flowing=True) == pv.STATUS_RUNNING

    def test_running_job_is_not_left_starting_by_freshness_c7264(self):
        """[C7264][defect][regression] A healthy RUNNING job leaves starting."""
        assert pv.mode_status(_deployed(), True, flowing=False) == pv.STATUS_RUNNING

    def test_starting_when_run_pending(self):
        # Coarse run life-cycle PENDING/STARTING → starting, regardless of flow.
        rt = {"running": False, "starting": True, "degraded": False, "detail": None}
        assert pv.mode_status(_deployed(), rt, flowing=False) == pv.STATUS_STARTING
        assert pv.mode_status(_deployed(), rt, flowing=True) == pv.STATUS_STARTING

    def test_stopped_when_deployed_but_not_running(self):
        assert pv.mode_status(_deployed(), False) == pv.STATUS_STOPPED

    def test_degraded_when_deployed_and_a_task_crashed(self):
        # Job run is up but a task died (e.g. producer) → degraded, NOT running.
        # Degraded wins even if the freshness probe somehow reports flowing.
        rt = {"running": True, "degraded": True, "detail": "data_producer: driver stopped"}
        assert pv.mode_status(_deployed(), rt, flowing=True) == pv.STATUS_DEGRADED

    def test_running_with_healthy_richer_runtime_and_flowing(self):
        rt = {"running": True, "degraded": False, "detail": None}
        assert pv.mode_status(_deployed(), rt, flowing=True) == pv.STATUS_RUNNING

    def test_running_with_healthy_richer_runtime_not_flowing_c7264(self):
        """[C7264][regression] Rich RUNNING state ignores a stale sink probe."""
        rt = {"running": True, "degraded": False, "detail": None}
        assert pv.mode_status(_deployed(), rt, flowing=False) == pv.STATUS_RUNNING

    def test_build_mode_view_surfaces_runtime_detail_and_tasks(self):
        rt = {
            "running": True,
            "degraded": True,
            "detail": "data_producer: boom",
            "restartable": True,
            "tasks": [{"task": "data_producer", "life_cycle": "INTERNAL_ERROR"}],
        }
        v = pv.build_mode_view(None, _deployed(), "microbatch", rt)
        assert v["status"] == pv.STATUS_DEGRADED
        assert v["runtime_detail"] == "data_producer: boom"
        assert v["restartable"] is True
        assert v["tasks"] and v["tasks"][0]["task"] == "data_producer"

    def test_build_mode_view_carries_flowing_and_deployed(self):
        # A deployed+RUNNING+flowing mode is running and exposes flowing/deployed.
        v = pv.build_mode_view(None, _deployed(), "microbatch", True, flowing=True)
        assert v["status"] == pv.STATUS_RUNNING
        assert v["deployed"] is True
        assert v["flowing"] is True

    def test_build_mode_view_not_deployed_never_flowing(self):
        # A non-deployed mode is draft and never reports flowing (even if passed).
        v = pv.build_mode_view(None, None, "microbatch", True, flowing=True)
        assert v["status"] == pv.STATUS_DRAFT
        assert v["deployed"] is False
        assert v["flowing"] is False

    def test_rollup_degraded_beats_running(self):
        views = [
            {"status": pv.STATUS_RUNNING},
            {"status": pv.STATUS_DEGRADED},
            {"status": pv.STATUS_DRAFT},
        ]
        assert pv.rollup_status(views) == pv.STATUS_DEGRADED

    def test_rollup_running_with_undeployed_modes_is_running(self):
        views = [{"status": pv.STATUS_DRAFT}, {"status": pv.STATUS_RUNNING}]
        assert pv.rollup_status(views) == pv.STATUS_RUNNING

    def test_rollup_starting_beats_stopped_and_draft(self):
        views = [
            {"status": pv.STATUS_DRAFT},
            {"status": pv.STATUS_STOPPED},
            {"status": pv.STATUS_STARTING},
        ]
        assert pv.rollup_status(views) == pv.STATUS_STARTING


# --- cdf / gate --------------------------------------------------------------
class TestCdfConfirmed:
    def test_non_realtime_always_confirmed(self):
        assert pv.cdf_confirmed({}, None, "microbatch") is True
        assert pv.cdf_confirmed({}, None, "zerobus") is True

    def test_realtime_not_confirmed_by_default(self):
        assert pv.cdf_confirmed({}, _deployed(), "realtime") is False

    def test_realtime_confirmed_via_cdf_verify_passed(self):
        state = {"steps": {"realtime:cdf_verify": {"status": sm.PASSED}}}
        assert pv.cdf_confirmed({"validation_state": state}, None, "realtime") is True

    def test_realtime_confirmed_via_deployment_flag(self):
        dep = _deployed(cdf_enabled=True)
        assert pv.cdf_confirmed({}, dep, "realtime") is True


class TestGateOpen:
    def test_empty_state_not_open(self):
        assert pv.gate_open({}, "realtime") is False

    def test_all_passed_is_open(self):
        setup = {"validation_state": _all_passed_state("microbatch")}
        assert pv.gate_open(setup, "microbatch") is True


# --- startable + health ------------------------------------------------------
class TestStartableAndHealth:
    def test_not_deployed_is_draft_health_and_not_startable(self):
        setup = {"validation_state": _all_passed_state("microbatch")}
        assert pv.mode_health(setup, None, "microbatch") == pv.HEALTH_DRAFT
        assert pv.mode_startable(setup, None, "microbatch") is False

    def test_realtime_cdf_incomplete_is_unhealthy_and_not_startable(self):
        # Everything passed EXCEPT cdf (manual step): gate not open -> unhealthy.
        keys = [k for k in sm.mode_required_keys("realtime") if "cdf" not in k]
        setup = {"validation_state": {"steps": {k: {"status": sm.PASSED} for k in keys}}}
        dep = _deployed("realtime")
        assert pv.mode_health(setup, dep, "realtime") == pv.HEALTH_UNHEALTHY
        assert pv.mode_startable(setup, dep, "realtime") is False

    def test_microbatch_fully_validated_and_deployed_is_healthy_startable(self):
        setup = {"validation_state": _all_passed_state("microbatch")}
        dep = _deployed("microbatch")
        assert pv.mode_health(setup, dep, "microbatch") == pv.HEALTH_HEALTHY
        assert pv.mode_startable(setup, dep, "microbatch") is True

    def test_realtime_full_gate_open_is_healthy_startable(self):
        # Gate open for realtime already implies cdf_verify passed.
        setup = {"validation_state": _all_passed_state("realtime")}
        dep = _deployed("realtime")
        assert pv.mode_startable(setup, dep, "realtime") is True
        assert pv.mode_health(setup, dep, "realtime") == pv.HEALTH_HEALTHY

    def test_deployed_but_gate_not_open_is_unhealthy(self):
        dep = _deployed("microbatch")
        assert pv.mode_health({}, dep, "microbatch") == pv.HEALTH_UNHEALTHY
        assert pv.mode_startable({}, dep, "microbatch") is False


# --- rollups + build_pipeline_view -------------------------------------------
class TestBuildPipelineView:
    def test_no_deployments_is_all_draft(self):
        setup = {"id": "s1", "industry": "pharma", "enabled_modes": ["realtime"]}
        view = pv.build_pipeline_view(setup, [], {})
        assert view["status"] == pv.STATUS_DRAFT
        assert view["health"] == pv.HEALTH_DRAFT
        assert view["startable"] is False
        assert view["ready"] is False
        assert view["modes"][0]["mode"] == "realtime"
        assert view["modes"][0]["status"] == pv.STATUS_DRAFT

    def test_current_realtime_pipeline_unhealthy_not_startable(self):
        # Deployed realtime with CDF not done -> unhealthy + not startable (the
        # real-world state of current pipelines).
        setup = {
            "id": "s1",
            "industry": "pharma",
            "enabled_modes": ["realtime"],
            "validation_state": {
                "steps": {
                    k: {"status": sm.PASSED}
                    for k in sm.mode_required_keys("realtime")
                    if "cdf" not in k
                }
            },
        }
        deployments = [_deployed("realtime")]
        view = pv.build_pipeline_view(setup, deployments, {"123": False})
        assert view["status"] == pv.STATUS_STOPPED
        assert view["health"] == pv.HEALTH_UNHEALTHY
        assert view["startable"] is False

    def test_running_job_preserves_flowing_signal_c7264(self):
        """[C7264][regression] Lifecycle and sink freshness stay independent."""
        setup = {
            "id": "s1",
            "industry": "pharma",
            "enabled_modes": ["microbatch"],
            "validation_state": _all_passed_state("microbatch"),
        }
        deployments = [_deployed("microbatch")]
        view = pv.build_pipeline_view(setup, deployments, {"123": True}, {"microbatch": True})
        assert view["status"] == pv.STATUS_RUNNING
        assert view["health"] == pv.HEALTH_HEALTHY
        assert view["startable"] is True
        assert view["modes"][0]["flowing"] is True

    def test_failed_task_makes_valid_pipeline_unhealthy_c7258(self):
        """[C7258][defect][regression] Runtime task failures gate health."""
        setup = {
            "id": "s1",
            "industry": "pharma",
            "enabled_modes": ["microbatch"],
            "validation_state": _all_passed_state("microbatch"),
        }
        runtime = {
            "123": {
                "running": False,
                "degraded": False,
                "restartable": True,
                "detail": "data_producer: Driver stopped unexpectedly",
            }
        }

        view = pv.build_pipeline_view(setup, [_deployed("microbatch")], runtime)

        assert view["status"] == pv.STATUS_STOPPED
        assert view["health"] == pv.HEALTH_UNHEALTHY
        assert view["modes"][0]["restartable"] is True

    def test_undeployed_mode_does_not_degrade_running_mode(self):
        setup = {
            "id": "s1",
            "industry": "pharma",
            "enabled_modes": ["microbatch", "zerobus"],
            "validation_state": _all_passed_state("microbatch"),
        }
        deployments = [_deployed("microbatch")]
        view = pv.build_pipeline_view(
            setup,
            deployments,
            {"123": True},
            {"microbatch": True},
        )
        assert view["status"] == pv.STATUS_RUNNING
        assert view["health"] == pv.HEALTH_HEALTHY
        assert view["startable"] is True
        assert view["validation_passed"] is False

    def test_running_job_without_flow_reports_freshness_separately_c7264(self):
        """[C7264][defect][regression] Missing freshness does not block RUNNING."""
        # Job lifecycle settles at RUNNING while sink freshness stays observable.
        setup = {
            "id": "s1",
            "industry": "pharma",
            "enabled_modes": ["microbatch"],
            "validation_state": _all_passed_state("microbatch"),
        }
        deployments = [_deployed("microbatch")]
        view = pv.build_pipeline_view(setup, deployments, {"123": True}, {})
        assert view["status"] == pv.STATUS_RUNNING
        assert view["modes"][0]["status"] == pv.STATUS_RUNNING
        assert view["modes"][0]["health"] == pv.HEALTH_HEALTHY
        assert view["modes"][0]["flowing"] is False

    def test_pending_run_is_starting(self):
        setup = {
            "id": "s1",
            "industry": "pharma",
            "enabled_modes": ["microbatch"],
            "validation_state": _all_passed_state("microbatch"),
        }
        deployments = [_deployed("microbatch")]
        job = {"123": {"running": False, "starting": True, "degraded": False}}
        view = pv.build_pipeline_view(setup, deployments, job, {})
        assert view["status"] == pv.STATUS_STARTING
        assert view["health"] == pv.HEALTH_HEALTHY

    def test_only_selected_modes_contribute_to_pipeline_health(self):
        setup = {"id": "s1", "industry": "x", "enabled_modes": ["zerobus"]}
        deployments = [{"mode": "realtime", "status": "deployed", "job_id": "9"}]
        view = pv.build_pipeline_view(setup, deployments, {})
        assert [m["mode"] for m in view["modes"]] == ["zerobus"]
        assert view["health"] == pv.HEALTH_DRAFT

    def test_rollup_status_reports_any_healthy_running_mode(self):
        assert pv.rollup_status([{"status": "stopped"}, {"status": "running"}]) == pv.STATUS_RUNNING
        assert pv.rollup_status([{"status": "draft"}, {"status": "stopped"}]) == pv.STATUS_STOPPED
        assert pv.rollup_status([{"status": "draft"}]) == pv.STATUS_DRAFT

    def test_rollup_health_unhealthy_wins(self):
        assert (
            pv.rollup_health([{"health": "healthy"}, {"health": "unhealthy"}])
            == pv.HEALTH_UNHEALTHY
        )
        assert pv.rollup_health([{"health": "healthy"}]) == pv.HEALTH_HEALTHY
        assert pv.rollup_health([{"health": "draft"}]) == pv.HEALTH_DRAFT
        assert pv.rollup_health([{"health": "healthy"}, {"health": "draft"}]) == pv.HEALTH_HEALTHY


class TestRollupApiHealth:
    def _c(self, tier, status):
        return {"name": "x", "tier": tier, "status": status, "detail": ""}

    def test_unhealthy_when_any_api_check_fails(self):
        checks = [self._c("api", "pass"), self._c("api", "fail")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_UNHEALTHY

    def test_healthy_when_pass_and_none_failed(self):
        checks = [self._c("api", "pass"), self._c("api", "skip")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_HEALTHY

    def test_unknown_when_all_skipped(self):
        checks = [self._c("api", "skip"), self._c("api", "skip")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_UNKNOWN

    def test_e2e_checks_never_gate_health(self):
        # A failing Tier-2 check must NOT make the pipeline unhealthy.
        checks = [self._c("api", "pass"), self._c("e2e", "fail")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_HEALTHY


class TestCollectJobIds:
    def test_collects_only_deployed_distinct(self):
        deployments = [
            _deployed("realtime", job_id="1"),
            {"mode": "microbatch", "status": "pending", "job_id": None},
            _deployed("zerobus", job_id="1"),  # duplicate id
        ]
        assert pv.collect_job_ids(deployments) == ["1"]
