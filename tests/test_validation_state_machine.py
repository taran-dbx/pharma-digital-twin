"""Unit tests for the pure validation state machine (plan Phase 4, spec §1.12).

Exhaustively covers sequence construction, transitions, retry-from-failure,
resume (next-runnable), the ``waiting_admin`` park, and the hard deploy gate —
no mocks required (the module is side-effect-free).
"""

from __future__ import annotations

import pytest

from iot_twin.services.validation import validation_state_machine as sm


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------
def test_step_key_roundtrip():
    assert sm.step_key(None, "provision_cluster") == "provision_cluster"
    assert sm.step_key("realtime", "write_test") == "realtime:write_test"
    assert sm.bare_step("realtime:write_test") == "write_test"
    assert sm.bare_step("provision_cluster") == "provision_cluster"
    assert sm.mode_of("realtime:write_test") == "realtime"
    assert sm.mode_of("provision_cluster") is None


# ---------------------------------------------------------------------------
# Sequence construction
# ---------------------------------------------------------------------------
def test_build_sequence_all_modes_order():
    seq = sm.build_sequence(["zerobus", "realtime", "microbatch"])
    # Common first regardless of input order.
    assert seq[:4] == list(sm.COMMON_STEPS)
    # Lanes in canonical MODE_ORDER (realtime, microbatch, zerobus).
    assert seq.index("realtime:write_test") < seq.index("microbatch:write_test")
    assert seq.index("microbatch:write_test") < seq.index("zerobus:create_table")
    # Terminal last.
    assert seq[-2:] == list(sm.TERMINAL_STEPS)


def test_build_sequence_single_mode_excludes_others():
    seq = sm.build_sequence(["microbatch"])
    assert not any(k.startswith("realtime:") for k in seq)
    assert not any(k.startswith("zerobus:") for k in seq)
    assert "microbatch:cleanup" in seq


def test_build_sequence_realtime_lane_order():
    seq = sm.build_sequence(["realtime"])
    rt = [sm.bare_step(k) for k in seq if k.startswith("realtime:")]
    assert rt == list(sm.MODE_STEPS["realtime"])


def test_build_sequence_zerobus_only_has_no_common_steps():
    """Zerobus-only setup must not include Kafka/interactive-cluster common steps."""
    seq = sm.build_sequence(["zerobus"])
    for common in sm.COMMON_STEPS:
        assert common not in seq, f"COMMON_STEP {common!r} must not appear in zerobus-only sequence"
    # The zerobus lane and terminal steps must still be present.
    assert "zerobus:create_table" in seq
    assert "zerobus:cleanup" in seq
    assert list(sm.TERMINAL_STEPS) == seq[-len(sm.TERMINAL_STEPS) :]


def test_build_sequence_realtime_and_zerobus_includes_common_steps():
    """When realtime (Kafka) is selected alongside zerobus, common steps are included."""
    seq = sm.build_sequence(["realtime", "zerobus"])
    for common in sm.COMMON_STEPS:
        assert common in seq, f"COMMON_STEP {common!r} must appear when realtime is enabled"
    # Both lanes present.
    assert any(k.startswith("realtime:") for k in seq)
    assert any(k.startswith("zerobus:") for k in seq)
    # Common steps precede any lane steps.
    first_lane_idx = next(i for i, k in enumerate(seq) if ":" in k)
    for common in sm.COMMON_STEPS:
        assert seq.index(common) < first_lane_idx


def test_initial_state_all_pending():
    state = sm.initial_state(["realtime", "microbatch"])
    assert state["enabled_modes"] == ["realtime", "microbatch"]
    assert all(s["status"] == sm.PENDING for s in state["steps"].values())
    assert state["cluster_id"] is None
    assert state["run_ids"] == {}
    assert set(state["steps"]) == set(state["sequence"])


def test_reconcile_modes_preserves_retained_mode_and_adds_new_mode_pending():
    state = sm.initial_state(["realtime", "microbatch"])
    for key in state["sequence"]:
        state = sm.set_step(state, key, sm.PASSED)

    reconciled = sm.reconcile_modes(state, ["realtime", "microbatch", "zerobus"])

    assert reconciled["enabled_modes"] == ["realtime", "microbatch", "zerobus"]
    assert reconciled["steps"]["realtime:write_test"]["status"] == sm.PASSED
    assert reconciled["steps"]["microbatch:write_test"]["status"] == sm.PASSED
    assert reconciled["steps"]["zerobus:create_table"]["status"] == sm.PENDING
    assert reconciled["steps"]["show_final_notebook"]["status"] == sm.PENDING
    assert reconciled["steps"]["done"]["status"] == sm.PENDING


def test_reconcile_modes_removes_disabled_lane_without_resetting_retained_lane():
    state = sm.initial_state(["realtime", "microbatch", "zerobus"])
    for key in state["sequence"]:
        state = sm.set_step(state, key, sm.PASSED)

    reconciled = sm.reconcile_modes(state, ["zerobus"])

    assert reconciled["enabled_modes"] == ["zerobus"]
    assert reconciled["steps"]["zerobus:produce_test"]["status"] == sm.PASSED
    assert not any(key.startswith("realtime:") for key in reconciled["steps"])
    assert not any(key.startswith("microbatch:") for key in reconciled["steps"])
    assert all(key not in reconciled["steps"] for key in sm.COMMON_STEPS)


# ---------------------------------------------------------------------------
# Transitions + immutability
# ---------------------------------------------------------------------------
def test_set_step_is_immutable():
    state = sm.initial_state(["microbatch"])
    new = sm.set_step(state, "provision_cluster", sm.PASSED)
    assert new["steps"]["provision_cluster"]["status"] == sm.PASSED
    # original untouched
    assert state["steps"]["provision_cluster"]["status"] == sm.PENDING


def test_set_step_rejects_unknown_key_and_status():
    state = sm.initial_state(["microbatch"])
    with pytest.raises(KeyError):
        sm.set_step(state, "nope", sm.PASSED)
    with pytest.raises(ValueError):
        sm.set_step(state, "provision_cluster", "banana")


# ---------------------------------------------------------------------------
# Resume — next_runnable_step
# ---------------------------------------------------------------------------
def test_next_runnable_is_first_pending():
    state = sm.initial_state(["microbatch"])
    assert sm.next_runnable_step(state) == "provision_cluster"


def test_next_runnable_skips_passed():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.PASSED)
    state = sm.set_step(state, "source_connectivity", sm.PASSED)
    assert sm.next_runnable_step(state) == "topics_created"


def test_next_runnable_returns_failed_step():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.FAILED, error="boom")
    assert sm.next_runnable_step(state) == "provision_cluster"


def test_next_runnable_blocks_on_waiting_admin():
    state = sm.initial_state(["realtime"])
    for k in (
        "provision_cluster",
        "source_connectivity",
        "topics_created",
        "schema_conformance",
        "realtime:partitions_precreate",
        "realtime:write_test",
    ):
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    # Parked — nothing auto-runs until the admin releases cdf_manual.
    assert sm.next_runnable_step(state) is None


def test_next_runnable_none_when_all_passed():
    state = sm.initial_state(["microbatch"])
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.next_runnable_step(state) is None


def test_next_runnable_parked_lane_does_not_block_sibling_lane():
    # Per-mode design: a realtime lane parked at cdf_manual must NOT block the
    # independent micro-batch lane — the driver advances to it.
    state = sm.initial_state(["realtime", "microbatch"])
    for k in (
        "provision_cluster",
        "source_connectivity",
        "topics_created",
        "schema_conformance",
        "realtime:partitions_precreate",
        "realtime:write_test",
    ):
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    # Realtime parks, but micro-batch's first lane step is now runnable.
    assert sm.next_runnable_step(state) == "microbatch:write_test"


def test_next_runnable_defers_terminal_while_a_lane_is_parked():
    # With one lane parked, all sibling lanes green, terminal steps still wait.
    state = sm.initial_state(["realtime", "microbatch"])
    for k in state["sequence"]:
        if sm.is_terminal(k) or k == "realtime:cdf_manual":
            continue
        # Downstream realtime steps stay pending (depend on the parked step).
        if sm.mode_of(k) == "realtime" and k not in (
            "realtime:partitions_precreate",
            "realtime:write_test",
        ):
            continue
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    # Micro-batch green + realtime parked → nothing runnable (terminal deferred).
    assert sm.next_runnable_step(state) is None


# ---------------------------------------------------------------------------
# Retry from failure point
# ---------------------------------------------------------------------------
def test_retry_from_resets_step_and_downstream_only():
    state = sm.initial_state(["microbatch"])
    # Pretend everything through dbsql_read passed, then a later step failed.
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "microbatch:dbsql_read", sm.FAILED, error="x")

    retried = sm.retry_from(state, "microbatch:dbsql_read")
    seq = retried["sequence"]
    idx = seq.index("microbatch:dbsql_read")
    # Upstream still passed.
    for k in seq[:idx]:
        assert retried["steps"][k]["status"] == sm.PASSED
    # Target + downstream reset to pending.
    for k in seq[idx:]:
        assert retried["steps"][k]["status"] == sm.PENDING


def test_retry_from_unknown_step_raises():
    state = sm.initial_state(["microbatch"])
    with pytest.raises(KeyError):
        sm.retry_from(state, "realtime:write_test")


def test_retry_after_fix_advances_past_failure():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.PASSED)
    state = sm.set_step(state, "source_connectivity", sm.FAILED, error="dns")
    # Retry that failed step; it and downstream go pending → runnable again.
    state = sm.retry_from(state, "source_connectivity")
    assert sm.next_runnable_step(state) == "source_connectivity"
    # Fix + pass it → next runnable advances.
    state = sm.set_step(state, "source_connectivity", sm.PASSED)
    assert sm.next_runnable_step(state) == "topics_created"


# ---------------------------------------------------------------------------
# CDF manual release
# ---------------------------------------------------------------------------
def test_mark_cdf_enabled_releases_park():
    state = sm.initial_state(["realtime"])
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    state = sm.mark_cdf_enabled(state, "realtime")
    assert state["steps"]["realtime:cdf_manual"]["status"] == sm.PASSED


def test_mark_cdf_enabled_no_realtime_lane_raises():
    state = sm.initial_state(["microbatch"])
    with pytest.raises(KeyError):
        sm.mark_cdf_enabled(state, "realtime")


def test_legacy_cdf_admin_park_becomes_api_runnable_c7209():
    """Old persisted admin parks migrate to API automation [C7209][regression]."""
    state = sm.initial_state(["realtime"])
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)

    migrated = sm.reset_legacy_cdf_park(state)

    assert migrated["steps"]["realtime:cdf_manual"]["status"] == sm.PENDING


# ---------------------------------------------------------------------------
# Hard deploy gate
# ---------------------------------------------------------------------------
def test_gate_closed_until_all_steps_pass():
    state = sm.initial_state(["realtime", "microbatch"])
    assert sm.is_gate_open(state) is False
    status = sm.gate_status(state)
    assert status["can_deploy"] is False
    assert "provision_cluster" in status["outstanding"]


def test_gate_ignores_terminal_steps():
    state = sm.initial_state(["microbatch"])
    # Pass every non-terminal step; leave terminal steps pending.
    for k in sm.validation_step_keys(state):
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.is_gate_open(state) is True
    assert sm.gate_status(state)["outstanding"] == []
    # Terminal steps still pending but the gate is open.
    assert state["steps"]["done"]["status"] == sm.PENDING


def test_gate_closed_if_cleanup_not_passed():
    state = sm.initial_state(["microbatch"])
    for k in sm.validation_step_keys(state):
        state = sm.set_step(state, k, sm.PASSED)
    # Reopen a cleanup step (data not purged) → gate closes.
    state = sm.set_step(state, "microbatch:cleanup", sm.PENDING)
    assert sm.data_purged(state) is False
    assert sm.is_gate_open(state) is False


def test_data_purged_true_when_all_cleanups_pass():
    state = sm.initial_state(["realtime", "microbatch", "zerobus"])
    cleanups = [k for k in state["sequence"] if sm.bare_step(k) == "cleanup"]
    assert len(cleanups) == 3
    for k in cleanups:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.data_purged(state) is True


# ---------------------------------------------------------------------------
# Per-mode deploy gate (single source of truth for the per-mode deploy)
# ---------------------------------------------------------------------------
def _pass_all(state: dict) -> dict:
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    return state


def test_mode_required_keys_common_plus_lane():
    keys = sm.mode_required_keys("microbatch")
    assert keys[:4] == list(sm.COMMON_STEPS)
    assert "microbatch:write_test" in keys
    assert "microbatch:cleanup" in keys
    # No other lane leaks in.
    assert not any(k.startswith("realtime:") for k in keys)


def test_mode_required_keys_zerobus_lane_only_no_kafka_common():
    keys = sm.mode_required_keys("zerobus")
    # Zerobus is non-Kafka/serverless — its gate must not require the common
    # Kafka-cluster steps (spec §1.10).
    for common in sm.COMMON_STEPS:
        assert common not in keys
    assert "zerobus:create_table" in keys
    assert "zerobus:cleanup" in keys


def test_mode_required_keys_unknown_mode_raises():
    with pytest.raises(ValueError):
        sm.mode_required_keys("bogus")


def test_is_mode_gate_open_true_on_fully_green_microbatch():
    state = _pass_all(sm.initial_state(["microbatch"]))
    assert sm.is_mode_gate_open(state, "microbatch") is True
    assert sm.mode_outstanding_steps(state, "microbatch") == []
    status = sm.mode_gate_status(state, "microbatch")
    assert status == {"can_deploy": True, "data_purged": True, "outstanding": []}


def test_is_mode_gate_open_false_with_nested_outstanding_keys():
    state = sm.initial_state(["microbatch"])
    # Pass everything except one lane step.
    for k in state["sequence"]:
        if k != "microbatch:write_test":
            state = sm.set_step(state, k, sm.PASSED)
    assert sm.is_mode_gate_open(state, "microbatch") is False
    outstanding = sm.mode_outstanding_steps(state, "microbatch")
    assert outstanding == ["microbatch:write_test"]  # nested key, not flat mb_*


def test_mode_gate_open_ignores_other_lane_status():
    # A three-mode run where only the microbatch lane is fully green.
    state = sm.initial_state(["realtime", "microbatch", "zerobus"])
    for k in list(sm.COMMON_STEPS) + [
        sm.step_key("microbatch", s) for s in sm.MODE_STEPS["microbatch"]
    ]:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.is_mode_gate_open(state, "microbatch") is True
    # Realtime lane still pending → its gate is closed.
    assert sm.is_mode_gate_open(state, "realtime") is False


def test_zerobus_gate_open_without_kafka_common_steps():
    # Only the zerobus lane passed; common Kafka steps left pending.
    state = sm.initial_state(["zerobus"])
    for s in sm.MODE_STEPS["zerobus"]:
        state = sm.set_step(state, sm.step_key("zerobus", s), sm.PASSED)
    assert sm.is_mode_gate_open(state, "zerobus") is True


def test_mode_gate_tolerates_empty_and_missing_state():
    assert sm.is_mode_gate_open({}, "microbatch") is False
    outstanding = sm.mode_outstanding_steps({}, "microbatch")
    assert outstanding == sm.mode_required_keys("microbatch")


def test_mode_data_purged_tracks_cleanup():
    state = _pass_all(sm.initial_state(["microbatch"]))
    assert sm.mode_gate_status(state, "microbatch")["data_purged"] is True
    state = sm.set_step(state, "microbatch:cleanup", sm.PENDING)
    assert sm.mode_gate_status(state, "microbatch")["data_purged"] is False


def test_multi_mode_gate_status_aggregates_enabled_modes():
    state = sm.initial_state(["microbatch", "zerobus"])
    # Only the microbatch lane green.
    for k in list(sm.COMMON_STEPS) + [
        sm.step_key("microbatch", s) for s in sm.MODE_STEPS["microbatch"]
    ]:
        state = sm.set_step(state, k, sm.PASSED)
    agg = sm.multi_mode_gate_status(state, ["microbatch", "zerobus"])
    assert set(agg["modes"]) == {"microbatch", "zerobus"}
    assert agg["modes"]["microbatch"]["can_deploy"] is True
    assert agg["modes"]["zerobus"]["can_deploy"] is False
    # Aggregate is only open when every enabled mode is open.
    assert agg["can_deploy"] is False
    # Outstanding is the de-duplicated union across enabled modes.
    assert "zerobus:create_table" in agg["outstanding"]
    assert agg["outstanding"].count("zerobus:create_table") == 1


# ---------------------------------------------------------------------------
# UI progress derivation — current_step + derive_phase (spec §1.12)
# ---------------------------------------------------------------------------
def test_current_step_returns_first_non_passed():
    state = sm.initial_state(["microbatch"])
    # Nothing run yet → the first step in the sequence.
    assert sm.current_step(state) == "provision_cluster"
    # Pass the first two common steps → advances to the third.
    state = sm.set_step(state, "provision_cluster", sm.PASSED)
    state = sm.set_step(state, "source_connectivity", sm.PASSED)
    assert sm.current_step(state) == "topics_created"


def test_current_step_surfaces_running_and_waiting_admin():
    state = sm.initial_state(["realtime"])
    state = sm.set_step(state, "provision_cluster", sm.RUNNING)
    assert sm.current_step(state) == "provision_cluster"
    # Park the realtime cdf_manual step with everything before it green.
    for k in [
        "provision_cluster",
        "source_connectivity",
        "topics_created",
        "schema_conformance",
        "realtime:partitions_precreate",
        "realtime:write_test",
    ]:
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    # next_runnable_step returns None (park is not runnable) but current_step
    # still surfaces the parked step so the UI can highlight it.
    assert sm.next_runnable_step(state) is None
    assert sm.current_step(state) == "realtime:cdf_manual"


def test_current_step_none_when_all_passed():
    state = sm.initial_state(["microbatch"])
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.current_step(state) is None


def test_derive_phase_provisioning_before_and_during_cluster():
    state = sm.initial_state(["microbatch"])
    # Idle before any run → still "provisioning" (about to bring up the cluster).
    assert sm.derive_phase(state) == sm.PHASE_PROVISIONING
    # provision_cluster running, cluster PENDING → provisioning.
    state = sm.set_step(state, "provision_cluster", sm.RUNNING)
    assert sm.derive_phase(state, cluster_state="PENDING") == sm.PHASE_PROVISIONING


def test_derive_phase_provisioning_from_cluster_state_only():
    # Cluster still coming up mid-lane (e.g. RESIZING) → provisioning phase even
    # though the current step is not provision_cluster.
    state = sm.initial_state(["microbatch"])
    for k in ["provision_cluster", "source_connectivity", "topics_created", "schema_conformance"]:
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "microbatch:write_test", sm.RUNNING)
    assert sm.derive_phase(state, cluster_state="RESTARTING") == sm.PHASE_PROVISIONING


def test_derive_phase_validating_when_cluster_running():
    state = sm.initial_state(["microbatch"])
    for k in ["provision_cluster", "source_connectivity", "topics_created", "schema_conformance"]:
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "microbatch:write_test", sm.RUNNING)
    assert sm.derive_phase(state, cluster_state="RUNNING") == sm.PHASE_VALIDATING


def test_derive_phase_waiting_admin():
    state = sm.initial_state(["realtime"])
    for k in [
        "provision_cluster",
        "source_connectivity",
        "topics_created",
        "schema_conformance",
        "realtime:partitions_precreate",
        "realtime:write_test",
    ]:
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    assert sm.derive_phase(state, cluster_state="RUNNING") == sm.PHASE_WAITING_ADMIN


def test_derive_phase_failed_wins():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.PASSED)
    state = sm.set_step(state, "source_connectivity", sm.FAILED, error="boom")
    # Failure outranks provisioning / validating.
    assert sm.derive_phase(state, cluster_state="RUNNING") == sm.PHASE_FAILED


def test_derive_phase_passed_when_gate_open():
    state = sm.initial_state(["microbatch"])
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.derive_phase(state, cluster_state="TERMINATED") == sm.PHASE_PASSED
