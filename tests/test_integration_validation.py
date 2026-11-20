"""Skippable live integration test for the validation harness (plan Phase 4).

Provisions the real interactive validation cluster (DBR 18.2, single-user,
Photon/autoscaling/spot OFF) on ``example-us-east`` and drives the common
lane against live MSK + UC.  **Opt-in only** — it creates real (billable) compute
and is skipped unless ``IOT_VALIDATION_INTEGRATION=1`` is set, so the mocked unit
suite runs everywhere.

Run with, e.g.::

    IOT_VALIDATION_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=example-us-east \\
        pytest tests/test_integration_validation.py -s
"""

from __future__ import annotations

import os

import pytest

from iot_twin.services.validation import validation_state_machine as sm

_ENABLED = os.getenv("IOT_VALIDATION_INTEGRATION") == "1"
_INDUSTRY = os.getenv("IOT_VALIDATION_INDUSTRY", "pharmaceutical")

skip_live = pytest.mark.skipif(
    not _ENABLED,
    reason="live validation integration disabled (set IOT_VALIDATION_INTEGRATION=1)",
)


@skip_live
@pytest.mark.asyncio
async def test_provision_cluster_live(app_backend_conn):
    """Provision the verified validation cluster and confirm it reaches RUNNING."""
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.pipelines import pipeline_setup_repo
    from iot_twin.services.validation import validation_service, validation_steps

    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "example-us-east")
    ws = WorkspaceClient(profile=profile)

    # Ensure a minimal setup row exists in the throwaway schema.
    setup = await pipeline_setup_repo.get_setup_by_industry(app_backend_conn, _INDUSTRY)
    if setup is None:
        setup = await pipeline_setup_repo.create_setup(
            app_backend_conn,
            industry=_INDUSTRY,
            enabled_modes=["microbatch"],
            bootstrap_servers=os.getenv("IOT_BOOTSTRAP", ""),
        )

    ctx = validation_service.StepContext(
        db=app_backend_conn,
        setup=setup,
        state=sm.initial_state(setup.get("enabled_modes") or []),
        key="provision_cluster",
        step="provision_cluster",
        mode=None,
        ws=ws,
    )
    result = await validation_steps.provision_cluster(ctx)
    try:
        assert result.status == sm.PASSED, result.error
        assert result.data.get("cluster_id")
    finally:
        cid = result.data.get("cluster_id")
        if cid:
            try:
                ws.clusters.permanent_delete(cluster_id=cid)
            except Exception:
                pass


def test_verified_cluster_config_is_locked(monkeypatch):
    """Guard the verified compute constants against accidental drift (no cluster).

    SERVICE_CREDENTIAL is now env-driven (UC_SERVICE_CREDENTIAL); the test sets
    the env var and reloads the module so the constant reflects the patched value.
    """
    expected_cred = "iot-rtm-sc"
    monkeypatch.setenv("UC_SERVICE_CREDENTIAL", expected_cred)
    import importlib

    import iot_twin.services.validation.validation_config as _vc

    importlib.reload(_vc)

    spec = _vc.build_validation_cluster_spec(
        node_type_id="m5d.xlarge",
        num_workers=2,
        single_user_name="a@b.com",
    )
    assert spec["spark_version"] == "18.2.x-scala2.13"
    assert spec["data_security_mode"] == "SINGLE_USER"
    assert spec["runtime_engine"] == "STANDARD"
    assert "autoscale" not in spec
    assert spec["aws_attributes"]["availability"] == "ON_DEMAND"
    # Verify the env-read value, not a hardcoded workspace string.
    assert expected_cred == _vc.SERVICE_CREDENTIAL
    assert _vc.MSK_PUBLIC_PORT == 9198
