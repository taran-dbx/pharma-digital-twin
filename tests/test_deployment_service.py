"""Tests for iot_twin.services.deployment.deployment_service (Phase 5b — per-mode deploy).

Covers the validation hard-gate, worker sizing (M1), per-mode job-spec
construction (DBR / access-mode / spark-conf / libs / tasks / Photon-autoscale-
spot OFF), ledger recording + deployment update, and start/stop/status — all
with the Databricks Jobs API mocked.  A live deploy is gated behind a skippable
integration test.
"""

from __future__ import annotations

import base64
import inspect
import os
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from iot_twin.services.deployment import deployment_service as ds
from iot_twin.services.production_control import PRODUCTION_CONTROL_PLACEHOLDER


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------
def _all_passed(mode: str) -> dict:
    """A nested validation_state with every required step for ``mode`` passed."""
    return {"steps": {key: {"status": "passed"} for key in ds.REQUIRED_STEPS[mode]}}


def _ws() -> MagicMock:
    w = MagicMock()
    w.current_user.me.return_value = MagicMock(user_name="user@example.com")
    w.workspace.mkdirs = MagicMock(return_value=None)
    w.workspace.import_ = MagicMock(return_value=None)
    w.jobs.list.return_value = []
    w.jobs.create.return_value = MagicMock(job_id=4242)
    w.jobs.reset = MagicMock(return_value=None)
    w.jobs.cancel_all_runs = MagicMock(return_value=None)
    w.config.host = "https://e2.cloud.databricks.com"
    return w


def _db(setup: dict, deployment: dict | None = None) -> MagicMock:
    """A mock DB whose repos return the given setup + deployment rows."""
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)

    async def _fetchrow(query: str, *args):
        q = query.lower()
        if "from pipeline_setup" in q:
            return setup
        if "from pipeline_deployment" in q:
            return deployment
        if "insert into pipeline_deployment" in q:
            return deployment or {"id": "dep-1", "mode": args[1] if len(args) > 1 else None}
        if "update pipeline_deployment" in q:
            return {**(deployment or {"id": "dep-1"}), "status": "deployed"}
        if "insert into provisioned_resource" in q:
            return {"id": "res-1"}
        return None

    db.fetchrow = AsyncMock(side_effect=_fetchrow)
    return db


# Secret scope *name* in the legacy consumer source (an identifier, not a secret).
LEGACY_SCOPE_NAME = "legacy-scope"

BASE_SETUP = {
    "id": "setup-1",
    "industry": "pharmaceutical",
    "derived": {"partitions": 6},
    "cluster_spec": {"node_type": "m5d.large"},
}


@pytest.fixture(autouse=True)
def _mock_cdf_reconciliation(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "current-app-sp")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "current-app-secret")
    monkeypatch.setenv("UC_CATALOG", "configured-catalog")
    monkeypatch.setenv("UC_SCHEMA", "configured-schema")
    monkeypatch.setattr(
        ds.uc_secrets,
        "put_secret",
        AsyncMock(return_value={"status": "updated"}),
    )
    reconcile = AsyncMock(return_value={"status": "streaming"})
    monkeypatch.setattr(ds.cdf_reconciliation_service, "reconcile_realtime_cdf", reconcile)
    return reconcile


class TestConsumerCredentialNormalization:
    def test_replaces_legacy_scope_with_uc_secret_c7215(self, monkeypatch):
        """Uploaded consumers use the app SP and UC secret [C7215][defect][regression]."""
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "current-app-sp")
        monkeypatch.setenv("UC_CATALOG", "configured-catalog")
        monkeypatch.setenv("UC_SCHEMA", "configured-schema")
        source = (
            f'APP_SECRET_SCOPE = "{LEGACY_SCOPE_NAME}"\n'
            '_lb_ci = dbutils.secrets.get(APP_SECRET_SCOPE, "sss-industries-client-id")\n'
            '_lb_cs = dbutils.secrets.get(APP_SECRET_SCOPE, "sss-industries-client-secret")\n'
        )

        normalized = ds._normalize_consumer_credentials(source)

        assert '_APP_SP_CLIENT_ID = "current-app-sp"' in normalized
        assert "_lb_ci = _APP_SP_CLIENT_ID" in normalized
        assert 'catalog="configured-catalog"' in normalized
        assert 'schema="configured-schema"' in normalized
        assert 'key="app_sp_oauth"' in normalized
        assert "APP_SECRET_SCOPE" not in normalized
        assert "sss-industries-client" not in normalized

    def test_rejects_missing_runtime_config_c7215(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.setenv("UC_CATALOG", "configured-catalog")
        monkeypatch.setenv("UC_SCHEMA", "configured-schema")
        source = '_lb_ci = dbutils.secrets.get(APP_SECRET_SCOPE, "sss-industries-client-id")'

        with pytest.raises(ds.RuntimeOAuthCredentialError, match="DATABRICKS_CLIENT_ID"):
            ds._normalize_consumer_credentials(source)

    def test_rejects_unrecognized_legacy_secret_syntax_c7215(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "current-app-sp")
        monkeypatch.setenv("UC_CATALOG", "configured-catalog")
        monkeypatch.setenv("UC_SCHEMA", "configured-schema")
        source = (
            "_lb_cs = dbutils.secrets.get("
            'scope=APP_SECRET_SCOPE, key="sss-industries-client-secret")'
        )

        with pytest.raises(ValueError, match="unsupported legacy secret syntax"):
            ds._normalize_consumer_credentials(source)

    def test_replaces_stale_metrics_client_id_c7256(self, monkeypatch):
        """[C7256][defect][regression] Metrics OAuth id matches app_sp_oauth."""
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "current-app-sp")
        source = 'ZB_CLIENT_ID = "legacy-zerobus-sp"\nprint(ZB_CLIENT_ID)\n'

        normalized = ds._normalize_consumer_credentials(source)

        assert 'ZB_CLIENT_ID = "current-app-sp"' in normalized
        assert "legacy-zerobus-sp" not in normalized


@pytest.mark.asyncio
class TestRuntimeOauthSecret:
    async def test_refreshes_app_sp_secret_c7257(self, monkeypatch):
        """[C7257][defect][regression] Starts refresh a rotated UC OAuth secret."""
        put_secret = AsyncMock(return_value={"status": "updated"})
        monkeypatch.setattr(ds.uc_secrets, "put_secret", put_secret)

        result = await ds.sync_runtime_oauth_secret(_ws())

        assert result["status"] == "updated"
        put_secret.assert_awaited_once_with(
            "configured-catalog",
            "configured-schema",
            "app_sp_oauth",
            "current-app-secret",
            ws=ANY,
        )

    async def test_rejects_failed_secret_refresh_c7257(self, monkeypatch):
        monkeypatch.setattr(
            ds.uc_secrets,
            "put_secret",
            AsyncMock(return_value={"status": "failed", "error": "permission denied"}),
        )

        with pytest.raises(ds.RuntimeOAuthCredentialError, match="permission denied"):
            await ds.sync_runtime_oauth_secret(_ws())


# ---------------------------------------------------------------------------
# Worker sizing (M1)
# ---------------------------------------------------------------------------
class TestComputeWorkers:
    @pytest.mark.parametrize(
        "partitions,expected",
        [(None, 1), (0, 1), (1, 1), (4, 1), (5, 2), (6, 2), (8, 2), (9, 3), (32, 8), (100, 8)],
    )
    def test_clamp_ceil(self, partitions, expected):
        assert ds.compute_workers(partitions) == expected

    def test_guarantees_reader_per_partition_under_cap(self):
        for p in range(1, 33):
            assert ds.compute_workers(p) * ds.CORES_PER_WORKER >= p

    def test_warning_only_over_cap(self):
        assert ds.worker_sizing_warning(32) is None
        assert ds.worker_sizing_warning(33) is not None


# ---------------------------------------------------------------------------
# Validation hard-gate (§1.12)
# ---------------------------------------------------------------------------
class TestGate:
    def test_all_passed_gate_open(self):
        for mode in ds.VALID_MODES:
            assert ds.gate_passed(_all_passed(mode), mode) is True
            assert ds.outstanding_steps(_all_passed(mode), mode) == []

    def test_one_pending_blocks(self):
        state = _all_passed("realtime")
        # Nested per-mode key, not the old flat rt_* key.
        state["steps"]["realtime:cdf_verify"] = {"status": "waiting_admin"}
        assert ds.gate_passed(state, "realtime") is False
        assert "realtime:cdf_verify" in ds.outstanding_steps(state, "realtime")

    def test_empty_state_all_outstanding(self):
        assert ds.outstanding_steps({}, "microbatch") == list(ds.REQUIRED_STEPS["microbatch"])
        assert ds.outstanding_steps(None, "zerobus") == list(ds.REQUIRED_STEPS["zerobus"])

    def test_nested_status_shape_supported(self):
        state = {"steps": {k: {"status": "passed"} for k in ds.REQUIRED_STEPS["zerobus"]}}
        assert ds.gate_passed(state, "zerobus") is True

    def test_zerobus_gate_ignores_common_kafka_steps(self):
        # Zerobus is non-Kafka: its gate must not require provision_cluster etc.
        assert "provision_cluster" not in ds.REQUIRED_STEPS["zerobus"]
        assert "topics_created" not in ds.REQUIRED_STEPS["zerobus"]
        # Its required keys are the zerobus lane only.
        assert all(k.startswith("zerobus:") for k in ds.REQUIRED_STEPS["zerobus"])

    def test_assert_raises(self):
        with pytest.raises(ds.DeployGateError) as exc:
            ds.assert_gate_passed({}, "realtime")
        assert "realtime" in str(exc.value)
        assert exc.value.outstanding

    def test_unknown_mode(self):
        with pytest.raises(ValueError):
            ds.outstanding_steps({}, "bogus")


# ---------------------------------------------------------------------------
# Job-cluster spec construction
# ---------------------------------------------------------------------------
class TestClusterSpec:
    def test_realtime_cluster_dbr_access_conf_photon_autoscale_spot(self):
        spec = ds.build_job_cluster_spec(
            node_type="m5d.large",
            num_workers=2,
            single_user_name="user@example.com",
            realtime=True,
            cloud="aws",
        )
        from databricks.sdk.service.compute import DataSecurityMode, RuntimeEngine

        assert spec.spark_version == "18.2.x-scala2.13"
        assert spec.data_security_mode == DataSecurityMode.SINGLE_USER
        assert spec.single_user_name == "user@example.com"
        # real-time mode ON
        assert spec.spark_conf["spark.databricks.streaming.realTimeMode.enabled"] == "true"
        # Photon OFF
        assert spec.runtime_engine == RuntimeEngine.STANDARD
        # autoscaling OFF — fixed workers, no autoscale block
        assert spec.num_workers == 2
        assert spec.autoscale is None
        # spot OFF — every node on-demand (first_on_demand > workers)
        assert spec.aws_attributes.first_on_demand == 3

    def test_microbatch_cluster_no_realtime_mode(self):
        spec = ds.build_job_cluster_spec(
            node_type="m5d.large",
            num_workers=1,
            single_user_name="u",
            realtime=False,
            cloud="aws",
        )
        # micro-batch must NOT enable real-time mode
        assert spec.spark_conf is None or "realTimeMode" not in str(spec.spark_conf)

    def test_producer_cluster_single_node(self):
        spec = ds.build_producer_cluster_spec(
            node_type="m5d.large", single_user_name="u", cloud="aws"
        )
        assert spec.num_workers == 0
        assert spec.spark_conf["spark.master"] == "local[*]"
        assert spec.custom_tags["ResourceClass"] == "SingleNode"


class TestClusterLibraries:
    def test_consumer_libs_exact_set_no_boto3_upgrade(self):
        libs = ds.build_cluster_libraries(ds.CLASSIC_CONSUMER_LIBRARIES)
        pkgs = {lib.pypi.package for lib in libs}
        assert pkgs == {
            "databricks-sdk==0.120.0",
            "kafka-python>=3",
            "aws-msk-iam-sasl-signer-python",
            "psycopg2-binary",
            "deltalake==1.6.1",
        }
        assert not any("boto3" in p for p in pkgs)


# ---------------------------------------------------------------------------
# Task construction per mode
# ---------------------------------------------------------------------------
class TestSparkTasks:
    def test_realtime_producer_plus_consumer(self):
        tasks = ds.build_spark_tasks(
            mode="realtime",
            consumer_notebook_path="/c",
            producer_notebook_path="/p",
            node_type="m5d.large",
            num_workers=2,
            single_user_name="u",
            cloud="aws",
        )
        keys = [t.task_key for t in tasks]
        assert keys == ["rtm_consumer", "data_producer"]
        consumer = tasks[0]
        assert consumer.notebook_task.notebook_path == "/c"
        assert (
            consumer.new_cluster.spark_conf["spark.databricks.streaming.realTimeMode.enabled"]
            == "true"
        )
        assert consumer.new_cluster.num_workers == 2
        # producer is a single-node cluster
        assert tasks[1].new_cluster.num_workers == 0

    def test_microbatch_consumer_key_and_no_rtm(self):
        tasks = ds.build_spark_tasks(
            mode="microbatch",
            consumer_notebook_path="/c",
            producer_notebook_path="/p",
            node_type="m5d.large",
            num_workers=1,
            single_user_name="u",
            cloud="aws",
        )
        assert tasks[0].task_key == "microbatch_consumer"
        conf = tasks[0].new_cluster.spark_conf
        assert conf is None or "realTimeMode" not in str(conf)

    def test_producer_omitted_when_no_notebook(self):
        tasks = ds.build_spark_tasks(
            mode="realtime",
            consumer_notebook_path="/c",
            producer_notebook_path=None,
            node_type="m5d.large",
            num_workers=1,
            single_user_name="u",
            cloud="aws",
        )
        assert [t.task_key for t in tasks] == ["rtm_consumer"]

    def test_pipeline_tasks_have_job_level_retry_safety_net(self):
        """[C7234][defect][regression] Pipeline tasks restart after sink failure."""
        source = inspect.getsource(ds.build_spark_tasks)

        assert source.count("max_retries=3") == 2
        assert source.count("min_retry_interval_millis=30_000") == 2
        assert source.count("retry_on_timeout=True") == 2


# ---------------------------------------------------------------------------
# deploy_pipeline — gate + creation + ledger + deployment update
# ---------------------------------------------------------------------------
class TestDeployPipeline:
    @pytest.mark.asyncio
    async def test_gate_blocks_deploy(self):
        setup = {**BASE_SETUP, "validation_state": {}}
        db = _db(setup, {"id": "dep-1", "mode": "realtime"})
        w = _ws()
        with pytest.raises(ds.DeployGateError):
            await ds.deploy_pipeline(db, "setup-1", "realtime", consumer_notebook="nb", w=w)
        w.jobs.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_realtime_creates_continuous_job_and_records(self):
        setup = {**BASE_SETUP, "validation_state": _all_passed("realtime")}
        deployment = {"id": "dep-rt", "mode": "realtime", "partition_count": 6}
        db = _db(setup, deployment)
        w = _ws()

        result = await ds.deploy_pipeline(
            db,
            "setup-1",
            "realtime",
            consumer_notebook="# consumer",
            producer_notebook="# producer",
            w=w,
        )

        assert result["job_id"] == 4242
        assert result["job_name"] == "iot-pharmaceutical-realtime"
        assert result["num_workers"] == 2  # 6 partitions / 4 cores → 2
        assert result["spark_version"] == "18.2.x-scala2.13"
        assert result["tasks"] == ["rtm_consumer", "data_producer"]

        # Continuous + unpaused
        from databricks.sdk.service.jobs import PauseStatus

        _, kwargs = w.jobs.create.call_args
        assert kwargs["continuous"].pause_status == PauseStatus.UNPAUSED
        # two notebooks uploaded (consumer + producer)
        assert w.workspace.import_.call_count == 2

        # ledger job row + deployment update happened
        calls = [c.args[0].lower() for c in db.fetchrow.call_args_list]
        assert any("insert into provisioned_resource" in c for c in calls)
        assert any("update pipeline_deployment" in c for c in calls)

    @pytest.mark.asyncio
    async def test_producer_gets_its_production_control_path_c7437_regression(self):
        """[C7437][regression] The producer polls the control file beside its own notebook."""
        setup = {**BASE_SETUP, "validation_state": _all_passed("realtime")}
        db = _db(setup, {"id": "dep-rt", "mode": "realtime", "partition_count": 6})
        w = _ws()

        await ds.deploy_pipeline(
            db,
            "setup-1",
            "realtime",
            consumer_notebook="# consumer",
            producer_notebook=f'PRODUCTION_CONTROL_FILE = "{PRODUCTION_CONTROL_PLACEHOLDER}"',
            w=w,
        )

        producer = w.workspace.import_.call_args_list[1].kwargs
        source = base64.b64decode(producer["content"]).decode()
        folder = producer["path"].rsplit("/", 1)[0]
        assert (
            source == f'PRODUCTION_CONTROL_FILE = "{folder}/production_control/pharmaceutical.json"'
        )

    @pytest.mark.asyncio
    async def test_fully_green_microbatch_deploys_without_override(self):
        # A fully-green nested micro-batch state must pass the gate on the happy
        # path — no gate_override needed (regression: nested vs flat key shape).
        setup = {**BASE_SETUP, "validation_state": _all_passed("microbatch")}
        db = _db(setup, {"id": "dep-mb", "mode": "microbatch"})
        w = _ws()
        result = await ds.deploy_pipeline(
            db,
            "setup-1",
            "microbatch",
            consumer_notebook="# c",
            producer_notebook="# p",
            w=w,
        )
        assert result["job_name"] == "iot-pharmaceutical-microbatch"
        w.jobs.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_gate_override_skips_check(self):
        setup = {**BASE_SETUP, "validation_state": {}}
        db = _db(setup, {"id": "dep-1", "mode": "microbatch"})
        w = _ws()
        result = await ds.deploy_pipeline(
            db,
            "setup-1",
            "microbatch",
            consumer_notebook="# c",
            producer_notebook="# p",
            gate_override=True,
            w=w,
        )
        assert result["job_name"] == "iot-pharmaceutical-microbatch"
        w.jobs.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_zerobus_serverless_producer_only(self):
        setup = {**BASE_SETUP, "validation_state": _all_passed("zerobus")}
        db = _db(setup, {"id": "dep-zb", "mode": "zerobus"})
        w = _ws()
        result = await ds.deploy_pipeline(db, "setup-1", "zerobus", producer_notebook="# zb", w=w)
        assert result["serverless"] is True
        assert result["tasks"] == ["zerobus_producer"]
        _, kwargs = w.jobs.create.call_args
        # serverless: environments present, no continuous, no new_cluster
        assert kwargs.get("environments")
        assert "continuous" not in kwargs
        assert kwargs["tasks"][0].new_cluster is None

    @pytest.mark.asyncio
    async def test_reuses_existing_job_by_name(self):
        setup = {**BASE_SETUP, "validation_state": _all_passed("realtime")}
        db = _db(setup, {"id": "dep-rt", "mode": "realtime", "partition_count": 6})
        w = _ws()
        existing = MagicMock(job_id=999, settings=MagicMock())
        existing.settings.name = "iot-pharmaceutical-realtime"
        w.jobs.list.return_value = [existing]
        calls = []
        w.jobs.update.side_effect = lambda **_kwargs: calls.append("pause")
        w.jobs.cancel_all_runs.side_effect = lambda **_kwargs: calls.append("cancel")
        w.jobs.reset.side_effect = lambda **_kwargs: calls.append("reset")

        result = await ds.deploy_pipeline(db, "setup-1", "realtime", consumer_notebook="# c", w=w)
        assert result["job_id"] == 999
        assert result["action"] == "updated"
        assert calls == ["pause", "cancel", "reset"]
        assert (
            w.jobs.update.call_args.kwargs["new_settings"].continuous.pause_status.value == "PAUSED"
        )
        w.jobs.reset.assert_called_once()
        w.jobs.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_mode_raises(self):
        db = _db(BASE_SETUP)
        with pytest.raises(ValueError):
            await ds.deploy_pipeline(db, "setup-1", "bogus", w=_ws())

    @pytest.mark.asyncio
    async def test_missing_setup_raises(self):
        db = _db(None)
        with pytest.raises(ValueError, match="not found"):
            await ds.deploy_pipeline(db, "nope", "realtime", w=_ws())


# ---------------------------------------------------------------------------
# Lifecycle + teardown deleter
# ---------------------------------------------------------------------------
class TestLifecycle:
    @pytest.mark.asyncio
    async def test_realtime_deploy_stops_before_job_on_cdf_failure_c7239(self, monkeypatch):
        """Realtime job mutation is blocked by CDF failure [C7239][defect][regression]."""
        setup = {**BASE_SETUP, "validation_state": _all_passed("realtime")}
        db = _db(setup, {"id": "dep-rt", "mode": "realtime"})
        w = _ws()
        monkeypatch.setattr(
            ds.cdf_reconciliation_service,
            "reconcile_realtime_cdf",
            AsyncMock(
                side_effect=ds.cdf_reconciliation_service.RealtimeCdfError(
                    "exact shadow not streaming"
                )
            ),
        )

        with pytest.raises(
            ds.cdf_reconciliation_service.RealtimeCdfError,
            match="exact shadow",
        ):
            await ds.deploy_pipeline(db, "setup-1", "realtime", consumer_notebook="# consumer", w=w)

        w.workspace.import_.assert_not_called()
        w.jobs.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_realtime_start_reconciles_then_unpauses_c7240(self, _mock_cdf_reconciliation):
        """Start cannot unpause before CDF readiness [C7240][smoke][regression]."""
        db = _db(
            BASE_SETUP,
            {"id": "dep-rt", "mode": "realtime", "job_id": "4242"},
        )
        w = _ws()
        w.jobs.run_now.return_value = MagicMock(run_id=555)

        result = await ds.start_deployment(db, "setup-1", "realtime", w=w)

        _mock_cdf_reconciliation.assert_awaited_once_with(db, "setup-1", ws=w)
        ds.uc_secrets.put_secret.assert_awaited_once()
        assert (
            w.jobs.update.call_args.kwargs["new_settings"].continuous.pause_status.value
            == "UNPAUSED"
        )
        w.jobs.run_now.assert_not_called()
        assert result["cdf"]["status"] == "streaming"

    def test_start_continuous_unpauses_c7259(self):
        w = _ws()
        out = ds.start_pipeline(4242, w=w)
        assert out["state"] == "STARTING"
        assert (
            w.jobs.update.call_args.kwargs["new_settings"].continuous.pause_status.value
            == "UNPAUSED"
        )
        w.jobs.run_now.assert_not_called()

    def test_start_run_once_uses_run_now(self):
        w = _ws()
        w.jobs.run_now.return_value = MagicMock(run_id=555)
        out = ds.start_pipeline(4242, w=w, continuous=False)
        assert out["run_id"] == 555
        w.jobs.run_now.assert_called_once_with(job_id=4242)

    def test_stop(self):
        w = _ws()
        calls = []
        w.jobs.update.side_effect = lambda **_kwargs: calls.append("pause")
        w.jobs.cancel_all_runs.side_effect = lambda **_kwargs: calls.append("cancel")
        out = ds.stop_pipeline(4242, w=w)
        assert out["state"] == "STOPPED"
        assert out["paused"] is True
        assert calls == ["pause", "cancel"]
        assert (
            w.jobs.update.call_args.kwargs["new_settings"].continuous.pause_status.value == "PAUSED"
        )
        w.jobs.cancel_all_runs.assert_called_once_with(job_id=4242)

    def test_status_running(self):
        w = _ws()
        run = MagicMock()
        run.state.life_cycle_state.value = "RUNNING"
        run.state.result_state = None
        run.tasks = None  # no task info → run-level state stands
        w.jobs.list_runs.return_value = [run]
        st = ds.pipeline_status(4242, w=w)
        assert st["state"] == "RUNNING"
        assert st["degraded"] is False

    def test_status_degraded_when_a_task_crashed(self):
        # Run life-cycle RUNNING (continuous job) but the producer task crashed —
        # must report degraded so the UI never implies data is flowing.
        w = _ws()
        run = MagicMock()
        run.state.life_cycle_state.value = "RUNNING"
        run.state.result_state = None
        run.run_id = 999
        consumer = MagicMock()
        consumer.task_key = "microbatch_consumer"
        consumer.state.life_cycle_state.value = "RUNNING"
        consumer.state.result_state = None
        consumer.state.state_message = "In run"
        producer = MagicMock()
        producer.task_key = "data_producer"
        producer.state.life_cycle_state.value = "INTERNAL_ERROR"
        producer.state.result_state.value = "FAILED"
        producer.state.state_message = "Driver stopped unexpectedly during the run."
        full = MagicMock()
        full.tasks = [consumer, producer]
        w.jobs.list_runs.return_value = [run]
        w.jobs.get_run.return_value = full
        st = ds.pipeline_status(4242, w=w)
        assert st["state"] == "RUNNING"
        assert st["degraded"] is True
        assert st["restartable"] is True
        assert "data_producer" in (st["detail"] or "")
        assert any(t["task"] == "data_producer" for t in st["tasks"])

    def test_status_terminal_task_failure_is_restartable_c7258(self):
        """[C7258][defect][regression] A fully failed run keeps its recovery signal."""
        w = _ws()
        run = MagicMock()
        run.run_id = 999
        run.state.life_cycle_state.value = "TERMINATED"
        run.state.result_state.value = "FAILED"
        producer = MagicMock()
        producer.task_key = "data_producer"
        producer.state.life_cycle_state.value = "TERMINATED"
        producer.state.result_state.value = "FAILED"
        producer.state.state_message = "Driver stopped unexpectedly."
        full = MagicMock(tasks=[producer])
        w.jobs.list_runs.return_value = [run]
        w.jobs.get_run.return_value = full

        st = ds.pipeline_status(4242, w=w)

        assert st["state"] == "FAILED"
        assert st["degraded"] is False
        assert st["restartable"] is True
        assert "data_producer" in st["detail"]

    def test_status_manual_cancellation_is_clean_stop_c7258(self):
        """[C7258][defect][regression] Manual Stop is not reported as a crash."""
        w = _ws()
        run = MagicMock()
        run.run_id = 1000
        run.state.life_cycle_state.value = "TERMINATED"
        run.state.result_state.value = "CANCELED"
        run.state.state_message = "Run canceled by user"
        w.jobs.list_runs.return_value = [run]
        w.jobs.get_run.return_value = MagicMock(tasks=[])

        st = ds.pipeline_status(4242, w=w)

        assert st["state"] == "IDLE"
        assert st["restartable"] is False
        assert st["detail"] is None

    def test_status_no_runs(self):
        w = _ws()
        w.jobs.list_runs.return_value = []
        assert ds.pipeline_status(4242, w=w)["state"] == "IDLE"

    @pytest.mark.asyncio
    async def test_job_deleter(self):
        w = _ws()
        w.jobs.delete = MagicMock(return_value=None)
        deleter = ds.make_job_deleter(w)
        await deleter(
            {
                "resource_type": "job",
                "identifier": "4242",
                "metadata": {
                    "notebook_path": "/Workspace/consumer",
                    "producer_notebook_path": "/Workspace/producer",
                },
            }
        )
        w.jobs.delete.assert_called_once_with(job_id=4242)
        w.jobs.update.assert_not_called()
        w.jobs.cancel_all_runs.assert_called_once_with(job_id=4242)
        assert w.workspace.delete.call_count == 2
        w.workspace.delete.assert_any_call(path="/Workspace/consumer", recursive=False)
        w.workspace.delete.assert_any_call(path="/Workspace/producer", recursive=False)

    @pytest.mark.asyncio
    async def test_job_deleter_is_idempotent_when_job_is_already_gone(self):
        class MissingJobError(RuntimeError):
            status_code = 404

        w = _ws()
        w.jobs.delete = MagicMock(side_effect=MissingJobError("job not found"))
        deleter = ds.make_job_deleter(w)

        await deleter(
            {
                "resource_type": "job",
                "identifier": "4242",
                "metadata": {"mode": "realtime"},
            }
        )

        w.jobs.delete.assert_called_once_with(job_id=4242)

    @pytest.mark.asyncio
    async def test_job_deleter_handles_missing_job_during_pause_c7267(self):
        """[C7267][defect][regression] Teardown is idempotent before delete."""

        class MissingJobError(RuntimeError):
            status_code = 404

        w = _ws()
        w.jobs.update = MagicMock(side_effect=MissingJobError("job not found"))
        deleter = ds.make_job_deleter(w)

        await deleter(
            {
                "resource_type": "job",
                "identifier": "4242",
                "metadata": {"mode": "realtime"},
            }
        )

        w.jobs.delete.assert_called_once_with(job_id=4242)
        w.jobs.cancel_all_runs.assert_not_called()


# ---------------------------------------------------------------------------
# Live deploy — integration (skipped unless explicitly enabled)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(
    os.getenv("RUN_DEPLOY_INTEGRATION") != "1",
    reason="live deploy integration test — set RUN_DEPLOY_INTEGRATION=1 to run",
)
class TestLiveDeploy:
    @pytest.mark.asyncio
    async def test_live_realtime_deploy(self):  # pragma: no cover - live only
        pytest.skip("live deploy harness wired by Phase 7 e2e suite")
