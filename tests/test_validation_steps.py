"""Unit tests for the effectful realtime validation executors (spec §1.10).

These exercise the *branching / wiring* of ``services/validation/validation_steps.py`` with
the cluster command execution and Lakebase connections mocked out — the live
streaming path stays behind ``tests/test_integration_validation.py`` (opt-in).

Focus (the Phase-7 design-gap fix):
  * realtime ``write_test`` PRODUCES test data then runs the RTM consumer that
    lands rows via a ForeachWriter (not a noop sink);
  * ``read_back`` verifies rows via Lakebase primary + replica;
  * ``cleanup`` empties the test rows and deletes the validation checkpoint;
  * ``cdf_verify`` keys on this pipeline's specific shadow table.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from iot_twin.services import health_checks as hc
from iot_twin.services.validation import validation_state_machine as sm
from iot_twin.services.validation import validation_steps as vs
from iot_twin.services.validation.validation_service import StepContext, StepResult
from tests._fakes import fake_credential

UC_CATALOG = "test_catalog"
UC_SCHEMA = "test_schema"
PIPELINES_LAKEBASE_PROJECT = "test_pipelines"

# UC service credential *name* (an identifier, not a secret) and a probe status.
UC_SC_NAME = "iot-rtm-sc"
HTTP_UNAUTHORIZED = "401"


@pytest.fixture(autouse=True)
def _one_schema_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin one-schema UC + pipelines project env for validation_steps._cfg()."""
    monkeypatch.setenv("UC_CATALOG", UC_CATALOG)
    monkeypatch.setenv("UC_SCHEMA", UC_SCHEMA)
    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", PIPELINES_LAKEBASE_PROJECT)


_SETUP = {
    "industry": "pharmaceutical",
    "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9198",
    "derived": {"region": "us-east-1", "broker_count": 3},
    "service_credential": UC_SC_NAME,
    "cluster_spec": {"partition_count": 3},
    "warehouse_id": "wh1",
}


def _ctx(mode: str) -> StepContext:
    return StepContext(
        db=None,
        setup=_SETUP,
        state={"cluster_id": "c-1"},
        key=f"{mode}:x",
        step="x",
        mode=mode,
        ws=object(),
    )


def test_resolve_lakebase_write_conn_uses_configured_project(monkeypatch):
    requested_names: list[str] = []

    class FakePostgres:
        def get_endpoint(self, *, name):
            requested_names.append(name)
            return SimpleNamespace(
                status=SimpleNamespace(hosts=SimpleNamespace(host="pg.example.com"))
            )

        def generate_database_credential(self, *, endpoint):
            assert endpoint == requested_names[-1]
            return SimpleNamespace(token=fake_credential("pg"))

    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-client-id")
    ctx = StepContext(
        db=None,
        setup=_SETUP,
        state={"cluster_id": "c-1"},
        key="realtime:x",
        step="x",
        mode="realtime",
        ws=SimpleNamespace(postgres=FakePostgres()),
    )

    connection = vs._resolve_lakebase_write_conn(ctx, "pharmaceutical")

    assert requested_names == ["projects/test_pipelines/branches/pharmaceutical/endpoints/primary"]
    assert connection == ("pg.example.com", "app-client-id", fake_credential("pg"))


def test_cluster_failure_extracts_json_error_and_preserves_root_cause():
    traceback = "Traceback (most recent call last):\n" + ("frame\n" * 100)
    traceback += "pyspark.errors.exceptions.AnalysisException: unsupported sink"
    output = f"{vs.snip.FAIL_SENTINEL} {json.dumps({'error': traceback})}"

    extracted = vs._extract_fail(output)
    compacted = vs._compact_error(extracted)

    assert extracted is not None
    assert extracted.startswith("Traceback")
    assert len(compacted) == 500
    assert compacted.startswith("Traceback")
    assert compacted.endswith("AnalysisException: unsupported sink")


def test_cluster_failure_preserves_structured_diagnostics():
    error = "connection failed\n" + ("trace frame\n" * 100)
    output = (
        f"{vs.snip.FAIL_SENTINEL} "
        f"{json.dumps({'error': error, 'public': {'tcp': False}, 'egress_ip': '203.0.113.1'})}"
    )

    extracted = vs._extract_fail(output)
    compacted = vs._compact_error(extracted)

    assert extracted is not None
    assert len(compacted) <= 500
    assert compacted.startswith("connection failed")
    assert '"public": {"tcp": false}' in compacted
    assert '"egress_ip": "203.0.113.1"' in compacted


@pytest.mark.asyncio
async def test_write_test_surfaces_metrics_flush_error_c7213(monkeypatch):
    """A hidden metrics write failure must fail the producing step [C7213][defect]."""

    async def fake_run(ctx, code, *, timeout_s=300):
        output = (
            f"{vs.snip.OK_SENTINEL} "
            '{"metrics_written": 0, "recent_progress": 1, '
            '"metrics_flush_error": "DeltaError: permission denied"}'
        )
        return StepResult(sm.PASSED, data={"output": output})

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)

    result = await vs._run_write_test(_ctx("realtime"), "code")

    assert result.status == sm.FAILED
    assert result.error == "metrics flush failed: DeltaError: permission denied"


@pytest.mark.asyncio
async def test_realtime_write_test_produces_then_consumes_c7242(monkeypatch):
    """realtime write_test must first produce sample data, then run the RTM
    consumer with the curated industry stamp [C7242][defect][regression]."""
    calls: list[str] = []

    async def fake_run(ctx, code, *, timeout_s=300):
        calls.append(code)
        return StepResult(sm.PASSED, data={})

    produced = {}

    def fake_produce(topic, bootstrap, region, cred, **kw):
        produced["topic"] = topic
        produced["producer_kw"] = kw
        return f"PRODUCE::{topic}"

    def fake_write(topic, bootstrap, cred, **kw):
        produced["consumer_kw"] = kw
        return f"CONSUME::{topic}"

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "rtm_produce", fake_produce)
    monkeypatch.setattr(vs.snip, "rtm_write_test", fake_write)
    monkeypatch.setattr(
        vs,
        "_resolve_lakebase_write_conn",
        lambda ctx, branch: ("pg-host", "pg-user", fake_credential("pg")),
    )
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace.example.com")
    monkeypatch.setenv("ZEROBUS_SERVER_ENDPOINT", "workspace.zerobus.example.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "configured-app-client")

    result = await vs.write_test(_ctx("realtime"))

    assert result.status == sm.PASSED
    # Producer runs before the consumer.
    assert len(calls) == 2
    assert calls[0].startswith("PRODUCE::")
    assert calls[1].startswith("CONSUME::")
    assert produced["producer_kw"]["industry"] == "pharmaceutical"
    assert produced["producer_kw"]["source"] == "validation"
    # The consumer is wired to the ChronoTable + a distinct validation checkpoint
    # + the buffered metrics listener target.
    kw = produced["consumer_kw"]
    assert kw["table"] == "sensor_readings"
    assert kw["checkpoint_path"] == (
        f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/stream_checkpoints/pharmaceutical/validation/realtime"
    )
    assert kw["metrics_table_fqn"] == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"
    assert "streaming_metrics_realtime/" in kw["metrics_table_location"]
    assert kw["workspace_url"] == "https://workspace.example.com"
    assert kw["zerobus_endpoint"] == "workspace.zerobus.example.com"
    assert kw["zerobus_client_id"] == "configured-app-client"
    assert kw["zerobus_secret_key"] == vs.cat.APP_SP_OAUTH_SECRET


@pytest.mark.asyncio
async def test_realtime_write_test_stops_if_producer_fails(monkeypatch):
    calls: list[str] = []

    async def fake_run(ctx, code, *, timeout_s=300):
        calls.append(code)
        return StepResult(sm.FAILED, error="produce boom")

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "rtm_produce", lambda *a, **k: "PRODUCE")
    monkeypatch.setattr(vs.snip, "rtm_write_test", lambda *a, **k: "CONSUME")

    result = await vs.write_test(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert len(calls) == 1  # consumer never runs if produce fails


@pytest.mark.asyncio
async def test_microbatch_write_test_unchanged(monkeypatch):
    captured = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        captured["code"] = code
        return StepResult(sm.PASSED, data={})

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(
        vs.snip,
        "microbatch_write_test",
        lambda *a, **k: "MICROBATCH",
    )
    result = await vs.write_test(_ctx("microbatch"))
    assert result.status == sm.PASSED
    assert captured["code"] == "MICROBATCH"


def _assert_existence_probe(sql: str, args: tuple) -> None:
    lowered = sql.lower()
    assert "count(" not in lowered
    assert "limit 1" in lowered
    assert "event_time >=" in lowered
    assert "event_time <" in lowered
    assert args[0] == vs.VALIDATION_SOURCE
    assert args[1] == "pharmaceutical"
    assert len(args) == 4
    assert args[2] < args[3]


@pytest.mark.asyncio
async def test_read_back_probes_existence_not_count_c7237(monkeypatch):
    class FakeConn:
        def __init__(self, value):
            self.value = value
            self.queries: list = []

        async def fetchval(self, sql, *args):
            self.queries.append((sql, args))
            return self.value

        async def close(self):
            pass

    primary = FakeConn(1)
    replica = FakeConn(1)

    async def fake_write_conn(branch, *, ws=None):
        return primary

    async def fake_read_conn(branch, *, ws=None):
        return replica

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)
    monkeypatch.setattr(vs.pps, "get_read_conn", fake_read_conn)

    result = await vs.read_back(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert result.data["found_primary"] is True
    assert result.data["found_replica"] is True
    assert primary.queries
    _assert_existence_probe(*primary.queries[0])
    _assert_existence_probe(*replica.queries[0])


@pytest.mark.asyncio
async def test_read_back_fails_when_no_recent_row_c7237(monkeypatch):
    class FakeConn:
        async def fetchval(self, sql, *args):
            return None

        async def close(self):
            pass

    async def fake_conn(branch, *, ws=None):
        return FakeConn()

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(vs.pps, "get_read_conn", fake_conn)

    result = await vs.read_back(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert result.error
    assert "count" not in result.error.lower()


@pytest.mark.asyncio
async def test_read_back_surfaces_empty_timeout_c7237(monkeypatch):
    class FakeConn:
        async def fetchval(self, sql, *args):
            raise TimeoutError()

        async def close(self):
            pass

    async def fake_write_conn(branch, *, ws=None):
        return FakeConn()

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)

    result = await vs.read_back(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert result.error
    assert "TimeoutError" in result.error


@pytest.mark.asyncio
async def test_read_back_window_uses_write_test_stamp_on_retry_c7237(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.queries: list = []

        async def fetchval(self, sql, *args):
            self.queries.append((sql, args))
            return 1

        async def close(self):
            pass

    conn = FakeConn()

    async def fake_conn(branch, *, ws=None):
        return conn

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(vs.pps, "get_read_conn", fake_conn)

    written_at = datetime(2026, 10, 13, 8, 0, tzinfo=UTC)
    ctx = _ctx("realtime")
    ctx = StepContext(
        db=ctx.db,
        setup=ctx.setup,
        state={
            "cluster_id": "c-1",
            "steps": {
                "realtime:write_test": {
                    "status": sm.PASSED,
                    "error": None,
                    "updated_at": written_at.isoformat(),
                }
            },
        },
        key=ctx.key,
        step=ctx.step,
        mode=ctx.mode,
        ws=ctx.ws,
    )
    result = await vs.read_back(ctx)
    assert result.status == sm.PASSED
    start, end = conn.queries[0][1][2], conn.queries[0][1][3]
    assert start == written_at - timedelta(minutes=30)
    assert end == written_at + timedelta(minutes=5)


@pytest.mark.asyncio
async def test_cleanup_purges_rows_and_deletes_checkpoint(monkeypatch):
    deleted = {}

    class FakeConn:
        async def execute(self, sql, *args):
            deleted["sql"] = sql
            deleted["args"] = args
            return "DELETE 100"

        async def close(self):
            pass

    async def fake_write_conn(branch, *, ws=None):
        return FakeConn()

    removed = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        removed["code"] = code
        return StepResult(sm.PASSED, data={})

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)
    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "remove_path", lambda p: f"RM::{p}")

    result = await vs.cleanup(_ctx("realtime"))
    assert result.status == sm.PASSED
    # Test rows purged (source-scoped) + checkpoint removed.
    assert "DELETE" in deleted["sql"].upper()
    assert "source" in deleted["sql"]
    assert removed["code"].startswith("RM::")
    assert removed["code"] == (
        f"RM::/Volumes/{UC_CATALOG}/{UC_SCHEMA}/stream_checkpoints/"
        "pharmaceutical/validation/realtime"
    )
    assert result.data["checkpoint_deleted"] is True


@pytest.mark.asyncio
async def test_cdf_manual_automates_api_enablement_c7206(monkeypatch):
    """The compatibility step prepares the shadow and API config [C7206][smoke]."""
    called = {}

    class FakeConn:
        async def close(self):
            called["closed"] = True

    async def fake_write_conn(branch, *, ws=None):
        called["branch"] = branch
        return FakeConn()

    async def fake_enable_sync(conn, *, shadow_table, table, schema="public"):
        called["shadow_table"] = shadow_table
        called["table"] = table
        called["schema"] = schema
        return {"status": "migrated"}

    def fake_ensure(parent, **kwargs):
        called["parent"] = parent
        called["cdf_kwargs"] = kwargs
        return {
            "name": f"{parent}/cdf-configs/lakets_cdf",
            "status": "created",
        }

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)
    monkeypatch.setattr(vs.lakets_svc, "reconcile_chronotable_shadow", fake_enable_sync)
    monkeypatch.setattr(vs.cdf_config_service, "ensure_cdf_config", fake_ensure)

    result = await vs.cdf_manual(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert called["table"] == "sensor_readings"
    assert called["schema"] == "public"
    assert called["cdf_kwargs"]["catalog"] == UC_CATALOG
    assert called["cdf_kwargs"]["schema"] == UC_SCHEMA
    assert result.data["cdf_shadow"] == "migrated"
    assert called["shadow_table"] == "_shadow_pharmaceutical_sensor_readings"
    assert result.data["shadow_table"] == ("lakets_cdf._shadow_pharmaceutical_sensor_readings")
    assert called.get("closed") is True


@pytest.mark.asyncio
async def test_cdf_manual_fails_if_shadow_prep_fails_c7207(monkeypatch):
    """API enablement cannot proceed without a shadow [C7207][regression]."""

    async def boom_conn(branch, *, ws=None):
        raise RuntimeError("branch auth failed")

    monkeypatch.setattr(vs.pps, "get_write_conn", boom_conn)

    result = await vs.cdf_manual(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert "branch auth failed" in str(result.error)


@pytest.mark.asyncio
async def test_dbsql_read_realtime_targets_cdf_synced_table_c7243(monkeypatch):
    """Realtime validation is industry/source scoped [C7243][defect][regression]."""
    captured = {}

    async def fake_dbsql(sql, wh):
        captured["sql"] = sql
        return {"data": [{"n": 3}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.dbsql_read(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert (
        f"{UC_CATALOG}.{UC_SCHEMA}.lb__shadow_pharmaceutical_sensor_readings_history"
    ) in captured["sql"]
    assert ".realtime.sensor_readings" not in captured["sql"]
    assert "industry = 'pharmaceutical'" in captured["sql"]
    assert "source = 'validation'" in captured["sql"]


@pytest.mark.asyncio
async def test_dbsql_read_microbatch_targets_sensor_data(monkeypatch):
    captured = {}

    async def fake_dbsql(sql, wh):
        captured["sql"] = sql
        return {"data": [{"n": 9}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.dbsql_read(_ctx("microbatch"))
    assert result.status == sm.PASSED
    assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_microbatch" in captured["sql"]
    assert ".microbatch.sensor_data" not in captured["sql"]


@pytest.mark.asyncio
async def test_dbsql_read_fails_when_zero_rows(monkeypatch):
    """No-data must FAIL, not pass — the old check returned PASSED regardless."""

    async def fake_dbsql(sql, wh):
        return {"data": [{"n": 0}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.dbsql_read(_ctx("microbatch"))
    assert result.status == sm.FAILED
    assert "0 rows" in str(result.error)


@pytest.mark.asyncio
async def test_metrics_check_fails_when_empty(monkeypatch):
    async def fake_dbsql(sql, wh):
        return {"data": [{"n": 0, "have_e2e": 0, "have_proc": 0}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.metrics_check(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert "no metrics rows" in str(result.error)


@pytest.mark.asyncio
async def test_metrics_check_reads_per_mode_table_and_passes(monkeypatch):
    captured = {}

    async def fake_dbsql(sql, wh):
        captured["sql"] = sql
        return {"data": [{"n": 7, "have_e2e": 7, "have_proc": 7}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.metrics_check(_ctx("microbatch"))
    assert result.status == sm.PASSED
    assert result.data["rows"] == 7
    assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics" in captured["sql"]
    assert "mode = 'microbatch'" in captured["sql"]
    assert "streaming_metrics_microbatch" not in captured["sql"]


@pytest.mark.asyncio
async def test_write_test_microbatch_passes_metrics_params(monkeypatch):
    captured = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        return StepResult(sm.PASSED, data={})

    def fake_mb(topic, bootstrap, cred, delta, checkpoint, **kw):
        captured.update(kw)
        return "MB"

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "microbatch_write_test", fake_mb)
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace.example.com")
    monkeypatch.setenv("ZEROBUS_SERVER_ENDPOINT", "workspace.zerobus.example.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "configured-app-client")
    result = await vs.write_test(_ctx("microbatch"))
    assert result.status == sm.PASSED
    # Micro-batch metrics land in the shared {industry}_metrics table (mode column).
    assert captured["metrics_table_fqn"] == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"
    assert "streaming_metrics_microbatch/" in captured["metrics_table_location"]
    assert captured["workspace_url"] == "https://workspace.example.com"
    assert captured["zerobus_endpoint"] == "workspace.zerobus.example.com"
    assert captured["zerobus_client_id"] == "configured-app-client"
    assert captured["zerobus_secret_key"] == vs.cat.APP_SP_OAUTH_SECRET


@pytest.mark.asyncio
async def test_zerobus_produce_test_driver_side_no_cluster(monkeypatch):
    """produce_test must call zbv.produce driver-side (NOT _run_on_cluster).

    Zerobus pipelines have no interactive cluster — the SDK produces in-process
    via asyncio.to_thread, authenticated the same way auth_probe does.
    """
    # Pin UC env so industry_table() resolves without raising RuntimeError.
    monkeypatch.setenv("UC_CATALOG", "iot_test_catalog")
    monkeypatch.setenv("UC_SCHEMA", "rtm_test_schema")
    monkeypatch.setenv("DATABRICKS_HOST", "https://x.databricks.com")
    monkeypatch.setenv("ZEROBUS_SERVER_ENDPOINT", "x.zerobus.us-east-1.cloud.databricks.com")

    cluster_calls: list = []

    async def fake_run_on_cluster(ctx, code, *, timeout_s=300):
        cluster_calls.append(code)
        return StepResult(sm.PASSED, data={})

    produce_calls: list = []

    def fake_produce(**kw):
        produce_calls.append(kw)
        return (True, {"produced": 100, "table": kw.get("table_fqn", "")})

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run_on_cluster)
    monkeypatch.setattr(vs.zbv, "produce", fake_produce)

    result = await vs.produce_test(_ctx("zerobus"))

    assert result.status == sm.PASSED
    # The cluster path must NOT have been touched — no cluster for zerobus.
    assert cluster_calls == [], "produce_test must not call _run_on_cluster for zerobus"
    # The driver-side producer must have been called exactly once.
    assert len(produce_calls) == 1
    call_kw = produce_calls[0]
    assert call_kw["workspace_url"] == "https://x.databricks.com"
    assert call_kw["server_endpoint"] == "x.zerobus.us-east-1.cloud.databricks.com"
    assert "pharmaceutical" in call_kw["table_fqn"]
    assert call_kw["num_records"] == 100
    assert call_kw["source"] == vs.VALIDATION_SOURCE


@pytest.mark.asyncio
async def test_zerobus_produce_test_fails_without_endpoint(monkeypatch):
    monkeypatch.delenv("ZEROBUS_SERVER_ENDPOINT", raising=False)
    result = await vs.produce_test(_ctx("zerobus"))
    assert result.status == sm.FAILED
    assert "ZEROBUS_SERVER_ENDPOINT" in str(result.error)


@pytest.mark.asyncio
async def test_zerobus_auth_probe_delegates_to_module(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://x.databricks.com")
    monkeypatch.setenv("ZEROBUS_SERVER_ENDPOINT", "x.zerobus.us-east-1.cloud.databricks.com")
    monkeypatch.setattr(vs.zbv, "auth_probe", lambda *a, **k: (True, {"ok": True}))
    result = await vs.auth_probe(_ctx("zerobus"))
    assert result.status == sm.PASSED

    monkeypatch.setattr(
        vs.zbv, "auth_probe", lambda *a, **k: (False, {"token_error": HTTP_UNAUTHORIZED})
    )
    result = await vs.auth_probe(_ctx("zerobus"))
    assert result.status == sm.FAILED


@pytest.mark.asyncio
async def test_cdf_verify_checks_lakebase_not_cluster(monkeypatch):
    """cdf_verify must query CDF status on the branch Lakebase (wal2delta.tables
    lives in Postgres, not Spark) via the Tier-1 check keyed on THIS shadow — a
    spark.sql query on the cluster can never see wal2delta.tables."""
    called = {}

    class FakeConn:
        async def close(self):
            called["closed"] = True

    async def fake_conn(branch, *, ws=None):
        called["branch"] = branch
        return FakeConn()

    async def fake_check(conn, *, shadow=None, **kw):
        called["shadow"] = shadow
        return {"name": "cdf_sync", "status": hc.PASS, "detail": "STREAMING + advancing"}

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(hc, "check_cdf_sync", fake_check)

    result = await vs.cdf_verify(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert called["branch"] == "pharmaceutical"
    assert called["shadow"] == "_shadow_pharmaceutical_sensor_readings"
    assert called.get("closed") is True


@pytest.mark.asyncio
async def test_cdf_verify_fails_when_not_streaming(monkeypatch):
    class FakeConn:
        async def close(self):
            pass

    async def fake_conn(branch, *, ws=None):
        return FakeConn()

    async def fake_check(conn, *, shadow=None, **kw):
        return {
            "name": "cdf_sync",
            "status": hc.FAIL,
            "detail": "lakets_cdf._shadow_sensor_readings not STREAMING — enable managed sync",
        }

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(hc, "check_cdf_sync", fake_check)

    result = await vs.cdf_verify(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert "STREAMING" in str(result.error)


# ---------------------------------------------------------------------------
# provision_cluster must NOT leak a cluster when bring-up / library install fails
# ---------------------------------------------------------------------------
class _FakeMe:
    user_name = "sp-app-id"


class _RecordingClusters:
    def __init__(self):
        self.deleted = None

    def delete(self, cluster_id=None):
        self.deleted = cluster_id


class _ProvisionWs:
    def __init__(self, clusters):
        self.clusters = clusters
        self.current_user = type("U", (), {"me": staticmethod(lambda: _FakeMe())})()


class _VersionedClusters(_RecordingClusters):
    """Recording clusters that also serve a latest-LTS spark version."""

    def __init__(self, version="99.9.x-scala2.13", raises=False):
        super().__init__()
        self._version = version
        self._raises = raises

    def select_spark_version(self, long_term_support=False, latest=False):
        if self._raises:
            raise RuntimeError("versions API down")
        return self._version


@pytest.mark.asyncio
async def test_provision_cluster_terminates_created_cluster_on_failure(monkeypatch):
    clusters = _RecordingClusters()
    ws = _ProvisionWs(clusters)

    monkeypatch.setattr(vs, "_pick_gp_node", lambda w: ("m5.xlarge", 4))
    monkeypatch.setattr(vs.vc, "worker_sizing", lambda parts, cores: {"workers": 1})
    monkeypatch.setattr(
        vs.vc, "build_validation_cluster_spec", lambda **k: {"cluster_name": "iot-validate-x"}
    )
    monkeypatch.setattr(vs, "_create_validation_cluster", lambda w, spec: "0706-created")

    async def boom(w, cluster_id):
        raise RuntimeError("cluster 0706-created entered PENDING timeout")

    monkeypatch.setattr(vs, "_wait_cluster_running", boom)

    ctx = StepContext(
        db=None,
        setup=_SETUP,
        state={},
        key="provision_cluster",
        step="provision_cluster",
        mode=None,
        ws=ws,
    )
    with pytest.raises(RuntimeError):
        await vs.provision_cluster(ctx)
    # The just-created cluster was terminated rather than leaked.
    assert clusters.deleted == "0706-created"


@pytest.mark.asyncio
async def test_provision_cluster_pins_verified_dbr(monkeypatch):
    # Workspace "latest LTS" is 16.4 (Spark 3.5, no realTime trigger) — provision
    # must IGNORE it and pin the verified DBR 18.2 (RTM + service creds).
    ws = _ProvisionWs(_VersionedClusters(version="16.4.x-scala2.12"))
    captured: dict = {}

    monkeypatch.setattr(vs, "_pick_gp_node", lambda w: ("m5.xlarge", 4))
    monkeypatch.setattr(vs.vc, "worker_sizing", lambda parts, cores: {"workers": 1})

    def _spec(**kwargs):
        captured.update(kwargs)
        return {"cluster_name": "iot-validate-x"}

    monkeypatch.setattr(vs.vc, "build_validation_cluster_spec", _spec)
    monkeypatch.setattr(vs, "_create_validation_cluster", lambda w, spec: "0706-ok")

    async def _ok(w, cluster_id):
        return None

    monkeypatch.setattr(vs, "_wait_cluster_running", _ok)

    async def _libs(w, cluster_id, packages):
        return None

    monkeypatch.setattr(vs, "_install_cluster_libraries", _libs)

    ctx = StepContext(
        db=None,
        setup=_SETUP,
        state={},
        key="provision_cluster",
        step="provision_cluster",
        mode=None,
        ws=ws,
    )
    result = await vs.provision_cluster(ctx)
    assert result.status == sm.PASSED
    # Pins the VERIFIED DBR (18.2), NOT the workspace's latest LTS (16.4).
    assert captured["spark_version"] == vs.vc.VALIDATION_DBR
    assert captured["spark_version"] == "18.2.x-scala2.13"


@pytest.mark.asyncio
async def test_latest_lts_falls_back_to_pinned_dbr_on_error():
    ws = _ProvisionWs(_VersionedClusters(raises=True))
    assert vs._latest_lts_spark_version(ws) == vs.vc.VALIDATION_DBR
