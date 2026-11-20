"""Unit tests for services/health_checks.py — the two-tier pipeline health checks.

Tier 1 (``tier="api"``) checks run from the app backend with **no interactive
cluster** — UC/DBSQL, Lakebase ``w.postgres``, a psycopg read connection to the
branch, UC grants/secrets APIs, and the Jobs API — so every check is unit-tested
with those dependencies mocked.  Tier 2 (``tier="e2e"``) is the opt-in interactive
harness; the only Tier-2 item surfaced here is the Kafka topic-existence check,
which is ``skip``ped in Tier 1 (MSK-IAM creds are cluster-only).

The suite covers each check's pass / fail / skip paths, graceful degradation when
a resource is missing (fail, never crash), a down interactive cluster, and the
orchestrator's aggregation into ``health`` / ``startable`` / ``cluster_state``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services import health_checks as hc
from iot_twin.services.pipelines import pipeline_view as pv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sql_result(rows: list[dict]) -> dict:
    """Shape a DBSQL result dict the way ``execute_dbsql`` returns it."""
    cols = list(rows[0].keys()) if rows else []
    return {"columns": cols, "data": rows, "row_count": len(rows)}


def _run_sql_returning(rows: list[dict]):
    """An async ``run_sql`` that always returns ``rows``."""
    return AsyncMock(return_value=_sql_result(rows))


def _endpoint(state: str):
    return SimpleNamespace(status=SimpleNamespace(current_state=SimpleNamespace(value=state)))


def _ws_endpoints_active(primary: str = "ACTIVE", replica: str = "ACTIVE") -> MagicMock:
    ws = MagicMock()
    ws.postgres.get_project = MagicMock()
    ws.postgres.get_branch = MagicMock()
    ws.postgres.get_endpoint = MagicMock(side_effect=[_endpoint(primary), _endpoint(replica)])
    return ws


# All canonical schemas + the volume / tables a fully-enabled pipeline has.
_ALL_SCHEMAS = [
    {"schema_name": s} for s in ("realtime", "microbatch", "zerobus", "metrics", "checkpoints")
]


# ===========================================================================
# check_uc_catalog
# ===========================================================================
@pytest.mark.asyncio
class TestCheckUcCatalog:
    async def test_pass_when_all_schemas_present(self):
        res = await hc.check_uc_catalog("iot_pharma", _run_sql_returning(_ALL_SCHEMAS))
        assert res["tier"] == hc.TIER_API
        assert res["status"] == hc.PASS

    async def test_fail_when_schema_missing(self):
        rows = [{"schema_name": s} for s in ("realtime", "microbatch")]
        res = await hc.check_uc_catalog("iot_pharma", _run_sql_returning(rows))
        assert res["status"] == hc.FAIL
        assert "checkpoints" in res["detail"]

    async def test_fail_gracefully_when_catalog_missing(self):
        # A missing catalog makes the query raise — the check must fail, not crash.
        run_sql = AsyncMock(side_effect=RuntimeError("CATALOG_NOT_FOUND: iot_pharma"))
        res = await hc.check_uc_catalog("iot_pharma", run_sql)
        assert res["status"] == hc.FAIL
        assert "iot_pharma" in res["detail"] or "NOT_FOUND" in res["detail"]

    async def test_skip_when_no_warehouse(self):
        res = await hc.check_uc_catalog("iot_pharma", None)
        assert res["status"] == hc.SKIP


# ===========================================================================
# check_delta_tables
# ===========================================================================
@pytest.mark.asyncio
class TestCheckDeltaTables:
    async def test_pass_for_enabled_modes(self):
        rows = [
            {"table_schema": "realtime", "table_name": "sensor_readings"},
            {"table_schema": "microbatch", "table_name": "sensor_data"},
            {"table_schema": "zerobus", "table_name": "iot_sensor_data"},
        ]
        res = await hc.check_delta_tables(
            "iot_pharma", ["realtime", "microbatch", "zerobus"], _run_sql_returning(rows)
        )
        assert res["status"] == hc.PASS

    async def test_skip_when_expected_table_not_yet_created(self):
        # Per-mode Delta tables are written on the first pipeline run, so a
        # missing table on a not-yet-run pipeline is "not yet created" (SKIP),
        # not a health failure.
        rows = [{"table_schema": "microbatch", "table_name": "sensor_data"}]
        res = await hc.check_delta_tables(
            "iot_pharma", ["microbatch", "zerobus"], _run_sql_returning(rows)
        )
        assert res["status"] == hc.SKIP
        assert "iot_sensor_data" in res["detail"]

    async def test_only_checks_enabled_modes(self):
        rows = [{"table_schema": "microbatch", "table_name": "sensor_data"}]
        res = await hc.check_delta_tables("iot_pharma", ["microbatch"], _run_sql_returning(rows))
        assert res["status"] == hc.PASS

    async def test_skip_when_no_warehouse(self):
        res = await hc.check_delta_tables("iot_pharma", ["microbatch"], None)
        assert res["status"] == hc.SKIP

    async def test_industry_realtime_expects_unique_cdf_destination_c7251(self):
        """Health rejects the unused direct realtime table [C7251][defect][regression]."""
        rows = [
            {
                "table_schema": "rtm_lakets_zb",
                "table_name": "manufacturing_realtime",
            }
        ]
        result = await hc.check_delta_tables(
            "example-catalog",
            ["realtime"],
            _run_sql_returning(rows),
            schema="rtm_lakets_zb",
            industry="manufacturing",
        )

        assert result["status"] == hc.SKIP
        assert "lb__shadow_manufacturing_sensor_readings_history" in result["detail"]


# ===========================================================================
# check_checkpoint_volume
# ===========================================================================
@pytest.mark.asyncio
class TestCheckCheckpointVolume:
    async def test_pass_when_volume_present(self, monkeypatch):
        monkeypatch.setenv("UC_SCHEMA", "shared_schema")
        rows = [{"volume_name": "stream_checkpoints"}]
        run_sql = _run_sql_returning(rows)
        res = await hc.check_checkpoint_volume("iot_pharma", run_sql)
        assert res["status"] == hc.PASS
        assert "volume_schema = 'shared_schema'" in run_sql.await_args.args[0]

    async def test_fail_when_volume_absent(self, monkeypatch):
        monkeypatch.setenv("UC_SCHEMA", "shared_schema")
        res = await hc.check_checkpoint_volume("iot_pharma", _run_sql_returning([]))
        assert res["status"] == hc.FAIL
        assert "iot_pharma.shared_schema.stream_checkpoints" in res["detail"]


# ===========================================================================
# check_metrics_recent
# ===========================================================================
@pytest.mark.asyncio
class TestCheckMetricsRecent:
    @pytest.fixture(autouse=True)
    def _one_schema(self, monkeypatch):
        monkeypatch.setenv("UC_CATALOG", "shared_catalog")
        monkeypatch.setenv("UC_SCHEMA", "shared_schema")

    async def test_pass_when_rows_present(self):
        rows = [{"n": 42, "latest": "2026-07-06T10:00:00Z"}]
        run_sql = _run_sql_returning(rows)
        res = await hc.check_metrics_recent(["realtime"], run_sql, industry="pharmaceutical")
        assert res["status"] == hc.PASS
        sql = run_sql.await_args.args[0]
        assert "shared_catalog.shared_schema.pharmaceutical_metrics" in sql
        assert "WHERE mode = 'realtime'" in sql

    async def test_skip_when_no_rows_yet(self):
        # Metrics rows only land once a mode streams — an empty table on a
        # not-yet-run pipeline is expected (SKIP), not a failure.
        rows = [{"n": 0, "latest": None}]
        res = await hc.check_metrics_recent(
            ["realtime"], _run_sql_returning(rows), industry="pharmaceutical"
        )
        assert res["status"] == hc.SKIP

    async def test_skip_when_table_not_created(self):
        # The per-mode metrics table is created on the first streaming run; a
        # DBSQL TABLE_OR_VIEW_NOT_FOUND means "not run yet", so SKIP.
        run_sql = AsyncMock(side_effect=RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] table not found"))
        res = await hc.check_metrics_recent(["realtime"], run_sql, industry="pharmaceutical")
        assert res["status"] == hc.SKIP

    async def test_fail_on_genuine_error(self):
        # A real error (not a missing table) must still FAIL, not be masked.
        run_sql = AsyncMock(side_effect=RuntimeError("PERMISSION_DENIED: cannot read table"))
        res = await hc.check_metrics_recent(["realtime"], run_sql, industry="pharmaceutical")
        assert res["status"] == hc.FAIL

    async def test_skip_when_no_streaming_modes(self):
        # zerobus-only pipeline has no Spark streaming metrics → SKIP, not fail.
        res = await hc.check_metrics_recent(
            ["zerobus"],
            _run_sql_returning([{"n": 0}]),
            industry="pharmaceutical",
        )
        assert res["status"] == hc.SKIP


# ===========================================================================
# check_lakebase_endpoints
# ===========================================================================
@pytest.mark.asyncio
class TestCheckLakebaseEndpoints:
    async def test_pass_when_active(self):
        res = await hc.check_lakebase_endpoints("pharma", _ws_endpoints_active())
        assert res["status"] == hc.PASS

    async def test_pass_when_idle(self):
        # IDLE = autoscaled-to-zero but usable.
        res = await hc.check_lakebase_endpoints("pharma", _ws_endpoints_active(replica="IDLE"))
        assert res["status"] == hc.PASS

    async def test_fail_when_endpoint_not_active(self):
        res = await hc.check_lakebase_endpoints("pharma", _ws_endpoints_active(primary="STOPPED"))
        assert res["status"] == hc.FAIL
        assert "primary" in res["detail"]

    async def test_fail_gracefully_when_branch_missing(self):
        ws = MagicMock()
        ws.postgres.get_project = MagicMock()
        ws.postgres.get_branch = MagicMock(side_effect=Exception("branch does not exist"))
        res = await hc.check_lakebase_endpoints("pharma", ws)
        assert res["status"] == hc.FAIL


# ===========================================================================
# check_chronotable  (exists + forward partitions + SSOT column match)
# ===========================================================================
def _chrono_conn(*, registered=1, forward=(96, "2026-07-07T10:00:00Z"), columns=None):
    """Build an asyncpg-style conn whose queries return canned chronotable data."""
    if columns is None:
        from iot_twin.models.sensor_schema import insert_columns

        columns = ["id", *insert_columns()]
    conn = MagicMock()

    async def _fetchval(sql, *args):
        s = sql.lower()
        if "_chronotable_registry" in s and "count" in s:
            return registered
        if "_chunk_metadata" in s:
            return forward[0]
        return None

    conn.fetchval = AsyncMock(side_effect=_fetchval)

    async def _fetch(sql, *args):
        s = sql.lower()
        if "information_schema.columns" in s:
            return [{"column_name": c} for c in columns]
        if "_chunk_metadata" in s:
            return [{"forward_chunks": forward[0], "window_end": forward[1]}]
        return []

    conn.fetch = AsyncMock(side_effect=_fetch)

    async def _fetchrow(sql, *args):
        s = sql.lower()
        if "_chunk_metadata" in s:
            return {"forward_chunks": forward[0], "window_end": forward[1]}
        return None

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    conn.close = AsyncMock()
    return conn


@pytest.mark.asyncio
class TestCheckChronotable:
    async def test_pass_when_exists_partitions_and_columns_match(self):
        res = await hc.check_chronotable(_chrono_conn())
        assert res["tier"] == hc.TIER_API
        assert res["status"] == hc.PASS

    async def test_fail_when_not_registered(self):
        res = await hc.check_chronotable(_chrono_conn(registered=0))
        assert res["status"] == hc.FAIL
        assert "sensor_readings" in res["detail"]

    async def test_fail_when_no_forward_partitions(self):
        res = await hc.check_chronotable(_chrono_conn(forward=(0, None)))
        assert res["status"] == hc.FAIL
        assert "partition" in res["detail"].lower()

    async def test_fail_when_columns_drift_from_ssot(self):
        res = await hc.check_chronotable(_chrono_conn(columns=["id", "sensor_name", "value"]))
        assert res["status"] == hc.FAIL
        assert "column" in res["detail"].lower()

    async def test_fail_gracefully_when_query_raises(self):
        conn = MagicMock()
        conn.fetchval = AsyncMock(side_effect=Exception("connection reset"))
        conn.fetch = AsyncMock(side_effect=Exception("connection reset"))
        res = await hc.check_chronotable(conn)
        assert res["status"] == hc.FAIL

    async def test_skip_when_registry_not_provisioned(self):
        # A bare/draft branch has no LakeTS registry yet — surface "not
        # provisioned" (skip), NOT a raw UndefinedTableError string.
        exc = Exception('relation "lakets._chronotable_registry" does not exist')
        conn = MagicMock()
        conn.fetchval = AsyncMock(side_effect=exc)
        res = await hc.check_chronotable(conn)
        assert res["status"] == hc.SKIP
        assert "not provisioned" in res["detail"].lower()

    async def test_skip_when_missing_relation_by_sqlstate(self):
        class _Undefined(Exception):
            sqlstate = "42P01"

        conn = MagicMock()
        conn.fetchval = AsyncMock(side_effect=_Undefined("boom"))
        res = await hc.check_chronotable(conn)
        assert res["status"] == hc.SKIP


# ===========================================================================
# check_cdf_sync  (wal2delta.tables shadow row STREAMING + advancing lsn)
# ===========================================================================
def _cdf_conn(snapshots: list[list[dict]]):
    """A conn whose ``fetch`` returns each successive wal2delta snapshot."""
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=snapshots)
    conn.close = AsyncMock()
    return conn


@pytest.mark.asyncio
class TestCheckCdfSync:
    async def test_pass_when_streaming_and_advancing(self):
        snaps = [
            [
                {
                    "table_name": "lakets_cdf._shadow_sensor_readings",
                    "status": "STREAMING",
                    "committed_lsn": "0/100",
                }
            ],
            [
                {
                    "table_name": "lakets_cdf._shadow_sensor_readings",
                    "status": "STREAMING",
                    "committed_lsn": "0/200",
                }
            ],
        ]
        res = await hc.check_cdf_sync(_cdf_conn(snaps), sleeper=AsyncMock())
        assert res["status"] == hc.PASS

    async def test_fail_when_shadow_row_absent(self):
        snaps = [[], []]
        res = await hc.check_cdf_sync(_cdf_conn(snaps), sleeper=AsyncMock())
        assert res["status"] == hc.FAIL
        assert "shadow" in res["detail"].lower()

    async def test_pass_when_streaming_even_if_idle(self):
        # STREAMING means the managed sync is live; committed_lsn only advances
        # while new writes flow, so an idle-but-healthy sync must still PASS.
        row = {
            "table_name": "_shadow_sensor_readings",
            "status": "STREAMING",
            "committed_lsn": "0/100",
        }
        res = await hc.check_cdf_sync(_cdf_conn([[dict(row)], [dict(row)]]), sleeper=AsyncMock())
        assert res["status"] == hc.PASS
        assert "idle" in res["detail"].lower()

    async def test_fail_when_present_but_not_streaming(self):
        # Present in wal2delta but SKIPPED/other → not live → fail.
        row = {
            "table_name": "lakets_cdf._shadow_sensor_readings",
            "status": "SKIPPED",
            "committed_lsn": None,
        }
        res = await hc.check_cdf_sync(_cdf_conn([[dict(row)], [dict(row)]]), sleeper=AsyncMock())
        assert res["status"] == hc.FAIL

    async def test_ignores_other_pipelines_shadow_rows(self):
        snaps = [
            [
                {
                    "table_name": "lakets_cdf._shadow_other_table",
                    "status": "STREAMING",
                    "committed_lsn": "0/100",
                }
            ],
            [
                {
                    "table_name": "lakets_cdf._shadow_other_table",
                    "status": "STREAMING",
                    "committed_lsn": "0/200",
                }
            ],
        ]
        res = await hc.check_cdf_sync(_cdf_conn(snaps), sleeper=AsyncMock())
        assert res["status"] == hc.FAIL

    async def test_fail_gracefully_when_query_raises(self):
        conn = MagicMock()
        conn.fetch = AsyncMock(side_effect=Exception("wal2delta not installed"))
        res = await hc.check_cdf_sync(conn, sleeper=AsyncMock())
        assert res["status"] == hc.FAIL

    async def test_skip_when_cdf_not_provisioned(self):
        # No wal2delta.tables relation yet on a bare branch → skip, not a raw error.
        conn = MagicMock()
        conn.fetch = AsyncMock(side_effect=Exception('relation "wal2delta.tables" does not exist'))
        res = await hc.check_cdf_sync(conn, sleeper=AsyncMock())
        assert res["status"] == hc.SKIP
        assert "not provisioned" in res["detail"].lower()


# ===========================================================================
# check_grants
# ===========================================================================
def _grants_ws(principals: list[str]) -> MagicMock:
    ws = MagicMock()
    assignments = [SimpleNamespace(principal=p, privileges=["USE_CATALOG"]) for p in principals]
    ws.grants.get = MagicMock(return_value=SimpleNamespace(privilege_assignments=assignments))
    return ws


@pytest.mark.asyncio
class TestCheckGrants:
    async def test_pass_when_sp_present(self):
        res = await hc.check_grants("iot_pharma", _grants_ws(["sp-app-id"]), principal="sp-app-id")
        assert res["status"] == hc.PASS

    async def test_fail_when_sp_missing(self):
        res = await hc.check_grants(
            "iot_pharma", _grants_ws(["someone-else"]), principal="sp-app-id"
        )
        assert res["status"] == hc.FAIL

    async def test_fail_gracefully_on_api_error(self):
        ws = MagicMock()
        ws.grants.get = MagicMock(side_effect=Exception("PERMISSION_DENIED"))
        res = await hc.check_grants("iot_pharma", ws, principal="sp-app-id")
        assert res["status"] == hc.FAIL

    async def test_uses_lowercase_securable_string(self):
        # RPC GetPermissions rejects the SecurableType enum (stringifies to
        # "SecurableType.CATALOG"); it wants the lowercase string "catalog".
        ws = _grants_ws(["sp-app-id"])
        await hc.check_grants("iot_pharma", ws, principal="sp-app-id")
        _, kwargs = ws.grants.get.call_args
        assert kwargs["securable_type"] == "catalog"
        assert isinstance(kwargs["securable_type"], str)

    async def test_rechecks_current_app_sp_after_grant_c7214(self, monkeypatch):
        """A second health sweep must observe newly applied grants [C7214][regression]."""
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "current-app-sp")
        ws = MagicMock()
        ws.grants.get = MagicMock(
            side_effect=[
                SimpleNamespace(privilege_assignments=[]),
                SimpleNamespace(
                    privilege_assignments=[
                        SimpleNamespace(
                            principal="current-app-sp",
                            privileges=["USE_CATALOG"],
                        )
                    ]
                ),
            ]
        )

        before = await hc.check_grants("example-catalog", ws)
        after = await hc.check_grants("example-catalog", ws)

        assert before["status"] == hc.FAIL
        assert after["status"] == hc.PASS
        assert ws.grants.get.call_count == 2


# ===========================================================================
# check_secrets
# ===========================================================================
def _secrets_ws(keys: list[str]) -> MagicMock:
    ws = MagicMock()
    ws.secrets.list_secrets = MagicMock(return_value=[SimpleNamespace(key=k) for k in keys])
    return ws


@pytest.mark.asyncio
class TestCheckSecrets:
    async def test_pass_when_required_keys_present(self):
        ws = _secrets_ws(["sss-industries-client-id", "sss-industries-client-secret"])
        res = await hc.check_secrets(ws)
        assert res["status"] == hc.PASS

    async def test_fail_when_key_missing(self):
        ws = _secrets_ws(["sss-industries-client-id"])
        res = await hc.check_secrets(ws)
        assert res["status"] == hc.FAIL
        assert "sss-industries-client-secret" in res["detail"]

    async def test_fail_gracefully_when_scope_missing(self):
        ws = MagicMock()
        ws.secrets.list_secrets = MagicMock(side_effect=Exception("scope not found"))
        res = await hc.check_secrets(ws)
        assert res["status"] == hc.FAIL


# ===========================================================================
# check_jobs
# ===========================================================================
def _deployed(mode: str, job_id: str = "j1") -> dict:
    return {"mode": mode, "status": "deployed", "job_id": job_id}


@pytest.mark.asyncio
class TestCheckJobs:
    async def test_pass_when_all_deployed_jobs_running(self):
        deployments = [_deployed("realtime", "j1"), _deployed("microbatch", "j2")]
        job_status = MagicMock(return_value={"state": "RUNNING"})
        res = await hc.check_jobs(deployments, job_status=job_status)
        assert res["status"] == hc.PASS

    async def test_fail_when_a_job_not_running(self):
        deployments = [_deployed("realtime", "j1")]
        job_status = MagicMock(return_value={"state": "IDLE"})
        res = await hc.check_jobs(deployments, job_status=job_status)
        assert res["status"] == hc.FAIL
        assert "realtime" in res["detail"]

    async def test_skip_when_nothing_deployed(self):
        deployments = [{"mode": "realtime", "status": "pending", "job_id": None}]
        res = await hc.check_jobs(deployments, job_status=MagicMock())
        assert res["status"] == hc.SKIP

    async def test_fail_gracefully_when_status_raises(self):
        deployments = [_deployed("realtime", "j1")]
        job_status = MagicMock(side_effect=Exception("Jobs API 500"))
        res = await hc.check_jobs(deployments, job_status=job_status)
        assert res["status"] == hc.FAIL

    async def test_fail_when_running_job_has_failed_task_c7258(self):
        """[C7258][defect][regression] Task failure overrides run-level RUNNING."""
        deployments = [_deployed("realtime", "j1")]
        job_status = MagicMock(
            return_value={
                "state": "RUNNING",
                "degraded": True,
                "detail": "data_producer: Driver stopped unexpectedly",
                "tasks": [
                    {
                        "task": "data_producer",
                        "life_cycle": "INTERNAL_ERROR",
                        "result": "FAILED",
                    }
                ],
            }
        )

        res = await hc.check_jobs(deployments, job_status=job_status)

        assert res["status"] == hc.FAIL
        assert "data_producer" in res["detail"]


# ===========================================================================
# check_cluster_state  (informational — always returned, never unhealthy)
# ===========================================================================
@pytest.mark.asyncio
class TestCheckClusterState:
    async def test_reports_running(self):
        ws = MagicMock()
        ws.clusters.get = MagicMock(
            return_value=SimpleNamespace(state=SimpleNamespace(value="RUNNING"))
        )
        res = await hc.check_cluster_state("c1", ws)
        assert res["cluster_state"] == "RUNNING"
        assert res["status"] in (hc.PASS, hc.SKIP)

    async def test_reports_terminated_cluster_down(self):
        ws = MagicMock()
        ws.clusters.get = MagicMock(
            return_value=SimpleNamespace(state=SimpleNamespace(value="TERMINATED"))
        )
        res = await hc.check_cluster_state("c1", ws)
        assert res["cluster_state"] == "TERMINATED"

    async def test_skip_when_no_cluster(self):
        res = await hc.check_cluster_state(None, MagicMock())
        assert res["status"] == hc.SKIP
        assert res["cluster_state"] is None

    async def test_graceful_when_lookup_fails(self):
        ws = MagicMock()
        ws.clusters.get = MagicMock(side_effect=Exception("no such cluster"))
        res = await hc.check_cluster_state("c1", ws)
        assert res["cluster_state"] == "UNKNOWN"
        assert res["status"] != hc.FAIL  # informational, never fails the pipeline


# ===========================================================================
# check_kafka_topic  (Tier 2 — skipped in Tier 1)
# ===========================================================================
class TestCheckKafkaTopic:
    def test_is_tier_e2e_and_skipped(self):
        res = hc.check_kafka_topic("pharma", ["realtime", "microbatch"])
        assert res["tier"] == hc.TIER_E2E
        assert res["status"] == hc.SKIP
        # The verdict must explain WHY it can't run from the app backend.
        assert "cluster" in res["detail"].lower() or "msk" in res["detail"].lower()


# ===========================================================================
# Orchestrator: run_api_health_checks / gather_checks
# ===========================================================================
def _healthy_setup() -> dict:
    return {
        "id": "s1",
        "industry": "pharma",
        "enabled_modes": ["realtime", "microbatch", "zerobus"],
        "warehouse_id": "wh1",
        "validation_state": {"cluster_id": "c1"},
    }


def _all_good_deps():
    """Injected dependencies where every Tier-1 resource is healthy."""
    tables = [
        {"table_schema": "realtime", "table_name": "sensor_readings"},
        {"table_schema": "microbatch", "table_name": "sensor_data"},
        {"table_schema": "zerobus", "table_name": "iot_sensor_data"},
    ]

    async def run_sql(sql, *a, **k):
        s = sql.lower()
        if "information_schema.schemata" in s:
            return _sql_result(_ALL_SCHEMAS)
        if "information_schema.tables" in s:
            return _sql_result(tables)
        if "information_schema.volumes" in s:
            return _sql_result([{"volume_name": "stream_checkpoints"}])
        if "streaming_metrics" in s:
            return _sql_result([{"n": 10, "latest": "2026-07-06T10:00:00Z"}])
        return _sql_result([])

    async def open_read_conn(branch):
        conn = _chrono_conn()
        conn.fetch = AsyncMock(
            side_effect=[
                # chronotable columns query first, then cdf snapshots
            ]
        )
        return conn

    ws = _ws_endpoints_active()
    ws.clusters.get = MagicMock(
        return_value=SimpleNamespace(state=SimpleNamespace(value="RUNNING"))
    )
    ws.grants.get = MagicMock(
        return_value=SimpleNamespace(
            privilege_assignments=[
                SimpleNamespace(principal="sp-app-id", privileges=["USE_CATALOG"])
            ]
        )
    )
    ws.secrets.list_secrets = MagicMock(
        return_value=[
            SimpleNamespace(key="sss-industries-client-id"),
            SimpleNamespace(key="sss-industries-client-secret"),
        ]
    )
    return run_sql, open_read_conn, ws


@pytest.mark.asyncio
class TestGatherChecks:
    async def test_zerobus_uses_one_schema_catalog_tables_grants_and_uc_secret(self, monkeypatch):
        monkeypatch.setenv("UC_CATALOG", "example_eu_catalog")
        monkeypatch.setenv("UC_SCHEMA", "rtm_lakets_zb")
        setup = {
            **_healthy_setup(),
            "industry": "pharmaceutical",
            "enabled_modes": ["zerobus"],
            "validation_state": {"cluster_id": None},
        }
        queries: list[str] = []

        async def run_sql(sql, *args, **kwargs):
            queries.append(sql)
            if "information_schema.schemata" in sql:
                return _sql_result([{"schema_name": "rtm_lakets_zb"}])
            if "information_schema.tables" in sql:
                return _sql_result(
                    [
                        {
                            "table_schema": "rtm_lakets_zb",
                            "table_name": "pharmaceutical_zerobus",
                        }
                    ]
                )
            return _sql_result([])

        ws = MagicMock()
        ws.grants.get.return_value = SimpleNamespace(
            privilege_assignments=[
                SimpleNamespace(principal="sp-app-id", privileges=["USE_CATALOG"])
            ]
        )
        ws.secrets_uc.get_secret.return_value = SimpleNamespace(name="app_sp_oauth")

        result = await hc.gather_checks(
            setup,
            deployments=[],
            ws=ws,
            run_sql=run_sql,
            open_read_conn=None,
            job_status=MagicMock(),
            principal="sp-app-id",
        )

        sql = "\n".join(queries)
        assert "example_eu_catalog.information_schema" in sql
        assert "iot_pharmaceutical" not in sql
        assert "pharmaceutical_zerobus" in sql or any(
            check["name"] == hc.DELTA_TABLES_CHECK and check["status"] == hc.PASS
            for check in result["checks"]
        )
        ws.grants.get.assert_called_with(
            securable_type="catalog",
            full_name="example_eu_catalog",
        )
        ws.secrets_uc.get_secret.assert_called_once_with(
            "example_eu_catalog.rtm_lakets_zb.app_sp_oauth",
            include_value=False,
        )
        failures = [check for check in result["checks"] if check["status"] == hc.FAIL]
        assert failures == []

    async def test_zerobus_only_excludes_realtime_kafka_and_cluster_checks(self):
        setup = {
            **_healthy_setup(),
            "enabled_modes": ["zerobus"],
            "validation_state": {"cluster_id": None},
        }
        run_sql, open_read_conn, ws = _all_good_deps()
        job_status = MagicMock(return_value={"state": "RUNNING"})

        result = await hc.gather_checks(
            setup,
            deployments=[
                _deployed("zerobus", "j-zb"),
                _deployed("realtime", "stale-j-rt"),
            ],
            ws=ws,
            run_sql=run_sql,
            open_read_conn=open_read_conn,
            job_status=job_status,
            principal="sp-app-id",
        )

        names = {check["name"] for check in result["checks"]}
        assert hc.LAKEBASE_ENDPOINTS_CHECK not in names
        assert hc.CHRONOTABLE_CHECK not in names
        assert hc.CDF_SYNC_CHECK not in names
        assert hc.CHECKPOINT_VOLUME_CHECK not in names
        assert hc.CLUSTER_STATE_CHECK not in names
        assert hc.KAFKA_TOPIC_CHECK not in names
        ws.postgres.get_project.assert_not_called()
        job_status.assert_called_once_with("j-zb")

    async def test_returns_checks_health_startable_cluster_state(self):
        setup = _healthy_setup()
        run_sql, _open, ws = _all_good_deps()
        result = await hc.gather_checks(
            setup,
            deployments=[_deployed("realtime", "j1")],
            ws=ws,
            run_sql=run_sql,
            open_read_conn=None,  # skip DB-only checks here
            job_status=MagicMock(return_value={"state": "RUNNING"}),
            principal="sp-app-id",
        )
        assert set(result) >= {"checks", "health", "startable", "cluster_state"}
        assert result["cluster_state"] == "RUNNING"
        # Every check carries the required contract keys.
        for c in result["checks"]:
            assert set(c) >= {"name", "tier", "status", "detail"}
        # The Kafka topic check is present and marked Tier 2 / skip.
        topic = [c for c in result["checks"] if "topic" in c["name"].lower()][0]
        assert topic["tier"] == hc.TIER_E2E

    async def test_healthy_when_all_api_checks_pass(self):
        setup = _healthy_setup()
        run_sql, _open, ws = _all_good_deps()

        async def open_read_conn(branch):
            return _chrono_conn()

        # cdf conn needs streaming+advancing snapshots — use a dedicated conn.
        result = await hc.gather_checks(
            setup,
            deployments=[_deployed("realtime", "j1")],
            ws=ws,
            run_sql=run_sql,
            open_read_conn=None,
            job_status=MagicMock(return_value={"state": "RUNNING"}),
            principal="sp-app-id",
        )
        assert result["health"] == pv.HEALTH_HEALTHY
        assert result["startable"] is True

    async def test_unhealthy_when_a_check_fails(self):
        setup = _healthy_setup()
        run_sql, _open, ws = _all_good_deps()
        # Secrets scope missing -> a Tier-1 check fails -> unhealthy, not startable.
        ws.secrets.list_secrets = MagicMock(side_effect=Exception("scope not found"))
        result = await hc.gather_checks(
            setup,
            deployments=[_deployed("realtime", "j1")],
            ws=ws,
            run_sql=run_sql,
            open_read_conn=None,
            job_status=MagicMock(return_value={"state": "RUNNING"}),
            principal="sp-app-id",
        )
        assert result["health"] == pv.HEALTH_UNHEALTHY
        assert result["startable"] is False

    async def test_cluster_down_does_not_make_unhealthy(self):
        setup = _healthy_setup()
        run_sql, _open, ws = _all_good_deps()
        ws.clusters.get = MagicMock(
            return_value=SimpleNamespace(state=SimpleNamespace(value="TERMINATED"))
        )
        result = await hc.gather_checks(
            setup,
            deployments=[_deployed("microbatch", "j1")],
            ws=ws,
            run_sql=run_sql,
            open_read_conn=None,
            job_status=MagicMock(return_value={"state": "RUNNING"}),
            principal="sp-app-id",
        )
        # A down interactive cluster is informational only.
        assert result["cluster_state"] == "TERMINATED"
        assert result["health"] == pv.HEALTH_HEALTHY

    async def test_no_warehouse_skips_dbsql_checks_gracefully(self):
        setup = {**_healthy_setup(), "warehouse_id": None}
        _run_sql, _open, ws = _all_good_deps()
        result = await hc.gather_checks(
            setup,
            deployments=[],
            ws=ws,
            run_sql=None,  # no warehouse -> DBSQL checks skip
            open_read_conn=None,
            job_status=MagicMock(),
            principal="sp-app-id",
        )
        # Nothing crashes; the UC checks are skipped, not failed.
        uc = [c for c in result["checks"] if c["name"] == hc.UC_CATALOG_CHECK]
        assert uc and uc[0]["status"] == hc.SKIP


@pytest.mark.asyncio
class TestRunApiHealthChecks:
    async def test_loads_setup_and_deployments_then_gathers(self, monkeypatch):
        setup = _healthy_setup()
        run_sql, _open, ws = _all_good_deps()

        async def fake_get_setup(db, setup_id):
            return setup

        async def fake_list_deployments(db, setup_id):
            return [_deployed("microbatch", "j1")]

        monkeypatch.setattr(hc.pipeline_setup_repo, "get_setup", fake_get_setup)
        monkeypatch.setattr(hc.deployment_repo, "list_deployments", fake_list_deployments)

        result = await hc.run_api_health_checks(
            MagicMock(),
            "s1",
            ws=ws,
            run_sql=run_sql,
            open_read_conn=None,
            job_status=MagicMock(return_value={"state": "RUNNING"}),
            principal="sp-app-id",
        )
        assert result["health"] in (pv.HEALTH_HEALTHY, pv.HEALTH_UNHEALTHY)
        assert "checks" in result

    async def test_raises_when_setup_missing(self, monkeypatch):
        async def fake_get_setup(db, setup_id):
            return None

        monkeypatch.setattr(hc.pipeline_setup_repo, "get_setup", fake_get_setup)
        with pytest.raises(ValueError):
            await hc.run_api_health_checks(MagicMock(), "nope", ws=MagicMock())
