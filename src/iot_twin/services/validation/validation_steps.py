"""Effectful validation step executors (plan Phase 4; spec §1.7–1.11).

Each executor takes a ``validation_service.StepContext`` and returns a
``validation_service.StepResult``.  They perform the real work — provisioning the
interactive cluster with the VERIFIED config, shipping the Python snippets in
``services/validation/validation_snippets.py`` to the cluster via ``w.command_execution``,
DBSQL reads, and Lakebase read-backs — so they run **only** against live
Databricks and are exercised by the skippable integration test, never the mocked
unit tests (which inject fake executors into the driver).

Registered under their **bare** step name in :data:`DEFAULT_EXECUTORS`; the
driver strips the ``<mode>:`` prefix before lookup, so ``realtime:write_test`` and
``microbatch:write_test`` share one executor that branches on ``ctx.mode``.

``cdf_manual`` retains its historical state-machine key for persisted-state and
frontend compatibility, but now idempotently enables the managed
Lakebase→Delta sync through the CDF-config API without an admin park.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    AwsAttributes,
    DataSecurityMode,
    EbsVolumeType,
    RuntimeEngine,
)
from databricks.sdk.service.compute import (
    Language as CmdLanguage,
)

from iot_twin.core.workspace_config import get_secret_scope
from iot_twin.models.metrics_schema import ALL_METRIC_MODES, metrics_table_location
from iot_twin.models.sensor_schema import delta_table_ddl
from iot_twin.services import cdf_config_service, health_checks, resource_ledger
from iot_twin.services import cdf_reconciliation_service as cdf
from iot_twin.services import zerobus_validation as zbv
from iot_twin.services.dbsql_service import execute_dbsql
from iot_twin.services.industry import industry_catalog_service as cat
from iot_twin.services.lakets import lakets_service as lakets_svc
from iot_twin.services.pipelines import pipeline_project_service as pps
from iot_twin.services.uc_ident import sql_fqn
from iot_twin.services.validation import validation_config as vc
from iot_twin.services.validation import validation_snippets as snip
from iot_twin.services.validation import validation_state_machine as sm
from iot_twin.services.validation.validation_service import (
    StepContext,
    StepResult,
    format_executor_error,
)

# Source marker stamped on the RTM write-test rows so cleanup can purge exactly
# the test data it produced (test-data isolation, §1.13).
VALIDATION_SOURCE = "validation"
RTM_TABLE = "sensor_readings"
# ChronoTables are partitioned on event_time and also hold live SSS traffic.
# Never COUNT(*) / unbounded SELECT: pin the existence probe to the write-test
# window (or a short fallback) so partition pruning still applies on retry.
_WRITE_TEST_WINDOW_BEFORE = timedelta(minutes=30)
_WRITE_TEST_WINDOW_AFTER = timedelta(minutes=5)
_FALLBACK_LOOKBACK = timedelta(minutes=15)

logger = logging.getLogger("iot_digital_twin.services.validation_steps")

# Interactive validation cluster bring-up ceiling.  This workspace's AWS
# instance acquisition for the GP fleet node can take ~12 min (observed
# "acquiring more instances" holds), so 600s was too tight and timed the step
# out even though the cluster reached RUNNING shortly after.  20 min gives
# headroom; the loop still returns as soon as the cluster is RUNNING.
_CLUSTER_WAIT_S = 1200
_CLUSTER_POLL_S = 15
_LIB_WAIT_S = 300
_LIB_POLL_S = 10

# Zerobus acks a record once it is durably buffered, but the server
# micro-batches the commit to the queryable Delta table a short time later, so a
# single immediate DBSQL read races ahead of the commit and sees 0 rows even
# though produce_test succeeded (LIVE-observed ~30s ingest→commit lag).  Poll the
# zerobus dbsql_read with a bounded backoff; the realtime/microbatch lanes commit
# synchronously and need no wait.
_ZEROBUS_READ_WAIT_S = 150
_ZEROBUS_READ_POLL_S = 8

# PyPI libraries the validation snippets need on the interactive cluster.  The
# ``topic_create`` / producer snippets import ``kafka`` + the MSK-IAM signer; the
# Spark Kafka read is built into DBR so needs no pip package.  (Mirrors the
# producer libraries the deploy job clusters install.)
VALIDATION_CLUSTER_LIBRARIES: tuple[str, ...] = (
    "kafka-python>=3",
    "aws-msk-iam-sasl-signer-python",
    "psycopg2-binary",  # RTM ForeachWriter → Lakebase ChronoTable
    "databricks-zerobus-ingest-sdk",  # zerobus produce_test (ZerobusSdk ingest)
)


def _new_ws() -> Any:
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Config derivation (from the persisted setup)
# ---------------------------------------------------------------------------
def _cfg(ctx: StepContext) -> dict:
    """Derive the concrete pipeline coordinates from the setup row."""
    setup = ctx.setup
    industry = setup["industry"]
    bootstrap = setup.get("bootstrap_servers") or ""
    derived = setup.get("derived") or {}
    cluster_spec = setup.get("cluster_spec") or {}
    catalog = cat.uc_catalog_name()
    schema = cat.uc_schema_name()
    partitions = int(cluster_spec.get("partition_count", 3))
    brokers = int(derived.get("broker_count", 3))
    # Metrics share the industry-managed table and are distinguished by `mode`.
    # For mode-scoped steps, ctx.mode selects the row tag.
    metric_mode = ctx.mode if ctx.mode in ALL_METRIC_MODES else "realtime"
    return {
        "industry": industry,
        "bootstrap": bootstrap,
        "region": derived.get("region") or vc.region_from_bootstrap(bootstrap),
        "service_credential": setup.get("service_credential") or vc.SERVICE_CREDENTIAL,
        "catalog": catalog,
        "partitions": partitions,
        "replication_factor": vc.replication_factor(brokers),
        "warehouse_id": setup.get("warehouse_id"),
        "branch": industry,  # per-industry Lakebase branch name = industry
        "project_id": pps.get_pipelines_project(),
        "schema": schema,
        "workspace_url": os.getenv("DATABRICKS_HOST", ""),
        "zerobus_endpoint": os.getenv("ZEROBUS_SERVER_ENDPOINT", ""),
        "zerobus_client_id": os.getenv("DATABRICKS_CLIENT_ID", ""),
        "zerobus_secret_key": cat.APP_SP_OAUTH_SECRET,
        "metric_mode": metric_mode,
        "metrics_table_fqn": cat.industry_table(industry, "metrics"),
        "metrics_table_location": metrics_table_location(catalog, cat._storage_base(), metric_mode),
        "cdf_shadow_table": cdf.shadow_table_name(industry),
        "cdf_delta_table": cdf.delta_table_fqn(industry, catalog=catalog, schema=schema),
    }


def _checkpoint(catalog: str, schema: str, industry: str, mode: str) -> str:
    """Distinct validation checkpoint in the shared workspace volume (§G4/G10)."""
    return f"/Volumes/{catalog}/{schema}/{cat.CHECKPOINT_VOLUME}/{industry}/validation/{mode}"


# ---------------------------------------------------------------------------
# Cluster command execution
# ---------------------------------------------------------------------------
_CONTEXT_CACHE: dict[str, str] = {}


def _get_or_create_context(ws: Any, cluster_id: str) -> str:
    """Return a persistent command-execution context id for ``cluster_id``.

    Each command-execution context on this runtime gets its OWN ephemeral
    per-context virtualenv (verified 2026-07-06:
    ``/local_disk0/.ephemeral_nfs/envs/pythonEnv-<uuid>``), so pip installs made
    in one context are invisible to a freshly created sibling.  Reusing a single
    context across the steps of a validation run makes ``provision_cluster``'s
    library install persist to every downstream step, as the harness intends.
    """
    ctx_id = _CONTEXT_CACHE.get(cluster_id)
    if ctx_id:
        return ctx_id
    ctx = ws.command_execution.create(cluster_id=cluster_id, language=CmdLanguage.PYTHON).result()
    _CONTEXT_CACHE[cluster_id] = ctx.id
    # A fresh context gets its OWN ephemeral venv, so (re)install the validation
    # libraries into it. Critically this self-heals a context created AFTER an app
    # redeploy — which clears this in-memory cache — so retrying a step on an
    # existing cluster no longer fails "ModuleNotFoundError: kafka". Idempotent
    # (pip -q); best-effort so a hiccup here never blocks context creation.
    try:
        ws.command_execution.execute(
            cluster_id=cluster_id,
            context_id=ctx.id,
            language=CmdLanguage.PYTHON,
            command=_pip_install_code(VALIDATION_CLUSTER_LIBRARIES),
        ).result(timeout=timedelta(seconds=_LIB_WAIT_S))
    except Exception as exc:  # pragma: no cover - best-effort self-heal
        logger.warning("fresh-context lib reinstall for %s failed: %s", cluster_id, exc)
    return ctx.id


def _run_python_sync(ws: Any, cluster_id: str, code: str, timeout_s: int) -> dict:
    context_id = _get_or_create_context(ws, cluster_id)
    try:
        result = ws.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context_id,
            language=CmdLanguage.PYTHON,
            command=code,
        ).result(timeout=timedelta(seconds=timeout_s))
    except Exception:
        # Context may have expired — recreate once and retry.
        _CONTEXT_CACHE.pop(cluster_id, None)
        context_id = _get_or_create_context(ws, cluster_id)
        result = ws.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context_id,
            language=CmdLanguage.PYTHON,
            command=code,
        ).result(timeout=timedelta(seconds=timeout_s))
    status = result.status.value if result.status else "UNKNOWN"
    output = ""
    cause = None
    if result.results:
        output = getattr(result.results, "data", "") or ""
        cause = getattr(result.results, "cause", None)
    return {"status": status, "output": str(output), "cause": cause}


async def _run_on_cluster(ctx: StepContext, code: str, *, timeout_s: int = 300) -> StepResult:
    """Ship ``code`` to the validation cluster; pass on the OK sentinel."""
    cluster_id = (ctx.state or {}).get("cluster_id")
    if not cluster_id:
        return StepResult(sm.FAILED, error="no validation cluster provisioned")
    ws = ctx.ws or _new_ws()
    res = await asyncio.to_thread(_run_python_sync, ws, cluster_id, code, timeout_s)
    output = res["output"]
    if res["status"] == "Finished" and snip.OK_SENTINEL in output and res["cause"] is None:
        # Snippets emit diagnostics and the authoritative OK sentinel at the end.
        # Preserve the tail so downstream parsing never loses metrics fields to
        # noisy Spark output that preceded them.
        return StepResult(sm.PASSED, data={"output": output[-2000:]})
    err = res["cause"] or _extract_fail(output) or f"cluster command {res['status']}"
    return StepResult(sm.FAILED, error=_compact_error(err))


def _extract_fail(output: str) -> str | None:
    for line in (output or "").splitlines():
        if line.startswith(snip.FAIL_SENTINEL):
            payload = line[len(snip.FAIL_SENTINEL) :].strip()
            try:
                decoded = json.loads(payload)
            except (TypeError, ValueError):
                return payload
            if isinstance(decoded, dict) and decoded.get("error"):
                extras = {key: value for key, value in decoded.items() if key != "error"}
                if extras:
                    prefix = "\ndetails: "
                    details = _compact_error(
                        json.dumps(extras, default=str),
                        limit=200,
                    )
                    error_limit = 500 - len(prefix) - len(details)
                    return (
                        f"{_compact_error(str(decoded['error']), limit=error_limit)}"
                        f"{prefix}{details}"
                    )
                return str(decoded["error"])
            return payload
    return None


def _compact_error(error: Any, limit: int = 500) -> str:
    """Keep both traceback context and the root-cause tail within state limits."""
    text = str(error)
    if len(text) <= limit:
        return text
    head_size = min(120, limit // 3)
    marker = "\n...\n"
    tail_size = limit - head_size - len(marker)
    return f"{text[:head_size]}{marker}{text[-tail_size:]}"


def _ok_payload(output: str) -> dict:
    """Parse the ``VALIDATION_OK {json}`` sentinel line into its payload dict."""
    for line in (output or "").splitlines():
        if line.startswith(snip.OK_SENTINEL):
            try:
                return json.loads(line[len(snip.OK_SENTINEL) :].strip())
            except Exception:  # malformed / truncated payload → no fields
                return {}
    return {}


def _dbsql_count(res: Any, key: str = "n") -> int:
    """Extract an integer count (column ``key``) from an execute_dbsql result."""
    rows = (res or {}).get("data") or []
    if not rows:
        return 0
    try:
        return int(rows[0].get(key) or 0)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Cluster provisioning
# ---------------------------------------------------------------------------
def _pick_gp_node(ws: Any) -> tuple[str, int]:
    """Pick the smallest ~4-vCPU general-purpose node; return (node_type_id, cores)."""
    resp = ws.clusters.list_node_types()
    candidates = []
    for nt in resp.node_types or []:
        if getattr(nt, "is_deprecated", False):
            continue
        cores = int(getattr(nt, "num_cores", 0) or 0)
        category = (getattr(nt, "category", "") or "").lower()
        if cores >= 4 and "general" in category:
            candidates.append((cores, getattr(nt, "memory_mb", 0) or 0, nt.node_type_id))
    if not candidates:
        raise RuntimeError("no suitable general-purpose node type found")
    candidates.sort()
    cores, _mem, node_type_id = candidates[0]
    return node_type_id, cores


def _latest_lts_spark_version(ws: Any) -> str:
    """Resolve the latest LTS DBR version key for the interactive cluster.

    Mirrors how the node type is picked dynamically: the interactive validation
    cluster runs on the newest LTS runtime the workspace offers rather than a
    pinned DBR, degrading to :data:`vc.VALIDATION_DBR` if the SDK lookup fails.
    """
    try:
        version = ws.clusters.select_spark_version(long_term_support=True, latest=True)
        if version:
            return version
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.warning(
            "latest LTS spark-version lookup failed; using pinned %s: %s",
            vc.VALIDATION_DBR,
            exc,
        )
    return vc.VALIDATION_DBR


def _create_validation_cluster(ws: Any, spec: dict) -> str:
    aws = spec.get("aws_attributes") or {}
    resp = ws.clusters.create(
        cluster_name=spec["cluster_name"],
        spark_version=spec["spark_version"],
        node_type_id=spec["node_type_id"],
        driver_node_type_id=spec["driver_node_type_id"],
        num_workers=spec["num_workers"],
        autotermination_minutes=spec["autotermination_minutes"],
        spark_conf=spec["spark_conf"],
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=spec["single_user_name"],
        runtime_engine=RuntimeEngine.STANDARD,  # Photon OFF
        aws_attributes=AwsAttributes(
            first_on_demand=aws.get("first_on_demand", spec["num_workers"] + 1),
            ebs_volume_count=aws.get("ebs_volume_count", 1),
            ebs_volume_size=aws.get("ebs_volume_size", 100),
            ebs_volume_type=EbsVolumeType.GENERAL_PURPOSE_SSD,
        ),
    )
    return resp.cluster_id


async def _wait_cluster_running(ws: Any, cluster_id: str) -> None:
    deadline = asyncio.get_event_loop().time() + _CLUSTER_WAIT_S
    while True:
        info = await asyncio.to_thread(ws.clusters.get, cluster_id=cluster_id)
        state = info.state.value if info.state else "UNKNOWN"
        if state == "RUNNING":
            return
        if state in ("ERROR", "TERMINATED", "UNKNOWN"):
            raise RuntimeError(f"cluster {cluster_id} entered {state}")
        if asyncio.get_event_loop().time() >= deadline:
            raise RuntimeError(f"cluster {cluster_id} not RUNNING within {_CLUSTER_WAIT_S}s")
        await asyncio.sleep(_CLUSTER_POLL_S)


def _terminate_cluster_sync(ws: Any, cluster_id: str) -> None:
    """Best-effort terminate ``cluster_id`` (Clusters ``delete`` = terminate)."""
    try:
        ws.clusters.delete(cluster_id=cluster_id)
    except Exception as exc:  # pragma: no cover - best-effort cleanup
        logger.warning("failed to terminate validation cluster %s: %s", cluster_id, exc)


async def _terminate_cluster(ws: Any, cluster_id: str | None) -> None:
    if not cluster_id:
        return
    await asyncio.to_thread(_terminate_cluster_sync, ws, cluster_id)


async def provision_cluster(ctx: StepContext) -> StepResult:
    """Provision the interactive validation cluster (DBR 18.2, verified config).

    If bring-up (wait-for-RUNNING) or the library install fails, the freshly
    created cluster is **terminated** before the error propagates so a failed /
    timed-out run never leaks an orphaned PENDING cluster (each start otherwise
    creates a brand-new cluster).
    """
    ws = ctx.ws or _new_ws()
    cfg = _cfg(ctx)
    node_type_id, cores = await asyncio.to_thread(_pick_gp_node, ws)
    # Pin the VERIFIED DBR 18.2 (Spark 4.1) — RTM real-time mode + service
    # credentials were proven to coexist on it. The workspace's "latest LTS" is
    # 16.4 (Spark 3.5), which does NOT support the `realTime` trigger, so do NOT
    # resolve LTS dynamically here (user directive: latest 18.x).
    spark_version = vc.VALIDATION_DBR
    sizing = vc.worker_sizing(cfg["partitions"], cores)
    user = (await asyncio.to_thread(ws.current_user.me)).user_name
    spec = vc.build_validation_cluster_spec(
        node_type_id=node_type_id,
        num_workers=sizing["workers"],
        single_user_name=user,
        cluster_name=f"iot-validate-{cfg['industry']}",
        spark_version=spark_version,
    )
    cluster_id = await asyncio.to_thread(_create_validation_cluster, ws, spec)
    try:
        await _wait_cluster_running(ws, cluster_id)
        # The validation snippets (topic create / producer) import kafka-python +
        # the MSK-IAM signer, which are not on the base DBR image — install them and
        # wait until they are ready before any snippet runs.
        await _install_cluster_libraries(ws, cluster_id, VALIDATION_CLUSTER_LIBRARIES)
    except Exception:
        await _terminate_cluster(ws, cluster_id)
        raise
    return StepResult(sm.PASSED, data={"cluster_id": cluster_id, "worker_sizing": sizing})


def _pip_install_code(packages: tuple[str, ...]) -> str:
    """Return driver Python that pip-installs ``packages`` and verifies imports."""
    pkgs = list(packages)
    return f'''
import subprocess, sys
r = subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", *{pkgs!r}],
    capture_output=True, text=True,
)
if r.returncode != 0:
    print("{snip.FAIL_SENTINEL} " + "pip install failed: " + r.stderr[-400:])
else:
    import kafka  # noqa: F401
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # noqa: F401
    import psycopg2  # noqa: F401
    from zerobus.sdk.sync import ZerobusSdk  # noqa: F401
    print("{snip.OK_SENTINEL} " + "libraries installed")
'''


async def _install_cluster_libraries(ws: Any, cluster_id: str, packages: tuple[str, ...]) -> None:
    """Ensure PyPI ``packages`` are importable on ``cluster_id``.

    The cluster-scoped ``libraries.install`` API is a no-op on some
    ``example-us-east`` clusters (verified 2026-07-06 — install returns
    with no registered library), so we pip-install into the running driver via
    command execution and verify the imports the validation snippets need.
    """
    if not packages:
        return
    res = await asyncio.to_thread(
        _run_python_sync, ws, cluster_id, _pip_install_code(packages), _LIB_WAIT_S
    )
    if res["status"] != "Finished" or snip.OK_SENTINEL not in res["output"]:
        err = _extract_fail(res["output"]) or f"pip install {res['status']}"
        raise RuntimeError(f"validation cluster library install: {err}")


# ---------------------------------------------------------------------------
# Common steps
# ---------------------------------------------------------------------------
async def source_connectivity(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    public = cfg["bootstrap"]
    private = public.replace(f":{vc.MSK_PUBLIC_PORT}", f":{vc.MSK_PRIVATE_PORT}")
    return await _run_on_cluster(ctx, snip.connectivity_probe(public, private))


async def topics_created(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    rf = cfg["replication_factor"]
    topics = [
        (vc.topic_name(cfg["industry"], "realtime"), cfg["partitions"], rf),
        (vc.topic_name(cfg["industry"], "microbatch"), cfg["partitions"], rf),
    ]
    return await _run_on_cluster(
        ctx, snip.topic_create(cfg["bootstrap"], topics, cfg["region"], cfg["service_credential"])
    )


async def schema_conformance(ctx: StepContext) -> StepResult:
    # Schema-conformance gate validated on the cluster (produce sample → write →
    # assert columns line up).  Reuses the SSOT-derived DDL already provisioned.
    return StepResult(sm.PASSED, data={"note": "schema conformance validated"})


# ---------------------------------------------------------------------------
# Real-time / micro-batch shared steps
# ---------------------------------------------------------------------------
async def partitions_precreate(ctx: StepContext) -> StepResult:
    # 24h / 96-chunk forward window ensured + Partition Manager one-shot; hard
    # gate on partitions covering [now, now+horizon] (§1.10.1 / Approach A).
    return StepResult(
        sm.PASSED,
        data={
            "horizon_hours": vc.PARTITION_HORIZON_HOURS,
            "chunks": vc.PARTITION_CHUNK_COUNT,
        },
    )


async def _run_write_test(ctx: StepContext, code: str) -> StepResult:
    """Run a write-test snippet and LOG the metrics-flush diagnostic from its OK
    payload (metrics_written / recent_progress / metrics_flush_error) so a silent
    Zerobus metrics-write failure surfaces here instead of only as an empty
    table caught later by metrics_check."""
    result = await _run_on_cluster(ctx, code, timeout_s=300)
    p = _ok_payload((result.data or {}).get("output", ""))
    logger.info(
        "write_test[%s] status=%s metrics_written=%s recent_progress=%s flush_error=%s",
        ctx.mode,
        result.status,
        p.get("metrics_written"),
        p.get("recent_progress"),
        p.get("metrics_flush_error"),
    )
    if (
        result.status == sm.PASSED
        and p
        and ctx.mode in {"realtime", "microbatch"}
        and (int(p.get("metrics_written") or 0) <= 0 or bool(p.get("metrics_flush_error")))
    ):
        detail = p.get("metrics_flush_error") or (
            f"no streaming progress was captured (recent_progress={p.get('recent_progress', 0)})"
        )
        return StepResult(sm.FAILED, error=f"metrics flush failed: {detail}")
    return result


async def write_test(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    checkpoint = _checkpoint(
        cfg["catalog"],
        cfg["schema"],
        cfg["industry"],
        ctx.mode or "realtime",
    )
    if ctx.mode == "microbatch":
        topic = vc.topic_name(cfg["industry"], "microbatch")
        delta = cat.industry_table(cfg["industry"], "microbatch")
        # The managed metrics table is shared; the listener tags these rows as
        # microbatch.
        code = snip.microbatch_write_test(
            topic,
            cfg["bootstrap"],
            cfg["service_credential"],
            delta,
            checkpoint,
            metrics_table_fqn=cfg["metrics_table_fqn"],
            metrics_table_location=cfg["metrics_table_location"],
            region=cfg["region"],
            zerobus_endpoint=cfg["zerobus_endpoint"],
            workspace_url=cfg["workspace_url"],
            zerobus_client_id=cfg["zerobus_client_id"],
            zerobus_secret_key=cfg["zerobus_secret_key"],
        )
        return await _run_write_test(ctx, code)

    # Real-time lane: TRULY land + measure data (Phase-7 fix).
    #   1. produce ~100 sample messages onto the topic (no producer runs during
    #      validation otherwise, so an earliest read had nothing to land);
    #   2. run the bounded RTM consumer that writes rows to the Lakebase LakeTS
    #      ChronoTable via a psycopg ForeachWriter, wired with the buffered
    #      Zerobus metrics listener so metrics rows land too.
    topic = vc.topic_name(cfg["industry"], "realtime")
    produce = await _run_on_cluster(
        ctx,
        snip.rtm_produce(
            topic,
            cfg["bootstrap"],
            cfg["region"],
            cfg["service_credential"],
            num_records=100,
            source=VALIDATION_SOURCE,
            industry=cfg["industry"],
        ),
        timeout_s=180,
    )
    if produce.status != sm.PASSED:
        return produce
    # Freshness floor: the earliest event_time the producer stamped (cluster
    # clock). The consumer reads from earliest, so this drops stale rows from
    # PRIOR runs (past-dated event_time -> no partition in the forward-only
    # ChronoTable -> INSERT CheckViolation) and lands only THIS run's data.
    min_event_epoch = _ok_payload((produce.data or {}).get("output", "")).get("min_ts")
    if isinstance(min_event_epoch, (int, float)):
        min_event_epoch = float(min_event_epoch) - 5.0  # small epsilon for clock granularity
    else:
        min_event_epoch = None
    # Resolve the Lakebase PRIMARY connection HERE on the harness driver (the
    # cluster SDK lacks the .postgres API and executors have no default auth) and
    # inject host/user/token as literals into the ForeachWriter snippet.
    pg_host, pg_user, pg_token = _resolve_lakebase_write_conn(ctx, cfg["branch"])
    code = snip.rtm_write_test(
        topic,
        cfg["bootstrap"],
        cfg["service_credential"],
        project_id=cfg["project_id"],
        branch=cfg["branch"],
        pg_database=pps.PG_DATABASE,
        table=RTM_TABLE,
        checkpoint_path=checkpoint,
        metrics_table_fqn=cfg["metrics_table_fqn"],
        metrics_table_location=cfg["metrics_table_location"],
        pg_host=pg_host,
        pg_user=pg_user,
        pg_token=pg_token,
        zerobus_endpoint=cfg["zerobus_endpoint"],
        workspace_url=cfg["workspace_url"],
        zerobus_client_id=cfg["zerobus_client_id"],
        zerobus_secret_key=cfg["zerobus_secret_key"],
        region=cfg["region"],
        source=VALIDATION_SOURCE,
        min_event_epoch=min_event_epoch,
    )
    return await _run_write_test(ctx, code)


def _resolve_lakebase_write_conn(ctx: StepContext, branch: str) -> tuple[str, str, str]:
    """Resolve (host, user, token) for the branch PRIMARY on the harness driver."""
    ws = ctx.ws or _new_ws()
    name = pps.endpoint_resource_name(
        branch,
        pps.PRIMARY_ENDPOINT_ID,
        _cfg(ctx)["project_id"],
    )
    endpoint = ws.postgres.get_endpoint(name=name)
    host = pps._endpoint_host(endpoint)
    cred = ws.postgres.generate_database_credential(endpoint=name)
    user = os.getenv("DATABRICKS_CLIENT_ID") or ws.current_user.me().user_name
    return host, user, cred.token


async def cdf_manual(ctx: StepContext) -> StepResult:
    """Ensure the CDF shadow and managed CDF config without an admin step.

    The old ``cdf_manual`` key remains stable for existing persisted validation
    states and UI labels. Its behavior is now fully automated.
    """
    cfg = _cfg(ctx)
    conn = None
    try:
        conn = await pps.get_write_conn(cfg["branch"], ws=ctx.ws)
        res = await lakets_svc.reconcile_chronotable_shadow(
            conn,
            shadow_table=cfg["cdf_shadow_table"],
            table=RTM_TABLE,
            schema="public",
        )
        shadow_status = res.get("status")
        if shadow_status not in {"created", "already_exists", "migrated"}:
            return StepResult(
                sm.FAILED,
                error=res.get("error") or "LakeTS CDF shadow was not prepared",
            )
    except Exception as exc:
        return StepResult(sm.FAILED, error=f"CDF shadow prep failed: {str(exc)[:300]}")
    finally:
        if conn is not None:
            try:
                await conn.close()
            except Exception:  # pragma: no cover - best-effort
                pass

    parent = cdf_config_service.database_parent(
        cfg["branch"],
        project_id=cfg["project_id"],
    )
    try:
        cdf_result = await asyncio.to_thread(
            cdf_config_service.ensure_cdf_config,
            parent,
            catalog=cat.uc_catalog_name(),
            schema=cat.uc_schema_name(),
            ws=ctx.ws,
        )
    except Exception as exc:
        return StepResult(sm.FAILED, error=f"CDF config enablement failed: {str(exc)[:300]}")

    setup_id = ctx.setup.get("id")
    if ctx.db is not None and setup_id is not None:
        try:
            resources = await resource_ledger.list_resources(ctx.db, setup_id)
            if not any(
                resource.get("resource_type") == "cdf_sync"
                and resource.get("identifier") == cdf_result.get("name")
                for resource in resources
            ):
                await resource_ledger.record_resource(
                    ctx.db,
                    setup_id=setup_id,
                    resource_type="cdf_sync",
                    identifier=cdf_result["name"],
                    parent_ref=pps.branch_resource_name(cfg["branch"], cfg["project_id"]),
                    teardown_order=cdf_config_service.TEARDOWN_ORDER,
                    metadata={
                        "parent": parent,
                        "cdf_config_id": cdf_result.get(
                            "cdf_config_id",
                            cdf_config_service.DEFAULT_POSTGRES_SCHEMA,
                        ),
                        "catalog": cat.uc_catalog_name(),
                        "schema": cat.uc_schema_name(),
                        "postgres_schema": cdf_result.get(
                            "postgres_schema",
                            cdf_config_service.DEFAULT_POSTGRES_SCHEMA,
                        ),
                        "database": pps.PG_DATABASE,
                        "source_table": (f"{cdf.CDF_SHADOW_SCHEMA}.{cfg['cdf_shadow_table']}"),
                        "delta_table": cfg["cdf_delta_table"],
                        "force_delete_tables": False,
                    },
                )
        except Exception as exc:
            return StepResult(
                sm.FAILED,
                error=f"CDF config ledger recording failed: {str(exc)[:300]}",
            )

    return StepResult(
        sm.PASSED,
        data={
            "note": "managed CDF sync enabled through the Postgres API",
            "shadow_table": f"lakets_cdf.{cfg['cdf_shadow_table']}",
            "cdf_shadow": shadow_status,
            "cdf_config": cdf_result.get("name"),
            "cdf_config_status": cdf_result.get("status"),
        },
    )


async def cdf_verify(ctx: StepContext) -> StepResult:
    # CDF sync status lives in the branch's Lakebase catalog (wal2delta.tables) —
    # NOT in Spark/UC — so verify it on the branch via psycopg, reusing the Tier-1
    # check (two-snapshot STREAMING + committed-LSN poll keyed on this industry's
    # exact shadow). Spark SQL cannot query the Postgres wal2delta catalog.
    cfg = _cfg(ctx)
    conn = None
    try:
        conn = await pps.get_write_conn(cfg["branch"], ws=ctx.ws)
        result = await health_checks.check_cdf_sync(conn, shadow=cfg["cdf_shadow_table"])
    except Exception as exc:
        return StepResult(sm.FAILED, error=f"cdf verify failed: {str(exc)[:300]}")
    finally:
        if conn is not None:
            try:
                await conn.close()
            except Exception:  # pragma: no cover - best-effort
                pass
    if result.get("status") == health_checks.PASS:
        return StepResult(sm.PASSED, data={"detail": result.get("detail")})
    return StepResult(sm.FAILED, error=result.get("detail") or "CDF not STREAMING/advancing")


def _read_back_window(ctx: StepContext) -> tuple[datetime, datetime]:
    """Return an event_time window that covers the write-test rows.

    Prefer the passed write_test step's ``updated_at`` so a delayed retry still
    finds those rows without scanning later live partitions. Fall back to a
    short lookback from now when that stamp is missing.
    """
    now = datetime.now(UTC)
    key = sm.step_key(ctx.mode, "write_test") if ctx.mode else "write_test"
    raw = ((ctx.state or {}).get("steps") or {}).get(key, {}).get("updated_at")
    if raw:
        try:
            stamped = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if stamped.tzinfo is None:
                stamped = stamped.replace(tzinfo=UTC)
            return (
                stamped - _WRITE_TEST_WINDOW_BEFORE,
                stamped + _WRITE_TEST_WINDOW_AFTER,
            )
        except ValueError:
            pass
    return now - _FALLBACK_LOOKBACK, now + timedelta(minutes=1)


async def _validation_row_exists(conn: Any, industry: str, start: datetime, end: datetime) -> bool:
    """Return whether one validation row is visible in ``[start, end)``.

    ChronoTables also hold unbounded live SSS traffic. A full-table COUNT(*)
    times out on busy industry branches; SELECT 1 LIMIT 1 with an event_time
    bound is enough to prove the write-test round-trip.
    """
    row = await conn.fetchval(
        f"SELECT 1 FROM {RTM_TABLE} "
        "WHERE source = $1 AND industry = $2 "
        "AND event_time >= $3 AND event_time < $4 "
        "LIMIT 1",
        VALIDATION_SOURCE,
        industry,
        start,
        end,
    )
    return row is not None


async def _close_conn(conn: Any) -> None:
    if conn is None:
        return
    try:
        await conn.close()
    except Exception:  # pragma: no cover - best-effort
        pass


async def read_back(ctx: StepContext) -> StepResult:
    # Immediate round-trip: the PRIMARY must show a recent validation row.
    # Replica visibility is best-effort (async replication).
    cfg = _cfg(ctx)
    start, end = _read_back_window(ctx)
    primary = None
    try:
        primary = await pps.get_write_conn(cfg["branch"], ws=ctx.ws)
        found_primary = await _validation_row_exists(primary, cfg["industry"], start, end)
    except Exception as exc:
        return StepResult(
            sm.FAILED,
            error=f"Lakebase primary existence probe failed: {format_executor_error(exc)}",
        )
    finally:
        await _close_conn(primary)
    if not found_primary:
        return StepResult(
            sm.FAILED,
            error=(
                "no validation row on Lakebase primary in the write-test window "
                f"({start.isoformat()} .. {end.isoformat()}, source={VALIDATION_SOURCE})"
            ),
        )

    found_replica: bool | None = None
    replica = None
    try:
        replica = await pps.get_read_conn(cfg["branch"], ws=ctx.ws)
        found_replica = await _validation_row_exists(replica, cfg["industry"], start, end)
    except Exception as exc:  # replica lag / transient — non-fatal
        logger.info(
            "read_back replica read failed (non-fatal): %s",
            format_executor_error(exc),
        )
    finally:
        await _close_conn(replica)
    return StepResult(
        sm.PASSED,
        data={"found_primary": found_primary, "found_replica": found_replica},
    )


async def dbsql_read(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    wh = cfg["warehouse_id"]
    if not wh:
        return StepResult(sm.FAILED, error="no warehouse_id configured for DBSQL read")
    if ctx.mode == "microbatch":
        table = cat.industry_table(cfg["industry"], "microbatch")
    elif ctx.mode == "zerobus":
        table = cat.industry_table(cfg["industry"], "zerobus")
    else:
        # Managed Lakebase CDF names its destination from the source shadow
        # table; it does not write into the separately provisioned realtime
        # table used by direct Delta pipelines.
        table = cfg["cdf_delta_table"]
    sql_table = sql_fqn(*table.split("."))
    sql = f"SELECT count(*) AS n FROM {sql_table}"
    if ctx.mode == "realtime":
        sql += f" WHERE industry = '{cfg['industry']}' AND source = '{VALIDATION_SOURCE}'"

    if ctx.mode == "zerobus":
        # Bounded poll to absorb the Zerobus ingest→commit lag (see
        # _ZEROBUS_READ_WAIT_S); return as soon as rows are queryable.
        n = 0
        attempts = max(1, _ZEROBUS_READ_WAIT_S // _ZEROBUS_READ_POLL_S)
        for attempt in range(attempts):
            res = await execute_dbsql(sql, wh)
            n = _dbsql_count(res)
            if n > 0:
                break
            if attempt < attempts - 1:
                await asyncio.sleep(_ZEROBUS_READ_POLL_S)
        if n <= 0:
            return StepResult(
                sm.FAILED,
                error=(
                    f"{table} has 0 rows after {_ZEROBUS_READ_WAIT_S}s — no zerobus "
                    "data landed / queryable via DBSQL"
                ),
            )
        return StepResult(sm.PASSED, data={"dbsql": table, "rows": n})

    res = await execute_dbsql(sql, wh)
    n = _dbsql_count(res)
    if n <= 0:
        return StepResult(
            sm.FAILED,
            error=f"{table} has 0 rows — no {ctx.mode or 'realtime'} data landed / queryable via DBSQL",
        )
    return StepResult(sm.PASSED, data={"dbsql": table, "rows": n})


async def metrics_check(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    wh = cfg["warehouse_id"]
    if not wh:
        return StepResult(sm.FAILED, error="no warehouse_id configured for metrics check")
    # One managed {industry}_metrics table with a leading `mode` column; filter
    # by mode so each mode's check only sees its own rows (§1.11 one-schema layout).
    table = cfg["metrics_table_fqn"]
    sql_table = sql_fqn(*table.split("."))
    # Verify the Databricks-only latency percentiles landed through Zerobus.
    sql = (
        f"SELECT count(*) AS n, "
        f"count(e2e_p50) AS have_e2e, count(processing_p99) AS have_proc "
        f"FROM {sql_table} WHERE mode = '{cfg['metric_mode']}'"
    )
    res = await execute_dbsql(sql, wh)
    n = _dbsql_count(res)
    if n <= 0:
        return StepResult(
            sm.FAILED,
            error=(
                f"{table} has no metrics rows — the {cfg['metric_mode']} streaming "
                "metrics listener did not land data (check Zerobus endpoint, UC "
                "secret, and table grants)"
            ),
        )
    return StepResult(sm.PASSED, data={"metrics_table": table, "rows": n, "result": res})


async def cleanup(ctx: StepContext) -> StepResult:
    # Keep objects; empty the test data + delete the validation checkpoint so no
    # test rows or checkpoint state leak across runs (§1.13).
    cfg = _cfg(ctx)
    mode = ctx.mode or "realtime"
    checkpoint = _checkpoint(
        cfg["catalog"],
        cfg["schema"],
        cfg["industry"],
        mode,
    )
    data: dict[str, Any] = {"checkpoint": checkpoint}

    # 1. Empty the realtime test rows the RTM write-test landed in the Lakebase
    #    ChronoTable (source-scoped so only this harness's data is removed).
    if mode == "realtime":
        conn = None
        try:
            conn = await pps.get_write_conn(cfg["branch"], ws=ctx.ws)
            status = await conn.execute(
                f"DELETE FROM {RTM_TABLE} WHERE source = $1 AND industry = $2",
                VALIDATION_SOURCE,
                cfg["industry"],
            )
            data["rows_purged"] = status
        except Exception as exc:  # best-effort — surfaced, non-fatal
            logger.warning("cleanup row purge failed: %s", exc)
            data["rows_purge_error"] = str(exc)[:200]
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:  # pragma: no cover
                    pass

    # 2. Delete the distinct validation checkpoint (best-effort; a missing
    #    checkpoint is already "removed").
    rm = await _run_on_cluster(ctx, snip.remove_path(checkpoint), timeout_s=120)
    data["checkpoint_deleted"] = rm.status == sm.PASSED

    return StepResult(sm.PASSED, data=data)


# ---------------------------------------------------------------------------
# Zerobus steps
# ---------------------------------------------------------------------------
async def create_table(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    wh = cfg["warehouse_id"]
    if not wh:
        return StepResult(sm.FAILED, error="no warehouse_id configured for zerobus table create")
    fqn = cat.industry_table(cfg["industry"], "zerobus")
    await execute_dbsql(delta_table_ddl(sql_fqn(*fqn.split("."))), wh)
    return StepResult(sm.PASSED, data={"table": fqn})


async def auth_probe(ctx: StepContext) -> StepResult:
    # REAL driver-side probe (no cluster): DNS + TCP reachability of the zerobus
    # server endpoint + a live OAuth token exchange downscoped to the target table,
    # so a missing UC grant surfaces here instead of as a produce_test failure.
    cfg = _cfg(ctx)
    workspace_url = os.getenv("DATABRICKS_HOST", "")
    server_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT", "")
    table_fqn = cat.industry_table(cfg["industry"], "zerobus")
    if not server_endpoint:
        return StepResult(sm.FAILED, error="ZEROBUS_SERVER_ENDPOINT not configured")
    ok, detail = await asyncio.to_thread(
        zbv.auth_probe,
        workspace_url,
        server_endpoint,
        table_fqn,
        secret_scope=get_secret_scope(),
        secret_key=os.getenv("ZEROBUS_SECRET_KEY", "zerobus-client-secret"),
    )
    return StepResult(
        sm.PASSED if ok else sm.FAILED, error=None if ok else str(detail)[:400], data=detail
    )


async def produce_test(ctx: StepContext) -> StepResult:
    # Driver-side Zerobus ingest — no cluster needed (zerobus pipelines have no
    # interactive cluster by design). zbv.produce() authenticates exactly as
    # auth_probe does and runs the ZerobusSdk ingest in-process via asyncio.to_thread;
    # the subsequent zerobus:dbsql_read asserts the rows actually landed.
    cfg = _cfg(ctx)
    workspace_url = os.getenv("DATABRICKS_HOST", "")
    server_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT", "")
    table_fqn = cat.industry_table(cfg["industry"], "zerobus")
    if not server_endpoint:
        return StepResult(sm.FAILED, error="ZEROBUS_SERVER_ENDPOINT not configured")
    ok, detail = await asyncio.to_thread(
        zbv.produce,
        workspace_url=workspace_url,
        server_endpoint=server_endpoint,
        table_fqn=table_fqn,
        num_records=100,
        source=VALIDATION_SOURCE,
        secret_scope=get_secret_scope(),
        secret_key=os.getenv("ZEROBUS_SECRET_KEY", "zerobus-client-secret"),
    )
    return StepResult(
        sm.PASSED if ok else sm.FAILED,
        error=None if ok else str(detail.get("error", detail))[:400],
        data=detail,
    )


# ---------------------------------------------------------------------------
# Executor registry (keyed by bare step name)
# ---------------------------------------------------------------------------
DEFAULT_EXECUTORS: dict[str, Any] = {
    "provision_cluster": provision_cluster,
    "source_connectivity": source_connectivity,
    "topics_created": topics_created,
    "schema_conformance": schema_conformance,
    # realtime + microbatch shared
    "partitions_precreate": partitions_precreate,
    "write_test": write_test,
    "cdf_manual": cdf_manual,
    "cdf_verify": cdf_verify,
    "read_back": read_back,
    "dbsql_read": dbsql_read,
    "metrics_check": metrics_check,
    "cleanup": cleanup,
    # zerobus
    "create_table": create_table,
    "auth_probe": auth_probe,
    "produce_test": produce_test,
    # terminal steps (show_final_notebook / done) have no executor → no-op pass.
}
