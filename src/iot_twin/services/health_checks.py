"""Two-tier pipeline health checks (spec §1.16, §2.8, §2.9).

**Tier 1 — fast API/DB checks (default, no interactive cluster, seconds).**  For a
configured/deployed industry pipeline these verify, from the app backend, that
the live resources are up *right now* using only the SDK / DBSQL / Lakebase
``w.postgres`` / a psycopg read connection to the branch / UC grants + secrets
APIs / the Jobs API:

- UC catalog + the 5 canonical schemas + per-mode Delta tables + the
  ``stream_checkpoints`` volume exist (DBSQL ``information_schema``).
- The shared Lakebase project + the industry branch + primary/replica endpoints
  are ACTIVE (``w.postgres``).
- The ChronoTable ``sensor_readings`` exists, has forward partitions
  (``lakets._chunk_metadata``), and its columns match the SSOT
  (``models/sensor_schema.py``) — via ``pipeline_project_service.get_read_conn``.
- CDF sync is live: the pipeline's industry-specific shadow row in
  ``wal2delta.tables`` is ``STREAMING`` with an advancing ``committed_lsn``.
- The shared streaming SP has UC grants on the catalog + the required secrets
  exist.
- The metrics external table has rows (DBSQL).
- The mode's job(s) are deployed and running (Jobs API).
- The interactive cluster's state (Clusters API) — **informational**, always
  returned, never makes the pipeline unhealthy.

Each check returns ``{name, tier: "api", status: pass|fail|skip, detail}``.  Every
check degrades gracefully — a missing resource or an API error yields ``fail``
(or ``skip`` when a prerequisite like the warehouse is absent), never an
exception — so a partial outage produces a useful report instead of a 500.
:func:`gather_checks` aggregates the Tier-1 results into ``health`` / ``startable``
(reusing :func:`iot_twin.services.pipelines.pipeline_view.rollup_api_health`) and surfaces the
interactive ``cluster_state``.  This is the primary health for
``GET /api/admin/pipelines/{setup_id}/health`` and the end-user pre-start gate.

**Tier 2 — interactive end-to-end test (opt-in).**  The existing validation
harness (``services/validation/validation_service.py`` state machine) stays as the on-demand
cluster test — source DNS/TCP/TLS connectivity, produce→RTM-consume→land rows,
metrics.  It is a distinct, ~10-minute path that spins the interactive cluster.
The only Tier-2 item surfaced in this module is Kafka **topic existence**: MSK-IAM
auth is available **only on a cluster** via
``dbutils.credentials.getServiceCredentialsProvider("iot-rtm-sc")`` (the app
backend / SP cannot mint an MSK-IAM token), so the topic check is marked
``tier="e2e"`` / ``skip`` here and belongs to the interactive test.

Additive only — this module reads live state; it never provisions or mutates
resources and does not touch ``app.yaml`` / ``core/lakebase.py`` / the legacy
``iot-lakets-demo`` wiring.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from iot_twin.models.sensor_schema import ID_COLUMN_NAME, insert_columns
from iot_twin.services import cdf_reconciliation_service as cdf
from iot_twin.services.deployment import deployment_repo, deployment_service
from iot_twin.services.industry import industry_catalog_service as cat
from iot_twin.services.pipelines import pipeline_project_service as pps
from iot_twin.services.pipelines import pipeline_setup_repo
from iot_twin.services.pipelines import pipeline_view as pv
from iot_twin.services.provisioning_service import (
    SHARED_STREAMING_SP_ID_KEY,
    SHARED_STREAMING_SP_SECRET_KEY,
    SHARED_STREAMING_SP_SECRET_SCOPE,
)
from iot_twin.services.uc_ident import sql_fqn, sql_ident

logger = logging.getLogger("iot_digital_twin.services.health_checks")

# --- Vocabulary (shared with pipeline_view so nothing drifts) ----------------
TIER_API = pv.TIER_API
TIER_E2E = pv.TIER_E2E
PASS = pv.CHECK_PASS
FAIL = pv.CHECK_FAIL
SKIP = pv.CHECK_SKIP

# --- Check names (stable identifiers the frontend keys on) -------------------
UC_CATALOG_CHECK = "uc_catalog_schemas"
DELTA_TABLES_CHECK = "delta_tables"
CHECKPOINT_VOLUME_CHECK = "checkpoint_volume"
METRICS_TABLE_CHECK = "metrics_table"
LAKEBASE_ENDPOINTS_CHECK = "lakebase_endpoints"
CHRONOTABLE_CHECK = "chronotable"
CDF_SYNC_CHECK = "cdf_sync"
GRANTS_CHECK = "uc_grants"
SCOPE_ACCESS_CHECK = "secrets"
JOBS_CHECK = "jobs"
CLUSTER_STATE_CHECK = "interactive_cluster"
KAFKA_TOPIC_CHECK = "kafka_topics"

# The real-time Lakebase ChronoTable (SSOT-derived, §1.9 / §2.5).
CHRONOTABLE = "sensor_readings"
CHRONOTABLE_SCHEMA = "public"
CDF_SHADOW_SCHEMA = "lakets_cdf"

# Legacy Delta locations. Industry-first realtime uses the CDF destination
# derived from its unique LakeTS shadow; metrics are checked separately.
_MODE_TABLES: dict[str, tuple[str, str]] = {
    "realtime": ("realtime", "sensor_readings"),
    "microbatch": ("microbatch", "sensor_data"),
    "zerobus": ("zerobus", "iot_sensor_data"),
}

# Secrets the shared streaming SP needs (spec §2.7).
_REQUIRED_SECRET_KEYS: tuple[str, ...] = (
    SHARED_STREAMING_SP_ID_KEY,
    SHARED_STREAMING_SP_SECRET_KEY,
)

# ~24h / 96-chunk forward window (spec §1.10 Approach A) — a healthy real-time
# ChronoTable keeps this many hours of future partitions ahead of now().
_PARTITION_HORIZON_HOURS = 24

# Short poll gap for the CDF advancing-LSN check (fast; injectable for tests).
_CDF_SLEEP_S = 3.0

# Callable types for injected dependencies (keeps the checks pure + testable).
RunSql = Callable[[str], Awaitable[dict]]
OpenConn = Callable[[str], Awaitable[Any]]
JobStatus = Callable[[Any], dict]
Sleeper = Callable[[float], Awaitable[None]]


# ---------------------------------------------------------------------------
# Result helper
# ---------------------------------------------------------------------------
def _result(name: str, status: str, detail: str, *, tier: str = TIER_API, **extra: Any) -> dict:
    """Return the canonical ``{name, tier, status, detail}`` check object."""
    return {"name": name, "tier": tier, "status": status, "detail": detail, **extra}


def _rows(result: dict | None) -> list[dict]:
    """Return the row dicts from an ``execute_dbsql`` result (empty if none)."""
    return list((result or {}).get("data") or [])


def _is_missing_relation(exc: Exception) -> bool:
    """True when the error is Postgres 'undefined table/schema' (42P01 / 3F000).

    On a bare / draft branch the LakeTS registry (``lakets._chronotable_registry``)
    and CDF (``wal2delta.tables``) objects legitimately don't exist yet — that is
    "not provisioned", not a health failure, so callers surface it as ``skip``
    rather than a raw ``UndefinedTableError`` string.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if sqlstate in ("42P01", "3F000"):  # undefined_table / invalid_schema_name
        return True
    msg = str(exc).lower()
    return "does not exist" in msg and ("relation" in msg or "schema" in msg)


# DBSQL / Spark markers for a not-yet-created table or its backing Delta path.
_MISSING_TABLE_MARKERS = (
    "table_or_view_not_found",
    "delta_table_not_found",
    "delta_missing_delta_table",
    "path_not_found",
    "table or view not found",
)


def _is_missing_table(exc: Exception) -> bool:
    """True when the error means the (DBSQL-queried) table has not been created.

    Covers both the Postgres relation errors (:func:`_is_missing_relation`) and
    the Spark/DBSQL markers raised when a UC/Delta table doesn't exist yet — the
    external metrics table only appears after the first streaming write.
    """
    if _is_missing_relation(exc):
        return True
    msg = str(exc).lower()
    return any(m in msg for m in _MISSING_TABLE_MARKERS)


# ---------------------------------------------------------------------------
# Tier 1 — Unity Catalog / DBSQL
# ---------------------------------------------------------------------------
async def check_uc_catalog(
    catalog: str | None,
    run_sql: RunSql | None,
    *,
    schema: str | None = None,
) -> dict:
    """Verify the configured catalog and required schema layout exist."""
    if run_sql is None:
        return _result(UC_CATALOG_CHECK, SKIP, "no SQL warehouse configured")
    try:
        res = await run_sql(
            f"SELECT schema_name FROM {sql_ident(catalog)}.information_schema.schemata"
        )
        present = {r.get("schema_name") for r in _rows(res)}
        expected_schemas = (schema,) if schema else cat.CANONICAL_SCHEMAS
        missing = [s for s in expected_schemas if s not in present]
        if missing:
            return _result(
                UC_CATALOG_CHECK,
                FAIL,
                f"catalog {catalog} missing schemas: {', '.join(missing)}",
            )
        return _result(UC_CATALOG_CHECK, PASS, f"catalog {catalog} + all schemas present")
    except Exception as exc:
        return _result(UC_CATALOG_CHECK, FAIL, f"catalog {catalog} check failed: {exc}")


async def check_delta_tables(
    catalog: str | None,
    enabled_modes: list[str],
    run_sql: RunSql | None,
    *,
    schema: str | None = None,
    industry: str | None = None,
) -> dict:
    """Verify the per-mode Delta tables for the enabled modes exist."""
    if run_sql is None:
        return _result(DELTA_TABLES_CHECK, SKIP, "no SQL warehouse configured")
    if schema and industry:
        expected = {
            (
                schema,
                (
                    cdf.delta_table_name(industry)
                    if mode == "realtime"
                    else cat.industry_table(industry, mode).rsplit(".", 1)[-1]
                ),
            )
            for mode in (enabled_modes or [])
            if mode in _MODE_TABLES
        }
    else:
        expected = {_MODE_TABLES[m] for m in (enabled_modes or []) if m in _MODE_TABLES}
    if not expected:
        return _result(DELTA_TABLES_CHECK, SKIP, "no Delta-backed modes enabled")
    try:
        res = await run_sql(
            f"SELECT table_schema, table_name FROM {sql_ident(catalog)}.information_schema.tables"
        )
        have = {(r.get("table_schema"), r.get("table_name")) for r in _rows(res)}
        missing = [f"{catalog}.{s}.{t}" for (s, t) in sorted(expected) if (s, t) not in have]
        if missing:
            # Per-mode Delta tables are created by the streaming write on the FIRST
            # pipeline run — before that they legitimately don't exist. That is
            # "not yet created", not a health failure, so surface it as SKIP.
            return _result(
                DELTA_TABLES_CHECK,
                SKIP,
                f"not yet created (written on first pipeline run): {', '.join(missing)}",
            )
        return _result(
            DELTA_TABLES_CHECK,
            PASS,
            f"{len(expected)} Delta table(s) present for {', '.join(sorted(enabled_modes))}",
        )
    except Exception as exc:
        return _result(DELTA_TABLES_CHECK, FAIL, f"delta-table check failed: {exc}")


async def check_checkpoint_volume(
    catalog: str | None,
    run_sql: RunSql | None,
    *,
    schema: str | None = None,
) -> dict:
    """Verify the configured shared-schema checkpoint volume exists."""
    if run_sql is None:
        return _result(CHECKPOINT_VOLUME_CHECK, SKIP, "no SQL warehouse configured")
    if schema is None:
        try:
            schema = cat.uc_schema_name()
        except RuntimeError:
            schema = cat.CHECKPOINT_SCHEMA
    try:
        res = await run_sql(
            f"SELECT volume_name FROM {sql_ident(catalog)}.information_schema.volumes "
            f"WHERE volume_schema = '{schema}'"
        )
        names = {r.get("volume_name") for r in _rows(res)}
        if cat.CHECKPOINT_VOLUME not in names:
            return _result(
                CHECKPOINT_VOLUME_CHECK,
                FAIL,
                f"{catalog}.{schema}.{cat.CHECKPOINT_VOLUME} not found",
            )
        return _result(CHECKPOINT_VOLUME_CHECK, PASS, "stream_checkpoints volume present")
    except Exception as exc:
        return _result(CHECKPOINT_VOLUME_CHECK, FAIL, f"volume check failed: {exc}")


# Modes that emit Spark structured-streaming query metrics (onQueryProgress);
# zerobus ingest is not a Spark stream, so it has no streaming_metrics_zerobus rows.
_STREAMING_METRIC_MODES = ("realtime", "microbatch")


async def check_metrics_recent(
    enabled_modes: list[str],
    run_sql: RunSql | None,
    *,
    industry: str,
    catalog: str | None = None,
    schema: str | None = None,
) -> dict:
    """Verify each enabled mode has rows in the shared industry metrics table.

    All modes write ``{UC_CATALOG}.{UC_SCHEMA}.{industry}_metrics`` and are
    distinguished by the ``mode`` column.
    """
    if run_sql is None:
        return _result(METRICS_TABLE_CHECK, SKIP, "no SQL warehouse configured")
    modes = [m for m in (enabled_modes or []) if m in _STREAMING_METRIC_MODES]
    if not modes:
        return _result(METRICS_TABLE_CHECK, SKIP, "no streaming-metrics modes enabled")
    if not catalog or not schema:
        try:
            catalog = cat.uc_catalog_name()
            schema = cat.uc_schema_name()
        except RuntimeError:
            return _result(
                METRICS_TABLE_CHECK,
                SKIP,
                "shared UC catalog/schema not configured",
            )
    have: list[str] = []
    empty: list[str] = []
    fqn = cat.industry_table(industry, "metrics")
    for mode in modes:
        try:
            res = await run_sql(
                f"SELECT count(*) AS n, max(`timestamp`) AS latest "
                f"FROM {sql_fqn(*fqn.split('.'))} WHERE mode = '{mode}'"
            )
            rows = _rows(res)
            row = rows[0] if rows else {}
            n = int(row.get("n") or 0)
            if n > 0:
                have.append(f"{mode}={n} (latest {row.get('latest')})")
            else:
                empty.append(mode)
        except Exception as exc:
            # Per-mode table is created on the first streaming run; a missing table
            # is "not run yet" (empty), not a failure.
            if _is_missing_table(exc):
                empty.append(mode)
            else:
                return _result(METRICS_TABLE_CHECK, FAIL, f"{fqn} metrics check failed: {exc}")
    if have and not empty:
        return _result(METRICS_TABLE_CHECK, PASS, f"metrics rows: {', '.join(have)}")
    if have:
        return _result(
            METRICS_TABLE_CHECK,
            PASS,
            f"metrics rows: {', '.join(have)}; none yet for {', '.join(empty)}",
        )
    return _result(
        METRICS_TABLE_CHECK,
        SKIP,
        f"no metrics rows yet for {', '.join(empty)} (populated once each mode streams)",
    )


# ---------------------------------------------------------------------------
# Tier 1 — Lakebase endpoints (w.postgres)
# ---------------------------------------------------------------------------
async def check_lakebase_endpoints(branch: str, ws: Any) -> dict:
    """Verify the shared project + branch + primary/replica endpoints are ACTIVE."""
    try:
        ws.postgres.get_project(name=pps.project_resource_name())
        ws.postgres.get_branch(name=pps.branch_resource_name(branch))
        states: dict[str, str | None] = {}
        for ep_id in (pps.PRIMARY_ENDPOINT_ID, pps.REPLICA_ENDPOINT_ID):
            ep = ws.postgres.get_endpoint(name=pps.endpoint_resource_name(branch, ep_id))
            states[ep_id] = pps._endpoint_state(ep)
        inactive = [f"{k}={v}" for k, v in states.items() if v not in pps._ACTIVE_STATES]
        if inactive:
            return _result(
                LAKEBASE_ENDPOINTS_CHECK,
                FAIL,
                f"endpoints not active: {', '.join(inactive)}",
            )
        return _result(
            LAKEBASE_ENDPOINTS_CHECK,
            PASS,
            f"branch {branch}: primary/replica {states}",
        )
    except Exception as exc:
        return _result(LAKEBASE_ENDPOINTS_CHECK, FAIL, f"lakebase check failed: {exc}")


# ---------------------------------------------------------------------------
# Tier 1 — ChronoTable (exists + forward partitions + SSOT columns)
# ---------------------------------------------------------------------------
def _expected_chronotable_columns() -> set[str]:
    """The SSOT column set for ``sensor_readings`` (id + insert columns)."""
    return {ID_COLUMN_NAME, *insert_columns()}


_REGISTERED_SQL = (
    "SELECT count(*) FROM lakets._chronotable_registry WHERE schema_name = $1 AND table_name = $2"
)
# Forward partitions: active chunks whose range extends past now(), joined to the
# ChronoTable registry so we count THIS table's future partitions only.
_FORWARD_PARTITIONS_SQL = (
    "SELECT count(*) AS forward_chunks, max(cm.range_end) AS window_end "
    "FROM lakets._chunk_metadata cm "
    "JOIN lakets._chronotable_registry c ON cm.chronotable_id = c.id "
    "WHERE c.schema_name = $1 AND c.table_name = $2 "
    "AND cm.status = 'active' AND cm.range_end > now()"
)
_COLUMNS_SQL = (
    "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
)


async def check_chronotable(conn: Any, *, table: str = CHRONOTABLE) -> dict:
    """Verify ``sensor_readings`` is a ChronoTable with forward partitions + SSOT columns."""
    try:
        registered = await conn.fetchval(_REGISTERED_SQL, CHRONOTABLE_SCHEMA, table)
        if not registered:
            return _result(
                CHRONOTABLE_CHECK,
                FAIL,
                f"{CHRONOTABLE_SCHEMA}.{table} is not a registered LakeTS ChronoTable",
            )

        prow = await conn.fetchrow(_FORWARD_PARTITIONS_SQL, CHRONOTABLE_SCHEMA, table)
        forward = int((prow or {}).get("forward_chunks") or 0)
        window_end = (prow or {}).get("window_end")
        if forward <= 0:
            return _result(
                CHRONOTABLE_CHECK,
                FAIL,
                f"{table} has no forward partitions (next chunk overdue)",
            )

        col_rows = await conn.fetch(_COLUMNS_SQL, CHRONOTABLE_SCHEMA, table)
        actual = {r.get("column_name") for r in (col_rows or [])}
        missing = _expected_chronotable_columns() - actual
        if missing:
            return _result(
                CHRONOTABLE_CHECK,
                FAIL,
                f"{table} columns drift from SSOT (missing: {', '.join(sorted(missing))})",
            )
        return _result(
            CHRONOTABLE_CHECK,
            PASS,
            f"{table}: ChronoTable OK, {forward} forward partitions (to {window_end}), columns match SSOT",
        )
    except Exception as exc:
        if _is_missing_relation(exc):
            return _result(
                CHRONOTABLE_CHECK,
                SKIP,
                "not provisioned — LakeTS ChronoTable not set up on this branch yet",
            )
        return _result(CHRONOTABLE_CHECK, FAIL, f"chronotable check failed: {exc}")


# ---------------------------------------------------------------------------
# Tier 1 — CDF sync (wal2delta.tables shadow row STREAMING + advancing lsn)
# ---------------------------------------------------------------------------
# wal2delta.tables identifies each synced table by its Postgres OID (table_oid),
# not a name column — join pg_class/pg_namespace to recover the schema.table name.
_WAL2DELTA_SQL = (
    "SELECT (n.nspname || '.' || c.relname) AS table_name, t.status, t.committed_lsn "
    "FROM wal2delta.tables t "
    "JOIN pg_class c ON c.oid = t.table_oid "
    "JOIN pg_namespace n ON n.oid = c.relnamespace"
)


def _shadow_match(table_name: Any, shadow: str, qualified: str) -> bool:
    """True when ``table_name`` is THIS pipeline's shadow row (§1.10.3)."""
    tn = str(table_name)
    return tn in (shadow, qualified) or tn.endswith("." + shadow)


async def check_cdf_sync(
    conn: Any,
    *,
    table: str = CHRONOTABLE,
    shadow: str | None = None,
    shadow_schema: str = CDF_SHADOW_SCHEMA,
    sleep_s: float = _CDF_SLEEP_S,
    sleeper: Sleeper = asyncio.sleep,
) -> dict:
    """Verify the pipeline's ``lakets_cdf._shadow_<table>`` sync is STREAMING + advancing."""
    shadow = shadow or f"_shadow_{table}"
    qualified = f"{shadow_schema}.{shadow}"

    async def _snap() -> dict[str, tuple[Any, Any]]:
        rows = await conn.fetch(_WAL2DELTA_SQL)
        return {
            str(r.get("table_name")): (r.get("status"), r.get("committed_lsn"))
            for r in (rows or [])
            if _shadow_match(r.get("table_name"), shadow, qualified)
        }

    try:
        first = await _snap()
        await sleeper(sleep_s)
        second = await _snap()
        if not second:
            return _result(
                CDF_SYNC_CHECK,
                FAIL,
                f"{qualified} not found in wal2delta.tables — enable managed Lakebase CDF sync",
            )
        streaming = [t for t, (s, _) in second.items() if str(s).upper() == "STREAMING"]
        if not streaming:
            statuses = {t: s for t, (s, _) in second.items()}
            return _result(
                CDF_SYNC_CHECK,
                FAIL,
                f"{qualified} present but not STREAMING (status={statuses}) — "
                "sync is starting or needs write activity on the source table",
            )
        # STREAMING means the managed Lakebase→Delta sync is live for this shadow.
        # committed_lsn only advances while NEW writes flow, so an idle-but-healthy
        # sync (no realtime traffic during the check window) is still a PASS — we
        # note whether it advanced rather than failing on an idle sync.
        advanced = [t for t in streaming if first.get(t, (None, None))[1] != second[t][1]]
        note = (
            "STREAMING + advancing" if advanced else "STREAMING (idle — no new writes during check)"
        )
        return _result(CDF_SYNC_CHECK, PASS, f"{qualified} {note}")
    except Exception as exc:
        if _is_missing_relation(exc):
            return _result(
                CDF_SYNC_CHECK,
                SKIP,
                "not provisioned — managed CDF sync not set up on this branch yet",
            )
        return _result(CDF_SYNC_CHECK, FAIL, f"cdf check failed: {exc}")


# ---------------------------------------------------------------------------
# Tier 1 — UC grants + secrets
# ---------------------------------------------------------------------------
async def check_grants(catalog: str | None, ws: Any, *, principal: str | None = None) -> dict:
    """Verify the current app SP holds grants on the pipeline's catalog.

    ``securable_type`` MUST be the lowercase string token (``"catalog"``), NOT the
    SDK ``SecurableType`` enum: the enum stringifies to ``"SecurableType.CATALOG"``
    and RPC GetPermissions rejects it with "SECURABLETYPE.CATALOG is not a valid
    securable type" (same gotcha as ``routes/provisioning.py::_grant_zerobus_sp``).
    """
    try:
        principal = principal or cat.resolve_streaming_sp_principal(ws)
        resp = ws.grants.get(securable_type="catalog", full_name=catalog)
        assignments = getattr(resp, "privilege_assignments", None) or []
        principals = {getattr(a, "principal", None) for a in assignments}
        if principal not in principals:
            return _result(
                GRANTS_CHECK,
                FAIL,
                f"app SP {principal} has no grants on catalog {catalog}",
            )
        return _result(GRANTS_CHECK, PASS, f"app SP {principal} granted on {catalog}")
    except Exception as exc:
        return _result(GRANTS_CHECK, FAIL, f"grants check failed: {exc}")


async def check_secrets(
    ws: Any,
    *,
    scope: str = SHARED_STREAMING_SP_SECRET_SCOPE,
    required_keys: tuple[str, ...] = _REQUIRED_SECRET_KEYS,
) -> dict:
    """Verify the shared secret scope holds the streaming SP's OAuth secrets."""
    try:
        secrets = list(ws.secrets.list_secrets(scope=scope))
        keys = {getattr(s, "key", None) for s in secrets}
        missing = [k for k in required_keys if k not in keys]
        if missing:
            return _result(
                SCOPE_ACCESS_CHECK,
                FAIL,
                f"scope {scope} missing secrets: {', '.join(missing)}",
            )
        return _result(
            SCOPE_ACCESS_CHECK, PASS, f"scope {scope}: {len(required_keys)} secrets present"
        )
    except Exception as exc:
        return _result(SCOPE_ACCESS_CHECK, FAIL, f"secrets check failed: {exc}")


async def check_uc_secret(catalog: str, schema: str, ws: Any) -> dict:
    """Verify the app OAuth secret used by serverless Zerobus jobs exists."""
    fqn = f"{catalog}.{schema}.{cat.APP_SP_OAUTH_SECRET}"
    try:
        ws.secrets_uc.get_secret(fqn, include_value=False)
        return _result(SCOPE_ACCESS_CHECK, PASS, f"UC secret {fqn} present")
    except Exception as exc:
        return _result(SCOPE_ACCESS_CHECK, FAIL, f"UC secret {fqn} check failed: {exc}")


# ---------------------------------------------------------------------------
# Tier 1 — Jobs (deployed + running)
# ---------------------------------------------------------------------------
def _default_job_status(ws: Any) -> JobStatus:
    def _status(job_id: Any) -> dict:
        return deployment_service.pipeline_status(job_id, w=ws)

    return _status


async def check_jobs(
    deployments: list[dict], *, job_status: JobStatus | None = None, ws: Any | None = None
) -> dict:
    """Verify every deployed mode's job is RUNNING (Jobs API)."""
    deployed = [d for d in (deployments or []) if pv.is_deployed(d)]
    if not deployed:
        return _result(JOBS_CHECK, SKIP, "no jobs deployed yet")
    status_fn = job_status or (_default_job_status(ws) if ws is not None else None)
    if status_fn is None:
        return _result(JOBS_CHECK, SKIP, "no Jobs API client available")

    not_running: list[str] = []
    for dep in deployed:
        job_id = dep.get("job_id")
        mode = dep.get("mode")
        try:
            runtime = status_fn(job_id) or {}
            state = runtime.get("state")
        except Exception as exc:  # a Jobs API error means we can't confirm running
            not_running.append(f"{mode}=error({str(exc)[:60]})")
            continue
        broken_tasks = [
            task
            for task in runtime.get("tasks") or []
            if deployment_service.task_state_is_broken(task.get("life_cycle"), task.get("result"))
        ]
        if runtime.get("degraded") or broken_tasks:
            detail = runtime.get("detail")
            if not detail and broken_tasks:
                detail = ", ".join(str(task.get("task") or "unknown") for task in broken_tasks)
            not_running.append(f"{mode}=failed({str(detail or 'task failure')[:80]})")
            continue
        if state != "RUNNING":
            not_running.append(f"{mode}={state}")
    if not_running:
        return _result(JOBS_CHECK, FAIL, f"job(s) not running: {', '.join(not_running)}")
    return _result(JOBS_CHECK, PASS, f"{len(deployed)} job(s) running")


# ---------------------------------------------------------------------------
# Tier 1 — Interactive cluster state (informational)
# ---------------------------------------------------------------------------
async def check_cluster_state(cluster_id: str | None, ws: Any) -> dict:
    """Report the interactive cluster's state — informational, never fails the pipeline.

    Carries an extra ``cluster_state`` key the orchestrator surfaces at the top
    level.  ``skip`` when there is no cluster; ``pass`` otherwise (even
    TERMINATED — the cluster is only needed for the opt-in Tier-2 e2e test).
    """
    if not cluster_id:
        return _result(
            CLUSTER_STATE_CHECK,
            SKIP,
            "no interactive cluster provisioned (spin one via the e2e test)",
            cluster_state=None,
        )
    try:
        info = ws.clusters.get(cluster_id=cluster_id)
        raw = getattr(info, "state", None)
        state = getattr(raw, "value", None) or (raw if isinstance(raw, str) else "UNKNOWN")
        return _result(
            CLUSTER_STATE_CHECK,
            PASS,
            f"interactive cluster {cluster_id} is {state}",
            cluster_state=state,
        )
    except Exception as exc:
        return _result(
            CLUSTER_STATE_CHECK,
            SKIP,
            f"cluster state unavailable: {exc}",
            cluster_state="UNKNOWN",
        )


# ---------------------------------------------------------------------------
# Tier 2 — Kafka topic existence (opt-in e2e; SKIPPED in Tier 1)
# ---------------------------------------------------------------------------
def check_kafka_topic(industry: str, enabled_modes: list[str]) -> dict:
    """Kafka topic existence — a **Tier-2** (interactive) check, skipped in Tier 1.

    Listing MSK topics needs a ``KafkaAdminClient`` authenticated with MSK-IAM,
    which is only available **on a cluster** via
    ``dbutils.credentials.getServiceCredentialsProvider("iot-rtm-sc")``.  The app
    backend / service principal has no way to mint that token (no
    ``dbutils.credentials``, and the public MSK bootstrap is not reachable from
    the Apps egress), so this check belongs to the opt-in interactive end-to-end
    test (``topics_created`` step), not the fast Tier-1 API sweep.
    """
    topics = [
        f"iot.{industry}.{m}" for m in (enabled_modes or []) if m in ("realtime", "microbatch")
    ]
    return _result(
        KAFKA_TOPIC_CHECK,
        SKIP,
        (
            "topic existence needs MSK-IAM (cluster-only via dbutils.credentials); "
            f"verified by the interactive e2e test for {topics or 'no Kafka modes'}"
        ),
        tier=TIER_E2E,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
async def gather_checks(
    setup: dict,
    deployments: list[dict],
    *,
    ws: Any | None = None,
    run_sql: RunSql | None = None,
    open_read_conn: OpenConn | None = None,
    job_status: JobStatus | None = None,
    principal: str | None = None,
    cdf_sleep_s: float = _CDF_SLEEP_S,
    sleeper: Sleeper = asyncio.sleep,
) -> dict:
    """Run every Tier-1 check + surface the Tier-2 topic verdict for one pipeline.

    Pure orchestration over injected dependencies (``ws`` / ``run_sql`` /
    ``open_read_conn`` / ``job_status``) so it is fully unit-testable.  Returns
    ``{checks, health, startable, cluster_state}`` where ``health`` /
    ``startable`` are the Tier-1 rollup (``pipeline_view.rollup_api_health``) and
    ``cluster_state`` is the informational interactive-cluster state.
    """
    industry = setup.get("industry")
    schema = None
    try:
        catalog = cat.uc_catalog_name()
        schema = cat.uc_schema_name()
    except RuntimeError:
        # Backward compatibility for legacy deployments that still use one
        # catalog with canonical sub-schemas per industry.
        catalog = cat.catalog_name(industry) if industry else None
    branch = industry
    enabled = setup.get("enabled_modes") or []
    enabled_set = set(enabled)
    has_kafka_modes = bool(enabled_set & {"realtime", "microbatch"})
    has_realtime = "realtime" in enabled_set
    cluster_id = (setup.get("validation_state") or {}).get("cluster_id")

    checks: list[dict] = []

    # --- UC / DBSQL ---------------------------------------------------------
    checks.append(await check_uc_catalog(catalog, run_sql, schema=schema))
    checks.append(
        await check_delta_tables(
            catalog,
            enabled,
            run_sql,
            schema=schema,
            industry=industry,
        )
    )
    if has_kafka_modes:
        checks.append(await check_checkpoint_volume(catalog, run_sql, schema=schema))
        checks.append(
            await check_metrics_recent(
                enabled,
                run_sql,
                industry=industry,
                catalog=catalog,
                schema=schema,
            )
        )

    # --- Lakebase endpoints -------------------------------------------------
    if has_realtime and ws is not None and branch:
        checks.append(await check_lakebase_endpoints(branch, ws))

    # --- ChronoTable + CDF (branch read connection) -------------------------
    if has_realtime and open_read_conn is not None and branch:
        conn = None
        try:
            conn = await open_read_conn(branch)
        except Exception as exc:
            detail = f"could not open branch read connection: {exc}"
            checks.append(_result(CHRONOTABLE_CHECK, FAIL, detail))
            checks.append(_result(CDF_SYNC_CHECK, FAIL, detail))
        if conn is not None:
            try:
                checks.append(await check_chronotable(conn))
                checks.append(
                    await check_cdf_sync(
                        conn,
                        shadow=cdf.shadow_table_name(industry),
                        sleep_s=cdf_sleep_s,
                        sleeper=sleeper,
                    )
                )
            finally:
                try:
                    await conn.close()
                except Exception as exc:  # pragma: no cover - best-effort
                    logger.debug("branch conn close failed: %s", exc)

    # --- Grants + secrets ---------------------------------------------------
    if ws is not None:
        checks.append(await check_grants(catalog, ws, principal=principal))
        if schema and catalog:
            checks.append(await check_uc_secret(catalog, schema, ws))
        else:
            checks.append(await check_secrets(ws))

    # --- Jobs ---------------------------------------------------------------
    selected_deployments = [
        deployment for deployment in deployments if deployment.get("mode") in enabled_set
    ]
    checks.append(await check_jobs(selected_deployments, job_status=job_status, ws=ws))

    # --- Interactive cluster (informational, Kafka modes only) ---------------
    cluster_state = None
    if has_kafka_modes:
        cluster_check = await check_cluster_state(cluster_id, ws)
        cluster_state = cluster_check.get("cluster_state")
        checks.append(cluster_check)

    # --- Tier 2: Kafka topic existence (skipped here) -----------------------
    if has_kafka_modes:
        checks.append(check_kafka_topic(industry, enabled))

    health = pv.rollup_api_health(checks)
    return {
        "checks": checks,
        "health": health,
        "startable": health == pv.HEALTH_HEALTHY,
        "cluster_state": cluster_state,
    }


async def run_api_health_checks(
    db: Any,
    setup_id: Any,
    *,
    ws: Any | None = None,
    run_sql: RunSql | None = None,
    open_read_conn: OpenConn | None = None,
    job_status: JobStatus | None = None,
    principal: str | None = None,
    cdf_sleep_s: float = _CDF_SLEEP_S,
) -> dict:
    """Load a setup + its deployments and run the Tier-1 API health sweep.

    Resolves the default live dependencies (a ``WorkspaceClient``, a DBSQL
    ``run_sql`` bound to the setup's warehouse, a branch read connection, and the
    Jobs-API ``pipeline_status``) unless the caller injects them (tests do).
    Raises ``ValueError`` when the setup is missing (the route maps this to 404).
    """
    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise ValueError(f"pipeline_setup {setup_id!r} not found")
    deployments = await deployment_repo.list_deployments(db, setup_id)

    if ws is None:
        ws = _new_ws()

    if run_sql is None:
        warehouse_id = setup.get("warehouse_id")
        if warehouse_id:
            from iot_twin.services.dbsql_service import execute_dbsql

            async def run_sql(sql: str) -> dict:  # noqa: A001 - local binding
                return await execute_dbsql(sql, warehouse_id)

    if open_read_conn is None:

        async def open_read_conn(branch: str) -> Any:  # noqa: A001 - local binding
            return await pps.get_read_conn(branch, ws=ws)

    return await gather_checks(
        setup,
        deployments,
        ws=ws,
        run_sql=run_sql,
        open_read_conn=open_read_conn,
        job_status=job_status,
        principal=principal,
        cdf_sleep_s=cdf_sleep_s,
    )


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()
