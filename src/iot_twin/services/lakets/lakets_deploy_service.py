"""Deploy the LakeTS operational notebooks as scheduled Databricks Jobs.

The notebook sources are INLINED in this module (e.g. ``_RETENTION_NOTEBOOK_SOURCE``)
and uploaded to the authenticated user's workspace via the SDK — nothing is read
from disk. Creates (or updates) the daily/scheduled Jobs that run them against the
supplied Lakebase project/branch.
"""

from __future__ import annotations

import base64
import logging
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.lakets_deploy")

# Notebook source is inlined rather than read from disk.  `databricks sync`
# treats any `.py` file starting with ``# Databricks notebook source`` as a
# workspace notebook and strips the ``.py`` extension during upload — which
# makes it unreachable via the deployed app's filesystem.  Keeping the source
# inline here sidesteps that round-trip entirely.
_RETENTION_NOTEBOOK_SOURCE = b"""# Databricks notebook source
# MAGIC %md
# MAGIC # LakeTS Retention
# MAGIC
# MAGIC Scheduled daily job.  Invokes `lakets.execute_retention(<table>)` for every
# MAGIC ChronoTable that has an enabled retention or tiered_retention policy on the
# MAGIC target Lakebase.  Prints a JSON summary of dropped-chunk counts per table.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.50.0" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import time
import psycopg2

dbutils.widgets.text("project_id", "", "Lakebase project")
dbutils.widgets.text("branch_id", "", "Lakebase branch")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")

project_id = dbutils.widgets.get("project_id").strip()
branch_id = dbutils.widgets.get("branch_id").strip()
if not branch_id:
    raise ValueError("branch_id widget is empty - refusing to silently target 'production'")
database = dbutils.widgets.get("database").strip() or "databricks_postgres"

if not project_id:
    raise ValueError("project_id widget is empty")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
# Target the WRITABLE PRIMARY endpoint BY NAME. list_endpoints returns the read
# REPLICA first, so endpoints[0] -> "cannot execute CREATE TABLE/DROP in a
# read-only transaction". The .../endpoints/primary endpoint is read-write.
primary = f"projects/{project_id}/branches/{branch_id}/endpoints/primary"
endpoint = w.postgres.get_endpoint(name=primary)
cred = w.postgres.generate_database_credential(endpoint=primary)
pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

MAX_CONNECT_ATTEMPTS = 12
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 30.0

def connect_once():
    return psycopg2.connect(
        host=endpoint.status.hosts.host,
        port=5432,
        dbname=database,
        user=pg_user,
        password=cred.token,
        sslmode="require",
    )

def connect_with_backoff():
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            return connect_once()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres connect attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

conn = connect_with_backoff()
conn.autocommit = True

def run_with_backoff(sql, params=None, fetch=None):
    global conn
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            if conn is None or getattr(conn, "closed", False):
                conn = connect_once()
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "all":
                    return cur.fetchall()
                if fetch == "one":
                    row = cur.fetchone()
                    return row[0] if row else None
                return None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres operation attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

results: dict = {}
tables = run_with_backoff(
    "SELECT DISTINCT c.schema_name, c.table_name "
    "FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
    "WHERE p.policy_type IN ('retention', 'tiered_retention') "
    "  AND p.enabled = TRUE",
    fetch="all",
)

for schema, table in tables:
    key = f"{schema}.{table}"
    try:
        dropped = run_with_backoff(
            "SELECT lakets.execute_retention(%s, %s)",
            (table, schema),
            fetch="one",
        )
        results[key] = dropped
        print(f"Retention on {key}: dropped {dropped} chunk(s)")
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        raise
    except Exception as exc:
        results[key] = f"error: {exc}"
        print(f"Retention on {key} FAILED: {exc}")

conn.close()

print("\\n==== Summary ====")
print(json.dumps(results, indent=2, default=str))

if any(isinstance(v, str) and v.startswith("error:") for v in results.values()):
    dbutils.notebook.exit(json.dumps({"status": "partial_failure", "results": results}))
else:
    dbutils.notebook.exit(json.dumps({"status": "ok", "results": results}))
"""

# Partition-manager notebook.  Runs every 10 minutes to keep forward 15-minute
# partitions provisioned for each ChronoTable.  Today `_ensure_partitions` runs
# only once at table-creation time, so the forward window goes stale; this job
# is the durable fix — it walks `lakets._chronotable_registry` and extends each
# table's future partitions by 8 (p_future_count := 8).
_PARTITION_MANAGER_NOTEBOOK_SOURCE = b"""# Databricks notebook source
# MAGIC %md
# MAGIC # LakeTS Partition Manager
# MAGIC
# MAGIC Scheduled every 10 minutes.  For every ChronoTable in
# MAGIC `lakets._chronotable_registry`, calls `lakets._ensure_partitions(id,
# MAGIC p_future_count := 8)` to maintain forward 15-minute partitions.  Prints a
# MAGIC JSON summary.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.50.0" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import time
import psycopg2

dbutils.widgets.text("project_id", "", "Lakebase project")
dbutils.widgets.text("branch_id", "", "Lakebase branch")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")

project_id = dbutils.widgets.get("project_id").strip()
branch_id = dbutils.widgets.get("branch_id").strip()
if not branch_id:
    raise ValueError("branch_id widget is empty - refusing to silently target 'production'")
database = dbutils.widgets.get("database").strip() or "databricks_postgres"

if not project_id:
    raise ValueError("project_id widget is empty")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
# Target the WRITABLE PRIMARY endpoint BY NAME. list_endpoints returns the read
# REPLICA first, so endpoints[0] -> "cannot execute CREATE TABLE/DROP in a
# read-only transaction". The .../endpoints/primary endpoint is read-write.
primary = f"projects/{project_id}/branches/{branch_id}/endpoints/primary"
endpoint = w.postgres.get_endpoint(name=primary)
cred = w.postgres.generate_database_credential(endpoint=primary)
pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

MAX_CONNECT_ATTEMPTS = 12
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 30.0

def connect_once():
    return psycopg2.connect(
        host=endpoint.status.hosts.host,
        port=5432,
        dbname=database,
        user=pg_user,
        password=cred.token,
        sslmode="require",
    )

def connect_with_backoff():
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            return connect_once()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres connect attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

conn = connect_with_backoff()
conn.autocommit = True

def run_with_backoff(sql, params=None, fetch=None):
    global conn
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            if conn is None or getattr(conn, "closed", False):
                conn = connect_once()
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "all":
                    return cur.fetchall()
                if fetch == "one":
                    row = cur.fetchone()
                    return row[0] if row else None
                return None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres operation attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

results: dict = {}
tables = run_with_backoff(
    "SELECT id, schema_name, table_name FROM lakets._chronotable_registry",
    fetch="all",
)

for table_id, schema, table in tables:
    key = f"{schema}.{table}"
    try:
        # 192 * 15-min chunks = 48h forward window, so coverage never lapses
        # between runs and fresh rows always land in an existing partition.
        created = run_with_backoff(
            "SELECT lakets._ensure_partitions(%s, p_future_count := 192)",
            (table_id,),
            fetch="one",
        )
        results[key] = created
        print(f"Partitions ensured on {key}: {created}")
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        raise
    except Exception as exc:
        results[key] = f"error: {exc}"
        print(f"Partition manager on {key} FAILED: {exc}")

conn.close()

print("\\n==== Summary ====")
print(json.dumps(results, indent=2, default=str))

if any(isinstance(v, str) and v.startswith("error:") for v in results.values()):
    dbutils.notebook.exit(json.dumps({"status": "partial_failure", "results": results}))
else:
    dbutils.notebook.exit(json.dumps({"status": "ok", "results": results}))
"""

# Rollup-refresh notebook.  Runs every 15 minutes and refreshes the full rollup
# cascade in one call.
_ROLLUP_REFRESH_NOTEBOOK_SOURCE = b"""# Databricks notebook source
# MAGIC %md
# MAGIC # LakeTS Rollup Refresh
# MAGIC
# MAGIC Scheduled every 15 minutes.  Calls `lakets.refresh_rollup_cascade()` to
# MAGIC refresh all continuous-aggregate rollups.  Prints a JSON summary.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.50.0" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import time
import psycopg2

dbutils.widgets.text("project_id", "", "Lakebase project")
dbutils.widgets.text("branch_id", "", "Lakebase branch")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")

project_id = dbutils.widgets.get("project_id").strip()
branch_id = dbutils.widgets.get("branch_id").strip()
if not branch_id:
    raise ValueError("branch_id widget is empty - refusing to silently target 'production'")
database = dbutils.widgets.get("database").strip() or "databricks_postgres"

if not project_id:
    raise ValueError("project_id widget is empty")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
# Target the WRITABLE PRIMARY endpoint BY NAME. list_endpoints returns the read
# REPLICA first, so endpoints[0] -> "cannot execute CREATE TABLE/DROP in a
# read-only transaction". The .../endpoints/primary endpoint is read-write.
primary = f"projects/{project_id}/branches/{branch_id}/endpoints/primary"
endpoint = w.postgres.get_endpoint(name=primary)
cred = w.postgres.generate_database_credential(endpoint=primary)
pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

MAX_CONNECT_ATTEMPTS = 12
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 30.0

def connect_once():
    return psycopg2.connect(
        host=endpoint.status.hosts.host,
        port=5432,
        dbname=database,
        user=pg_user,
        password=cred.token,
        sslmode="require",
    )

def connect_with_backoff():
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            return connect_once()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres connect attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

conn = connect_with_backoff()
conn.autocommit = True

def run_with_backoff(sql, params=None, fetch=None):
    global conn
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            if conn is None or getattr(conn, "closed", False):
                conn = connect_once()
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "all":
                    return cur.fetchall()
                if fetch == "one":
                    row = cur.fetchone()
                    return row[0] if row else None
                return None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres operation attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

result: dict = {}
try:
    result["refresh_rollup_cascade"] = run_with_backoff(
        "SELECT lakets.refresh_rollup_cascade()",
        fetch="one",
    )
    result["status"] = "ok"
    print(f"Rollup cascade refreshed: {result['refresh_rollup_cascade']}")
except (psycopg2.OperationalError, psycopg2.InterfaceError):
    raise
except Exception as exc:
    result["status"] = "error"
    result["error"] = str(exc)
    print(f"Rollup refresh FAILED: {exc}")
finally:
    if conn is not None:
        conn.close()

print("\\n==== Summary ====")
print(json.dumps(result, indent=2, default=str))

dbutils.notebook.exit(json.dumps(result))
"""

# Tiering notebook.  Runs every 30 minutes.  For every ChronoTable with an
# enabled tiering policy, asks `lakets._get_chunks_to_tier(table, schema)` for
# the validation candidates (active chunks older than the policy `after`, and
# only when the shadow is STREAMING) and calls `lakets.tier_chunk(chunk_name)`
# on each.  `tier_chunk` is FAIL-CLOSED on the CDF durability gate: it returns
# TRUE when it flags a chunk 'tiered' and FALSE when it defers (CDF not yet
# flushed / not streaming), so this job is safe to run before CDF is enabled —
# it simply tiers nothing.  Prints a JSON summary {tiered, deferred, errors}.
_TIERING_NOTEBOOK_SOURCE = b"""# Databricks notebook source
# MAGIC %md
# MAGIC # LakeTS Tiering
# MAGIC
# MAGIC Scheduled every 30 minutes.  For each ChronoTable with an enabled tiering
# MAGIC policy, validates cold chunks via `lakets.tier_chunk(...)` and flags those
# MAGIC durably flushed to Unity Catalog as `tiered`.  Fail-closed on the CDF
# MAGIC durability gate -- defers (tiers nothing) until CDF is streaming.  Prints a
# MAGIC JSON summary {tiered, deferred, errors}.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.50.0" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import time
import psycopg2

dbutils.widgets.text("project_id", "", "Lakebase project")
dbutils.widgets.text("branch_id", "", "Lakebase branch")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")

project_id = dbutils.widgets.get("project_id").strip()
branch_id = dbutils.widgets.get("branch_id").strip()
if not branch_id:
    raise ValueError("branch_id widget is empty - refusing to silently target 'production'")
database = dbutils.widgets.get("database").strip() or "databricks_postgres"

if not project_id:
    raise ValueError("project_id widget is empty")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
# Target the WRITABLE PRIMARY endpoint BY NAME. list_endpoints returns the read
# REPLICA first, so endpoints[0] -> "cannot execute CREATE TABLE/DROP in a
# read-only transaction". The .../endpoints/primary endpoint is read-write.
primary = f"projects/{project_id}/branches/{branch_id}/endpoints/primary"
endpoint = w.postgres.get_endpoint(name=primary)
cred = w.postgres.generate_database_credential(endpoint=primary)
pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

MAX_CONNECT_ATTEMPTS = 12
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 30.0

def connect_once():
    return psycopg2.connect(
        host=endpoint.status.hosts.host,
        port=5432,
        dbname=database,
        user=pg_user,
        password=cred.token,
        sslmode="require",
    )

def connect_with_backoff():
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            return connect_once()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres connect attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

conn = connect_with_backoff()
conn.autocommit = True

def run_with_backoff(sql, params=None, fetch=None):
    global conn
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            if conn is None or getattr(conn, "closed", False):
                conn = connect_once()
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "all":
                    return cur.fetchall()
                if fetch == "one":
                    row = cur.fetchone()
                    return row[0] if row else None
                return None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            delay = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            print(f"Postgres operation attempt {attempt} failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)

# Tables with an enabled tiering policy.
tables = run_with_backoff(
    "SELECT DISTINCT c.schema_name, c.table_name "
    "FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
    "WHERE p.policy_type = 'tiering' AND p.enabled = TRUE",
    fetch="all",
)

results: dict = {}
for schema, table in tables:
    key = f"{schema}.{table}"
    tiered = 0
    deferred = 0
    errors: list = []
    try:
        rows = run_with_backoff(
            "SELECT chunk_name FROM lakets._get_chunks_to_tier(%s, %s)",
            (table, schema),
            fetch="all",
        )
        chunk_names = [row[0] for row in rows]
        for chunk_name in chunk_names:
            try:
                flagged = run_with_backoff(
                    "SELECT lakets.tier_chunk(%s)",
                    (chunk_name,),
                    fetch="one",
                )
                if flagged:
                    tiered += 1
                else:
                    deferred += 1
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                raise
            except Exception as exc:
                errors.append(f"{chunk_name}: {exc}")
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        raise
    except Exception as exc:
        errors.append(f"{key}: {exc}")

    results[key] = {"tiered": tiered, "deferred": deferred, "errors": errors}
    print(f"Tiering on {key}: tiered={tiered} deferred={deferred} errors={len(errors)}")

conn.close()

print("\\n==== Summary ====")
print(json.dumps(results, indent=2, default=str))

if any(r["errors"] for r in results.values()):
    dbutils.notebook.exit(json.dumps({"status": "partial_failure", "results": results}))
else:
    dbutils.notebook.exit(json.dumps({"status": "ok", "results": results}))
"""

# Workspace path where the notebook is uploaded (under the running user's home).
_WS_SUBDIR = "iot-digital-twin/lakets"
_WS_FILENAME = "retention"  # no extension — Databricks infers from import format

# Daily 03:00 UTC.  Quartz syntax required by the Jobs API (seconds + ? for DOW).
_DEFAULT_CRON = "0 0 3 * * ?"

# Operational-job crons (quartz).  The partition manager pre-creates a 48h
# forward window (p_future_count := 192), so a single DAILY run keeps coverage
# well ahead of ingestion — far cheaper than spinning a cluster every 10 min and
# with a full day of safety margin if a run is missed.
_PARTITION_MANAGER_CRON = "0 0 2 * * ?"  # daily @ 02:00 UTC
_ROLLUP_REFRESH_CRON = "0 0/15 * * * ?"
_TIERING_CRON = "0 0/30 * * * ?"

# Serverless dependencies shared by every LakeTS notebook job.
_NOTEBOOK_DEPENDENCIES = ["psycopg2-binary", "databricks-sdk>=0.50.0"]


def _notebook_source_bytes() -> bytes:
    """Return the inlined retention-notebook source as raw bytes."""
    return _RETENTION_NOTEBOOK_SOURCE


def _job_display_name(project_id: str, branch_id: str) -> str:
    return f"LakeTS Retention — {project_id}/{branch_id}"


def deploy_retention_job(
    project_id: str,
    branch_id: str = "production",
    database: str = "databricks_postgres",
    *,
    schedule_cron: str = _DEFAULT_CRON,
    timezone: str = "UTC",
) -> dict:
    """Upload the retention notebook and create / update the scheduled Job.

    Returns a dict with ``{job_id, notebook_path, schedule, created_or_updated}``.
    """
    if not project_id:
        raise ValueError("project_id is required")

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs as dj
    from databricks.sdk.service import workspace as dw
    from databricks.sdk.service.compute import Environment

    w = WorkspaceClient()
    user = w.current_user.me()
    user_email = user.user_name
    workspace_path = f"/Workspace/Users/{user_email}/{_WS_SUBDIR}/{_WS_FILENAME}"

    # ── 1. Ensure parent folders exist ────────────────────────────────────
    parent = f"/Workspace/Users/{user_email}/{_WS_SUBDIR}"
    try:
        w.workspace.mkdirs(parent)
    except Exception as exc:
        logger.debug("mkdirs(%s) skipped: %s", parent, exc)

    # ── 2. Upload notebook (overwrite on every deploy) ────────────────────
    content = _notebook_source_bytes()
    w.workspace.import_(
        path=workspace_path,
        format=dw.ImportFormat.SOURCE,
        language=dw.Language.PYTHON,
        overwrite=True,
        content=base64.b64encode(content).decode("ascii"),
    )
    logger.info("Uploaded retention notebook to %s (%d bytes)", workspace_path, len(content))

    # ── 3. Find existing job by exact name ────────────────────────────────
    job_name = _job_display_name(project_id, branch_id)
    existing_job_id: int | None = None
    for j in w.jobs.list(name=job_name):
        if j.settings and j.settings.name == job_name:
            existing_job_id = j.job_id
            break

    # ── 4. Build job settings (serverless + performance-optimized) ───────
    # Notebook task references the job-level serverless environment via
    # environment_key.  No new_cluster — compute is provisioned by the
    # serverless job runtime, and the job-level performance_target pins it
    # to PERFORMANCE_OPTIMIZED for fast startup (daily cron, short runs).
    env_key = "lakets_serverless"
    task = dj.Task(
        task_key="lakets_retention",
        notebook_task=dj.NotebookTask(
            notebook_path=workspace_path,
            source=dj.Source.WORKSPACE,
            base_parameters={
                "project_id": project_id,
                "branch_id": branch_id,
                "database": database,
            },
        ),
        environment_key=env_key,
        timeout_seconds=600,
        max_retries=3,
        min_retry_interval_millis=30_000,
        retry_on_timeout=True,
    )

    job_settings = dj.JobSettings(
        name=job_name,
        tasks=[task],
        environments=[
            dj.JobEnvironment(
                environment_key=env_key,
                spec=Environment(
                    client="2",
                    dependencies=["psycopg2-binary", "databricks-sdk>=0.50.0"],
                ),
            ),
        ],
        performance_target=dj.PerformanceTarget.PERFORMANCE_OPTIMIZED,
        schedule=dj.CronSchedule(
            quartz_cron_expression=schedule_cron,
            timezone_id=timezone,
            pause_status=dj.PauseStatus.UNPAUSED,
        ),
        max_concurrent_runs=1,
        tags={
            "app": "iot-digital-twin",
            "component": "lakets-retention",
            "project": project_id,
        },
    )

    # ── 5. Create or reset in place ───────────────────────────────────────
    if existing_job_id is not None:
        w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
        job_id = existing_job_id
        action = "updated"
    else:
        created = w.jobs.create(
            name=job_settings.name,
            tasks=job_settings.tasks,
            environments=job_settings.environments,
            performance_target=job_settings.performance_target,
            schedule=job_settings.schedule,
            max_concurrent_runs=job_settings.max_concurrent_runs,
            tags=job_settings.tags,
        )
        job_id = created.job_id
        action = "created"

    logger.info("Retention job %s (id=%s) for %s", action, job_id, job_name)

    host = w.config.host.rstrip("/") if w.config.host else ""
    return {
        "job_id": job_id,
        "job_name": job_name,
        "notebook_path": workspace_path,
        "schedule": {"cron": schedule_cron, "timezone": timezone},
        "action": action,
        "url": f"{host}/jobs/{job_id}" if host else None,
    }


# ── Operational-job deployers ────────────────────────────────────────────────
# These follow the same inline-notebook / serverless / idempotent pattern as
# ``deploy_retention_job`` above, but take an already-constructed
# ``WorkspaceClient`` so callers can share one client across several deploys.


def _ws_path(w: Any, filename: str) -> str:
    """Workspace path for ``filename`` under the running user's LakeTS subdir."""
    user_email = w.current_user.me().user_name
    return f"/Workspace/Users/{user_email}/{_WS_SUBDIR}/{filename}"


def _upload_notebook(w: Any, dw: Any, workspace_path: str, content: bytes) -> None:
    """Ensure the parent folder exists and import ``content`` as a notebook."""
    parent = workspace_path.rsplit("/", 1)[0]
    try:
        w.workspace.mkdirs(parent)
    except Exception as exc:  # pragma: no cover - best-effort
        logger.debug("mkdirs(%s) skipped: %s", parent, exc)

    w.workspace.import_(
        path=workspace_path,
        format=dw.ImportFormat.SOURCE,
        language=dw.Language.PYTHON,
        overwrite=True,
        content=base64.b64encode(content).decode("ascii"),
    )
    logger.info("Uploaded notebook to %s (%d bytes)", workspace_path, len(content))


def _deploy_ops_job(
    w: Any,
    *,
    job_name: str,
    task_key: str,
    component: str,
    notebook_source: bytes,
    filename: str,
    project_id: str,
    branch_id: str,
    database: str,
    cron: str,
    timezone: str = "UTC",
) -> dict:
    """Create or refresh a single serverless LakeTS notebook job.

    Returns ``{"job": <name>, "job_id": <id>, "status": "created"|"updated"}``.
    """
    if not project_id:
        raise ValueError("project_id is required")

    from databricks.sdk.service import jobs as dj
    from databricks.sdk.service import workspace as dw
    from databricks.sdk.service.compute import Environment

    workspace_path = _ws_path(w, filename)
    _upload_notebook(w, dw, workspace_path, notebook_source)

    # Find existing job by exact name (idempotent re-deploy).
    existing_job_id: int | None = None
    for j in w.jobs.list(name=job_name):
        if j.settings and j.settings.name == job_name:
            existing_job_id = j.job_id
            break

    env_key = "lakets_serverless"
    task = dj.Task(
        task_key=task_key,
        notebook_task=dj.NotebookTask(
            notebook_path=workspace_path,
            source=dj.Source.WORKSPACE,
            base_parameters={
                "project_id": project_id,
                "branch_id": branch_id,
                "database": database,
            },
        ),
        environment_key=env_key,
        timeout_seconds=600,
        max_retries=3,
        min_retry_interval_millis=30_000,
        retry_on_timeout=True,
    )

    job_settings = dj.JobSettings(
        name=job_name,
        tasks=[task],
        environments=[
            dj.JobEnvironment(
                environment_key=env_key,
                spec=Environment(
                    client="2",
                    dependencies=list(_NOTEBOOK_DEPENDENCIES),
                ),
            ),
        ],
        performance_target=dj.PerformanceTarget.PERFORMANCE_OPTIMIZED,
        schedule=dj.CronSchedule(
            quartz_cron_expression=cron,
            timezone_id=timezone,
            pause_status=dj.PauseStatus.UNPAUSED,
        ),
        max_concurrent_runs=1,
        tags={
            "app": "iot-digital-twin",
            "component": component,
            "project": project_id,
        },
    )

    if existing_job_id is not None:
        w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
        job_id = existing_job_id
        status = "updated"
    else:
        created = w.jobs.create(
            name=job_settings.name,
            tasks=job_settings.tasks,
            environments=job_settings.environments,
            performance_target=job_settings.performance_target,
            schedule=job_settings.schedule,
            max_concurrent_runs=job_settings.max_concurrent_runs,
            tags=job_settings.tags,
        )
        job_id = created.job_id
        status = "created"

    logger.info("LakeTS job %s (id=%s) for %s", status, job_id, job_name)
    return {"job": job_name, "job_id": job_id, "status": status}


def deploy_partition_manager_job(
    w: Any,
    project_id: str,
    branch_id: str = "production",
    database: str = "databricks_postgres",
) -> dict:
    """Create/refresh the every-10-minute partition-manager job.

    Maintains forward 15-minute partitions for every ChronoTable so the
    forward window never goes stale.  Returns
    ``{"job", "job_id", "status"}``.
    """
    return _deploy_ops_job(
        w,
        job_name=f"LakeTS Partition Manager — {project_id}/{branch_id}",
        task_key="lakets_partition_manager",
        component="lakets-partition-manager",
        notebook_source=_PARTITION_MANAGER_NOTEBOOK_SOURCE,
        filename="partition_manager",
        project_id=project_id,
        branch_id=branch_id,
        database=database,
        cron=_PARTITION_MANAGER_CRON,
    )


def deploy_rollup_refresh_job(
    w: Any,
    project_id: str,
    branch_id: str = "production",
    database: str = "databricks_postgres",
) -> dict:
    """Create/refresh the every-15-minute rollup-refresh job.

    Calls ``lakets.refresh_rollup_cascade()``.  Returns
    ``{"job", "job_id", "status"}``.
    """
    return _deploy_ops_job(
        w,
        job_name=f"LakeTS Rollup Refresh — {project_id}/{branch_id}",
        task_key="lakets_rollup_refresh",
        component="lakets-rollup-refresh",
        notebook_source=_ROLLUP_REFRESH_NOTEBOOK_SOURCE,
        filename="rollup_refresh",
        project_id=project_id,
        branch_id=branch_id,
        database=database,
        cron=_ROLLUP_REFRESH_CRON,
    )


def deploy_tiering_job(
    w: Any,
    project_id: str,
    branch_id: str = "production",
    database: str = "databricks_postgres",
) -> dict:
    """Create/refresh the every-30-minute cold-tiering job.

    For each ChronoTable with a tiering policy, validates eligible chunks via
    ``lakets.tier_chunk(...)`` (fail-closed on the CDF durability gate).
    Returns ``{"job", "job_id", "status"}``.
    """
    return _deploy_ops_job(
        w,
        job_name=f"LakeTS Tiering — {project_id}/{branch_id}",
        task_key="lakets_tiering",
        component="lakets-tiering",
        notebook_source=_TIERING_NOTEBOOK_SOURCE,
        filename="tiering",
        project_id=project_id,
        branch_id=branch_id,
        database=database,
        cron=_TIERING_CRON,
    )


def deploy_lakets_ops_jobs(
    w: Any,
    project_id: str,
    branch_id: str = "production",
    database: str = "databricks_postgres",
    *,
    partition_manager: bool = True,
    rollup: bool = True,
    retention: bool = True,
    tiering: bool = True,
) -> list[dict]:
    """Deploy the requested LakeTS operational jobs.

    Public orchestrator.  Deploys the partition-manager, rollup-refresh,
    retention, and cold-tiering jobs (each gated by its flag) and returns one
    ``{"job", "job_id", "status"}`` entry per deployed job.
    """
    if not project_id:
        raise ValueError("project_id is required")

    results: list[dict] = []

    if partition_manager:
        results.append(deploy_partition_manager_job(w, project_id, branch_id, database))

    if rollup:
        results.append(deploy_rollup_refresh_job(w, project_id, branch_id, database))

    if tiering:
        results.append(deploy_tiering_job(w, project_id, branch_id, database))

    if retention:
        ret = deploy_retention_job(project_id, branch_id, database)
        results.append(
            {
                "job": ret["job_name"],
                "job_id": ret["job_id"],
                "status": ret["action"],
            }
        )

    return results
