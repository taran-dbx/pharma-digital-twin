# Operations tooling

## Pipeline health check (`notebooks/pipeline_health_check.ipynb`)

A standalone Databricks notebook the owner runs by hand to evaluate **one deployed pipeline**
(`industry` x `mode`) end to end. It is not part of the app and does not import the
`iot_twin` package: every helper lives inside the notebook, so it can be imported into a
workspace on its own.

- **Read-only.** Only `SELECT` / `DESCRIBE` / `SHOW` statements and SDK `GET` / `list` calls.
  Lakebase connections are read-only transactions with a statement timeout and row limits; every
  statement is also checked by an allow-list guard before it runs. Nothing is written to
  Lakebase, LakeTS, Unity Catalog, Kafka, jobs or the workspace.
- **Inline only.** The report is rendered in the notebook (`displayHTML` / inline SVG charts).
  No files, volumes or tables are created.
- **Degrades gracefully.** Each check is wrapped: a missing object, missing permission,
  timeout or not-applicable mode becomes a `SKIPPED` row with a reason (`NOT FOUND`,
  `NO ACCESS`, `TIMEOUT`, `NOT APPLICABLE`) instead of a traceback.

### Import into a workspace

```bash
# one file
databricks workspace import /Users/<you>/pipeline_health_check \
  --file ops/notebooks/pipeline_health_check.ipynb --format JUPYTER --language PYTHON \
  --profile <profile>
```

Or add this repository as a **Git folder** (Workspace > Create > Git folder) and open
`ops/notebooks/pipeline_health_check.ipynb`. Attach **serverless** compute (or any cluster),
set the widgets, then *Run all*.

Requires Python 3.11+ (serverless, or a cluster on DBR 15.4 LTS or newer). The live mode needs `databricks-sdk>=0.133.0` (for the `postgres` API) and `psycopg2`.
Add them in the notebook's **Environment** panel (serverless) or the job environment. As a
fallback the first cell installs them when missing on Databricks and restarts Python once; if
the run stops, press *Run all* again.

### Permissions (read-only)

| Surface | Needed |
|---|---|
| Control plane (app-backend Lakebase project) | A Postgres role that can `SELECT` from `pipeline_setup`, `pipeline_deployment`, `provisioned_resource`, `alert`, `alert_event` |
| Pipeline Lakebase project (branch = industry) | A role with `SELECT` on `public.sensor_readings`, the `lakets` schema (registry/chunk/policy/rollup tables) and `wal2delta.tables` |
| Unity Catalog | `USE CATALOG`, `USE SCHEMA` and `SELECT` on the industry tables (`<industry>_<mode>`, `<industry>_metrics`, `lb__shadow_<industry>_sensor_readings_history`) |
| System tables | `SELECT` on `system.lakeflow.zerobus_ingest` (zerobus mode only) |
| SQL warehouse | `CAN USE` on the warehouse recorded in the setup, or run on serverless/cluster compute so `spark.sql` is used |
| Jobs | Permission to view the pipeline job, its runs and the LakeTS ops jobs (`CAN VIEW`) |

Lakebase access uses an OAuth credential generated through the SDK (the same pattern as
`core/lakebase.py`): the token is the password and the role is your user. If your user has no
Postgres role on a project, the affected checks report `NO ACCESS`.

### Widgets

| Widget | Default | Meaning |
|---|---|---|
| `industry` | `manufacturing` | `manufacturing`, `pharmaceutical`, `energy`, `automotive` |
| `mode` | `realtime` | `realtime`, `microbatch` or `zerobus` (one mode per run) |
| `setup_id` | empty | Pipeline setup id; empty resolves the setup + deployment of the industry and mode from the app's control plane |
| `lookback_hours` | `24` | Window for counts, rates and charts (1-720) |
| `redact` | `true` | Mask hosts, ids, emails, ARNs/accounts, buckets and user names in **all** output |
| `sample` | `false` on Databricks, `true` elsewhere | Render the report from the built-in synthetic dataset (no workspace access) |
| `app_backend_project`, `pipelines_project`, `uc_catalog`, `uc_schema`, `warehouse_id` | empty | Advanced overrides; normally discovered from the app's own records |

Widgets can also be passed as job parameters, or (outside Databricks) as `HEALTH_<NAME>`
environment variables, e.g. `HEALTH_SAMPLE=true HEALTH_MODE=zerobus`.

### What it checks

Sections (the scorecard at the top is filled in once every section has run; thresholds are
all in one configuration cell, `Thresholds`):

1. **Header**: parameters, resolved setup/deployment (ids, mode, status, created/updated),
   environment, data sources reached, run timestamp, redaction state.
2. **Pipeline and job health**: `deployment_status`, `cdf_enabled` (realtime), `job_last_run`,
   `job_recent_failures`, `job_task_health`, `job_compute` (real-time mode conf / serverless),
   plus the run history, job cluster configuration and the latest streaming-query progress.
3. **Lakebase hot tier** (realtime only): `lb_connect`, `lb_chronotable`, `lb_rows_window`,
   `lb_freshness`, `lb_ingest_rate`, `lb_silent_sensors`, `lb_nulls`, `lb_duplicates`,
   `lb_out_of_order`, `lb_size`; per-minute and per-hour ingest charts and per-sensor cardinality.
4. **LakeTS outcomes** (realtime only): `lakets_installed`, `lakets_partitions`,
   `lakets_tiering`, `lakets_rollups`, `lakets_lvc`, `lakets_gap_fill`,
   `lakets_rate_of_change`, `lakets_value_range`, `lakets_cdf_state`, `lakets_ops_jobs`
   (partition manager, rollup refresh, tiering, retention jobs), and the partition lifecycle
   (rows tiered/dropped, partitions created).
5. **CDF / Unity Catalog cold tier**: `uc_connect`, `uc_tables`, `uc_rows_window`,
   `uc_freshness`, `uc_last_commit` (`DESCRIBE HISTORY`), `uc_small_files` (`DESCRIBE DETAIL`),
   `uc_cdf_state`, `uc_cdf_lag`.
6. **Streaming metrics**: `metrics_table`, `metrics_freshness`, `metrics_e2e_latency`,
   `metrics_batch_duration`, `metrics_throughput` (realtime/microbatch, from
   `<industry>_metrics`) and `zerobus_ingest` (`system.lakeflow.zerobus_ingest`), with latency,
   throughput and batch-duration charts.
7. **Cross-checks and data quality**: `xc_reconciliation` (Lakebase vs Delta over the same
   closed window), `xc_schema_hot`, `xc_schema_cold` (drift against the sensor schema),
   `xc_duplicates_cold`, `xc_nulls_cold`, `xc_e2e_latency`, `xc_sse_evidence`.

Modes that have no Lakebase tier (`microbatch`, `zerobus`) report the hot-tier and LakeTS
checks as `SKIPPED` with `NOT APPLICABLE`. When the hot tier holds millions of rows the exact
scans only cover the newest slice that fits the query budget (`EXACT_SCAN_MAX_ROWS`); the row
count and hourly chart are then estimates from partition statistics and say so.

### Reading the scorecard

Every check is a row with `status`, measured `value`, `threshold` and an explanation.

| Status | Meaning |
|---|---|
| `PASS` | Within thresholds |
| `WARN` | Past the warn threshold: degraded, look soon |
| `FAIL` | Past the fail threshold or a hard requirement is missing: act now |
| `SKIPPED` | Could not be evaluated (see the reason) or not applicable; never counted as a failure |

The overall status is the worst of all evaluated checks (`NOT EVALUATED` when the pipeline could
not be located). **Recommended next steps** lists the advice attached to every `WARN`/`FAIL`
row, de-duplicated.

### Redaction and sample mode

With `redact=true` a central helper masks hosts, workspace/warehouse/cluster/job/service-principal
and app ids, UUIDs, emails, ARNs and account ids, buckets, user names and every runtime-resolved
identifier (catalog, schema, setup and deployment ids, ...) in **everything** rendered:
tables, charts, stdout/stderr and tracebacks. Secret-shaped values (tokens, passwords, DSNs) are
masked even with `redact=false`.

`sample=true` renders the full report from a deterministic synthetic dataset, clearly bannered
**SAMPLE DATA**. The committed notebook carries the outputs of such a run, so the file renders
fully populated on GitHub and in any viewer. The sample deliberately contains a few `WARN` rows
and one `FAIL` (Lakebase vs Delta reconciliation) to show the recommended-next-steps table.

### Refreshing the saved outputs

Outputs are kept in git on purpose (do not strip them). After changing the notebook:

```bash
pip install -r requirements.txt -e ".[dev]"
python ops/refresh_notebook_outputs.py      # sample-mode run in a fresh kernel, saved in place
ruff format ops && ruff check ops
python scripts/check_identifiers.py ops/notebooks/pipeline_health_check.ipynb
pytest tests/test_pipeline_health_notebook.py
```

`tests/test_pipeline_health_notebook.py` validates the notebook, re-executes it offline in
sample mode, and fails if the committed outputs are missing or contain host/id/email patterns.
Real-workspace runs are not auto-regenerated and are not stored next to the sample notebook.
Deliberately kept, dated, redacted live-run snapshots live in
[`notebooks/runs/`](notebooks/runs/README.md) (how they are produced and re-run is described
there); always run with `sample=false redact=true`.
