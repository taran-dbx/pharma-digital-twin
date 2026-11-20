# Use-case notebooks

Customer-facing notebooks that turn a deployed digital-twin pipeline into business KPIs. They
tell the use-case story first and show only the pipeline context a business reader needs.
Pipeline operations (job health, LakeTS partitions, CDF lag, data-quality checks) are covered
by the [pipeline health check](../ops/notebooks/pipeline_health_check.ipynb), not here.

| Notebook | Industry | What it shows |
|---|---|---|
| [`manufacturing/manufacturing_kpis.ipynb`](manufacturing/manufacturing_kpis.ipynb) | Manufacturing | OEE, downtime and alarms, sensor health trends, throughput versus target |

## Manufacturing KPIs

The committed notebook is a **redacted live run** (first cell: `LIVE RUN, redacted, <date>`)
against the deployed manufacturing real-time pipeline, so it renders fully populated on GitHub
and in any viewer: every output carries `text/html`, `text/markdown` and `text/plain` (charts
also `image/svg+xml`). Hosts, ids, catalog and schema names are replaced by placeholders such as
`<catalog>`.

### Architecture in one paragraph

Sensors stream through Kafka into a Spark Real-Time Mode job that writes every reading to a
Lakebase (Postgres) LakeTS ChronoTable, the **hot tier** that the app dashboard and its SSE live
feed read. Lakehouse Sync (CDF) mirrors that table into a Unity Catalog Delta table, the **cold
tier**. Each streaming batch also records its latency and throughput in a **metrics table**, and
the app's alert engine stores every alert episode in its own Lakebase **alert log**. The notebook
asks the hot tier only for the newest minutes and its freshness; every KPI that aggregates
hours of data runs on Delta, which scans millions of readings cheaply (hot-tier window scans on
thousands of partitions time out).

### Import and run

```bash
databricks workspace import /Users/<you>/manufacturing_kpis \
  --file usecases/manufacturing/manufacturing_kpis.ipynb --format JUPYTER --language PYTHON \
  --profile <profile>
```

Or add the repository as a Git folder and open the notebook. Attach **serverless** compute and
*Run all*. Live mode needs `databricks-sdk>=0.133.0` and `pg8000` (add them in the
Environment panel; the first cell installs them once as a fallback). The notebook uses the
pure-Python `pg8000` driver for Lakebase: importing `psycopg2`'s native library aborted the
serverless Python process in two of three trial runs. With `sample=true` it runs
anywhere, including a laptop, from a synthetic dataset bannered **SAMPLE DATA**.

### Permissions (read-only)

| Surface | Needed |
|---|---|
| App control plane (Lakebase project `iot-app-backend*`, branch `production`) | A Postgres role for your user with `SELECT` on `pipeline_setup`, `pipeline_deployment`, `provisioned_resource`, `alert`, `alert_event`, `production_event` |
| Pipeline Lakebase (branch = `manufacturing`, real-time mode only) | `SELECT` on `public.sensor_readings` and the `lakets` registry tables |
| Unity Catalog | `USE CATALOG`, `USE SCHEMA`, `SELECT` on the telemetry table (`lb__shadow_manufacturing_sensor_readings_history`, or `manufacturing_<mode>`) and `manufacturing_metrics` |
| Compute | Serverless notebook compute (Spark is used for Unity Catalog), or `CAN USE` on the setup's SQL warehouse |

Lakebase access uses an OAuth database credential generated through the SDK for your user.
Every statement is a single `SELECT` checked by an allow-list guard; Lakebase sessions are
read-only transactions with a statement timeout and a row limit. Nothing is written anywhere.

### Widgets

| Widget | Default | Meaning |
|---|---|---|
| `setup_id` | empty | Pipeline setup id; empty resolves the latest manufacturing setup from the app's control plane |
| `mode` | `realtime` | `realtime`, `microbatch` or `zerobus`; a mode that is not deployed shows a clear "not available" state |
| `lookback_hours` | `24` | KPI window (1-168 hours) |
| `redact` | `true` | Mask hosts, ids, emails, catalog/schema names and user names in all output |
| `sample` | `false` on Databricks, `true` elsewhere | Synthetic **SAMPLE DATA** instead of the workspace |

Outside Databricks the same values can be set as `KPI_<NAME>` environment variables, for
example `KPI_SAMPLE=true KPI_MODE=zerobus`.

### KPI definitions

OEE uses the app's own definition (`services/genie/genie_space_content.py`), so the notebook and
the app's Genie space answer the same numbers. The plant model (assets, components, alert rules)
mirrors `src/iot_twin/domain/catalog.py`.

| KPI | Formula | Source tier and table |
|---|---|---|
| Availability | 1 - fault minutes / observed minutes (distinct minutes; at asset level a minute is a fault minute when any of the asset's sensors reports `fault_active`) | Delta cold tier, telemetry table |
| Performance | `AVG(LEAST(value / nominal_value, 1))` over readings with `nominal_value > 0` | Delta cold tier |
| Quality | share of readings with `min_value <= value <= max_value` | Delta cold tier |
| OEE | Availability x Performance x Quality (a missing factor counts as 1), per sensor, per asset (line/cell) and plant-wide; facility (HVAC) sensors excluded | Delta cold tier |
| Alerts | count of `opened` events (one per alert episode) by fault code, asset and severity | Control-plane Lakebase `alert`, `alert_event` |
| Mean time between alerts | (last opening - first opening) / (alerts - 1) = mean gap between openings | Control-plane Lakebase |
| Fault downtime | distinct minutes with `fault_active` per asset | Delta cold tier |
| Operator stops | time the production cell was stopped from the app | Control-plane Lakebase `production_event` |
| Sensor drift | (mean of the newest hour - mean of the window) / nominal; flagged at >= 5% | Delta cold tier (hourly) |
| Past threshold | share of readings beyond the sensor's alert rule (spindle temperature > 60 °C, vibration > 8 mm/s, press pressure < 120 bar); flagged at >= 1% | Delta cold tier (hourly) |
| Live now | mean of the newest 15 minutes | Lakebase hot tier `public.sensor_readings` (real-time mode) |
| Throughput | mean rate per hour x 60 (conveyor m/min to metres per hour, spindle RPM to revolutions per hour) versus nominal x 60 | Delta cold tier (hourly) |
| Pipeline at a glance | freshness, rows/min, rows in window (Delta exact, Lakebase estimate from partition statistics), e2e latency p50 (median of per-batch p50) and p95 (95th percentile of per-batch p99) | Delta, Lakebase, `manufacturing_metrics` |

**Not available from this pipeline:** production counts (units per hour). The only counter-like
signal, `press_1_cycle_count`, is emitted as a level that fluctuates around its nominal value,
not as an incrementing count, and the plant model has no good-part or reject sensor. The
notebook checks this on every run (a real counter never decreases) and shows a "not available"
block instead of inventing numbers; throughput is reported as rate versus target.

### Refreshing the stored outputs

The stored notebook goes stale; refresh it deliberately, always with `sample=false redact=true`:

1. Import the notebook to a temporary path under your home folder (command above).
2. Submit a one-time serverless run:

   ```bash
   databricks jobs submit --no-wait -p <profile> --json '{
     "run_name": "manufacturing-kpis",
     "tasks": [{"task_key": "main",
       "notebook_task": {"notebook_path": "<temp path>",
         "base_parameters": {"mode": "realtime", "lookback_hours": "24",
           "sample": "false", "redact": "true"}},
       "environment_key": "e"}],
     "environments": [{"environment_key": "e", "spec": {"client": "4",
       "dependencies": ["databricks-sdk>=0.133.0", "pg8000"]}}]}'
   ```

3. Export the finished task run and convert it with the ops exporter (it keeps the outputs, adds
   the text fallbacks, replaces the `LIVE RUN` label and refuses partial runs):

   ```bash
   databricks jobs export-run <task_run_id> --views-to-export ALL -p <profile> -o json > export.json
   python ops/export_run_notebook.py export.json usecases/manufacturing/manufacturing_kpis.ipynb \
     --date <YYYY-MM-DD> --source usecases/manufacturing/manufacturing_kpis.ipynb
   ```

4. Scan, test, and delete the temporary workspace notebook:

   ```bash
   python scripts/check_identifiers.py usecases/manufacturing/manufacturing_kpis.ipynb
   pytest tests/test_manufacturing_kpi_notebook.py
   ```

Never commit a run made with `redact=false`. After editing the notebook's code, run
`ruff format usecases && ruff check usecases`, then refresh the stored outputs as above
(`pytest` re-executes the notebook offline in sample mode, so code changes are tested without a
workspace).
