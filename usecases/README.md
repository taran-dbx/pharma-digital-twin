# Use-case notebooks

Customer-facing notebooks that turn a deployed digital-twin pipeline into business KPIs. They
tell the use-case story first and show only the pipeline context a business reader needs.
Pipeline operations (job health, LakeTS partitions, CDF lag, data-quality checks) are covered
by the [pipeline health check](../ops/notebooks/pipeline_health_check.ipynb), not here.

| Notebook | Industry | What it shows |
|---|---|---|
| [`manufacturing/manufacturing_kpis.ipynb`](manufacturing/manufacturing_kpis.ipynb) | Medical Devices (id `manufacturing`) | Process health index (telemetry approximation), downtime and alarms (validated-limit excursions), sensor health trends, rate versus target |
| [`medtech/medtech_kpis.ipynb`](medtech/medtech_kpis.ipynb) | Medical Devices (id `manufacturing`) | EXECUTIVE: cost of poor quality avoided (value model), lots quarantined versus released, exposure remaining. OPERATIONS: time to detect / acknowledge, NCRs and CMM inspections, projected right-first-time, MTTR, streaming latency distribution. MODEL RISK SCORE: predicted alerts and their lead time. AI ACTION PLANS: grounded plans, grounding pass rate, applied |

## Medical-device KPIs

**The story.** Trinity Medical machines orthopaedic implants in Warsaw, IN. Spindle 3 on the CNC-1
5-axis mill cuts Ti-6Al-4V femoral hip stems and acetabular cups; parts flow MILL -> CMM INSPECT ->
CLEAN / PASSIVATE -> STERILE PACK. The Spindle 3 temperature limit (60 °C) is a validated process
parameter, so an excursion (`VPP-SPINDLE3-TEMP`) puts the parts cut during it at risk of a
dimensional nonconformance. The notebook serves the two KPI owners:

| Owner | KPIs |
|---|---|
| Alex Morgan, VP Quality & Site Head | cost of poor quality, lots scrapped / quarantined, lot-release lead time |
| Shreya, Head of Manufacturing Operations | time-to-detect, NCRs opened, projected right-first-time (first-pass yield proxy), MTTR |

It reports what the telemetry and the app's alert log support: process health per asset (a
telemetry index, not first-pass yield), excursion counts and timing, drift toward validated limits and rates.
CMM pass/fail results, NCR dispositions and lot release are not in the pipeline; lots, parts at
risk and signed decisions are in the app's decision log (see
[docs/domain-medtech.md](../docs/domain-medtech.md)). The notebook keeps the internal industry
id `manufacturing`, so table and branch names are unchanged.

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
| App control plane (Lakebase project `pharma-twin-backend*` (`<RESOURCE_PREFIX>-backend`), branch `production`) | A Postgres role for your user with `SELECT` on `pipeline_setup`, `pipeline_deployment`, `provisioned_resource`, `alert`, `alert_event`, `production_event` |
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

The process health index uses the app's own definition (`services/genie/genie_space_content.py`):
availability x performance x quality, the classic OEE product, labelled a *process health index
(telemetry approximation)*. It is not first-pass yield. The only first-pass-yield proxy is the
parts-based one the app and Genie share: **Projected right-first-time** = (parts started − parts at risk) / parts started, over the lots an excursion touched. Parts at risk are the parts whose cut overlapped the time Spindle 3 was outside its validated limit. *Projected*, because no CMM results exist yet. The notebook and the app's Genie space answer the same numbers. The plant model (assets, components, alert rules)
mirrors `src/iot_twin/domain/catalog.py`.

| KPI | Formula | Source tier and table |
|---|---|---|
| Availability | 1 - fault minutes / observed minutes (distinct minutes; at asset level a minute is a fault minute when any of the asset's sensors reports `fault_active`) | Delta cold tier, telemetry table |
| Performance | `AVG(LEAST(value / nominal_value, 1))` over readings with `nominal_value > 0` | Delta cold tier |
| Quality | share of readings with `min_value <= value <= max_value` | Delta cold tier |
| Process health index (telemetry approximation) | Availability x Performance x Quality (a missing factor counts as 1), per sensor, per asset (line/cell) and plant-wide; facility (HVAC) and CMM lab environment sensors excluded | Delta cold tier |
| Alerts | count of `opened` events (one per alert episode) by fault code, asset and severity | Control-plane Lakebase `alert`, `alert_event` |
| Mean time between alerts | (last opening - first opening) / (alerts - 1) = mean gap between openings | Control-plane Lakebase |
| Fault downtime | distinct minutes with `fault_active` per asset | Delta cold tier |
| Operator stops | time the production cell was stopped from the app (for example by a `request_cmm_inspection` decision) | Control-plane Lakebase `production_event` |
| Sensor drift | (mean of the newest hour - mean of the window) / nominal; flagged at >= 5% | Delta cold tier (hourly) |
| Past threshold | share of readings beyond the sensor's alert rule (Spindle 3 temperature > 60 °C validated limit, vibration > 8 mm/s, passivation line pressure < 120 bar); flagged at >= 1% | Delta cold tier (hourly) |
| Live now | mean of the newest 15 minutes | Lakebase hot tier `public.sensor_readings` (real-time mode) |
| Throughput | mean rate per hour x 60 (sterile pack line m/min to metres per hour, spindle RPM to revolutions per hour) versus nominal x 60 | Delta cold tier (hourly) |
| Pipeline at a glance | freshness, rows/min, rows in window (Delta exact, Lakebase estimate from partition statistics), e2e latency p50 (median of per-batch p50) and p95 (95th percentile of per-batch p99) | Delta, Lakebase, `manufacturing_metrics` |

**Not available from this pipeline:** production counts (units per hour). The only counter-like
signal, `press_1_cycle_count`, is emitted as a level that fluctuates around its nominal value,
not as an incrementing count, and the plant model has no CMM pass/fail or reject sensor. The
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

## Medical-device executive, operations, model and AI KPIs (`medtech/medtech_kpis.ipynb`)

A two-persona view of the same Trinity Medical implant cell, built from the alert episodes and
**signed decisions** the app records (Part 11-style `alert_response` rows), the lot context on
`cnc_1_*` readings and the streaming metrics table. The committed notebook is a redacted live run
(first cell `LIVE RUN, redacted, <date>`) against the isolated `pharma-twin` deployment.

| Section | Owner | KPIs | Source |
|---|---|---|---|
| EXECUTIVE | Alex Morgan, VP Quality & Site Head | cost of poor quality avoided, lots / parts quarantined versus released, exposure remaining | control-plane Lakebase (`alert_event`, `alert_response`), Delta lot volume |
| OPERATIONS | Shreya, Head of Manufacturing Operations | time to detect, time to acknowledge, NCRs and CMM inspections, projected right-first-time, MTTR, e2e latency distribution | control-plane Lakebase, Delta breach onsets, `<industry>_metrics` |
| MODEL RISK SCORE | both | predicted alerts, breaches warned >= 30 s ahead and the lead, time open, scoring lag | control-plane Lakebase (`alert_event` of rule `medtech-model-spindle-risk`) |
| AI ACTION PLANS | both | plans generated, grounding check passed, applied by a person, latency and tokens, latest plan summaries | control-plane Lakebase (`genai_action_plan_audit`, append-only; requester never read) |

**Value model.** `COPQ avoided = events x capture rate x (lot cost - inspection cost)`, where
*events* are Spindle 3 excursion episodes (alert rule `mfg-alert-spindle-temp`) in the window. Lot
cost, inspection cost and capture rate are widgets (`lot_cost_usd`, `inspection_cost_usd`,
`capture_rate`; defaults 24,000 / 1,500 / 0.8 are illustrative assumptions) and a sensitivity table
shows the result at 50-100 % capture. *Exposure remaining* is the lots cut during excursions that
have no signed quarantine decision, times the lot cost.

**Definitions.** Time to detect = alert opened minus the first second the raw telemetry crossed
60 °C (onsets sessionised in Delta). Time to acknowledge / MTTR = acknowledged / cleared event minus
the opened event of the same episode (one alert row is reused per rule, so episodes are paired in
order). *Projected right-first-time* uses the app's single definition
(`iot_twin.domain.lot_context.PROJECTED_RFT_DEFINITION`, pooled over every excursion); the lot clock
and that formula are mirrored in the notebook and pinned to the app by
`tests/test_medtech_kpi_notebook.py`. The notebook never reports an A x P x Q index as yield.
*Lead time* credits each predicted opening to at most one breach, the first one at or after it
within one 300 s heat cycle; the notebook says plainly that a lead shorter than the demo's heat
cycle cannot be told apart from chance.

**Reading the committed run (6 h window, 15:07-21:07 UTC on 2026-11-20).** 64 Spindle 3 excursions;
COPQ avoided $1.15 M at the default assumptions; 1 lot quarantined by a signed decision;
projected right-first-time 67.0 % (the app's `/api/ops/summary` reports 67.88 % over 24 h); time to
detect median 10 s; MTTR median 53 s; 4 predicted alerts, 2 of 64 breaches warned >= 30 s ahead;
1 AI action plan, grounding passed, not applied. The cold tier lagged the stream in the last hour
of that window (Lakehouse Sync stalled at 19:29, see `docs/evidence/README.md`).

**Run it.** Same import and job-submit recipe as the manufacturing notebook (above), with `notebook_path` pointing at
`medtech_kpis`, `MEDTECH_<NAME>` environment variables outside Databricks, and these widgets:
`setup_id`, `mode`, `lookback_hours`, `redact`, `sample`, `app_backend_project` (default
`pharma-twin-backend`), `pipelines_project` (default `pharma-twin-pipelines`) and the three
value-model widgets. The Lakebase projects are always explicit (never discovered by prefix), so
the notebook cannot read the upstream deployment's `iot-*` projects in the same workspace. Then
export and convert with `python ops/export_run_notebook.py export.json
usecases/medtech/medtech_kpis.ipynb --date <YYYY-MM-DD> --source usecases/medtech/medtech_kpis.ipynb`,
remove the `snapshot-guard` cell the exporter adds (this notebook is meant to stay runnable; the
guard is for the stored health-check snapshots), scan with `scripts/check_identifiers.py` and run `pytest tests/test_medtech_kpi_notebook.py`.
