# Deployment evidence

Proof that the isolated `pharma-twin` deployment works end to end, captured from the live
workspace. Every file is redacted: hosts, workspace / warehouse / job / Genie ids, UUIDs, emails,
the catalog name and the owner's handle are replaced by placeholders such as `<catalog>`,
`<workspace-host>` and `02ab07ab-<sp-client-id>`. `scripts/check_identifiers.py` passes on every file.

## 2026-11-20: medical-device (`manufacturing`) real-time pipeline

**Deployment.** App `pharma-twin-dev` (bundle `pharma-digital-twin`, target `dev`, URL
`https://pharma-twin-dev-<workspace-id>.aws.databricksapps.com`) with its **own** service principal
(`02ab07ab-…`), separate from the upstream apps and their principals. Code: `deploy/integration`
with `feat/isolation-prefix` and `feat/medtech-domain` merged. Pipeline: industry `manufacturing`
(Trinity Medical implant cell), mode `realtime` (Kafka on MSK -> Spark Real-Time Mode -> Lakebase
LakeTS ChronoTable -> Lakehouse Sync CDF -> Unity Catalog).

| Claim | Evidence |
|---|---|
| Infrastructure is new and isolated; the app SP holds only the grants it needs | [`00_infra_and_grants.txt`](2026-11-20/00_infra_and_grants.txt) |
| App reachable, `/api/health` reports `db_available: true`, resource prefix `pharma-twin` | [`01_app_health.txt`](2026-11-20/01_app_health.txt) |
| Validation passed all 14 steps for real-time mode; `can-deploy` is true | [`02_validation.txt`](2026-11-20/02_validation.txt) |
| Deploy (without gate override) created the job and a per-pipeline Genie space; start set CDF to streaming | [`03_deploy_start.txt`](2026-11-20/03_deploy_start.txt) |
| Every generated job, topic, branch and ledger resource carries the `pharma-twin` prefix; upstream apps untouched | [`04_resource_names.txt`](2026-11-20/04_resource_names.txt) |
| Producer -> MSK -> RTM consumer -> Lakebase rows land over time; consumer ceiling at 2000 Hz (~950 rows/s) | [`05a_rows_landing_2000hz.txt`](2026-11-20/05a_rows_landing_2000hz.txt) |
| At 500 Hz the pipeline is in steady state (~488 rows/s, ~0 s lag) | [`05b_rows_landing_500hz.txt`](2026-11-20/05b_rows_landing_500hz.txt) |
| The SSE live feed streams events (325 events / 32,500 readings / 19 sensors in 35 s) | [`06_sse_live_feed.txt`](2026-11-20/06_sse_live_feed.txt) |
| Change Data Feed / UC history table is populated (2.1 M inserts, a Delta commit every ~15 s) | [`07_cdf_uc_history.txt`](2026-11-20/07_cdf_uc_history.txt) |
| A Spindle 3 alert was acknowledged and signed decisions recorded (quarantine, NCR, CMM -> cell stopped, then resumed) | [`08_alert_ack_responses.txt`](2026-11-20/08_alert_ack_responses.txt) |
| A **reopened** Spindle 3 alert's quarantine scope covers only that episode's parts (5/5 checks pass) | [`09_reopen_scope_check.txt`](2026-11-20/09_reopen_scope_check.txt) |
| Genie conversation: 3 questions with generated SQL and result rows (with a reviewer note on one SQL) | [`10_genie_transcript.txt`](2026-11-20/10_genie_transcript.txt) |
| `/api/latency/streaming` returns data; measured P50 / P99 over stated windows, tail included | [`11_streaming_latency.txt`](2026-11-20/11_streaming_latency.txt) |
| Pipeline health check run against the new deployment (Overall FAIL, explained) | [`12_health_check_summary.txt`](2026-11-20/12_health_check_summary.txt), snapshot [`ops/notebooks/runs/manufacturing_realtime_2026-11-20.ipynb`](../../ops/notebooks/runs/manufacturing_realtime_2026-11-20.ipynb) |
| Medical-device KPI notebook run on live data (executive + operations) | [`usecases/medtech/medtech_kpis.ipynb`](../../usecases/medtech/medtech_kpis.ipynb) |

### Measured latency (from `<catalog>.pharma_twin.manufacturing_metrics`)

| Window (UTC, batch completion) | Producer | Batches / records | e2e P50 | e2e P99 |
|---|---|---|---|---|
| 16:01:00-16:16:37 | 500 Hz | 3 / 372,257 | 7 ms (every batch) | 15-29 ms per batch, worst 29 ms |
| 15:05-15:40 | 2000 Hz | 2 / 1,598,263 | 7 ms, then 17.9 s | 23 ms, then **317.2 s** (queuing behind the consumer) |

The values are Spark's own per-batch percentiles; the window P99 is bounded above by the worst
batch P99. No earlier latency figure is reused.

### Known gaps found while collecting this evidence

- At the default producer rate (2000 Hz) the real-time consumer writes about 950 rows/s into
  Lakebase with 1 or 2 workers, so it falls behind; the run above uses 500 Hz.
- The real-time job cluster uses `SPOT_WITH_FALLBACK` availability (see the health-check snapshot).
- Industry provisioning does not create the `stream_checkpoints` volume; it was created by hand.
- `provision_lakebase_role` takes the first endpoint, which can be the read-only replica
  (`cannot execute GRANT in a read-only transaction`); harmless here because the SP owns the objects.
- The shared copilot space reports `manual_required` (the SP has CAN_VIEW only, so the table list
  cannot be inspected); the per-pipeline Genie space works.
