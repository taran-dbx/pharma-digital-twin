# Deployment evidence

Proof that the isolated `pharma-twin` deployment works end to end, captured from the live
workspace. Every file is redacted: hosts, workspace / warehouse / job / Genie ids, UUIDs, emails,
the catalog name and the owner's handle are replaced by placeholders such as `<catalog>`,
`<workspace-host>` and `02ab07ab-<uuid>`. `scripts/check_identifiers.py` and
`scripts/check_credential_literals.py` pass on every file. The ML model card and endpoint queries
from the risk-model pull request are in [`ml/`](ml/README.md).

## 2026-11-20: medical-device (`manufacturing`) real-time pipeline

**Deployment.** App `pharma-twin-dev` (bundle `pharma-digital-twin`, target `dev`,
`mode: development`, URL `https://pharma-twin-dev-<workspace-id>.aws.databricksapps.com`) with its
**own** service principal (`02ab07ab-…`), separate from the upstream apps and their principals.
Code: `deploy/integration` with `feat/isolation-prefix` @571cce0, `feat/medtech-domain` @0cabab1,
`feat/value-views` @5cf9843, `feat/risk-model` @922d0c0 and `feat/grounded-action-plan` @12c452a
merged. Pipeline: industry `manufacturing` (Trinity Medical implant cell), mode `realtime` (Kafka
on MSK -> Spark Real-Time Mode -> Lakebase LakeTS ChronoTable -> Lakehouse Sync CDF -> Unity
Catalog). The risk model is UC model `spindle_thermal_risk` v4, served by the bundle's dev endpoint
`dev_<user>_pharma-twin-spindle-risk`; the action plans use `databricks-claude-sonnet-5-5`.

Files `01`-`12` were captured between 14:30 and 16:30 UTC (isolation + medtech code); `01b`, `05c`,
`06b`, `07b`, `11b` and `12`-`16` after the final redeploy (~20:00-21:15 UTC).

| Claim | Evidence |
|---|---|
| Infrastructure is new and isolated; the app SP holds only the grants it needs | [`00_infra_and_grants.txt`](2026-11-20/00_infra_and_grants.txt) |
| App reachable, `/api/health` reports `db_available: true`, resource prefix `pharma-twin` | [`01_app_health.txt`](2026-11-20/01_app_health.txt) |
| Final deployment: app name, URL, own SP, bound resources (risk endpoint, GenAI endpoint, salt secret), health, admin route working | [`01b_app_health_final.txt`](2026-11-20/01b_app_health_final.txt) |
| Validation passed all 14 steps for real-time mode; `can-deploy` is true | [`02_validation.txt`](2026-11-20/02_validation.txt) |
| Deploy (without gate override) created the job and a per-pipeline Genie space; start set CDF to streaming | [`03_deploy_start.txt`](2026-11-20/03_deploy_start.txt) |
| Every generated job, topic, branch and ledger resource carries the `pharma-twin` prefix; upstream apps untouched | [`04_resource_names.txt`](2026-11-20/04_resource_names.txt) |
| Producer -> MSK -> RTM consumer -> Lakebase rows land over time; consumer ceiling at 2000 Hz (~950 rows/s) | [`05a_rows_landing_2000hz.txt`](2026-11-20/05a_rows_landing_2000hz.txt) |
| At 500 Hz the pipeline is in steady state (~488 rows/s, ~0 s lag) | [`05b_rows_landing_500hz.txt`](2026-11-20/05b_rows_landing_500hz.txt) |
| Rows still land after the final redeploy, but the consumer runs ~10 min behind and the pipeline compute restarts | [`05c_rows_landing_final.txt`](2026-11-20/05c_rows_landing_final.txt) |
| The SSE live feed streams events (325 events / 32,500 readings / 19 sensors in 35 s; and again on the final deployment) | [`06_sse_live_feed.txt`](2026-11-20/06_sse_live_feed.txt), [`06b_sse_live_feed_final.txt`](2026-11-20/06b_sse_live_feed_final.txt) |
| Change Data Feed / UC history table is populated (2.1 M inserts by 15:58, a Delta commit every ~15 s; 8.1 M rows by 19:29) | [`07_cdf_uc_history.txt`](2026-11-20/07_cdf_uc_history.txt) |
| **Open issue:** Lakehouse Sync stopped at Delta version 1000 at 19:29:32 | [`07b_cdf_stall.txt`](2026-11-20/07b_cdf_stall.txt) |
| A Spindle 3 alert was acknowledged and signed decisions recorded (quarantine, NCR, CMM -> cell stopped, then resumed) | [`08_alert_ack_responses.txt`](2026-11-20/08_alert_ack_responses.txt) |
| A **reopened** Spindle 3 alert's quarantine scope covers only that episode's parts (5/5 checks pass) | [`09_reopen_scope_check.txt`](2026-11-20/09_reopen_scope_check.txt) |
| Genie: question, generated SQL and result rows (3 questions, with a reviewer note on one SQL) | [`10_genie_transcript.txt`](2026-11-20/10_genie_transcript.txt) |
| `/api/latency/streaming` returns data; P50 / P99 measured over stated windows | [`11_streaming_latency.txt`](2026-11-20/11_streaming_latency.txt) |
| Latency: 3.5 h steady state, then the degradation tail, from the metrics table | [`11b_streaming_latency_steady_and_tail.txt`](2026-11-20/11b_streaming_latency_steady_and_tail.txt) |
| Pipeline health check run against the new deployment (Overall FAIL: two causes, the CDF stall and the consumer falling behind) | [`12_health_check_summary.txt`](2026-11-20/12_health_check_summary.txt), snapshot [`ops/notebooks/runs/manufacturing_realtime_2026-11-20.ipynb`](../../ops/notebooks/runs/manufacturing_realtime_2026-11-20.ipynb) |
| `/api/value/summary` returns real data; recaptured after the predicted-alert fix (73 excursions == Lakebase; 6 predicted alerts reported apart in `meta.predicted_alerts`) | [`13_value_summary.json.txt`](2026-11-20/13_value_summary.json.txt) |
| `/api/ops/summary` returns real data, recaptured likewise; one percentile defect noted | [`14_ops_summary.json.txt`](2026-11-20/14_ops_summary.json.txt) |
| A grounded AI action plan for a real Spindle 3 alert: grounding check, Lakebase audit row, UC mirror row, and Genie answering "which rows did the AI use?" | [`15_genai_action_plan.txt`](2026-11-20/15_genai_action_plan.txt) |
| A predicted (model) alert appears and clears; its lead time measured honestly | [`16_predicted_alert.txt`](2026-11-20/16_predicted_alert.txt) |
| Medical-device KPI notebook on live data: EXECUTIVE, OPERATIONS, MODEL RISK SCORE, AI ACTION PLANS | [`usecases/medtech/medtech_kpis.ipynb`](../../usecases/medtech/medtech_kpis.ipynb) |

### Measured latency (from `<catalog>.pharma_twin.manufacturing_metrics`, mode `realtime`)

| Window (UTC, batch completion) | Producer | Batches / records | e2e P50 | e2e P99 |
|---|---|---|---|---|
| 16:01:00-19:27:13 (steady state) | 500 Hz | 41 / 5,953,735 | 7 ms in every batch | median batch 13 ms, worst batch 29 ms |
| 19:30:00-20:49:28 (degradation) | 500 Hz | 9 / 2,121,825 | 7 ms, worst batch 103.6 s | median batch 124.3 s, worst batch **496.1 s** |
| 15:05-15:40 | 2000 Hz | 2 / 1,598,263 | 7 ms, then 17.9 s | 23 ms, then **317.2 s** |

**The degradation continued past the capture window:** the worst batch P99 from 16:01 to 21:07 is
**579.4 s** (batch 50 at 21:07:58; 51 batches, 53 counting the two 2000 Hz batches at 15:15 and
15:38), and later batches reached 800.0 s (21:27) and 861.5 s (21:49).

The values are Spark's own per-batch percentiles; a window's P99 is bounded above by its worst
batch P99. The tails are queuing (`processing_p99` stays 1 ms): the consumer fell behind the
producer. No earlier latency figure is reused.

### Known gaps found while collecting this evidence

- **Health check FAIL, two causes.** (1) Lakehouse Sync stopped, so the cold-tier, CDF and
  reconciliation checks fail; (2) the consumer fell behind the producer, so the freshness,
  silent-sensor and latency checks fail. `lakets_gap_fill` at 40.2 % of buckets is **not explained**
  by either and needs its own look.
- **Predicted alerts counted as excursions (fixed here).** With the risk model merged, every
  predicted alert became a value/ops episode; the scoped CTE now excludes the model rule
  (`13`, `14` recaptured).
- **Lakehouse Sync stall (open).** The UC history table stopped at Delta version 1000 (19:29:32);
  2.2 GB of WAL lag; the pipeline branch's compute then returned `Couldn't connect to compute node`
  and restarted. Consumer throughput fell below the producer rate from 19:32 (`07b`, `11b`).
- At the default producer rate (2000 Hz) the real-time consumer writes about 950 rows/s into
  Lakebase with 1 or 2 workers, so it falls behind; these runs use 500 Hz.
- The thermal-risk model gave no demonstrated early warning on this signal (`16`): the demo heat
  event has no precursor, and the scorer read a hot tier ~8 min behind during the degradation.
- `/api/ops/summary` computes `time_to_acknowledge` / `time_to_disposition` percentiles over all
  episodes with unacknowledged ones as 0 (`14`).
- Industry provisioning does not create the `stream_checkpoints` volume; it was created by hand.
- `provision_lakebase_role` takes the first endpoint, which can be the read-only replica
  (`cannot execute GRANT in a read-only transaction`); harmless here because the SP owns the objects.
- The real-time job cluster uses `SPOT_WITH_FALLBACK` availability (see the health-check snapshot).
- The shared copilot space reports `manual_required` (the SP has CAN_VIEW only); the per-pipeline
  Genie space works and now includes `intelligence_genai_audit`.
