# Deployment evidence

Proof that the isolated `pharma-twin` deployment works end to end, captured from the live
workspace. Every file is redacted: hosts, workspace / warehouse / job / Genie ids, UUIDs, emails,
the catalog name and the owner's handle are replaced by placeholders such as `<catalog>`,
`<workspace-host>` and `02ab07ab-<uuid>`. `scripts/check_identifiers.py` and
`scripts/check_credential_literals.py` pass on every file. The ML model card and endpoint queries
from the risk-model pull request are in [`ml/`](ml/README.md).

## 2026-11-21: real-time throughput and the Lakehouse Sync stall

**Change.** `fix/pipeline-throughput` (stacked on `deploy/integration`): LakeTS's per-row INSERT triggers on
`public.sensor_readings` (last-value cache and CDF shadow) replaced by statement-level ones; a stuck-sync check,
a lag-proof gap-fill check and a runbook in the health check. App deployed to `pharma-twin-dev` at 08:11 UTC; the
swap ran live through `POST /api/admin/pipelines/<setup-id>/realtime/reconcile-cdf` at 08:13 (as the app SP).

| Claim | Evidence |
|---|---|
| The sync stopped three times, each right after a multiple of `wal2delta.checkpoint_interval` (200). Lakebase kept receiving rows the whole time, and it is not a schema, replica-identity, long-transaction or input problem | [`01_sync_version_timeline.txt`](2026-11-21/01_sync_version_timeline.txt), [`10_sync_after.txt`](2026-11-21/10_sync_after.txt) |
| The consumer is not row-by-row; the cost is two per-row PL/pgSQL triggers in Postgres (~0.7-1.0 ms per row), plus a 6.6 GB never-trimmed shadow | [`02_lakebase_write_path.txt`](2026-11-21/02_lakebase_write_path.txt) |
| With 3 Kafka partitions keyed by sensor, one partition carries 47.4 % of the rows, so one writer must do ~850 rows/s at 2000 Hz; more Spark workers cannot help | [`03_partition_skew.txt`](2026-11-21/03_partition_skew.txt) |
| On a branch of the live database with the real `lakets.sql`: 96 ms -> 11 ms per 100-row flush (~1,030 -> ~8,000 rows/s per writer), 822 -> 600 B WAL/row; `synchronous_commit=off` would add only ~7 % (not used) | [`04_write_benchmark.txt`](2026-11-21/04_write_benchmark.txt) |
| `lakets_gap_fill` 40.2 % on 2026-11-20 was 12 minutes of ingest lag; the same window is 570/570 buckets today | [`05_gap_fill_explained.txt`](2026-11-21/05_gap_fill_explained.txt) |
| The live swap kept the shadow exact (30,000 = 30,000, 0 missing / extra / duplicate) and the LVC fresh | [`06_trigger_swap_live.txt`](2026-11-21/06_trigger_swap_live.txt) |
| Before (500 Hz, old triggers): ~488 rows/s, P50 7 ms, P99 median 15 ms, worst 28.3 s | [`07_before_500hz_metrics.txt`](2026-11-21/07_before_500hz_metrics.txt) |
| After, at the default 2000 Hz target for 32 min: ~1,780 rows/s written, which is all the producer sends (0 s lag); at a 2400 target for 18 min: 2,114-2,150 rows/s, 0 s lag. WAL 1.3-1.56 MB/s; the slot holds 11-52 MB; Delta commits every 15 s | [`08_run_samples_after.txt`](2026-11-21/08_run_samples_after.txt) |
| e2e latency after the fix (Spark per-batch percentiles) | [`09_latency_after.txt`](2026-11-21/09_latency_after.txt) |
| Lakebase vs Delta exact over closed windows (2,136,493 and 1,922,667 rows) | [`10_sync_after.txt`](2026-11-21/10_sync_after.txt) |
| Escalation brief for the Lakebase / Lakehouse Sync owners (pattern, timestamps, file counts, what we cannot see, the ask) | [`12_lakehouse_sync_escalation.md`](2026-11-21/12_lakehouse_sync_escalation.md) |
| Producer shortfall diagnosed (sleep-per-tick drift, ~11 %) and fixed with a wall-clock pacer; sensors pinned round-robin to partitions (36.8 % busiest on 3, 21.1 % on 6); 6-partition plan. Code and tests only: live proof waits for the next redeploy | [`13_producer_rate_and_partitions.md`](2026-11-21/13_producer_rate_and_partitions.md) |
| Health check re-run on the live deployment | [`11_health_check_summary.txt`](2026-11-21/11_health_check_summary.txt), snapshot [`ops/notebooks/runs/manufacturing_realtime_2026-11-21.ipynb`](../../ops/notebooks/runs/manufacturing_realtime_2026-11-21.ipynb) |

### Measured throughput and latency (from `<catalog>.pharma_twin.manufacturing_metrics` and the hot-tier samples)

| Window (UTC) | Producer target | Rows/s written | Ingest lag | e2e P50 (median / worst batch) | e2e P99 (median / worst batch) |
|---|---|---|---|---|---|
| 2026-11-20 15:05-15:40 (old triggers) | 2000 | ~950 (producer ~1,795) | grew ~10 s/min | 7 ms / 17.9 s | - / 317.2 s |
| 2026-11-21 08:30:51-08:55:56, 6 batches, 3,216,700 records | 2000 (default) | 1,778-1,785 | -0.2..0.1 s | 7 / 7 ms | 19 / 40 ms |
| 2026-11-21 09:12:33-09:22:35, 3 batches, 1,935,379 records | 2400 | 2,141-2,146 | 0.0-0.1 s | 7 / 8 ms | 22 / 379 ms |

At its default target of 2000/s the producer delivers ~1,780 rows/s. That is the producer's own rate: it also
acked ~1,795 on 2026-11-20. The 2400 run shows the consumer sustaining more than 2,000 rows/s with no backlog.

## 2026-11-20: medical-device (`manufacturing`) real-time pipeline

**Deployment.** App `pharma-twin-dev` (bundle `pharma-digital-twin`, target `dev`,
`mode: development`, URL `https://pharma-twin-dev-<workspace-id>.aws.databricksapps.com`) with its
**own** service principal (`02ab07ab-…`), separate from the upstream apps and their principals.
Code: `deploy/integration` with `feat/isolation-prefix` @571cce0, `feat/medtech-domain` @0cabab1,
`feat/value-views` @5cf9843, `feat/risk-model` @922d0c0 (later @7a90b0a, see `17`) and `feat/grounded-action-plan` @12c452a
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
| Post-redeploy check after merging `feat/risk-model` @7a90b0a (head 64bd902, deployed 22:10:04 UTC): health ok, a predicted alert scored with model version 4 | [`17_post_redeploy_check.txt`](2026-11-20/17_post_redeploy_check.txt) |
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

### Known gaps found while collecting this evidence (status updated 2026-11-21)

- **Health check FAIL, two causes.** (1) Lakehouse Sync stopped, so the cold-tier, CDF and
  reconciliation checks fail; (2) the consumer fell behind the producer, so the freshness,
  silent-sensor and latency checks fail. *2026-11-21:* `lakets_gap_fill` 40.2 % was ingest lag (12 min);
  the check now ignores lag ([`05`](2026-11-21/05_gap_fill_explained.txt)). Re-run: [`11`](2026-11-21/11_health_check_summary.txt).
- **Predicted alerts counted as excursions (fixed here).** With the risk model merged, every
  predicted alert became a value/ops episode; the scoped CTE now excludes the model rule
  (`13`, `14` recaptured).
- **Lakehouse Sync stall (open, escalated; detection added).** The UC history table stopped at Delta version 1000 (19:29:32);
  2.2 GB of WAL lag; the pipeline branch's compute then returned `Couldn't connect to compute node`
  and restarted. Consumer throughput fell below the producer rate from 19:32 (`07b`, `11b`).
  *2026-11-21:* the same sync resumed at 21:55 and has committed through version 2300+. It stalled again
  after 1800 (354 min) and 2000 (9.7 min). All three stalls begin right after a multiple of
  `wal2delta.checkpoint_interval` (200), two end exactly when a predictive-optimization OPTIMIZE commits, and
  one happened at 500 Hz with an empty slot. So the cause sits inside the managed writer, not in this app's WAL
  volume ([`01`](2026-11-21/01_sync_version_timeline.txt), [`10`](2026-11-21/10_sync_after.txt)).
  `lakets_sync_slot` now flags it within 2-10 min; runbook in [`ops/README.md`](../../ops/README.md).
- ~~At the default producer rate (2000 Hz) the real-time consumer writes about 950 rows/s into
  Lakebase with 1 or 2 workers, so it falls behind; these runs use 500 Hz.~~ *Fixed 2026-11-21*: per-row
  LakeTS triggers replaced by statement-level ones. The consumer now keeps up with 1 worker (~1,780 rows/s at
  the default target, 2,140 at 2400, 0 s lag) ([`08`](2026-11-21/08_run_samples_after.txt)).
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
- *2026-11-21, fixed in code (live proof pending a redeploy):* with 3 partitions keyed by `sensor_name`,
  one partition carried 47.4 % of the rows ([`03`](2026-11-21/03_partition_skew.txt)). The producer now pins
  each sensor to one partition round-robin (7/6/6, busiest 36.8 %; ordering per sensor kept). A 3 -> 6
  partition plan is written but NOT executed ([`13`](2026-11-21/13_producer_rate_and_partitions.md)).
- *2026-11-21, fixed in code (live proof pending a redeploy):* the CDF shadow (6.6 GB, 28 M rows) and the
  hot tier (240 chunks, 7.2 GB) had no retention, ~1.7 GB/h at 2000 Hz.
  - Hot tier: LakeTS tiering + tiered retention, tier after 1 h, drop after 24 h, only once durable in Delta.
  - Shadow: `lakets.prune_cdf_shadow()` deletes only rows whose inserting transaction finished before a WAL
    mark the sync has committed. It fails closed when the shadow is not STREAMING; covered by local-Postgres
    tests for a stall and an in-flight writer.
  - Off by default (`LAKETS_RETENTION_ENABLED`): when enabled, CDF reconciliation installs it and the
    Tiering job prunes the shadow. Kept off live pending a data-loss review of the shadow deletes.
- *2026-11-21, fixed in code (live proof pending a redeploy):* at its default target the producer sent
  ~1,780 rows/s. The simulator's sleep-per-tick loop lost ~11 %; a wall-clock pacer now meets the target
  (unit-tested to within 0.5 %).
- *2026-11-21, fixed:* `metrics_batch_duration` FAILed on Real-Time Mode's normal 300 s trigger. For RTM it
  now judges the per-record processing p99 (WARN 250 ms / FAIL 1 s); micro-batch keeps the batch-duration rule.
- *2026-11-21, fixed:* the app's `cdf_sync` check reported a stuck sync as "STREAMING (idle)" (PASS). It now
  FAILs after 10 min without a wal2delta write while rows keep landing.
