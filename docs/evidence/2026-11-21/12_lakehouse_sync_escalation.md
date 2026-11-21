# Escalation: Lakehouse Sync (wal2delta) writer stalls at checkpoint-interval versions

**For:** Lakebase / Lakehouse Sync owners. **From:** the `pharma-twin` demo deployment
(AWS us-east-1, one workspace). **Impact:** each stall stops Lakebase -> Unity Catalog CDF
for minutes to hours, while the replication slot holds every WAL byte (2.2 GB after 1.5 h at
500 rows/s). On 2026-11-20 the branch compute then refused connections and restarted.

## What we see

One CDF config (`lakets_cdf` schema -> `<catalog>.pharma_twin`), one STREAMING table
`lakets_cdf._shadow_manufacturing_sensor_readings` (REPLICA IDENTITY FULL, insert-only,
~500-2,100 rows/s). It is published to
`<catalog>.pharma_twin.lb__shadow_manufacturing_sensor_readings_history` (managed Delta,
predictive optimization inherited ENABLE). The writer commits one `wal2delta.append` every
15 s (`wal2delta.refresh_interval = 15`), i.e. ~240 small files per hour.

| Stall | Last version before | Stopped (UTC) | Resumed (UTC) | Length | What ended it | Files compacted by that OPTIMIZE |
|---|---|---|---|---|---|---|
| 1 | 1000 | 2026-11-20 19:29:32 | 21:55:27 (v1002) | 146 min | v1001 predictive OPTIMIZE committed 21:55:06 | 767 |
| 2 | 1800 | 2026-11-21 01:14:10 | 07:08:11 (v1801) | 354 min | resumed on its own (branch compute had restarted at 05:12:50) | - |
| 3 | 2000 | 2026-11-21 07:44:42 | 07:54:36 (v2002) | 9.7 min | v2001 predictive OPTIMIZE committed 07:54:25 | 997 |

- **Pattern:** all three begin right after a multiple of `wal2delta.checkpoint_interval = 200`.
  The other eight multiples passed: 200, 400, 600, 800, 1200, 1400, 1600 and 2200 (2200 at 08:49 under 1,780 rows/s).
- **File count at each boundary** (appends since the last compaction): every boundary with
  fewer than ~600 uncompacted files passed. The three that stalled had ~770-1,000 (v1000: 767,
  v1800: ~800, v2000: 997). After each compaction the next boundaries passed again.
- **The OPTIMIZE did not block the writer.** `system.storage.predictive_optimization_operations_history`
  shows the stall-1 COMPACTION ran 21:54:58.764 -> 21:55:08.950 (10 s), starting 2 h 25 min
  into the stall; the writer committed 21 s after it finished. The v234 compaction (231 files,
  16:18, 10 s) caused no stall. Stall 3's compaction is not in the system table yet (latency).
- **Not the cause** (checked on the branch):
  - no schema change: no ALTER, and no Delta operations other than appends, predictive OPTIMIZE and v1 resnapshot;
  - replica identity is fine: the shadow is REPLICA IDENTITY FULL and STREAMING throughout;
  - no long or large transactions: `pg_stat_replication_slots`: 0 spilled, 0 streamed of 349,864 txns;
  - input never stopped: 1.76 M rows/h kept landing during stalls 1 and 2;
  - not this app's WAL volume: stall 3 began at 500 rows/s with ~0.7 MB retained.
- During every stall `wal2delta.tables` kept `status = STREAMING` with `committed_lsn` and
  `last_write_time` frozen, `pg_replication_slots` showed the slot `active`, and
  `list-cdf-statuses` reported `CDF_STATE_STREAMING`. Nothing reported an error.

## What we cannot see

`wal2delta.info()` and `wal2delta.get_delta_lsn()` return `permission denied` to workspace
users (including `databricks_superuser` members), and the writer's logs are not exposed. So
we cannot tell whether it hangs, errors and retries, or waits on storage during the
checkpoint write.

## The one plausible app-side factor

Small files. The 15 s commit cadence makes one ~0.5-4 MB file per commit (the health check
`uc_small_files` WARNs: 378 files, avg 4.2 MB). The stalls line up with ~750+ uncompacted
files at a checkpoint boundary. We cannot change the commit cadence, and predictive
optimization compacts only every few hours.

## Ask

1. Please check the writer's logs for this branch at the three timestamps above (slot
   `wal2delta_main_slot_<id>`, branch `manufacturing` of project `pharma-twin-pipelines`).
   Is the checkpoint write at a multiple of 200 versions sensitive to the number of
   active files, and what does the writer do while it is stuck?
2. Is there a supported way to make the writer retry or restart without dropping the slot
   (we never drop it; a re-snapshot would replay 6.6 GB)?
3. Should we compact the history table on a schedule (e.g. every 30 min) as a workaround,
   or raise `refresh_interval` / `checkpoint_interval` for high-rate tables?
4. Can workspace admins get read access to `wal2delta.info()` for diagnosis?

## What we did meanwhile

- Detection: the health check `lakets_sync_slot` and the app's `cdf_sync` check fail
  after 10 minutes without a write while rows keep landing (previously the app reported
  "STREAMING (idle)" as healthy).
- Runbook: ops/README.md, "stuck Lakehouse Sync".
- Opt-in remediation, off by default: `POST /api/admin/pipelines/<setup-id>/realtime/optimize-history`
  (needs `CDF_HISTORY_OPTIMIZE_ENABLED=true`). It runs OPTIMIZE on this deployment's own
  history table only.
- Less WAL per row (820 -> ~600-760 B) and a hot-tier / shadow retention that never prunes
  past the sync's committed LSN, so WAL can still pile up behind a stall but storage no
  longer grows without bound.
