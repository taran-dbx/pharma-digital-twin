# Lakebase CU experiment: pipeline primary 0.5-4 CU -> 4-8 CU (short-window result)

**Decision (human):** raise only `projects/pharma-twin-pipelines/branches/manufacturing/endpoints/primary`
to min 4 / max 8 CU, after the live proof (`15`). The replica (0.5-2 CU) was left alone. The
planned ~8 h watch over ~10 checkpoint boundaries was cancelled by the human after ~30 min,
so **this is a short-window result and does not test the stall hypothesis.**

## Before (read-only, 11:46:59 UTC)
- Endpoint ACTIVE, autoscaling 0.5-4 CU, suspend 24 h, no HA; `shared_buffers` 230 MB;
  `neon.file_cache_size_limit` 6069 MB; postmaster up since 05:12:50.
- Sync: `wal2delta.tables` STREAMING, last write 10 s old; slot retained 34 MB; Delta v2844 at
  11:46:51; walsender (`application_name=wal2delta`, from 127.0.0.1) session since 07:54:35.
- Parallel read-only investigation (human): billing showed the primary pinned at its 4 CU ceiling
  throughout, at boundaries that stalled and ones that passed. No scale-to-zero and no scale
  events at the stall starts. Restarts came after the stalls, not before.

## Change
`databricks postgres update-endpoint .../endpoints/primary "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu"
--json '{"spec":{"autoscaling_limit_min_cu":4,"autoscaling_limit_max_cu":8}}'` at 11:47:35; `update_time`
11:47:38; endpoint ACTIVE at 4-8 CU.

## What happened
| Signal | Result |
|---|---|
| Compute restart | yes: postmaster start 11:47:38 (immediately); `shared_buffers` now 455 MB |
| Sync writer | walsender reconnected 11:47:41 (3 s later) |
| Delta commits | v2847 11:47:36 -> v2848 11:47:46 -> every 15 s (no gap; 128 commits to v2975 at 12:19:28) |
| Consumer | Spark batches after 11:50: 1,999.7-2,000.9 rows/s processed, e2e P50 6 ms, P99 16 ms (6 batches); ingest lag ~0 s throughout |
| Slot | 11-50 MB (bounded) |
| Connection errors | none seen by the samplers (60 s and 20 s cadence) |
| Checkpoint boundaries since the change | **1: v3000 passed cleanly at 12:25:43** (recorded by the parallel investigation) |

Before the change, 14 boundaries had been observed (v200 ... v2800) and 3 stalled (v1000, v1800,
v2000). v2400, v2600 and v2800 passed at 4 CU under 2,000 rows/s (v2800 at 11:35:51, ~819
uncompacted files).

## Conclusion (honest)
- The change is **safe and cheap to apply**: one immediate compute restart, the sync resumed in
  3 s with no missed commit, and the consumer held 2,000 rows/s with ~0 lag.
- It **says almost nothing yet about the stalls.** One boundary (v3000) was crossed after the change, cleanly; 15 of 18 boundaries are now clean overall. With a base
  rate of 3 stalls in 14 boundaries (~21 %), even 10 clean boundaries at 8 CU would only weakly
  suggest an effect (0.79^10 = ~9 % chance of 10 clean ones by luck alone).
- The read-only investigation already makes undersizing an unlikely cause. The compute sat at its
  4 CU ceiling at boundaries that passed too. The restarts followed the stalls. Stall 3 ended
  with the writer **reconnecting** (walsender session 07:54:35, right after the 07:54:25 OPTIMIZE
  commit). The writer runs on the compute, though, so CU could matter in principle.
- **Mitigation stays:** stall detection (`lakets_sync_slot`, the app's `cdf_sync`), the runbook,
  and the escalation brief (`12`). The endpoint is left at 4-8 CU pending the human's decision.
  A longer window is needed to conclude anything.
