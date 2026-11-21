# Risk model v13, measured LIVE for 60 minutes (simulated Spindle 3 scenario)

**What ran.** The app was deployed from `main` @3a9f06d (#18 + #20) on 2026-11-21 at 16:28-16:29 UTC:
`bundle deploy -t dev --var risk_model_version=13` plus `bundle run`. The dev endpoint
`dev_<user>_pharma-twin-spindle-risk` served **v13** (READY, no pending config). The realtime
pipeline was redeployed through the app at 16:30:42-16:31:15 (stop -> deploy -> start), with
the producer regenerated from main's generator, i.e. #20's shared spindle scenario. The consumer
notebook was unchanged. Rows flowed from 16:36:47.

Unchanged: the statement-level INSERT triggers (one shadow INSERT path) and the 4-8 CU pipeline
compute. Retention and pruning are off. **The data is SIMULATED physics, not machine behaviour.**

**Producer rate.** 1,760-1,789 rows/s at the 2000 target, written with 0.0-0.1 s ingest lag.
Main does not have #21's wall-clock pacer, so the ~11 % shortfall of `Simulator.run()` is back.
Delta committed every 15 s (v3985 -> v4053+ ...) and the slot stayed at 9-30 MB.

**Method** (`docs/ml-risk-model.md`, "the live measurement"):
- Episodes come from `alert_event` in the app backend (read-only).
  - Breaches: rule `mfg-alert-spindle-temp`; onset = `excursion_started_at`, clear = `cleared_at`.
  - Predicted alerts: rule `medtech-model-spindle-risk`; `opened_at` / `cleared_at`.
- Scored with main's `iot_twin.ml.lead_metrics.event_report` and `iot_twin.ml.gate_stats`
  (`cycle_outcomes`, `bootstrap`, 2,000 stratified draws).
- Lead counted with a 240 s lookback; early warning = lead >= 30 s.
- Window: **16:40:00-17:40:00 UTC, 12 full 300 s cycles.** It starts after the producer's
  start-up transient.
- Cycle kind comes from the deterministic plan (`spindle_cycle_plan(epoch // 300)`). The plan
  matched the observed breaches in **12/12 cycles**: a breach in every excursion cycle, none elsewhere.
- Clock baselines are the v13 training run's windows: duty-matched 20 s + 100 s, any-duty 0 s + 240 s.

## Result

| | Live v13 (this hour) | Offline v13, 231 cycles (`docs/ml-risk-model.md`) | Clock, duty-matched (20 s + 100 s) | Clock, any duty (0 s + 240 s) |
|---|---|---|---|---|
| Breaches | 9 | 162 | 9 | 9 |
| Warned at all | 9 / 9 | - | - | - |
| **Warned >= 30 s ahead** | **7 / 9 = 77.8 %** [44.4, 100] | 68.5 % [64.2, 72.2] | 3 / 9 = 33.3 % | 9 / 9 = 100 % |
| **Median lead** | **73.6 s** (p25 42.0, p75 83.6) | - | 80.0 s | 141.4 s |
| **False alarms per hour** | **1.0** [0.0, 3.0] (1 in 1 h) | 1.35 [0.99, 1.71] | 3.0 | 3.0 |
| Breach-free cycles with a false alarm | 1 / 3 | - | 3 / 3 | 3 / 3 |
| Discrimination (warned share - false-alarm cycle share) | 0.444 [-0.22, 1.0]; plan-standardised 0.432 | 0.375 [0.291, 0.457] | -0.667 | 0.000 |
| Cycle-permutation null: warned >= 30 s (mean / p95) | 0.25 / 0.44 -> **p = 0.001** (the floor at 1,000 permutations) | p <= 0.001 | p = 1.0 | p = 1.0 |
| Threshold rule's own lead | -2.15 s (opens 2.15 s after onset) | - | - | - |

**Per-breach leads (s), in cycle order:** 42.0, 75.8, 9.0, 10.7, 59.7, 112.8, 83.6, 121.6, 73.6.

**By cycle kind:**
- strong precursor: 4 cycles, 4 breaches, 3 warned >= 30 s;
- weak precursor: 5 cycles, 5 breaches, 4 warned >= 30 s;
- no precursor: 0 cycles;
- quiet: 3 cycles, 1 with a false alarm (17:39:03, 1 alarm);
- fade / bump (false-alarm cycles): 0 occurred in this hour.

The two late warnings (9.0 s and 10.7 s) came in a strong and a weak cycle. Alert open fraction: 28 %.

| cycle (UTC) | kind | precursor | breach | lead (s) | false alarms |
|---|---|---|---|---|---|
| 16:40 | excursion | weak | yes | 42.0 | 0 |
| 16:45 | excursion | weak | yes | 75.8 | 0 |
| 16:50 | quiet | - | no | - | 0 |
| 16:55 | excursion | strong | yes | 9.0 | 0 |
| 17:00 | excursion | weak | yes | 10.7 | 0 |
| 17:05 | excursion | strong | yes | 59.7 | 0 |
| 17:10 | excursion | weak | yes | 112.8 | 0 |
| 17:15 | excursion | weak | yes | 83.6 | 0 |
| 17:20 | excursion | strong | yes | 121.6 | 0 |
| 17:25 | excursion | strong | yes | 73.6 | 0 |
| 17:30 | quiet | - | no | - | 0 |
| 17:35 | quiet | - | no | - | 1 |

**Counting note.** `event_report` over every alert episode read (window ± the 240 s
lookback) reports 5 false alarms. Four of them opened outside the window:
- 16:36:47, the producer's start-up;
- 17:40:51, 17:41:41 and 17:42:21, in the 17:40 cycle, whose breach lies after the window end.

Restricted to alerts opened inside the window there is **1** false alarm. That is the figure
above, and it equals the cycle-level `gate_stats` count.

## Reading (honest)

- On this live hour v13 behaves like its offline evaluation. It warned 7 of 9 breaches at least
  30 s ahead (median lead 74 s) with 1 false alarm. The cycle-permutation test puts that at the
  test's floor (p = 0.001), and the duty-matched clock does much worse (3 / 9, 3 false alarms/h).
- **The sample is small.** 12 cycles and 9 breaches; every interval above is wide (warned share
  44-100 %, discrimination -0.22 to 1.0).
- The hour contained **no no-precursor excursion and no fade / bump false-alarm cycle**, the two
  kinds where the model is weakest offline (none 0 / 35 warned; fade 16 / 19 alarmed). So this
  hour cannot confirm or refute those weaknesses. A multi-hour run would.
- Scenario, producer and model share one simulated generator (#20). Agreement with the offline
  numbers shows the deployed pipeline reproduces the evaluation set-up end to end; it says
  nothing about real machines.
