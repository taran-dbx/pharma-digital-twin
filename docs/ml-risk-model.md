# Spindle 3 thermal-risk model

Spindle 3 on CNC-1 (Ti-6Al-4V hip stems) has a validated thermal limit of 60 °C
(OQ-TM-CNC1-014). The threshold rule `mfg-alert-spindle-temp` opens an alert
once the limit is **breached**. The thermal-risk model scores the spindle every few
seconds and opens a **predicted** alert *before* the limit trips. That alert goes
to the same alert table, lifecycle, SSE stream, Genie snapshot and cell UI.
The early warning was measured on **simulated** data only. The Spindle 3 scenario is
simulated physics, and the updated producer was **not live yet**. On 8.8 h of fresh,
held-out data, model v7 warned **72 % of breaches at least 30 s ahead** at 1.4 false
alarms per hour. Its warnings track *which* cycles have a precursor (cycle-permutation
p = 0.001), and it beats a sensor-blind clock. On a separate, fresh 4 h replay through
the dev endpoint it warned 19 of 26 breaches, but its discrimination there (0.16) was
below the 0.30 bar. This is not proof on a real machine; see
[Measured early warning](#measured-early-warning-fresh-data-not-live).

```
readings ─► evaluate_and_emit ─► threshold rules ─────────────► AlertEngine ─► alert / alert_event (Lakebase)
                    │                                              ▲              ├► SSE  ─► cell UI (badge)
                    └► SpindleRiskScorer ─► serving endpoint ──────┘              └► intelligence_alert_snapshot ─► Genie
                       (shared features)    pharma-twin-spindle-risk
                                            (UC model @champion)
training job pharma-twin-risk-model-train ─► MLflow run ─► UC model version ─► @champion ─► endpoint update
```

| Piece | Where |
| --- | --- |
| Feature + label code (single source of truth) | `src/iot_twin/ml/spindle_features.py` |
| SIMULATED Spindle 3 scenario (one generator: live producer + training) | `src/iot_twin/scenario_engine/spindle_scenario.py` (embedded via `client/src/lib/generated/spindle-scenario-python.ts`) |
| Live-format training data (producer code path, virtual clock, ~25 Hz) | `src/iot_twin/ml/live_format_telemetry.py` |
| Older 1 Hz synthetic data (v1–v4) | `src/iot_twin/ml/synthetic_telemetry.py` |
| Per-breach lead / false-alarm / chance metrics | `src/iot_twin/ml/lead_metrics.py` |
| Endpoint replay | `scripts/replay_risk_endpoint.py`, `src/iot_twin/ml/replay.py` |
| Alert contract (rule id, metadata keys, names) | `src/iot_twin/ml/risk_contract.py` |
| Training task | `ml/train_spindle_risk.py` (serverless `spark_python_task`) |
| Served model (MLflow models-from-code) | `ml/spindle_risk_model.py` |
| Job + endpoint bundle resources | `resources/risk_model.yml` |
| App resource and env | `resources/app.yml` (`risk-endpoint`, `CAN_QUERY`) |
| App scorer | `src/iot_twin/services/ml/risk_scorer.py` |
| Alert ingest | `AlertEngine.ingest_model_scores` in `services/alerts/alert_engine.py` |
| UI | `client/src/lib/manufacturing-risk.ts`, `components/cell/risk-badge.tsx` |
| Bootstrap / ad-hoc retrain | `scripts/submit_risk_model_training.py` |
| Live-run evidence | `docs/evidence/ml/` |

## Names and configuration

The names follow the `RESOURCE_PREFIX` convention (default `pharma-twin` /
`pharma_twin`). Each one is a bundle variable (`databricks.yml`):

| Variable | Default | Feeds |
| --- | --- | --- |
| `risk_model_schema` | `pharma_twin` | UC model `<uc_catalog>.<schema>.<name>` |
| `risk_model_short_name` | `spindle_thermal_risk` | UC model name, `RISK_MODEL_NAME` |
| `risk_endpoint_name` | `pharma-twin-spindle-risk` | serving endpoint, `RISK_ENDPOINT_NAME` (via `value_from: risk-endpoint`) |
| `risk_job_name` | `pharma-twin-risk-model-train` | training job |
| `risk_model_version` | `7` (the current `@champion`) | version the endpoint serves after a `bundle deploy`. **Keep it equal to `@champion`**: each training run prints the `--var risk_model_version=N` to use |
| `risk_training_data_source` | `auto` | `auto` (real if enough, else `live_format`), `live_format`, `synthetic` or `real` |
| `risk_alert_threshold` | `model` | `RISK_ALERT_THRESHOLD`: `model` uses the cut-off the served version ships; a number overrides it |

The app scorer also reads these optional env vars: `RISK_ALERT_HYSTERESIS` (unset: the model's clear level, else 0.05),
`RISK_SCORE_INTERVAL_S` (5), `RISK_QUERY_TIMEOUT_S` (3), `RISK_BREAKER_FAILURES` (3),
`RISK_BREAKER_COOLDOWN_S` (60) and `RISK_HORIZON_S` (120). With no
`RISK_ENDPOINT_NAME` the scorer is off. That covers local runs, tests and
`app.yaml` deployments without the endpoint. Invalid values fall back to the
defaults and log a warning.

## Model card

**Intended use.** An early warning for operators and QA that Spindle 3 is
likely to exceed its validated thermal limit within the label horizon. It does
*not* replace the validated limit check: the threshold rule still opens the
breach alert, and only that alert drives excursion windows, parts at risk and
the QA hold. The predicted alert is advisory (`warn`).

**Features.** 15 features over a 60 s window, defined once in `FEATURE_SPEC`.
A window needs at least 3 readings per signal, otherwise the scorer does not
call the endpoint.

| Feature | Meaning |
| --- | --- |
| `temp_mean`, `temp_slope`, `temp_max` | Spindle temperature: rolling mean, least-squares slope (°C/min), rolling max |
| `vibration_mean`, `vibration_slope`, `vibration_max` | Spindle vibration (mm/s) |
| `load_mean`, `load_slope`, `load_max` | Spindle load: cutting torque `9549 · P[kW] / n[rpm]` (N·m); the mill has no separate load sensor |
| `power_mean`, `power_slope`, `power_max` | Spindle power (kW) |
| `temp_last` | Latest temperature |
| `temp_margin_c` | Distance to the validated limit: `60 − temp_last` |
| `hours_since_maintenance` | Hours since the last Spindle 3 maintenance in the catalog (`mfg-mnt-001`), clipped to the 0–3000 h training range at train *and* serve time |

**Label.** Forward-looking: 1 when a **breach** of the validated limit starts in
`(t, t + horizon]`. The default horizon is 120 s (`--horizon-s`). Samples are taken
every 5 s. For live-format data a breach is the threshold rule's episode
(`lead_metrics.breach_episodes`): it opens when the temperature stays over the limit
for `duration_s` (2 s), its onset is the first over-limit reading of that run, and it
clears at limit − hysteresis (55 °C). At the live ~25 Hz rate, single noisy samples
over the limit are common in the cool-down and never open an alert. Labelling every
over-limit sample (the older rule, still used for `synthetic` and `real`) would have
counted about 72 "onsets" an hour against about 8 real breaches. Rows inside a
breach are dropped, because the threshold rule handles those.

### The simulated Spindle 3 scenario (one generator for live and training)

**This is simulated physics, not measured machine behaviour.** It is a plausible
stand-in for what precedes spindle thermal runaway on a real mill. Bearing preload
or wear friction shows up as rising vibration and spindle power (so torque) and a
slow temperature creep, tens of seconds to minutes before the temperature takes off.
Coolant flow is not modelled. There is a coolant sensor, but it is not a model input.

* **One generator.** `src/iot_twin/scenario_engine/spindle_scenario.py` is the only
  implementation, and two consumers share it:
  * the realtime producer notebook embeds its source verbatim
    (`client/src/lib/generated/spindle-scenario-python.ts`, written by
    `scripts/sync_spindle_scenario.py` and checked by a test);
  * the training data and the replay import it.
* **Per-cycle plan.** Every 300 s lot cycle draws a deterministic plan from a
  per-cycle PRNG, so the dashboard can still show when the next excursion is due.
  The TS port ships only the PRNG and its first two draws (excursion or not, ramp
  start), and a test checks that both agree.
* **The ramp start is jittered.** It is uniform in 30–210 s into the cycle. Real
  spindles are not periodic, and with a fixed ramp (210 s, as in v4–v6) a
  sensor-blind clock predicted every breach.
  * A precursor whose lead is longer than its ramp start begins in the previous
    cycle, but never before that cycle's episode has ended.
  * The 300 s lot cycle is unchanged, and the lot, part, value-view and
    caught-in-time maths are all data-driven (alert times and the lot clock).

| Cycle plan | Share | What happens |
| --- | --- | --- |
| Excursion, strong precursor | 70 % × 55 % | vibration +1.1–4.2 mm/s (capped at +4.0), power +5–21 %, temperature creep 0.9–3.5 °C; then a ramp to a 20–29 °C peak |
| Excursion, weak precursor | 70 % × 25 % | the same shape at 15–35 % of the amplitude |
| Excursion, no precursor | 70 % × 20 % | temperature ramp only |
| False alarm, fade | 15 % × 60 % | a precursor as strong as a real one that fades over 20–60 s; no breach |
| False alarm, warm bump | 15 % × 40 % | a +1–2.5 °C bump and a mild precursor; stays under the limit |
| Quiet | 15 % | nothing |

* **Lead.** Uniform in 30–180 s before the ramp, clipped so it never overlaps the
  previous cycle's episode. The breach onset follows the ramp start by about 16–21 s.
* **Shape.** The envelope is `u^γ` with γ in 0.5–2.0. Vibration and power get a slow
  wobble of two random sinusoids (±50 %); the temperature creep and bump follow the
  envelope without wobble.
* **Noise.** Per-sample noise comes from the caller's RNG, on top of the simulator's
  own noise and drift.
* **Hard bounds**, tested, so the scenario trips no other catalog rule:
  * **Vibration.** The precursor adds at most 4.0 mm/s, noise included, and a
    reading it touches is capped at 7.5 mm/s. That is below `mfg-alert-vibration`
    (> 8.0 mm/s, no duration, no hysteresis). The first draft of this PR reached
    about 9.4 mm/s on strong precursors and tripped that rule.
  * **Temperature.** In false-alarm and quiet cycles the offset is at most creep
    plus bump, about 3.7 °C. With a 4σ simulator sample (45 °C × 1.05 + 8 °C) that
    stays at least 1 °C under the 60 °C limit, and a breach also needs 2 s over it.
* **Choosing the amplitudes.** The first draft (vibration ×2.0, power ×0.12,
  γ 0.6–2.5) was too faint and back-loaded for any lead. We raised them once,
  before any model was trained on the scenario.
* **Live producer behaviour.** Only Spindle 3 temperature is tagged with
  `phase`/`fault_active`, and only during the heat episode. The precursor carries no
  tag, so the model cannot read the answer from metadata.

**Data.** `--data-source` takes four values. The choice is logged as the
`data_origin` param, run tag and model-version tag.

* `auto` (the default) uses real `manufacturing_realtime` telemetry when the last
  14 days contain at least 2,000 labelled rows and 20 excursions. Otherwise it falls
  back to `live_format`.
* `live_format` (`iot_twin.ml.live_format_telemetry`, `data_origin=live_format_simulated`)
  runs the live producer's own code path offline on a virtual clock:
  * the `iot_simulator` `SensorSimulator` objects for the four spindle sensors;
  * calibrated like the producer's `_calibrate_simulator` (noise in sensor units,
    random faults off);
  * **at the live sample rate**: 500 records/s over 19 sensors, measured at 25.7 Hz
    per spindle sensor on the live pipeline (the generator uses 25 Hz);
  * the shared scenario.

  Defaults are 36 runs × 1 h. `--seed` and `--live-start-epoch` choose fresh cycles.
  `hours_since_maintenance` is random per run, but it does not affect the signals,
  because the producer has no wear model.
* `synthetic` (`iot_twin.ml.synthetic_telemetry`) is the older 1 Hz generator that
  trained v1–v4.
* `real` uses real telemetry only.

There is no live-format data in Delta yet: the live pipeline had not run the
updated scenario.

**Why v4 showed no live lead.** v4 was trained at 1 Hz on a precursor the live
producer did not have, and the live producer only had a temperature ramp. The
sample rate differs too: window maxima and slopes over about 1,500 points per
minute differ from 60 points per minute.

**Training.** The model is LightGBM (`LGBMClassifier`, 400 trees max, early stopping
on validation PR-AUC).

* **Split.** Time-ordered: train → validation (20 % of train) → test (last 25 %).
* **Embargo.** `window_s + horizon_s` (180 s) of training rows are dropped before
  each boundary.
* **Cut-off and clear level** (`--threshold-rule event`, the default for
  live-format data) are chosen **on validation only**. The rule replays every
  (cut-off, clear band) pair through the predicted alert's open / clear hysteresis
  and keeps the pair with the best **discrimination** among those with at most
  `--false-alarm-budget` (3) false alarms per hour.
  * Bands are 0.05, 0.10, 0.15 or 0.20.
  * Pairs whose clear level is below the steady-running floor are skipped. The floor
    is the 90th percentile of the model's scores in steady running: at least 240 s
    before the next breach onset and 60 s after the last clear.
* **Older sources.** `--threshold-rule precision` keeps the v4 rule: highest recall
  at precision ≥ `--min-precision`.
* **Shipping and test.** Both levels ship in the model version (the `decision`
  artifact), and the test split is scored once.

**Event metrics** (`iot_twin.ml.lead_metrics`, scored every 5 s like the app):

| Term | Definition |
| --- | --- |
| Breach lead | onset minus the open time of the predicted alert that is still open at the onset and opened at most 240 s before it. Otherwise the breach is missed |
| False alarm | a predicted alert that opens outside every breach and precedes no breach onset within 240 s |
| Cycle permutation (the null used for significance) | each alert keeps its offset inside its 300 s cycle, but alerts move to randomly permuted cycles (1,000 draws). A clock is unchanged by it; an alert that tracks *which* cycles have a precursor is not. `permutation_p = (1 + #draws warning at least as many breaches ≥ 30 s ahead) / 1001` |
| Clock baseline | a sensor-blind alert open over one fixed window of every cycle. *Duty-matched* means the best window chosen on validation whose length is at most the model's alert duty; *any duty* means the best window of any length |
| Discrimination | breaches warned ≥ 30 s ahead minus the share of breach-free cycles with a false alarm. A clock alerts in every cycle alike, so its discrimination is about 0 or below |
| Shift baseline | the alerts circularly shifted in time. It is **not** a valid null here: under a periodic schedule a clock keeps its phase under most shifts. It is reported only for comparison with earlier versions of this card |
| Alert duty | the share of time a predicted alert is open |
| Threshold rule | opens 2 s after the onset: lead −2 s, never warns |

**Promotion (`--promote if_better`, default).** The job registers the new version
with alias `challenger`. It then scores the current `@champion` and every
`--compare-versions` version on the *same* validation and test rows, each model at
its own shipped levels. Every model must **qualify**
(`evaluation.qualifies`). All four conditions are blocking:

* its clear level is at or above its steady-running p90 (this also applies to
  `--promote always`);
* it warns more breaches ≥ 30 s ahead than its duty-matched clock;
* cycle-permutation p < 0.05;
* discrimination ≥ 0.30.

A qualifying challenger replaces a champion that does not qualify. Against a
qualifying champion it must warn strictly more breaches ≥ 30 s ahead, have false
alarms within 1 per hour of the champion's, and a PR-AUC that is not lower. If
nothing is promoted and the champion fails, `champion` falls back to the newest
qualifying compared version. Each version's metrics, `qualifies` flag and problems
are logged; `promotion_decision.json` and `null_counts.json` (the permutation and
shift counts) are run artifacts.

**Metrics on fresh data** (`docs/evidence/ml/training_run_v7.json`):

* the data is seed 11, with 36 one-hour runs from epoch 1,800,000,000, cycles no
  earlier model saw;
* the test split is 8.8 h with 76 breaches;
* v4, v6 and v7 are scored on the same rows;
* each clock is chosen on validation.

<!-- metrics:start -->
| Metric (test, 8.8 h) | v7 (champion) | v6 | v4 | Clock, duty-matched (140 s + 90 s) | Clock, any duty (0 s + 230 s) |
| --- | --- | --- | --- | --- | --- |
| Cut-off / clear level | 0.59 / 0.54 | 0.40 / 0.35 | 0.47 / 0.42 | – | – |
| Steady-running p90 (validation) | 0.539 | **0.384 (above its clear level)** | 0.179 | – | – |
| Breaches warned ≥ 30 s ahead | **72.4 %** | 75.0 % | 61.8 % | 35.5 % | 100 % |
| Median lead | 53 s | 69 s | 46 s | – | – |
| False alarms per hour | 1.36 | 2.04 | 1.36 | 4.19 | 3.62 |
| Breach-free cycles with a false alarm | 31 % | 45 % | 31 % | – | – |
| Discrimination | **0.41** | 0.30 | 0.31 | −0.61 | 0.00 |
| Cycle-permutation p (p95 of the null) | 0.001 (48.7 %) | 0.001 (52.6 %) | 0.001 (43.4 %) | – | – |
| Alert duty | 32 % | 36 % | 31 % | – | 77 % |
| PR-AUC | 0.767 | 0.784 | 0.695 | – | – |
| Qualifies | **yes** | **no** | yes | – | – |
<!-- metrics:end -->

The job promoted v7. v6, the champion before this run, fails the steady-running
floor: its alerts would stay open through calm running on the jittered scenario.
v7 also beats v4 under the comparison rule: it warns more breaches, has the same
false alarms and a higher PR-AUC.

### Measured early warning (fresh data, not live)

The task asked for a live measurement over at least 60 minutes. The session that
owns the app and pipeline redeploys had not deployed the updated producer: the
producer job's notebook still had the old ramp-only heat event and no
`apply_spindle_scenario`. So **no live measurement was possible**. Instead,
`scripts/replay_risk_endpoint.py` replays **4 h** of fresh live-format data through
the real endpoints:

* the cycles start at epoch 1,802,000,000 and the seed is 9002, used by no training
  or selection run;
* there are 48 cycles with 26 breaches;
* features come from the scorer's path every 5 s;
* v7 was on the dev endpoint, v6 on the dev endpoint before it was repointed, and v4
  on `pharma-twin-spindle-risk`;
* the clock windows are the ones the training job chose on validation.

The result is in `docs/evidence/ml/replay_fresh_v7_v6_v4_clock.json`:

| Fresh replay, 4 h, 26 breaches | v7 | v6 | v4 | Clock 140 s + 90 s | Clock 0 s + 230 s | Threshold rule |
| --- | --- | --- | --- | --- | --- | --- |
| Warned ≥ 30 s ahead | 19 / 26 (73 %) | 22 / 26 (85 %) | 16 / 26 (62 %) | 9 / 26 | 26 / 26 | 0 |
| Median lead (p25–p75) | 45 s (23–69 s) | 87 s (48–119 s) | 46 s (16–104 s) | 41 s | 150 s | −2 s |
| False alarms per hour | 5.0 | 5.5 | 4.3 | 5.8 | 5.5 | 0 |
| Discrimination | 0.16 | 0.23 | 0.14 | −0.56 | 0.00 | – |
| Cycle-permutation p | 0.001 | 0.001 | 0.001 | 1.0 | 1.0 | – |
| Alert duty | 24 % | 34 % | 25 % | 30 % | 77 % | – |

Cycle-level view on the same replay (47 full cycles). Breach cells are warned ≥ 30 s / breaches;
the other cells are cycles with a false alarm / cycles.

| Cycle plan | v7 | v6 | v4 | Clock 0 s + 230 s |
| --- | --- | --- | --- | --- |
| Excursion, strong precursor (14) | 14 / 14 | 14 / 14 | 13 / 14 | 14 / 14 |
| Excursion, weak precursor (8) | 5 / 8 | 8 / 8 | 3 / 8 | 8 / 8 |
| Excursion, no precursor (4) | **0 / 4** | 0 / 4 | 0 / 4 | 4 / 4 |
| False alarm, fade (9): cycles with a false alarm | 8 / 9 | 7 / 9 | 7 / 9 | 9 / 9 |
| False alarm, bump (4): cycles with a false alarm | 4 / 4 | 4 / 4 | 3 / 4 | 4 / 4 |
| Quiet (8): cycles with a false alarm | **0 / 8** | 2 / 8 | 0 / 8 | 8 / 8 |

**Reading it honestly:**

* **The warnings track the precursor, not the clock.** For every model the
  permutation p is 0.001: no permutation of the 1,000 that kept each alert's in-cycle
  offset warned as many breaches. For the clocks it is 1.0. v7 warns no breach that
  had no precursor and raises no false alarm in quiet cycles. The clock "warns"
  no-precursor breaches only because it is open most of the cycle.
* **Precursor-like false alarms are not separable, by design.** Fades and bumps
  look like real precursors until the ramp, so most of those cycles get a false
  alarm.
* **This replay fails the discrimination bar.** It had more of those cycles (13 of
  48) than expected (about 7). So false alarms per hour (5.0) and discrimination
  (0.16) are worse than on the 8.8 h test (1.36, 0.41), and **on this replay no
  model reaches the 0.30 discrimination bar**. The gate ran on the larger test split.
  One 4 h sample is noisy (26 breaches), and we did not re-tune on it.
* **v6 against v7.** v6 warns more on the replay, but it fails the steady-running
  floor: it has 2 quiet-cycle false alarms here, and the floor fails on fresh
  validation.

**Adaptive reuse (disclosure).** v5 and v6 were selected, and the gate and cut-off
rules were redesigned, after looking at the seed-7 test split and the seed-9001
replay. Those numbers are therefore optimistic, and they are kept only as history
(`training_run_v5.json`, `training_run_v6.json`, `replay_v4_vs_v6.json`, which used
the fixed-phase scenario). Every number above comes from fresh seeds and epochs that
no model selection used.

**Still to do: the live measurement.** Once the producer is redeployed with this
change, measure over at least 60 minutes from the alert tables. For breaches, take
`excursion_started_at`; for predicted alerts, take `opened_at` and `cleared_at`.
Then compute `lead_metrics.event_report`.

**Redeploy checklist.** Expect these to shift once the producer runs this scenario:

* **Fault-minute and availability KPIs** (the KPI notebook and Genie) fall: only
  about 70 % of cycles now have a breach, at random times;
* **Value views** (excursions, caught in time, deadline-before-alert, cost avoided)
  change with the breach count and with the ramp's position relative to the lot end;
* **The cell UI countdown** points to a varying point in the cycle;
* **Value-view evidence and screenshots** from the fixed 210 s schedule
  (`docs/evidence/2026-11-20/`) no longer match. Re-capture them.

**Limitations.**

* **Simulated precursor physics.** The lead exists because the simulated scenario
  has a precursor most of the time. It shows that the pipeline (features, model,
  thresholds, alerting) can turn a noisy precursor into a lead. It does **not** show
  that real spindles behave like this. Retrain on real telemetry
  (`--data-source real`) before using the score for anything but the demo.
* **No precursor, no early warning.** Breaches with no precursor (about 14 % of
  cycles) are only predictable once the ramp starts.
* **False alarms are structural.** Fading precursors and warm bumps (about 15 % of
  cycles) are indistinguishable from real ones until the ramp.
* **Uncalibrated scores.** Probabilities are not calibrated. v7 scores up to about
  0.54 in steady running (p90), so its clear level sits at that floor;
  `RISK_ALERT_THRESHOLD` overrides the shipped cut-off.
* **Clipped maintenance age.** `hours_since_maintenance` comes from the catalog
  fixture and is clipped at 3000 h. With live-format data it carries no signal.
* **Scoring delay and buffer.** The app scores at most every `RISK_SCORE_INTERVAL_S`
  (5 s). The scorer's buffer is sized for 2 × 60 s at up to 30 Hz per sensor
  (`SCORER_MAX_RATE_HZ`).
* **Top features.** These are TreeSHAP contributions in log-odds. They explain the
  model, not the physics.

**History.**

* **v1–v4:** trained on 1 Hz synthetic data. v4's only live lead was 7 s before onset
  on the old ramp-only event.
* **v5:** the first live-format version, rolled back. Its clear level of 0.14 was
  below steady-running scores. `champion` was moved back to v4 by hand before any
  endpoint served it. MLflow tags: `superseded_by=6`, `status=rolled_back`.
* **v6:** the same booster as v5 with a corrected clear level, trained on the
  fixed-phase scenario. Superseded on fresh, jittered data, where it fails the
  steady-running floor. MLflow tag: `superseded_by=7`.
* **v7 (champion):** trained on the jittered scenario with seed 11, chosen by the
  qualification gate. It is served on the dev endpoint, and the bundle default is
  `risk_model_version=7`.

## Serving and the app

* The endpoint `pharma-twin-spindle-risk` is Small and scales to zero. It serves
  the UC model version the training job last marked `champion`: the job sets the
  alias, then calls `update_config` on the endpoint (no wait).
* The app's `risk-endpoint` resource grants `CAN_QUERY` and injects the
  resolved endpoint name as `RISK_ENDPOINT_NAME` (`value_from`), so dev-mode
  name prefixes stay correct.
* **One lane per pipeline.** The scorer keeps 2 × 60 s of spindle readings and a
  scoring timer *per `pipeline_id`*, so two pipelines (or a test call to
  `POST /api/alerts/evaluate`) never mix readings or share a score. It keeps at most
  32 lanes and evicts the least recently seen.
* **Never blocks the app.** Each lane runs at most one round every
  `RISK_SCORE_INTERVAL_S`, in the background, so ingest never waits.
  * Calls run on the scorer's own **single-worker executor**, not the default
    `asyncio.to_thread` pool that Lakebase credential refresh uses.
  * The scorer has its own `WorkspaceClient`, with `http_timeout_seconds` and
    `retry_timeout_seconds` equal to `RISK_QUERY_TIMEOUT_S`.
  * After an await times out, the scorer stays *busy* until the worker thread
    returns, so hung calls never pile up.
  * After `RISK_BREAKER_FAILURES` consecutive failures, which includes a cold
    scale-to-zero start, the breaker opens for `RISK_BREAKER_COOLDOWN_S` and then
    allows one trial call.
* The model version comes from the endpoint's served entity, cached for 5 min.
  If the app cannot read it, it falls back to `RISK_MODEL_NAME` and version
  `unknown`. When the app SP can read the UC model, the scorer also reads the
  `champion` alias and logs a warning when the endpoint serves a different version.

### The endpoint version and `bundle deploy`

The training job repoints the endpoint to the new champion. A later
`databricks bundle deploy` **reverts** it to `var.risk_model_version`, because the
bundle owns the endpoint config. The default is `7`, the current champion. **After
every training run, update it** (or always deploy with `--var risk_model_version=N`):
the job log and its summary print the override to use. While the endpoint serves
a version that is not `@champion`, the app does two things:

* it logs `serves … but @champion is v…`;
* it **ignores that version's embedded cut-off and clear level**, logging one
  warning that names them, and falls back to the contract defaults (0.47 / 0.42,
  v4's levels; v7 ships 0.59 / 0.54).
  `RISK_ALERT_THRESHOLD` still overrides either way.

### Prerequisites and grants

* **Job identity:**
  * `USE CATALOG` on `uc_catalog`;
  * `CREATE SCHEMA` there, or an existing `pharma_twin` schema. The job runs
    `CREATE SCHEMA IF NOT EXISTS`;
  * `USE SCHEMA` and `CREATE MODEL` on the schema;
  * `SELECT` on `manufacturing_realtime` for `--data-source auto|real`;
  * permission to create or manage the serving endpoint.
* **App service principal:**
  * `CAN_QUERY` on the endpoint, which the `risk-endpoint` resource grants;
  * `EXECUTE` (or `BROWSE`) on the UC model, for the champion check. It is not
    an app resource type, so grant it in UC:
    `GRANT EXECUTE ON FUNCTION <catalog>.pharma_twin.spindle_thermal_risk TO <app-sp>`,
    or use Catalog Explorer → model → Permissions. Without it, the app logs one
    warning (`served-vs-champion check is off`) and uses the served version's
    thresholds.

### Bootstrap on a new workspace

The endpoint needs a registered model version, so a brand-new workspace needs one
training run before the first `bundle deploy` (which creates the endpoint and the app):

```bash
python scripts/submit_risk_model_training.py --profile <cli-profile> \
  --catalog <uc_catalog> --schema pharma_twin --data-source live_format --wait
databricks bundle deploy -t dev --profile <cli-profile>
databricks bundle run risk_model_train -t dev --profile <cli-profile>   # later retrains
```

In `dev` (development mode) the bundle's endpoint and job are
`dev_<user>_pharma-twin-spindle-risk` and `[dev <user>] pharma-twin-risk-model-train`.
They serve and train the same UC model, and the app follows the endpoint through
its resource reference. Only `prod` (or another non-development target) uses the
unprefixed `pharma-twin-spindle-risk`; to adopt one created by the submit script,
see [deployment.md](deployment.md#adopting-the-script-created-risk-endpoint).

## Integration contract (alerts → GenAI, Genie, UI)

A predicted alert is an ordinary alert row. Read it from `GET /api/alerts`,
from the SSE lifecycle events, from `AlertRecord.to_dict()` or from
`intelligence_alert_snapshot`:

| Field | Predicted alert | Threshold alert |
| --- | --- | --- |
| `rule_id` | `medtech-model-spindle-risk` | e.g. `mfg-alert-spindle-temp` |
| `alert_kind` (top level) | `predicted` | `breached` |
| `risk_score` (top level) | float 0–1 | `null` |
| `value` | risk score, rounded to 0.01 | sensor reading |
| `threshold` | alert cut-off | validated limit |
| `severity` / `fault_code` | `warn` / `PRED-SPINDLE3-THERMAL` | from the catalog rule |
| `sensor_id` / `sensor_ids` | `mfg-sns-cnc_1_spindle_temp` / the four spindle sensors | rule sensors |

`metadata` on a predicted alert. These keys are stable; consumers must ignore
keys they do not know:

| Key | Type | Meaning |
| --- | --- | --- |
| `alert_kind` | `"predicted"` | Same as the top-level field |
| `risk_score` | number | Latest score, 4 decimals |
| `risk_threshold` | number | Cut-off the alert opened against |
| `risk_clear_threshold` | number | Score at or below which the alert clears (`risk_threshold − hysteresis`) |
| `model_name` | string | UC model, e.g. `<catalog>.pharma_twin.spindle_thermal_risk` |
| `model_version` | string | UC model version the endpoint served (`unknown` if not readable) |
| `top_features` | list of `{feature, contribution, value}` | Up to 3 TreeSHAP drivers (log-odds; positive raises risk), `feature` ∈ `FEATURE_NAMES` |
| `horizon_s` | number | Label horizon the score refers to |
| `scored_at` | ISO-8601 string | Device time of the scored window |
| `lot_id`, `part_serial`, `work_order`, `part_number`, `udi_di`, `dhr_ref` | string | Lot context of the part being cut (from the reading, else the lot clock) |
| `parameter_type`, `parameter`, `validated_limit` | string | Rule references (`predicted_process_risk`) |

A predicted alert never carries `excursion_started_at`; only breach alerts do.

Example:

```json
{
  "rule_id": "medtech-model-spindle-risk",
  "alert_kind": "predicted",
  "risk_score": 0.8123,
  "value": 0.81,
  "threshold": 0.47,
  "severity": "warn",
  "status": "open",
  "metadata": {
    "alert_kind": "predicted",
    "risk_score": 0.8123,
    "risk_threshold": 0.47,
    "risk_clear_threshold": 0.42,
    "model_name": "<catalog>.pharma_twin.spindle_thermal_risk",
    "model_version": "4",
    "top_features": [{"feature": "temp_slope", "contribution": 1.21, "value": 9.5}],
    "horizon_s": 120.0,
    "scored_at": "2026-11-20T12:00:00+00:00",
    "lot_id": "TM-HS12-261120-001"
  }
}
```

**Semantics.** Predicted alerts follow the same state machine as threshold rules:

* One fingerprint per `rule_id` × Spindle 3 × pipeline: `fingerprint_for(MODEL_RULE_ID, ["mfg-sns-cnc_1_spindle_temp"], pipeline_id)`.
* The alert opens at `risk ≥ threshold`.
* Later scores update the same alert, refreshing `value` and the score fields of `metadata`, but only when the rounded score changes.
* This includes scores inside the band `(risk_clear_threshold, threshold)`: the alert stays open but shows the current score.
* An episode keeps the lot context it opened on; a reopen takes the new lot.
* It clears at `risk ≤ risk_clear_threshold`. The default band is 0.05 (cut-off 0.47 → clear at 0.42). Calm scores stay at 0.27 or below even for a worn spindle.
* A later rise reopens the same row as a new episode.
* Predicted and breach alerts never share a fingerprint, so both can be open at once.

**Genie / SQL.** `intelligence_alert_snapshot` has `alert_kind`, `risk_score`,
`model_name`, `model_version`, `top_features` (JSON string) and `metadata` (JSON
string). Snapshot tables created before these columns existed are migrated with
`ALTER TABLE … ADD COLUMNS` on provision. For example:

```sql
SELECT alert_id, status, risk_score, model_version, get_json_object(metadata, '$.lot_id') AS lot_id
FROM <catalog>.<schema>.intelligence_alert_snapshot
WHERE alert_kind = 'predicted'
ORDER BY updated_at DESC;
```
