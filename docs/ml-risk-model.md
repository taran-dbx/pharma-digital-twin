# Spindle 3 thermal-risk model

Spindle 3 on CNC-1 (Ti-6Al-4V hip stems) has a validated thermal limit of 60 °C
(OQ-TM-CNC1-014). The threshold rule `mfg-alert-spindle-temp` opens an alert
once the limit is **breached**. The thermal-risk model scores the spindle every few
seconds and opens a **predicted** alert *before* the limit trips. That alert goes
to the same alert table, lifecycle, SSE stream, Genie snapshot and cell UI.
The early warning was measured on **simulated** data only. The Spindle 3 scenario is
simulated physics, and the updated producer was **not live yet**. On 19 h of fresh,
pooled held-out cycles, the champion v7 warned 59 % [95 % CI 54–65 %] of breaches at
least 30 s ahead at 1.9 [1.5–2.4] false alarms per hour.

* Its plan-mix-standardised discrimination is 0.21 [0.12–0.30], against 0.30 for a
  plan-aware oracle and about 0 for a sensor-blind clock.
* Cycle permutation gives p ≤ 0.001, the floor of 1,000 permutations.
* This is not proof on a real machine; see
  [Measured early warning](#measured-early-warning-fresh-simulated-data-not-live).

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
  `hours_since_maintenance` is pinned at 3000 h in training rows. The producer has no
  wear model, so the feature carries no signal, and a random value let v7's trees
  learn a spurious dependence (see the replay diagnosis).
* `synthetic` (`iot_twin.ml.synthetic_telemetry`) is the older 1 Hz generator that
  trained v1–v4.
* `real` uses real telemetry only.

There is no live-format data in Delta yet: the live pipeline had not run the
updated scenario.

**Why v4 showed no live lead.** v4 was trained at 1 Hz on a precursor the live
producer did not have, and the live producer only had a temperature ramp. The
sample rate differs too: window maxima and slopes over about 1,500 points per
minute differ from 60 points per minute.

**Training.** The model is LightGBM (`LGBMClassifier`, 400 trees max). Early
stopping runs on validation **log-loss**. v8 used average precision instead: on a
few hours of validation it is noisy, it stopped after 5 trees, and the compressed
scores left no usable cut-off.

* **Split.** Time-ordered: train → validation (20 % of train) → test (last 25 %).
* **Embargo.** `window_s + horizon_s` (180 s) of training rows are dropped before
  each boundary.
* **Cut-off and clear level** (`--threshold-rule event`, the default for
  live-format data) are chosen **on validation only**. The rule replays every
  (cut-off, clear band) pair through the predicted alert's open / clear hysteresis
  and keeps the pair with the best discrimination among those with at most 3 false
  alarms per hour.
  * Bands are 0.05–0.20.
  * The clear level must be at least the steady-running p90 + 0.02. The margin keeps
    the next run's gate from flipping on it. An earlier p95 margin hit the tail of
    fading precursors and pushed v9's cut-off to 0.91.
* **Older sources.** `--threshold-rule precision` keeps the v4 rule.

**Steady running.** This is the floor the clear level is checked against.

* **Live-format (simulated) data.** A row is calm when its 60 s feature window
  holds no precursor and no heat episode (`training_set.scenario_calm`), at least
  60 s after a breach cleared.
* **Real telemetry.** A plan-free fallback: at least 240 s before the next breach
  onset.
* **Why not the fallback on simulated data.** It also counts fading precursors as
  calm, so its p90 swung with how many of them a 3 h validation slice held. v4's
  ranged 0.18–0.69 over four fresh samples, and the gate flipped from run to run.

**Event and gate metrics** (`iot_twin.ml.lead_metrics`, `iot_twin.ml.gate_stats`).
Scores are taken every 5 s, as in the app:

| Term | Definition |
| --- | --- |
| Breach lead | onset minus the open time of the predicted alert that is still open at the onset and opened at most 240 s before it. Otherwise the breach is missed |
| False alarm | a predicted alert that opens outside every breach and precedes no breach onset within 240 s. Every flap counts |
| Cycle record | one per full 300 s cycle: plan group (strong / weak / none excursions, fade / bump false alarms, quiet), breach, warned ≥ 30 s, false alarms |
| Standardised discrimination | the per-group rates re-weighted to the nominal plan mix: warned share over strong / weak / none at 0.55 / 0.25 / 0.20, minus the false-alarm-cycle share over fade / bump / quiet at 0.30 / 0.20 / 0.50. A sample that happens to hold many fades does not move it. A plan-aware **oracle**, which alerts on every precursor and only then, scores 0.80 − 0.50 = **0.30**: the ceiling for a precursor detector here |
| 95 % CI | a bootstrap over cycles, stratified by plan group (2,000 draws). Every table reports it |
| Cycle permutation | each alert keeps its offset inside its cycle, but alerts move to randomly permuted cycles (1,000 draws; the permutation seed for period i is `seed + i`). A period with fewer than 2 full cycles contributes its observed count to every draw. `p = (1 + #draws ≥ observed) / 1001`, so **p ≤ 0.001 is the floor of 1,000 permutations** |
| Clock baseline | a sensor-blind alert over one fixed window of every cycle. *Duty-matched* is the best window chosen on validation whose length is at most the model's alert duty; *any duty* is the best window of any length |
| Shift baseline | the alerts circularly shifted in time. It is **not** a valid null under a periodic schedule; it is reported for history only |

**Qualification** (`evaluation.qualifies`). The gate pools the **held-out test
split** with a **fresh 12 h replay**:

* the replay is run inside the training job through the scorer's feature path
  (`build_features` on the last 120 s, every 5 s, rows inside a breach included) with
  the catalog maintenance age, as live;
* its seed and epoch are fresh: `--replay-seed`, `--replay-start-epoch`;
* every compared version is scored on the same cycles, at its own shipped levels.

All five conditions are blocking:

1. the clear level is at or above the steady-running p90 (this also applies to
   `--promote always`);
2. pooled false alarms are at most **3 per hour** (the design budget);
3. pooled breaches warned ≥ 30 s ahead exceed the duty-matched clock;
4. pooled cycle-permutation p < 0.05;
5. the **lower 95 % bound of standardised discrimination is above 0**. This is the
   gate metric.

**Choosing `champion`:**

* **Against a qualifying champion,** a challenger must show a **paired-bootstrap
  gain**: the challenger-minus-champion difference in standardised discrimination,
  resampled on the same cycles, must have a lower 95 % bound above 0. Its false
  alarms must also stay within 1 per hour. Otherwise the incumbent stays.
* **If the champion does not qualify,** `champion` moves to the qualifying version
  with the best gate metric.
* **If no version qualifies,** it moves to the best version by the gate metric and
  tags it `status=unqualified`.
* **Logging.** The decision, the paired difference, each version's metrics, and the
  cycle records and null counts (`null_counts.json`) are all logged.

### Measured early warning (fresh, simulated data, not live)

**Final run: v12** (`docs/evidence/ml/training_run_v12.json`).

* The training data is seed 101, epochs from 1,832,000,000.
* The fresh replay is 12 h, seed 111, epoch 1,836,000,000.
* Pooled, that is **231 cycles with 166 breaches**, none used by any earlier
  selection.
* The rules were frozen before this run.

Values are point estimates with their [95 % CI]:

<!-- metrics:start -->
| Pooled test + fresh replay (231 cycles) | **v7 (champion)** | v12 | v11 | v6 | v4 | Clock, any duty |
| --- | --- | --- | --- | --- | --- | --- |
| Cut-off / clear level | 0.59 / 0.54 | 0.54 / 0.34 | 0.73 / 0.53 | 0.40 / 0.35 | 0.47 / 0.42 | 0 s + 230 s |
| Steady-running p90 | 0.10 | 0.29 | 0.31 | 0.32 | 0.16 | – |
| Breaches warned ≥ 30 s ahead | 59.0 % [53.6, 64.5] | 72.9 % [69.3, 75.9] | 66.9 % [62.0, 71.7] | 78.3 % [76.5, 79.5] | 63.9 % [59.6, 68.1] | 98.8 % |
| Median lead | 47 s | 71 s | 49 s | 80 s | 52 s | – |
| False alarms per hour | 1.92 [1.46, 2.39] | 1.92 [1.51, 2.39] | 1.35 [0.99, 1.71] | 2.23 [1.82, 2.65] | 1.82 [1.51, 2.18] | 3.38 |
| Standardised discrimination | **0.21 [0.12, 0.30]** | 0.29 [0.21, 0.36] | 0.32 [0.23, 0.42] | 0.21 [0.13, 0.29] | 0.20 [0.14, 0.27] | −0.01 [−0.03, 0.00] |
| Duty-matched clock: warned ≥ 30 s | 29.5 % [22.9, 36.1] | 33.7 % | 22.9 % | 36.7 % | 29.5 % | – |
| Cycle permutation | p ≤ 0.001 | p ≤ 0.001 | p ≤ 0.001 | p ≤ 0.001 | p ≤ 0.001 | p = 1.0 |
| Alert duty | 28 % | 32 % | 26 % | 38 % | 31 % | 77 % |
| Qualifies | yes | yes | yes | yes | yes | – |
<!-- metrics:end -->

* **v12 against v7.** The paired gain is +0.075 [−0.001, +0.149]. The interval
  includes 0, so **v7 stays champion**.
* **v11.** It has the highest point estimate. It was not the challenger in this run:
  it had been promoted over v7 by 0.003, and that was reverted by hand once the
  paired rule was adopted.
* **The oracle ceiling.** Point estimates of 0.20–0.32 sit around the oracle's 0.30:
  the models use most of what a precursor-only detector can get on this scenario.

Cycle-level view on the same pooled cycles. Breach groups show warned ≥ 30 s out of
breaches; the other groups show cycles with a false alarm out of cycles:

| Plan group | Cycles | v7 | v12 |
| --- | --- | --- | --- |
| Excursion, strong precursor | 88 | 73 / 88 | 84 / 88 |
| Excursion, weak precursor | 44 | 25 / 44 | 37 / 44 |
| Excursion, no precursor | 34 | **0 / 34** | **0 / 34** |
| False alarm, fade | 17 | 16 / 17 (23 false alarms) | 16 / 17 (17) |
| False alarm, bump | 13 | 6 / 13 (9) | 10 / 13 (15) |
| Quiet | 35 | **1 / 35** | **1 / 35** |

**Reading it honestly:**

* **The warnings track precursors.** Both models warn most precursor breaches, none
  without a precursor (0 / 34), and almost never alert in quiet cycles. Cycle
  permutation is at its floor (p ≤ 0.001), against p = 1.0 for every clock.
* **Fades and bumps are the cost.** They look like real precursors and get
  alerts, as they would for the oracle.
* **The CIs are wide.** v7's standardised discrimination is 0.21 [0.12, 0.30]. 19 h
  of simulated running is still a small sample.
* **This is not real-machine proof.** It is all simulated physics.

**Why the 4 h endpoint replay looked worse** (`diagnosis_offline_vs_replay.json`).
That replay (seed 9002, `replay_fresh_v7_v6_v4_clock.json`) showed 20 false alarms
where the test rate predicted about 10. I scored the seed-11 test cycles three ways
with v7's logged booster:

* **Test path vs replay path, same maintenance age.** False alarms and warnings
  are identical: the feature window, cadence, hysteresis, buffer and sample rate
  all agree. The replay has more alert *episodes* because it also scores rows
  inside a breach, but those are not false alarms.
* **Replay path with the catalog maintenance age, clipped to 3000 h.** False alarms
  rise 12 → 17, and weak-precursor warnings fall from 15 / 17 to 7 / 17. v7 had
  learned a spurious dependence on `hours_since_maintenance`. Training drew it at
  random per run, although the simulated producer has no wear, and 3000 h was the
  edge of that range.
* **The sample.** On top of that, the 4 h sample was alarm-heavy: 13 false-alarm
  cycles where about 7 are expected. Standardising to the nominal plan mix moves
  v7's discrimination on that replay from 0.16 [0.04, 0.29] raw to 0.24
  [0.14, 0.34], in line with the pooled result.
* **The fix.** Live-format training rows now pin the feature at 3000 h. A constant
  column is never split on, and older versions see the same value on test and
  replay. The gate pools a fresh 12 h replay, so a path or input difference shows
  up in the gate itself.
* **A related check.** v4 and v7 had the same 12 test false alarms in the earlier
  run. They are different alerts at different times on the same fade and bump
  cycles: a coincidence of counts, not a scoring bug.

**Adaptive reuse (disclosure).** These earlier results were looked at while the
rules were designed, so they are optimistic and are kept only as history:

* **v5–v7:** the seed-7 and seed-11 test splits, and the 9001 and 9002 replays;
* **v8–v11:** their pooled runs, each of which led to a rule change: log-loss early
  stopping, the clear margin, the scenario-calm floor, the paired rule, and a fix to
  the permutation pooling.

Only the v12 run, with its seeds chosen after the last rule change, is quoted as
the result.

**Still to do: the live measurement.** Once the producer is redeployed with this
change, measure over at least 60 minutes from the alert tables. For breaches, take
`excursion_started_at`; for predicted alerts, take `opened_at` and `cleared_at`. Then
compute `lead_metrics.event_report` and `gate_stats.bootstrap`.

**Redeploy checklist.** Expect these to shift once the producer runs this scenario:

* **Fault-minute and availability KPIs** (the KPI notebook and Genie) fall: only
  about 70 % of cycles now have a breach, at random times;
* **Value views** (excursions, caught in time, deadline-before-alert, cost avoided)
  change with the breach count and with the ramp's position relative to the lot end;
* **The cell UI** labels the countdown as a *scheduled demo excursion (simulator
  clock, not a model output)*: it knows the scenario plan and predicts the ramp
  exactly;
* **Value-view evidence and screenshots** from the fixed 210 s schedule
  (`docs/evidence/2026-11-20/`) no longer match. Re-capture them.

**Lot on predicted alerts.** A predicted alert takes the lot being cut when it
opens (`SpindleRiskScorer._lot_for`), and an episode keeps the lot it opened on. A
precursor can now start in the previous cycle, so a predicted alert can carry the
previous lot. On the 4 h endpoint replay that happened for 1 of 26 warned breaches
(v7). Breach alerts, excursion windows and parts at risk use the breach onset, so
containment is unaffected. This is documented rather than changed: re-tagging an
open episode would break the episode semantics above.

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
* **The gate is simulation-only.** Standardisation and the steady-running floor use
  the scenario plan. On real telemetry the floor falls back to the plan-free
  definition, and discrimination is not standardised.
* **Uncalibrated scores.** Probabilities are not calibrated.
* **Maintenance age.** `hours_since_maintenance` carries no signal in live-format
  data and is pinned in training. v7, trained before the pin, still depends on it a
  little: live is about 1,950 h today and clips at 3000 h from 2027-01-04.
* **Scoring delay and buffer.** The app scores at most every
  `RISK_SCORE_INTERVAL_S` (5 s). The buffer is sized for 2 × 60 s at up to 30 Hz per
  sensor.
* **Top features.** These are TreeSHAP contributions in log-odds. They explain the
  model, not the physics.

**History.**

| Version | Status (MLflow `status` tag) | Why |
| --- | --- | --- |
| v1–v4 | – | 1 Hz synthetic data. v4's only live lead was 7 s on the old ramp-only event |
| v5 | `rolled_back`, `superseded_by=6` | its clear level of 0.14 was below calm scores |
| v6 | `superseded_by=7` | the fixed-phase scenario; replaced by v7 at seed 11 under the old plan-free floor; qualifies at v12 but is not the incumbent |
| **v7** | **champion** | the jittered scenario, seed 11; qualifies on the v11 and v12 pooled runs |
| v8 | `rejected` | average-precision early stopping stopped at 5 trees, so it never alerts |
| v9 | `rejected` | the p95 search margin set its cut-off at 0.91, so it never alerts |
| v10 | `rejected` | gate lower bound −0.013 |
| v11 | `promotion_reverted` | promoted over v7 by 0.003; not significant under the paired rule |
| v12 | `not_promoted` | qualifies; its paired gain over v7 is not significant |

The dev endpoint serves v7, and the bundle default is `risk_model_version=7`.

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
