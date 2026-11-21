# Thermal-risk model: evidence

## 2026-11-21: pre-registered deciding run; v13 champion

| File | What it shows |
| --- | --- |
| `training_run_v13.json` | The deciding run: seeds named in the model card at `code_sha` eec0bbc **before** submission, with `code_dirty=false` and matching upload and runtime source hashes. It holds the neutral selection over v4, v6, v7, v11, v12 and v13 on 231 pooled cycles (CIs, block CIs, invariance at 1500 / 2500 / 3000 h). The winner is v13 |
| `deciding_run_promotion_decision.json` | The decision artifact: per-version metrics, problems, and paired differences against the winner |
| `deciding_run_null_counts.json` | Per-version cycle records (group, breach, warned, false alarms, period) and the permutation and shift null counts, so every claim can be recomputed |
| `deciding_run_version_tags.json` | `selection_outcome`, `known_defect`, `maintenance_pinned` and `status` tags per version |

## 2026-11-21 (earlier): pooled, CI-based gate; v7 champion (superseded)

These files are redacted: `<workspace-host>`, `<user>`, `<catalog>`. The scenario
physics is **SIMULATED** (`src/iot_twin/scenario_engine/spindle_scenario.py`), and
the ramp start is jittered 30–210 s into each 300 s cycle. All data is
`data_origin=live_format_simulated`: the live producer's code path, run offline at
25 Hz.

**The result to quote** (rules frozen before this run; seeds used by no earlier
selection):

| File | What it shows |
| --- | --- |
| `training_run_v12.json` | The v12 run: training seed 101 (epochs from 1,832,000,000) and a fresh 12 h replay, seed 111 (epoch 1,836,000,000), on the scorer path. It pools 231 cycles with 166 breaches. For v4, v6, v7, v11 and v12 on the same cycles it holds: qualification metrics with bootstrap 95 % CIs (warned ≥ 30 s, false alarms per hour, plan-mix-standardised discrimination), cycle permutation, duty-matched and any-duty clocks, the calm-running floor, per-group cycle counts and problems. The decision is champion v7 kept, because v12's paired gain of +0.075 [−0.001, +0.149] is not significant. Cycle records and null counts are the run artifact `null_counts.json` |
| `diagnosis_offline_vs_replay.json` | Why the 4 h endpoint replay showed about 2× the test false-alarm rate. The test path and the replay path agree. The gap is the clipped catalog maintenance age (a spurious feature dependence) plus an alarm-heavy sample. It also holds the v4/v7 false-alarm spot check |
| `registered_model_current.json` | Aliases (`champion` → 7) and versions 4–12 |
| `endpoint_dev_v7.json` | `dev_<user>_pharma-twin-spindle-risk`: `READY`, serving version 7 |

**History** (each run below changed a rule, so these results are selection-contaminated; do not quote them):

| File | Status |
| --- | --- |
| `training_run_v5.json`, `training_run_v6.json`, `replay_v4_vs_v6.json` | The fixed-phase scenario, where a clock warned 38 of 38. v5 was rolled back and v6 superseded |
| `training_run_v7.json`, `replay_fresh_v7_v6_v4_clock.json` | Seed 11 and the 4 h endpoint replay (seed 9002); the gate was then test-only, with a 0.30 discrimination floor |
| `training_run_v8.json` | `rejected`: average-precision early stopping stopped at 5 trees |
| `training_run_v9.json` | `rejected`: the p95 clear margin gave a cut-off of 0.91 |
| `training_run_v10.json` | `rejected` by the gate; the plan-free calm floor swung there |
| `training_run_v11.json` | `promotion_reverted`: promoted over v7 by 0.003 before the paired rule |

**How the final run was produced.**

```bash
python scripts/submit_risk_model_training.py --profile <cli-profile> --catalog <catalog> \
  --schema pharma_twin --data-source live_format --promote if_better \
  --workspace-dir /Workspace/Users/<user>/pharma-twin-risk-model-live-lead \
  --experiment /Users/<user>/pharma-twin-risk-model/experiment \
  --extra=--seed=101 --extra=--live-start-epoch=1832000000 \
  --extra=--compare-versions=4,6,7,11 --extra=--replay-hours=12 \
  --extra=--replay-seed=111 --extra=--replay-start-epoch=1836000000
```

**How `champion` and the endpoint changed.** The registry and the dev endpoint were
changed directly, by the gated runs and by hand (alias moves and tags). The dev
endpoint was set with `serving-endpoints update-config` and the bundle's own entity
config; `bundle deploy` would update the app and re-sync its source folder, which
another session owns. The PR sets `risk_model_version=7`.

**Not live.** The producer job's notebook still had the old ramp-only heat event.

## 2026-11-20: v1–v4 (synthetic, 1 Hz)

Captured from one Databricks workspace with the names this PR uses (`pharma_twin`
schema, `pharma-twin-*` endpoint and run names). The workspace host, workspace
id, user and catalog are redacted (`<workspace-host>`, `<user>`, `<catalog>`).
The training data is **SYNTHETIC** (`data_origin=synthetic`, see
[../../ml-risk-model.md](../../ml-risk-model.md)).

| File | What it shows |
| --- | --- |
| `mlflow_run_v4.json` | MLflow run of the champion: params, test metrics (ROC-AUC, PR-AUC, precision/recall at the cut-off, lead time vs the threshold rule), the shipped cut-off / clear level, calm-score spread, embargo |
| `registered_model.json` | UC model `<catalog>.pharma_twin.spindle_thermal_risk`, version 4 `READY`, alias `champion` → 4 |
| `endpoint.json` | `pharma-twin-spindle-risk`: `READY`, serving version 4, Small, scale-to-zero, 100 % traffic |
| `endpoint_query.json` | Calm windows with simulator noise at 200–3260 h since maintenance, and a window with the synthetic bearing precursor, as `dataframe_records` built by `build_features`, and the response (score, top features, `decision_threshold`, `clear_threshold`) |
| `endpoint_query_no_precursor_ramp.json` | The live producer's heat event (temperature ramp only, no precursor), sampled every 2 s up to the onset: what lead time looks like on the real demo signal |

How it was produced, with `scripts/submit_risk_model_training.py --catalog <catalog> --schema pharma_twin --data-source synthetic --endpoint-name pharma-twin-spindle-risk --wait`:

1. **Version 1:** with `--create-endpoint`; registered v1, set `champion` and created the endpoint.
2. **Version 2:** after the onset-based lead metric was added; same data and model as v1.
3. **Version 3:** after review fixes, which shipped:
   * the time-split embargo (180 s);
   * clipping `hours_since_maintenance` to 0–3000 h;
   * the cut-off and clear level inside the model.

4. **Version 4:** the precision floor raised to 0.9. At 0.8 the cut-off was 0.26,
   inside the calm range of a worn spindle (0.20–0.27). The job moved `champion`
   to v4, waited for any in-progress endpoint update, repointed the endpoint and
   logged `--var risk_model_version=4`.

**This is the training-evidence endpoint.** `pharma-twin-spindle-risk` was created
by the submit script, outside the bundle. Leave it as it is:

* **Dev:** a `bundle deploy -t dev` creates its own
  `dev_<user>_pharma-twin-spindle-risk`, serving the same v4. The dev app queries
  that one, through the resource reference.
* **Prod / staging:** only a non-development target that owns the unprefixed name
  binds this endpoint, before its first deploy:

```bash
databricks bundle deployment bind spindle_risk pharma-twin-spindle-risk -t prod --profile <cli-profile>
databricks bundle deploy -t prod --var risk_model_version=4 --profile <cli-profile>
```

Never bind it from `dev`. The prefixed name differs, so that would plan a rename
of an immutable endpoint name.

**Reading the queries.**

* **Calm windows:** score 0.11–0.27 (higher with wear; 3260 h scores like 3000 h
  because the feature is clipped). That is under the 0.42 clear level, so a
  predicted alert clears in calm running.
* **Precursor window:** scores high, and its top TreeSHAP drivers are vibration
  features. The model learned the *synthetic* bearing-friction precursor; see
  the model card's limitations.
* **No-precursor ramp:** shows the honest lead time on the live producer's heat
  event. The score first reaches the 0.47 cut-off 12 s into the ramp. The first
  over-limit reading is at 19 s, so the lead is **7 s before the onset** (about 9 s
  ahead of the threshold rule), not minutes.
