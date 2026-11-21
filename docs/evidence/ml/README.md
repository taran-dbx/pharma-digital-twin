# Thermal-risk model: evidence

## 2026-11-21: live-format retrain (v5 superseded, v6 champion) and endpoint replay

These files are redacted: `<workspace-host>`, `<user>`, `<catalog>`. The scenario
physics is **SIMULATED** (`src/iot_twin/scenario_engine/spindle_scenario.py`; see
[../../ml-risk-model.md](../../ml-risk-model.md#the-simulated-spindle-3-scenario-one-generator-for-live-and-training)).
The training data is `data_origin=live_format_simulated`: the live producer's code
path, run offline on a virtual clock at 25 Hz.

| File | What it shows |
| --- | --- |
| `training_run_v6.json` | Job summary of the run that registered **v6**: test metrics, the event-level cut-off / clear level, and the challenger-vs-champion comparison against v4 on the same held-out rows (`challenger_beats_champion: true`, `promoted: true`) |
| `training_run_v5.json` | **v5, superseded.** The same data and booster, but the cut-off search picked clear level 0.14, below steady-running scores. The first gate promoted it; `champion` was moved back to v4 by hand before any endpoint used it |
| `registered_model_v6.json` | Aliases after the run (`champion` → 6) and versions 4–6 |
| `endpoint_dev_v6.json` | `dev_<user>_pharma-twin-spindle-risk`: `READY`, serving version 6 |
| `replay_v4_vs_v6.json` | A 4 h held-out replay (48 cycles, 38 breaches) through the scorer's feature path and both real endpoints. It holds the per-breach leads with each cycle's scenario plan, false alarms per hour and by cycle plan, the chance baseline and alert duty |

**How it was produced.**

```bash
python scripts/submit_risk_model_training.py --profile <cli-profile> --catalog <catalog> \
  --schema pharma_twin --data-source live_format --promote if_better \
  --workspace-dir /Workspace/Users/<user>/pharma-twin-risk-model-live-lead \
  --experiment /Users/<user>/pharma-twin-risk-model/experiment --wait
databricks serving-endpoints update-config dev_<user>_pharma-twin-spindle-risk --json \
  '{"served_entities":[{"entity_name":"<catalog>.pharma_twin.spindle_thermal_risk","entity_version":"6","workload_size":"Small","scale_to_zero_enabled":true}]}'
python scripts/replay_risk_endpoint.py --profile <cli-profile> \
  --endpoint dev_<user>_pharma-twin-spindle-risk --endpoint pharma-twin-spindle-risk \
  --hours 4 --output docs/evidence/ml/replay_v4_vs_v6.json
```

**Why the endpoint was updated directly.** The dev endpoint was updated with
`update-config` and the same entity config the bundle declares, not with
`bundle deploy`. A `bundle deploy -t dev` from this branch plans updates to the app
and the training job as well. It also re-syncs the bundle `files/` folder, which is
the app's source path, and another session owns redeploying the app. The PR sets
`risk_model_version` to `6`, so the next bundle deploy keeps the endpoint on v6.

**Not live.** When this was measured, the producer job's notebook still had the old
ramp-only heat event (no `apply_spindle_scenario`). The replay stands in for the live
measurement until the producer is redeployed.

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
