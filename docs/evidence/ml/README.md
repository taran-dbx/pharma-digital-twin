# Thermal-risk model: live-run evidence (2026-11-20)

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
