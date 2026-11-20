# Thermal-risk model: live-run evidence (2026-11-20)

Captured from one Databricks workspace with the names this PR uses (`pharma_twin`
schema, `pharma-twin-*` endpoint and run names). The workspace host, workspace
id, user and catalog are redacted (`<workspace-host>`, `<user>`, `<catalog>`).
The training data is **SYNTHETIC** (`data_origin=synthetic`, see
[../../ml-risk-model.md](../../ml-risk-model.md)).

| File | What it shows |
| --- | --- |
| `mlflow_run_v2.json` | MLflow run of the champion: params, test metrics (ROC-AUC, PR-AUC, precision/recall at the cut-off, lead time vs the threshold rule), tags |
| `registered_model.json` | UC model `<catalog>.pharma_twin.spindle_thermal_risk`, version 2 `READY`, alias `champion` → 2 |
| `endpoint.json` | `pharma-twin-spindle-risk`: `READY`, serving version 2, Small, scale-to-zero, 100 % traffic |
| `endpoint_query.json` | `dataframe_records` query with two feature rows built by `iot_twin.ml.spindle_features.build_features`, and the response |

How it was produced:

1. `scripts/submit_risk_model_training.py --catalog <catalog> --schema pharma_twin --data-source synthetic --endpoint-name pharma-twin-spindle-risk --create-endpoint --wait`
   registered version 1, set `champion` and created the endpoint.
2. The same command without `--create-endpoint`, after the onset-based lead metric
   was added, registered version 2, moved `champion` and repointed the endpoint
   (`update_config`). Same seed and data, so the model metrics are identical.

In the query, the calm window scores 0.16 (under the 0.22 cut-off). The window
where the ramp starts scores 0.90, and its top TreeSHAP drivers are vibration
features: the model learned the synthetic bearing-friction precursor. This is
the limitation the model card describes.
