# Spindle 3 thermal-risk model

Spindle 3 on CNC-1 (Ti-6Al-4V hip stems) has a validated thermal limit of 60 °C
(OQ-TM-CNC1-014). The threshold rule `mfg-alert-spindle-temp` opens an alert
once the limit is **breached**. The thermal-risk model scores the spindle every few
seconds and opens a **predicted** alert *before* the limit trips. That alert goes
to the same alert table, lifecycle, SSE stream, Genie snapshot and cell UI.

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
| Synthetic training data (SYNTHETIC, labelled) | `src/iot_twin/ml/synthetic_telemetry.py` |
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
| `risk_model_version` | `1` | version the endpoint serves at deploy time (the job repoints it to the champion) |
| `risk_training_data_source` | `auto` | `auto`, `synthetic` or `real` |
| `risk_alert_threshold` | `0.22` | `RISK_ALERT_THRESHOLD`, the alert cut-off |

The app scorer also reads these optional env vars: `RISK_ALERT_HYSTERESIS` (0.15),
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
| `hours_since_maintenance` | Hours since the last Spindle 3 maintenance in the catalog (`mfg-mnt-001`) |

**Label.** Forward-looking: 1 when any temperature reading exceeds the validated
limit in `(t, t + horizon]`. The default horizon is 120 s (`--horizon-s`). Rows
where the spindle is *already* over the limit are dropped, because the threshold
rule handles those and the model's job is to warn before the trip. Samples are
taken every 5 s.

**Data.** `--data-source auto` uses real `manufacturing_realtime` telemetry when
the last 14 days contain at least 2,000 labelled rows and 20 excursions. Otherwise
it trains on **SYNTHETIC** runs, and the choice is logged as the `data_origin`
param, run tag and model-version tag. Live telemetry has one scheduled heat event
every 5 minutes and is too regular to train on, so the shipped model is
synthetic. The synthetic runs come from:

* the `iot_simulator` catalog (nominal, noise, range and cycles of the four spindle
  sensors, with noise in sensor units as the producer emits it);
* the producer's scheduled heat event (`SPINDLE_HEAT_SCENARIO`, mirrored and
  checked by a test) on half of the 5-minute cycles, with a random peak so
  some ramps stay under the limit;
* `ScenarioEngine` degraded → failed → recovering phase effects and lot metadata on
  a quarter of the runs (15-minute phases);
* spindle wear (hotter and rougher with hours since maintenance) and, on 65 % of
  heat events, a bearing-friction precursor: vibration and load rise 45–150 s
  before the ramp. **This precursor is a modelling assumption, not measured
  behaviour.**

Every synthetic reading is tagged `data_origin=synthetic`. Defaults are 24 runs
× 2 h at 1 Hz, giving 30,897 labelled rows with 23 % positive.

**Training.** LightGBM (`LGBMClassifier`, 400 trees max, early stopping on
validation PR-AUC). The split is time-ordered, train → validation (20 % of
train) → test (last 25 %), so every test row is later than every training row.
The cut-off is chosen on validation as the highest-recall threshold with
precision ≥ 0.8 (fallback: best F1), then evaluated once on test.

**Metrics** (test split, synthetic, live run, model version 2; see
`docs/evidence/ml/`):

<!-- metrics:start -->
| Metric | Value |
| --- | --- |
| ROC-AUC | 0.814 |
| PR-AUC | 0.710 (positive rate 0.218) |
| Cut-off (chosen on validation) | 0.22 (`max_recall_at_precision>=0.8`) |
| Precision / recall / F1 at the cut-off | 0.736 / 0.620 / 0.673 |
| Excursions in the test period | 171 |
| Predicted before the onset | 165 (96.5 %) |
| Mean lead before onset | 83.0 s |
| Lead time gained vs the threshold rule | mean 84.9 s, median 106 s |
| Rows train / validation / test | 18,537 / 4,635 / 7,725 |
<!-- metrics:end -->

"Lead time" is measured per excursion. The onset is the first over-limit
reading; the alert engine records it on the breach alert as
`excursion_started_at`. The threshold rule opens `duration_s` = 2 s after the
onset. The model "fires" at the first sample in `[onset − horizon, onset)` with
score ≥ cut-off. *Lead before onset* is onset − fire; *gain vs the threshold
rule* is (onset + 2 s) − fire. Missed events count as 0 s. In the UI, the risk
badge shows the same quantity for the live episode: the breach alert's
`excursion_started_at` minus the predicted alert's `opened_at`.

**Limitations.**

* Trained on synthetic data; the precursor physics is assumed. Retrain on real
  telemetry (`--data-source real`) before using the score for anything but the demo.
* The cut-off picked on validation (0.22) is low because positives are
  imbalanced and probabilities are uncalibrated. Test precision at that cut-off
  is 0.74, so roughly 1 in 4 predicted windows has no excursion within 2 min.
  `RISK_ALERT_THRESHOLD` is the knob.
* Heat events without a precursor are only predictable once the ramp starts.
  On the live producer's scheduled event, which has no precursor, the lead comes
  from the temperature slope and is seconds, not minutes.
* `hours_since_maintenance` comes from the catalog fixture. With no maintenance
  feed, it grows without bound in a long-running demo.
* Top features are TreeSHAP contributions in log-odds, which explain the model,
  not the physics.

## Serving and the app

* The endpoint `pharma-twin-spindle-risk` is Small and scales to zero. It serves
  the UC model version the training job last marked `champion`: the job sets the
  alias, then calls `update_config` on the endpoint (no wait).
* The app's `risk-endpoint` resource grants `CAN_QUERY` and injects the
  resolved endpoint name as `RISK_ENDPOINT_NAME` (`value_from`), so dev-mode
  name prefixes stay correct.
* The scorer keeps 2 × 60 s of spindle readings from every batch that reaches
  `evaluate_and_emit`. It runs at most one round every `RISK_SCORE_INTERVAL_S`, in
  the background, so ingest never waits. Each call is
  `WorkspaceClient().serving_endpoints.query(dataframe_records=[features])` with
  a `RISK_QUERY_TIMEOUT_S` timeout. After `RISK_BREAKER_FAILURES` consecutive
  failures, which includes a cold scale-to-zero start, the breaker opens for
  `RISK_BREAKER_COOLDOWN_S` and then allows one trial call.
* The model version comes from the endpoint's served entity, cached for 5 min.
  If the app cannot read it, it falls back to `RISK_MODEL_NAME` and version `unknown`.

### Bootstrap on a new workspace

The endpoint needs a registered model version, so a brand-new workspace needs one
training run before the first `bundle deploy` (which creates the endpoint and the app):

```bash
python scripts/submit_risk_model_training.py --profile <cli-profile> \
  --catalog <uc_catalog> --schema pharma_twin --data-source synthetic --wait
databricks bundle deploy -t dev --profile <cli-profile>
databricks bundle run risk_model_train -t dev --profile <cli-profile>   # later retrains
```

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
  "threshold": 0.22,
  "severity": "warn",
  "status": "open",
  "metadata": {
    "alert_kind": "predicted",
    "risk_score": 0.8123,
    "risk_threshold": 0.22,
    "model_name": "<catalog>.pharma_twin.spindle_thermal_risk",
    "model_version": "2",
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
* Later scores update the same alert, refreshing `value` and `metadata`, but only when the rounded score changes.
* While the score sits inside the band `(threshold − hysteresis, threshold)` the alert stays open and no event is emitted.
* It clears at `risk ≤ threshold − hysteresis`.
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
