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
An embargo of `window_s + horizon_s` (180 s) drops the training rows just before
each boundary, because their feature window or label horizon overlaps the next
period. The cut-off is chosen on validation as the highest-recall threshold with
precision ≥ 0.9 (fallback: best F1), then evaluated once on test. The clear
level is the cut-off minus `--clear-hysteresis` (0.05). The job logs the
calm-score spread (`val_calm_score_p50/p90`) and warns if the clear level is under
the p90. Both thresholds are stored in the model version (a `decision`
artifact), so the served pyfunc returns `decision_threshold` and
`clear_threshold` with every score. A single-class split fails training on
train/validation; on test it skips ROC-AUC / PR-AUC with a warning.

**Metrics** (test split, synthetic, live run, model version 4; see
`docs/evidence/ml/`):

<!-- metrics:start -->
| Metric | Value |
| --- | --- |
| ROC-AUC | 0.814 |
| PR-AUC | 0.718 (positive rate 0.218) |
| Cut-off / clear level (shipped in the model) | 0.47 / 0.42 (`max_recall_at_precision>=0.9`, band 0.05) |
| Calm validation scores | p50 0.115, p90 0.169; live calm windows 0.11-0.27 depending on wear (see evidence) |
| Precision / recall / F1 at the cut-off | 0.899 / 0.577 / 0.703 |
| Excursions in the test period | 171 |
| Predicted before the onset | 161 (94.2 %) |
| Mean lead before onset | 75.9 s |
| Lead time gained vs the threshold rule | mean 77.8 s, median 101 s |
| Rows train / validation / test (after the 180 s embargo) | 18,479 / 4,628 / 7,725 |
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
* Probabilities are uncalibrated, and calm-running scores rise with wear: 0.11–0.16
  below 1000 h, up to about 0.27 at 1950 h or more. A precision floor of 0.8 put
  the cut-off at 0.26, inside that calm range, so a worn spindle could open
  predicted alerts while running calm. The floor is 0.9 (cut-off 0.47); about 1 in 10
  predicted windows still has no excursion within 2 min. `RISK_ALERT_THRESHOLD`
  overrides the shipped cut-off.
* Heat events without a precursor are only predictable once the ramp starts. On
  the live producer's scheduled event, which has none, v4 first crosses the
  cut-off 12 s into the ramp, **7 s before the onset** (about 9 s ahead of the
  rule): seconds, not minutes (`docs/evidence/ml/endpoint_query_no_precursor_ramp.json`).
* `hours_since_maintenance` comes from the catalog fixture. With no maintenance
  feed it would grow without bound, so it is clipped at 3000 h; past that the
  model cannot tell more wear apart.
* Top features are TreeSHAP contributions in log-odds, which explain the model,
  not the physics.

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
`databricks bundle deploy` **reverts** it to `var.risk_model_version` (default
`1`), because the bundle owns the endpoint config. The job log and its summary
print the override to use, for example `--var risk_model_version=3`. Set it in
the target's variable overrides, or rerun the job after a deploy. The app logs
`serves … but @champion is v…` while the two differ.

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
  * optionally `EXECUTE` (or `BROWSE`) on the UC model, for the champion-mismatch
    warning.

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
