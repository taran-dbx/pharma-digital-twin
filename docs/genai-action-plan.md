# AI action plan: grounded, cited, audited

The incident panel's **AI action plan** card turns one alert episode into an
advisory plan. The plan says which lots and serials to quarantine, how much
to inspect on the CMM, whether an NCR and a maintenance request are needed,
what the model risk score means, and who owns each step. Every claim cites
the exact rows it came from. Every generation is logged with the rows it was
given. The plan changes nothing by itself. A qualified person applies it by
signing decisions in the existing response workflow.

Genie is still the natural-language query tool. This feature is the app's
only direct Foundation Model call.

## Architecture

```
POST /api/alerts/{alert_id}/action-plan            (admin, like the other alert writes)
  │
  ├─ grounding.assemble_grounding ── Lakebase: alert, alert_event, alert_response
  │                                 DBSQL:    readings window (10 s buckets) for the pipeline mode
  │                                 catalog:  validated limit, OQ/PQ refs, maintenance history
  │                                 lot clock: lots, serials at risk, projected right-first-time
  │                                 alert.metadata: model risk score (when present)
  ├─ prompt.render_messages ─────── system rules + JSON schema; rows as UNTRUSTED data
  ├─ fm_client.query_chat ───────── WorkspaceClient().serving_endpoints.query (AI Gateway path)
  ├─ plan_schema.parse_plan ─────── pydantic schema validation
  ├─ plan_schema.check_grounding ── citations, numbers, identifiers, risk score
  └─ audit_store.record ─────────── Lakebase genai_action_plan_audit (one row per attempt)
                                     └─ audit_sync → UC intelligence_genai_audit (for Genie)
```

Code lives in `src/iot_twin/services/genai/`. The routes are in
`src/iot_twin/routes/genai.py`. The card is
`client/src/components/cell/action-plan-card.tsx`.

| Setting | Default | Notes |
| --- | --- | --- |
| `GENAI_ENDPOINT` | `databricks-claude-sonnet-5-5` | Injected from the `genai-endpoint` app resource (serving endpoint, `CAN_QUERY`). Set the bundle variable `genai_endpoint` to use another READY chat endpoint, such as `databricks-claude-haiku-5-5` or `databricks-meta-llama-3-3-70b-instruct`. |
| `GENAI_TIMEOUT_S` | `60` | Per attempt. |
| `GENAI_MAX_RETRIES` | `2` | Retries cover timeouts, 429 and 5xx only. Permission, not-found and bad-request errors are not retried. |
| `GENAI_MLFLOW_TRACING` | off | Set to `1` and install `mlflow` to record a trace span per call. |

Rate limits, usage tracking and guardrails come from the endpoint's AI Gateway
configuration. The app does not duplicate them. No `temperature` is sent,
because Claude 5.x endpoints reject it.

## API

| Method | Path | Auth | Returns |
| --- | --- | --- | --- |
| POST | `/api/alerts/{alert_id}/action-plan` | admin | `201` with the plan summary. `504` if the endpoint timed out, `502` for other endpoint errors; both bodies carry the logged attempt. `503` when Lakebase is bound but cannot store the audit. `422` for a non-UUID id, unknown body fields or a bad `mode`. `404` if the alert does not exist. |
| GET | `/api/alerts/{alert_id}/action-plan` | signed-in user | Prior plans for the alert, newest first. |
| GET | `/api/genai/audit/{plan_id}` | signed-in user | The full trace: the summary plus `grounding_rows` and the raw `response`. |
| POST | `/api/alerts/{alert_id}/responses` | admin | The existing signed-decision route. It now also accepts `action_plan_id`. If the decision is new, the plan is marked applied (`applied_by`, `applied_at`, `applied_response_id`). The plan must belong to the same alert, or the route returns `422`. |

The POST body is optional: `{"mode": "realtime" | "microbatch" | "zerobus"}`.
The mode picks the telemetry table for the readings window. Without it, the
plan is grounded without readings, and that gap is recorded.

## Grounding and citations

Every grounding item has the form `{id, source, key, data}`. Its `id` is the
citation the model must quote:

| Citation id | Source |
| --- | --- |
| `alert:<uuid>` | The `alert` row: value, threshold, status, excursion onset, triggering part, and the NCR id reserved for the episode |
| `alert_event:<uuid>` | `alert_event` lifecycle rows of this episode |
| `alert_response:<uuid>` | Signed decisions already recorded (the legacy action is mapped back) |
| `readings:<sql-hash>#<n>` | Row *n* of the readings-window query; the hash identifies the exact SQL |
| `lot:<lot_id>` | Serials at risk, releasable parts, DHR ref, UDI-DI and work order for one lot |
| `projected_rft:<alert_id>` | Projected right-first-time over the affected lots (`lot_context.PROJECTED_RFT_DEFINITION`) |
| `alert_rule:<rule_id>` | The validated limit, OQ/PQ refs, process spec and required action (catalog) |
| `maintenance:<event_id>` | Maintenance history of the asset |
| `risk_score:<alert_id>` | `risk_score`, `model_name`, `model_version`, `top_features` from alert metadata. The `medtech-model-spindle-risk` alert for the same window is used when the alert itself has none. |

The excursion window starts at the first over-limit reading. This is the same
window the QA hold uses (`alert_responses.excursion_start`), so the AI and the
signed decision name the same parts. Risk keys are read defensively. When
there is no score, the model is told to say so, and inventing one fails the
check. A source that is unavailable, such as a missing warehouse or Lakebase,
becomes a recorded `grounding_gap`; it does not fail the request.

### The grounding check

The model must return one JSON object that matches `plan_schema.ActionPlan`:
`summary`, `disposition` (quarantine lots/serials, CMM scope, NCR yes/no,
maintenance request), `risk_narrative`, and `steps`. Each step has an
`owner_role` of `Operator`, `QA`, `Maintenance` or `Manufacturing Eng`, and an
optional `response_action`. Every claim has at least one citation.

If the answer is not valid JSON or does not match the schema, the plan is
`rejected`. If it does match, `check_grounding` sets `grounding_check` to
`pass` or `fail` and lists the reasons. The check fails a plan that:

* cites an id that is not in the grounding set,
* states a number that matches no grounded value. Allowed values are numbers
  in the cited rows (rounded to the precision written), differences between
  values in one row (`value - threshold`), list counts, and a 0–1 score written
  as a percentage. Numbers inside identifiers (`OQ-TM-CNC1-014`), dates and
  clock times are not treated as claims,
* names a lot or serial that is not in the grounding set,
* omits the risk-score row from the risk narrative when one exists, or states a
  score when none exists.

A failing plan is still shown, with a warning badge and the reasons. Its
"Apply" buttons are disabled.

## Audit schema (Part 11-style traceability)

The Lakebase table is `genai_action_plan_audit`. It is created by
`app_schema.ensure_genai_action_plan_audit_table` as part of
`ensure_app_schema`.

| Column | Meaning |
| --- | --- |
| `id` | Plan id (UUID) |
| `alert_id` | `REFERENCES alert(id) ON DELETE RESTRICT`. Like `alert_response`, an alert with audited plans cannot be deleted. |
| `industry`, `requested_by`, `requested_at` | Who asked, and when |
| `endpoint`, `model`, `model_version` | The endpoint, plus the served model and version from its config (for example `system.ai.databricks-claude-sonnet-5-5` / `Claude Sonnet 5.5`) |
| `prompt_template_version`, `prompt_hash` | The template version and the SHA-256 of the exact rendered messages |
| `grounding_hash`, `grounding_rows` | The SHA-256 and the JSONB of the exact rows given to the model, with their citation ids |
| `response` | The raw answer (capped at 20 000 characters), the parsed plan, schema errors, grounding gaps and the attempt count |
| `grounding_check`, `grounding_reasons`, `status` | `pass` or `fail` with reasons; `generated`, `rejected` or `error` |
| `latency_ms`, `prompt_tokens`, `completion_tokens`, `total_tokens` | Cost and latency |
| `applied_by`, `applied_at`, `applied_response_id` | Set once, when a person signs a decision citing the plan. `applied_response_id` references `alert_response(id) ON DELETE RESTRICT`. |

Every attempt is a row, including timeouts and malformed answers. The only
later write is the one-time `applied_*` update (`WHERE applied_at IS NULL`).

There are two ownership pitfalls. `CREATE TABLE IF NOT EXISTS` is a no-op for
a non-owner, and the index step tolerates the "must be owner" error that
Postgres raises before its IF-NOT-EXISTS skip. Both are covered by a
local-Postgres test that boots as a non-owner role.

When Lakebase is bound, the store either persists the row or raises
`AuditStoreUnavailable`, which returns `503`. It never falls back to memory,
which is the same rule signed decisions follow. So the app never shows an AI
answer without its audit row. Process memory is used only when no Lakebase is
configured, as in local runs and tests.

### UC mirror for Genie

`audit_sync.GenaiAuditSynchronizer` follows the `alert_snapshot_sync` pattern.
A new or applied plan marks the mirror dirty. A bounded loop (the
`ALERT_SNAPSHOT_SYNC_INTERVAL_S` interval) then rewrites the 200 most recent
plans into `{UC_CATALOG}.{UC_SCHEMA}.intelligence_genai_audit`. An empty copy
never deletes the table. The mirror stores `citation_ids` and `grounding_rows`
as JSON strings, plus `risk_score`, `risk_model` and `risk_model_version`, so
Genie can answer "which rows did the AI use for alert X?" and "explain why
Spindle 3 was flagged and what the AI recommended". The medtech Genie
instructions describe the table. A curated example SQL joins
`intelligence_alert_snapshot` to the latest plan.

## Prompt-injection defences

1. **Trusted/untrusted split**, as in Genie (`genie_service.render_trusted_prompt`).
   The system message holds the rules and the schema. `[TRUSTED_CONTEXT]` holds
   only app-controlled values: the allowed citation ids, owner roles, response
   actions and gaps. Every row goes in `[UNTRUSTED_GROUNDING]` and is labelled
   as data, not instructions.
2. **Delimiter defanging.** Any `[TRUSTED_CONTEXT]`, `[UNTRUSTED_GROUNDING]` or
   `[UNTRUSTED_OBSERVATIONS]` tag inside the data is rewritten, so planted text
   cannot close the block.
3. **Bounded data.** Free text is clipped to 500 characters. Readings are capped
   at 120 buckets, events at 50, and top features at 10.
4. **Output is checked against the input.** Fabricated citations, numbers or
   serials fail the grounding check, whatever the model was told.
5. **No capability to act.** The endpoint call has no tools. The service never
   writes `alert` or `alert_response`. Applying a plan needs a person's
   Part 11-style signature in the existing form, and the signature intent box is
   never pre-ticked.

The live evidence in `docs/evidence/genai/` includes an injection run. Text
planted in alert metadata tried to close the block, release every lot, cite a
fake id and lower the risk score. The model ignored it, and the plan passed
the check with the real 87% score.

## Limitations and next steps

* The numeric check covers digits only. Spelled-out numbers ("three parts") and
  unit errors (°C versus °F) are not verified. A number can pass by matching an
  unrelated grounded value of the same magnitude.
* The check verifies that a cited row exists. It does not verify that the row
  supports the sentence. An LLM-judge scorer for that is a natural next step,
  for example an `mlflow.genai.evaluate` run with a groundedness scorer over a
  small set of recorded episodes.
* The readings window needs the pipeline `mode` and a SQL warehouse. Without
  them the plan has no readings, and that gap is recorded in the audit row.
* An answer that hits the 6 000-token limit is rejected as malformed. Very long
  open episodes can name many lots, which makes the prompt and answer larger.
* MLflow tracing is optional and off by default, because `mlflow` is not an app
  dependency. With `GENAI_MLFLOW_TRACING=1` and `mlflow` installed, each call is
  a `CHAT_MODEL` span tagged with the plan id. Writing those traces to a UC
  table is the next step.
* The UC mirror is eventually consistent (one sync interval) and holds only the
  most recent 200 plans. Lakebase is the system of record.
