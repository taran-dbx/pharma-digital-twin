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
  ├─ limits.GenaiLimiter ────────── concurrency cap · one per alert · per-user cooldown → 429
  ├─ grounding.assemble_grounding ── Lakebase: alert, alert_event, alert_response
  │                                 DBSQL:    readings window (10 s buckets) for the pipeline mode
  │                                 catalog:  validated limit, OQ/PQ refs, maintenance history
  │                                 lot clock: lots, serials at risk, projected right-first-time
  │                                 alert.metadata: model risk score (when present)
  ├─ action_plan.reusable_plan ──── same grounding hash + template + endpoint → reuse (200)
  ├─ prompt.render_messages ─────── system rules + JSON schema; rows as UNTRUSTED data
  ├─ fm_client.query_chat ───────── serving_endpoints.query on a bounded client + dedicated pool
  ├─ plan_schema.parse_plan ─────── pydantic schema validation
  ├─ plan_schema.check_grounding ── per-claim citations and numbers, lot/serial ids, risk score
  └─ audit_store.record ─────────── Lakebase genai_action_plan_audit (append-only, one row per attempt)
                                     └─ audit_sync → UC intelligence_genai_audit (pseudonymised, for Genie)
```

Code lives in `src/iot_twin/services/genai/`. The routes are in
`src/iot_twin/routes/genai.py`. The card is
`client/src/components/cell/action-plan-card.tsx`.

| Setting | Default | Notes |
| --- | --- | --- |
| `GENAI_ENDPOINT` | `databricks-claude-sonnet-5-5` | Injected from the `genai-endpoint` app resource (serving endpoint, `CAN_QUERY`). Set the bundle variable `genai_endpoint` to use another READY chat endpoint, such as `databricks-claude-haiku-5-5` or `databricks-meta-llama-3-3-70b-instruct`. |
| `GENAI_REQUEST_BUDGET_S` | `90` | Upper bound for the whole model call. |
| `GENAI_TIMEOUT_S` | `40` | Per attempt. The attempt count is clamped so that `timeout × attempts + backoff ≤ budget`. |
| `GENAI_MAX_RETRIES` | `1` | Retries cover timeouts, 429 and 5xx only. Permission, not-found and bad-request errors are not retried. |
| `GENAI_MAX_CONCURRENT` | `2` | Plans generated at once, process-wide. |
| `GENAI_USER_COOLDOWN_S` | `20` | Minimum gap between one person's requests. |
| `GENAI_PSEUDONYM_SALT` | from the `genai-pseudonym-salt` secret resource | HMAC key (at least 16 characters) for `person-<16 hex>` pseudonyms. If it is not set, the app uses a random key persisted in Lakebase. See [People and free text](#people-and-free-text-pii). |
| `GENAI_MLFLOW_TRACING` | off | Set to `1` and install `mlflow` to record a trace span per call. |

### Bounded endpoint calls

`fm_client.build_client` creates a dedicated `WorkspaceClient` whose
`Config(http_timeout_seconds = limit − 2, retry_timeout_seconds = limit)`
fits inside one attempt. The SDK therefore cannot retry for minutes behind the
app's back, and a worker thread outlives its attempt by at most one HTTP
timeout. Calls run on a 4-thread executor (`genai-fm-*`), never the event
loop's shared default executor that DBSQL, Genie and Lakebase helpers use.

An attempt that times out is still written to the audit as an `error` row. If
the endpoint finished the call afterwards and billed it, the audit shows the
attempt but not its token count, and the AI Gateway usage table is the
reference for cost.

The endpoint's AI Gateway configuration (rate limits, usage tracking,
guardrails) applies on top of all this. No `temperature` is sent, because
Claude 5.x endpoints reject it.

## API

| Method | Path | Auth | Returns |
| --- | --- | --- | --- |
| POST | `/api/alerts/{alert_id}/action-plan` | admin | See the result table below. |
| GET | `/api/alerts/{alert_id}/action-plan` | signed-in user | Prior plans for the alert, newest first. |
| GET | `/api/genai/audit/{plan_id}` | signed-in user | The full trace: the summary plus `grounding_rows` and the raw `response`. |
| POST | `/api/alerts/{alert_id}/responses` | admin | The existing signed-decision route, with optional `action_plan_id` and `apply_override_reason`. See [Applying a plan](#applying-a-plan). |

Results of `POST /api/alerts/{alert_id}/action-plan`:

| Status | When |
| --- | --- |
| `201` | A new plan was generated. The body is the plan summary. |
| `200` with `reused: true` | The latest plan's grounding is unchanged, so it is returned instead. |
| `429` with `Retry-After` | The request was refused by the concurrency cap, the per-alert in-flight check, or the per-user cooldown. |
| `504` | The endpoint timed out. The body carries the logged attempt. |
| `502` | Any other endpoint error. The body carries the logged attempt. |
| `503` | Lakebase is bound but cannot store the audit. |
| `422` | A non-UUID id, unknown body fields, a bad `mode`, or a non-boolean `force`. |
| `404` | The alert does not exist. |

`GET /api/genai/status` (admin only, like the other GenAI admin surfaces) returns:

* `pseudonym_key.source`: `secret`, `lakebase` or `redacted`;
* `append_only_enforced`: `true`, `false`, or `null` without Lakebase;
* the call limits in force.

The POST body is optional: `{"mode": "realtime" | "microbatch" | "zerobus", "force": false}`.

* `mode` picks the telemetry table for the readings window. Without it, the
  plan is grounded without readings, and that gap is recorded.
* `force: true` regenerates even when the grounding is unchanged. The card's
  **Regenerate** button sends it.
* **Reuse key.** A plan is reused only when all of these match: the grounding
  hash, `PROMPT_TEMPLATE_VERSION` (now `v4`), the endpoint, and the grounding
  checker version (`plan_schema.CHECKER_VERSION`, recorded in each answer).
  After any prompt or checker change the next request therefore regenerates.

### Applying a plan

A person signs a decision with `action_plan_id`. The server enforces two
rules, whatever the UI does:

* **Only grounded plans apply by default.** A plan applies only when its
  `status` is `generated` and its `grounding_check` is `pass`. For any other
  plan the route returns `422`, unless the request includes
  `apply_override_reason` (at most 280 characters). That reason is stored in
  the audit row as `applied_override_reason`, next to `applied_by`. The UI
  never sends an override; it disables Apply on such plans.
* **The decision and the plan link commit together.** With Lakebase, the
  signed `alert_response` and the `applied_*` update run in one transaction
  (`services/genai/apply.py`). If either fails, both roll back and the route
  returns `503`. The error says plainly that the decision was not recorded.

  The decision store retries on un-upgraded tables, for example with the
  legacy columns, or with the CNC-era action plus the real action stashed.
  Each insert attempt therefore runs in its own savepoint
  (`services/db/savepoint.py`), so a failed attempt does not abort the
  enclosing transaction. The `applied_*` update works the same way.
* **Audit table without the override column.** If the table was created
  before `applied_override_reason` existed and its owner has not upgraded it:
  * an apply without an override still works;
  * an apply with an override returns `503`, with a message saying the owner
    must run `ensure_app_schema`.

## Grounding and citations

Every grounding item has the form `{id, source, ref, data}`. Its `id` is the
citation the model must quote, and `ref` is the row's own key within its source
(a UUID, rule id, lot id and so on). The field was called `key` until
2026-11-20. It was renamed because stock secret scanners (gitleaks
`generic-api-key`) read `"key": "<uuid>"` in stored audit JSON as an API key.
Audit rows written before the rename still carry `key`, and their
`grounding_hash` was computed over that name. Because the field name is part of
the hashed rows, the first request after upgrading regenerates a plan instead of
reusing an older one.

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

### People and free text (PII)

* **The model never sees logins.** `acknowledged_by` and `recorded_by` become
  stable pseudonyms (`person-<16 hex>`, HMAC-SHA256). Email addresses inside
  free text, such as reasons, metadata and dict keys, are replaced with
  `[email]`.
* **The pseudonym key never falls back to a constant.** It is resolved at boot,
  in this order:
  1. **`secret`:** `GENAI_PSEUDONYM_SALT`, injected from the
     `genai-pseudonym-salt` app resource. This is a secret with `READ` on scope
     `${var.secret_scope}` (default `pharma-twin-secrets`), key
     `${var.genai_pseudonym_salt_key}`.
  2. **`lakebase`:** a random 32-byte key generated once and stored in the
     Lakebase `genai_settings` table. The app logs a warning at boot. This
     applies only to local and non-bundle runs. A bundle deploy always declares
     the secret resource and fails if the secret is missing (see
     [deployment.md](deployment.md#fresh-app-prerequisites)).
  3. **`redacted`:** no key at all. Every person becomes `person`.

  `GET /api/genai/status` shows which source is in use. Create the secret
  before deploying the bundle, and before redeploying an app that predates it:

  ```bash
  databricks secrets create-scope pharma-twin-secrets --profile <PROFILE>
  databricks secrets put-secret pharma-twin-secrets genai-pseudonym-salt \
    --string-value "$(openssl rand -hex 32)" --profile <PROFILE>
  ```

  Rotating the key changes every pseudonym. Earlier audit rows keep their old
  pseudonyms; Lakebase still holds the real names.
* **The UC mirror** also pseudonymises `requested_by` and `applied_by`.
* **The Lakebase audit table** keeps the real `requested_by` and `applied_by`.
  It is the system of record for Part 11 traceability, and access to it is the
  app's Lakebase grants (the app service principal and admins).

### The grounding check

The model must return one JSON object that matches `plan_schema.ActionPlan`:
`summary`, `disposition` (quarantine lots/serials, CMM scope, NCR yes/no,
maintenance request), `risk_narrative`, and `steps`. Each step has an
`owner_role` of `Operator`, `QA`, `Maintenance` or `Manufacturing Eng`, and an
optional `response_action`. Every claim has at least one citation.

If the answer is not valid JSON or does not match the schema, the plan is
`rejected`. If it does match, `check_grounding` sets `grounding_check` to
`pass` or `fail` and lists the reasons. The check fails a plan when:

* **A citation is fabricated:** the claim cites an id that is not in the
  grounding set.
* **A number is unsupported by the claim's own citations.** Numbers are
  checked per claim, against only the rows that claim cites. Unit-suffixed
  forms (`0.05mm`, `5min`, `64.2°C`, `87%`) count as numbers. The allowed
  values are:
  * typed numbers in the cited rows, rounded to the precision written;
  * differences within a row, or across the cited alert, rule, risk and RFT
    rows (`value - threshold`);
  * list counts.

  Percentages are stricter. An `N%` or `N percent` claim must match a cited
  0–1 value ×100 (`0.87` → `87%`) or a cited `pct` / `percent` field (the
  projected RFT). Counts and other raw numbers never support a percentage.

  Further allowed values:
  * numbers in the text of structured rows the claim cites: the catalog rule
    (`60 °C`), and readings rows including bucket labels such as `T+30s`;
  * the window length of a cited risk-model feature, in that unit only. Feature
    names come from untrusted alert metadata, so only a strict identifier that
    ends in a window token counts (`^[a-z][a-z0-9_]*_(\d+)(s|m|h)$`):
    `spindle_temp_slope_5m` supports "5-minute", "5 min" and "5m", but not
    "5 s", a bare "5", "4-minute" or "500%". Window lengths never feed
    differences or percentages, and any other digits in a feature name are
    ignored. A planted name such as `release_all_99_lots_score_0.01` grounds
    nothing. When the risk model's feature contract lands, its definitions
    should replace this parsing as the trusted source;
  * named spans in the cited alert row: onset → opened (time to detect),
    opened → acknowledged, opened → cleared, and onset → cleared. Also the span
    from the first to the last cited readings bucket. These are allowed in
    seconds with a ±1 s tolerance, and in minutes and hours. Spans between
    other timestamps, such as acknowledged → cleared or maintenance dates, do
    not count.

  What still does not count:
  * Numbers inside untrusted free text (notes, reasons, model names) are never
    allowed values.
  * Numbers from the system prompt are never allowed values.
  * Fixed names and references (`Spindle 3`, `CNC-1`, `21 CFR 820`,
    `ISO 13485`, `Part 11`, `rev C`, `5-axis`) are not quantities.
  * Numbers inside identifiers, dates and clock times are not checked.

  Hyphenated units count as numbers too, so `5-minute` is checked.
* **A reference does not match.** "step N" must be a step in the plan.
  "version N" and "vN" must match the `model_version` of a row that the claim
  itself cites.
* **A lot id or serial is not a lot row's.** Every `TM-HS12-…` id must match a
  lot row's `lot_id` or `parts_at_risk`, whether it appears in the disposition
  lists or anywhere in the text.
* **The risk score is misreported.** The risk narrative omits the risk-score
  row when one exists, or states a score when none exists.

A failing plan is still shown, with a warning badge and the reasons. It cannot
be applied without an override (see [Applying a plan](#applying-a-plan)).

The rules are versioned (`CHECKER_VERSION`). To re-check every saved plan
under the current rules, offline, run
`python scripts/genai_recheck_evidence.py --markdown`.

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
| `grounding_hash`, `grounding_rows` | The SHA-256 and the JSONB of the exact rows given to the model (already cleaned and pseudonymised), with their citation ids |
| `response` | The raw answer (capped at 20 000 characters), the parsed plan, schema errors, grounding gaps and the attempt count |
| `grounding_check`, `grounding_reasons`, `status` | `pass` or `fail` with reasons; `generated`, `rejected` or `error` |
| `latency_ms`, `prompt_tokens`, `completion_tokens`, `total_tokens` | Cost and latency |
| `applied_by`, `applied_at`, `applied_response_id`, `applied_override_reason` | Set once, when a person signs a decision citing the plan. `applied_response_id` references `alert_response(id) ON DELETE RESTRICT`. |

**The table is append-only, enforced in the database.** A trigger
(`genai_action_plan_audit_append_only`) refuses `DELETE` and `TRUNCATE`. It
allows an `UPDATE` only while `applied_at` is NULL, and only to set the
`applied_*` columns. Every attempt is a row, including timeouts and malformed
answers.

The trigger protects against the app and other non-owner roles. **The table
owner can still drop or disable it**, for example with
`ALTER TABLE … DISABLE TRIGGER`, and a superuser can bypass it. Restrict
ownership accordingly. To detect a missing or disabled trigger:

* `GET /api/genai/status` (admin) reports `append_only_enforced` (it reads
  `pg_trigger`);
* the app logs a boot warning when the trigger is missing or disabled.

There are two ownership pitfalls:

* **Table, index and late column.** `CREATE TABLE IF NOT EXISTS` is a no-op for
  a non-owner. The index step tolerates the "must be owner" error that Postgres
  raises before its IF-NOT-EXISTS skip. The late column and the trigger are
  created only when missing. A non-owner boot logs a warning instead of
  failing.
* **Coverage.** A local-Postgres test boots as a non-owner role. Further
  local-Postgres tests cover the trigger and the transaction rollback.

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
never deletes the table.

* **Contents.** The mirror stores `citation_ids` and `grounding_rows` as JSON
  strings, plus `risk_score`, `risk_model` and `risk_model_version`. People are
  pseudonymised. Genie can answer "which rows did the AI use for alert X?" and
  "explain why Spindle 3 was flagged and what the AI recommended".
* **Escaping.** Values are written as SQL literals with backslashes and quotes
  backslash-escaped (`intelligence_catalog_service._sql_str`), because
  summaries and model names are model- or metadata-controlled.
* **Access.** Access to the mirror is the UC grants on the schema. Treat it
  like the other `intelligence_*` tables.

### Genie upgrade

`intelligence_genai_audit` is now part of every Genie space's expected table
set (`reference_tables()`).

* **Creating the table.** At boot, the alert-snapshot and AI-audit sync loops
  create it with the other `intelligence_*` tables, but only when audit sync is
  configured: `UC_CATALOG`, `UC_SCHEMA`, `DATABRICKS_SQL_WAREHOUSE_ID` and
  `GENIE_SPACE_ID` must all be set. Otherwise the loops stay disabled, and the
  table is created only by the admin intelligence sync
  (`POST /api/admin/intelligence/sync`).
* **New spaces.** Creating a new space requires every `intelligence_*` table,
  including this one, to exist. If it is missing, creation fails with the
  intelligence-sync hint, so run the admin sync first.
* **Existing spaces.** An existing space without the table stays authorized,
  because the table is listed in `genie_service.OPTIONAL_SPACE_TABLES`. The
  Genie status reports it in `missing_optional_tables`, and the app logs a
  warning. That space simply cannot answer the AI-audit questions yet.

To upgrade an existing space:

1. Make sure the UC table exists. Either boot the app with audit sync
   configured, or run the admin intelligence sync.
2. Re-provision each pipeline's space with
   `POST /api/admin/pipelines/{setup_id}/genie/{mode}`, or redeploy the
   pipeline; its deploy hook updates the space in place. Either way, the new
   instructions, starters and table are applied.
3. Check that `missing_optional_tables` is empty in the Genie status.

## Prompt-injection defences

1. **Trusted/untrusted split.** This is the same split Genie uses
   (`genie_service.render_trusted_prompt`). The system message holds the rules
   and the schema. `[TRUSTED_CONTEXT]` holds only app-controlled values: the
   allowed citation ids, owner roles, response actions and gaps. Every row goes
   in `[UNTRUSTED_GROUNDING]` and is labelled as data, not instructions.
2. **Normalisation, then defanging.** Every untrusted string is cleaned in
   `redact.clean_text`, and that includes dict keys:
   1. NFKC-normalised, so full-width `［` becomes `[`;
   2. stripped of zero-width and bidi control characters;
   3. any `[TRUSTED_CONTEXT]`, `[UNTRUSTED_GROUNDING]` or
      `[UNTRUSTED_OBSERVATIONS]` tag is rewritten;
   4. email addresses are redacted.

   The rendered block then gets one more tag-only pass. That pass does not
   re-normalise, so clean values such as `µm` or `10⁻³` reach the model
   unchanged. Look-alike tags cannot close the block.
3. **Bounded data.** Free text is clipped to 500 characters. Readings are capped
   at 120 buckets, events at 50, and top features at 10.
4. **Output is checked against the input.** Fabricated citations, numbers or
   serials fail the grounding check, whatever the model was told. Failing plans
   cannot be applied without a recorded override.
5. **No capability to act.** The endpoint call has no tools. The service never
   writes `alert` or `alert_response`. Applying a plan needs a person's
   Part 11-style signature in the existing form, and the signature intent box is
   never pre-ticked.

The live evidence in `docs/evidence/genai/` includes an injection run. Text
planted in alert metadata tried to close the block, release every lot, cite a
fake id and lower the risk score. The model ignored it, and the plan passed
the check with the real 87% score.

## Limitations and next steps

* Spelled-out numbers ("three parts") are not verified. A number can still pass
  by matching an unrelated value in a row the claim cites.
* The check verifies that a cited row exists and holds the numbers. It does not
  verify that the row supports the sentence. An LLM-judge scorer for that is a
  natural next step, for example an `mlflow.genai.evaluate` run with a
  groundedness scorer over a small set of recorded episodes.
* The admission limits are per process. With several app replicas, the limits
  multiply; the AI Gateway rate limit is the global cap.
* The readings window needs the pipeline `mode` and a SQL warehouse. Without
  them the plan has no readings, and that gap is recorded in the audit row.
* An answer that hits the 6 000-token limit is rejected as malformed. Very long
  open episodes can name many lots, which makes the prompt and answer larger.
* MLflow tracing is optional and off by default, because `mlflow` is not an app
  dependency. Writing traces to a UC table is the next step.
* The UC mirror is eventually consistent (one sync interval) and holds only the
  most recent 200 plans. Lakebase is the system of record.
