# Genie

The app uses Genie in two ways:

| Surface | Space | Routes | Config |
|---|---|---|---|
| Shared copilot (single space) | one space bound to the app, `GENIE_SPACE_ID` | `/api/genie/status`, `/api/genie/conversations…` | `GENIE_SPACE_ID` (app resource `genie-space`, `CAN_VIEW`) |
| Per-pipeline spaces | one space per `(setup_id, mode)`, created by the app after a deploy | `/api/genie/pipelines/{setup_id}/{mode}/…` | none required; optional `GENIE_SPACE_PARENT_PATH` |

Both go through `services/genie_service.py`: the trusted/untrusted prompt wrapper,
the table allowlist (`inspect_space_tables`, `space_table_allowlist_status`,
`require_table_allowlist`, `cached_allowlist_status`), the 20 s SDK timeout and the
shared semaphore. Alerts "explain" and the intelligence catalog keep using the
single shared space; nothing about them changed.

## Requirements

**Shared copilot.** The app resource `genie-space` (CAN_VIEW) injects
`GENIE_SPACE_ID`. The space must contain exactly the tables the copilot expects
(or `GENIE_ALLOWLIST_CONFIRMED` is set by an operator who verified them).

**Per-pipeline spaces.** No extra app resource or env var is required. The app
service principal creates the spaces itself, so it needs:

- permission to create Genie spaces in the workspace (the SP is the owner of every
  space it creates; users reach them through the app, not directly);
- `CAN_USE` on the SQL warehouse (the setup's `warehouse_id`, else
  `DATABRICKS_SQL_WAREHOUSE_ID` / the app-global warehouse setting);
- `SELECT` on the telemetry, metrics and `intelligence_*` tables in
  `{UC_CATALOG}.{UC_SCHEMA}` (already granted by industry provisioning).

Optional: `GENIE_SPACE_PARENT_PATH` sets the workspace folder for new spaces
(default: the service principal's home folder). It is not in `app.yaml` or the
bundle because it is optional; add it to the app `env` if you want a shared folder.

The `intelligence_*` tables must exist before a space can be created; if they do not,
provisioning is skipped with a warning that names the sync endpoint
(`POST /api/admin/intelligence/sync`).

### Service principal grants

A fresh bundle-created app has no grants, so provisioning is skipped with warnings
and health shows `db_available: false` (`permission denied for schema public`) until
they are made. Grant the app SP (`service_principal_client_id` from
`databricks apps get <app-name> -o json`):

```sql
-- Unity Catalog: read the telemetry/metrics/intelligence tables the spaces expose,
-- and create/maintain the intelligence_* tables.
GRANT USE CATALOG ON CATALOG `<uc_catalog>` TO `<sp-client-id>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA `<uc_catalog>`.`<uc_schema>` TO `<sp-client-id>`;
```
```sql
-- Lakebase (databricks_postgres), run as an owner/admin of the project:
GRANT USAGE, CREATE ON SCHEMA public TO "<sp-client-id>";
```

The app connects to Lakebase as the role named after the SP client id with an OAuth
credential, so no password grant is involved. Creating Genie spaces is a workspace
permission rather than SQL; the SP is the owner of every space it creates. Full
checklist: [deployment.md](deployment.md#fresh-app-prerequisites).

### Table allowlist and `GENIE_ALLOWLIST_CONFIRMED`

`GET /api/genie/status` reports `table_allowlist.status`:

| `status` | Meaning |
|---|---|
| `matched` | the shared space's data sources equal the expected `intelligence_*` set; the copilot is enabled |
| `mismatch` | the space's tables differ; `enabled: false` with the reason |
| `manual_required` | the space could not be inspected (for example the SP lacks access to the space, or `GENIE_SPACE_ID` is unset or not readable) |

For the **shared space**, `manual_required` leaves `enabled: false` and
`authorization_enforced: false`. An operator who has verified that the space contains
exactly the `expected_tables` can set `GENIE_ALLOWLIST_CONFIRMED=true` (`1`/`yes` also
work) as an app env entry; the status stays `manual_required` but with
`manual_confirmed: true`, and the copilot is enabled. It is not in the bundle (an
unset variable would render as an empty env value); add an env entry with a real value
to `resources/app.yml` to use it.

For **per-pipeline spaces** the override is never honoured: an uninspectable space
fails closed, and a mismatch is fixed by redeploying the pipeline (or the admin
"Enable Genie" call), which rewrites the space.

## Industries and modes

Per-pipeline spaces exist for **every industry the app offers** (the curated
templates: manufacturing, pharmaceutical, energy, automotive) and every mode
(`realtime`, `microbatch`, `zerobus`). Wording, sample sensors and units are derived
from the industry template and domain catalog (`services/genie/genie_industry_profile.py`).
Manufacturing keeps its OEE wording; other industries use an "operating health index"
(availability x performance x quality) with their own sensors and units. An industry
the app does not offer reports `supported: false` on `/status` and 404 on the other
routes; it never fails a deploy.

Tables per mode (only tables that exist for the mode are included):

| Mode | Telemetry | Metrics (`{industry}_metrics`) |
|---|---|---|
| realtime | CDF shadow history table | yes |
| microbatch | `{industry}_microbatch` | yes |
| zerobus | `{industry}_zerobus` | no (zerobus emits no streaming metrics) |

plus the five `intelligence_*` reference tables in every case.

## Lifecycle

1. **Deploy**: after a successful `deploy_pipeline` the hook creates or updates the
   space. It is best-effort and capped at 60 s: any failure or timeout is returned as
   `warnings` on the deploy result and never fails the deploy.
2. **Backfill**: `POST /api/admin/pipelines/{setup_id}/genie/{mode}` provisions a space
   for an already-deployed pipeline of any industry.
3. **Redeploy**: updates the space in place (recreates it if it was trashed).
4. **Remove a mode / delete a pipeline or industry**: the `genie_space` ledger deleter
   trashes the space (`trash_space`); an already-absent space counts as deleted.

## Known limits

- Conversation ownership is process-local; after an app restart every conversation
  returns 404.
- The allowlist is strict: a space whose tables differ from the expected set is
  blocked (503), including spaces edited by hand.
- Realtime telemetry lags the Lakehouse Sync mirror, so a new realtime space may
  answer from an empty table for a short time.
- The shared copilot space and per-pipeline spaces are independent; editing one never
  changes the other.

## Behaviour changes for the shared space

Porting the per-pipeline code also touched shared-copilot code paths:

- `genie_service._pack_for` now resolves the warehouse with `resolve_warehouse_id` (app-global or user
  warehouse fallback; previously only the env var), so alert "explain" can include a telemetry summary,
  capped at 5 s.
- `inspect_space_tables` now parses `serialized_space`, so a shared space can reach the `matched`
  allowlist state.
- The deploy hook can add up to 60 s to the deploy request. It is best-effort and never fails the deploy.
- There is no per-`(setup, mode)` lock: a deploy racing a backfill can create two spaces. The effect is
  bounded: the newest ledger row wins and teardown trashes every active row.
- Zerobus spaces created by the earlier manufacturing-only version lose the metrics table on the next
  redeploy or backfill, and report unavailable (allowlist mismatch) until then.

# Per-pipeline Genie API

Each pipeline `(setup_id, mode)` gets its own Genie space,
created automatically after a successful deploy
(`POST /api/admin/pipelines/{setup_id}/deploy/{mode}`) and updated in place on
redeploy. The static copilot
(`/api/genie/status`, `/api/genie/conversations…`, backed by `GENIE_SPACE_ID`) is separate.

`mode` is one of `realtime` | `microbatch` | `zerobus`.

**`{setup_id}` path segment:** accepts either the `pipeline_setup.id` or the
mode's `pipeline_deployment.id`. A deployment id resolves to its setup and must
belong to `{mode}`. A value that is not a UUID, an unknown UUID, or a
deployment id for a different mode returns **404** without querying the database
for invalid ids. The dashboard gets both ids from
`GET /api/industries/catalog` → `industries[].modes[]`, as
`deployment_id` and **`setup_id`** (both `null` until the mode is deployed).

## Space contents

| Data source | FQN |
|---|---|
| Mode telemetry, **realtime** | the CDF shadow history table, `cdf_reconciliation_service.delta_table_fqn(industry)` (e.g. `lb__shadow_<industry>_sensor_readings_history`) |
| Mode telemetry, microbatch / zerobus | `{UC_CATALOG}.{UC_SCHEMA}.{industry}_{mode}` |
| Streaming metrics (realtime, microbatch only) | `{UC_CATALOG}.{UC_SCHEMA}.{industry}_metrics` |
| Intelligence reference | `{UC_CATALOG}.{UC_SCHEMA}.intelligence_{assets,sensor_bindings,maintenance_records,alert_rules,alert_snapshot}` |

**Why realtime is different:** the RTM stream writes to Lakebase, and Lakehouse Sync mirrors it into the
CDF shadow history table, so `{industry}_realtime` stays empty. Read-only check on a test
workspace, 2026-11-18:

- `manufacturing_realtime` had 0 rows. The shadow table is live and insert-only.
- The shadow table has the same sensor columns: `sensor_name, industry, value, unit, sensor_type,
  event_time, min_value, max_value, nominal_value, fault_active, metadata`.
- It also has `id`, `source` and the CDC columns `_pg_change_type, _pg_lsn, _pg_xid, _timestamp, _sort_by`.

So the OEE, downtime and anomaly definitions are unchanged. For realtime, the instructions, example SQL,
SQL snippets (measures and filters) and the prompt's recent-telemetry summary all apply the dashboard's
row filter from `routes/sensor_data.py`: `_pg_change_type IN ('insert', 'update_postimage') AND
industry = '<industry>'`. The filter comes from `genie_space_content.telemetry_row_filter`, because
the shadow table can hold several industries. The CDC bookkeeping columns are excluded from Genie
context. `telemetry_table(industry, mode)` in `services/genie_space_service.py` is the single mapping;
the allowlist and the telemetry summary use the same function.

If the deploy hook's or the admin endpoint's 60 s timeout fires while a space is being created, creation
and the ledger write keep running in the background (`asyncio.shield`). The space therefore still gets
its ledger row and can be torn down later. The background task is tracked in a module-level set until it finishes.
A done-callback logs the outcome (space id, error or cancellation) and drops the task, so failures after
a timeout are logged rather than lost. For realtime, each filter snippet is fully parenthesized:
`(<row filter> AND <condition>)`.

Microbatch and zerobus write `{industry}_{mode}` directly (`services/pipeline_freshness.py`). Those
tables exist but were empty in that workspace, because only realtime was running there. Zerobus emits no streaming metrics, so
its space does not include the metrics table.

The space also carries these instructions (manufacturing wording shown; other industries say "operating health index" instead of OEE):

- **Downtime**: intervals where `fault_active` is true, counted in distinct minutes.
- **Availability**: 1 − fault minutes / observed minutes.
- **Performance**: `value / nominal_value`, capped at 1.
- **Quality**: the share of readings inside `[min_value, max_value]`.
- **OEE**: the product of the three above. It is an approximation.
- **Anomaly**: a value outside `[min_value, max_value]`.

It also has example SQL, SQL snippets (measures, filters, expressions) and
sample questions that match `/starters`. No new tables are created. Builder:
`services/genie_space_service.py` and `services/genie_space_content.py`.

**Allowlist:** status, conversation, poll and result calls check the space with
`get_space(include_serialized_space=True)`.

- It parses `data_sources.tables` and `data_sources.metric_views`, and requires an exact match with the
  expected set above.
- Any other non-empty `data_sources` kind counts as a mismatch.
- A mismatch blocks the call with 503.
- If the space cannot be inspected, the call fails closed. `GENIE_ALLOWLIST_CONFIRMED` is **not** honored
  for per-pipeline spaces; it applies only to the static copilot.
- Verified results are cached per space id for 60 s. The cache is cleared whenever
  `ensure_pipeline_space` touches that space. A mismatch is never cached.

**Persistence (design choice):** the space id is stored in the `provisioned_resource` ledger as
`resource_type='genie_space'`, with `metadata.mode` set and `teardown_order=45`. There is no
`pipeline_deployment` column and no migration. The lookup is
`genie_space_service.find_space_id(db, setup_id, mode)`, which returns the newest `active` row for the mode.
Teardown trashes the space through the `genie_space` deleter, which calls `trash_space`. A space that is
already gone counts as deleted.

Removing a single mode (`PATCH /api/admin/pipelines/{setup_id}/modes`) also trashes that mode's space
before `delete_deployment` runs. The ledger row has `deployment_id … ON DELETE CASCADE`, so trashing it
first is what keeps the workspace space from being orphaned. If the trash fails, the route returns 502 and
keeps the deployment so the admin can retry.

Before **creating** a space, deploy checks that the `intelligence_*` tables exist (`tables.exists`).
If any are missing, the deploy warning reads `intelligence tables missing: …; run intelligence sync
(POST /api/admin/intelligence/sync) then redeploy`. If the check itself errors, creation goes ahead.

**Config:**

- `GENIE_SPACE_PARENT_PATH` (optional) sets the workspace folder for new spaces. When it is unset, the
  space is created in the app service principal's home folder. That is fine, because users reach the space
  through the app.
- The warehouse is the setup's `warehouse_id`. If it is not set, the app uses `resolve_warehouse_id`
  (`DATABRICKS_SQL_WAREHOUSE_ID` env, then the app-global settings).
- The app service principal needs permission to create Genie spaces, `CAN_USE` on the warehouse, and
  `SELECT` on the tables.

**Deploy result:** on success it adds
`"genie_space": {"space_id": str, "action": "created"|"updated"|"recreated", "tables": [str]}`. On failure it
adds `"warnings": ["Genie space was not provisioned: …"]`, and the deploy itself still succeeds.
Provisioning is capped at 60 s; on timeout the warning is
`"Genie space provisioning timed out after 60s; redeploy to retry"`.

## Common behaviour

- Auth: `require_identity` (forwarded Databricks Apps identity; 401 when missing in the Apps runtime).
- Conversation ownership is process-local (`AppState.genie_conversations`). A conversation is bound to
  `(owner, setup_id, mode, space_id)`. Any other user, pipeline or mode gets **404**. After an app restart
  every conversation returns 404, so the client should show "ask again".
- The recent-telemetry context added to the prompt is capped at 5 s. On timeout it is left out and the
  question is still sent.
- Questions go to Genie inside the trusted/untrusted prompt wrapper. Request bodies accept only
  `{"question": string}` (1–4000 chars). Any other field returns 422.
- SDK calls run off the event loop under the shared semaphore with a 20 s timeout. A timeout returns 504.

| Status | Meaning |
|---|---|
| 401 | identity required |
| 404 | non-UUID or unknown setup/deployment id / deployment id of another mode / setup of an industry the app does not offer (conversation routes and starters) / unknown or foreign conversation / Genie resource not found |
| 422 | unknown mode, or invalid body |
| 503 | app DB unavailable, no space provisioned yet, or table allowlist not verified |
| 504 | Genie SDK timeout |
| 500 | other Genie failure (sanitized message) |

Error bodies follow FastAPI's format: `{"detail": string}`.

## Endpoints

Base: `/api/genie/pipelines/{setup_id}/{mode}`

### `GET …/status`

```json
{"available": true, "space_id": "01ef…", "industry": "pharmaceutical", "supported": true}
```
```json
{"available": false, "space_id": null, "industry": "pharmaceutical", "supported": true, "reason": "No Genie space is provisioned for this pipeline yet. Redeploy the pipeline to create one."}
```

`reason` is set only when `available` is false and is safe to show to operators. `space_id` is non-null
when a space exists but cannot be used (for example, an allowlist mismatch). Industries the app does not offer
return `available: false` with `supported: false`. An unknown setup returns 404.

### `GET …/starters`

```json
{"questions": ["What is the approximate OEE per sensor over the last 24 hours?", "…"]}
```

The list depends on the industry and mode: five shared questions (health index or OEE, downtime, anomalies,
faults, maintenance), two per-sensor questions for industries other than manufacturing (average reading per
hour of their first two sample sensors), and two for the mode. That is 7 questions for manufacturing and 9
for pharmaceutical, energy and automotive, in every mode.

### `POST …/conversations`

Request: `{"question": "How much downtime did we have today?"}`

Response:
```json
{"conversation_id": "01ef…", "message_id": "01ef…"}
```

### `POST …/conversations/{conversation_id}/messages`

Request: `{"question": "And for yesterday?"}`

Response:
```json
{"conversation_id": "01ef…", "message_id": "01ef…"}
```

### `GET …/conversations/{conversation_id}/messages/{message_id}`

Poll until `status` is terminal (`COMPLETED`, `FAILED` or `CANCELLED`). Other values include
`SUBMITTED`, `FETCHING_METADATA`, `ASKING_AI`, `EXECUTING_QUERY` and so on.

```json
{
  "conversation_id": "01ef…",
  "message_id": "01ef…",
  "status": "COMPLETED",
  "content": "How much downtime did we have today?",
  "error": null,
  "attachments": [
    {
      "attachment_id": "01ef…",
      "text": null,
      "query": {"sql": "SELECT sensor_name, …", "description": "Downtime minutes per sensor"},
      "suggested_questions": []
    },
    {
      "attachment_id": "01ef…",
      "text": "Spindle-3 had the most downtime (42 min).",
      "query": null,
      "suggested_questions": ["What was the OEE for spindle-3?"]
    }
  ]
}
```

`content` is only the user's original question. Genie receives the question inside the
trusted/untrusted prompt wrapper (catalog, allowed tables, observations), but the API never returns that
wrapper. `genie_service.extract_user_question` strips it deterministically from Genie's stored message,
so this keeps working after an app restart. If the wrapper can't be parsed, `content` is `null`, never
the wrapper. The static `/api/genie/conversations/…` poll route behaves the same way.

Every attachment always has all four keys. `text`, `query` and `query.description` can be null, and
`suggested_questions` is always an array. `error` is a string or null.

### `GET …/conversations/{cid}/messages/{mid}/attachments/{attachment_id}/result`

The result of a query attachment, from `get_message_attachment_query_result`. It is capped at **500 rows**.

```json
{
  "columns": [{"name": "sensor_name", "type": "STRING"}, {"name": "downtime_minutes", "type": "LONG"}],
  "rows": [["spindle-3", "42"], ["press-1", "7"]],
  "truncated": false
}
```

`rows` holds string cells, or null, in column order, exactly as Genie returns them. `truncated` is true
when more rows exist than were returned. If there is no result yet, the response is
`{"columns": [], "rows": [], "truncated": false}`.

## Admin: provision a space for an already-deployed pipeline ("Enable Genie")

Pipelines deployed before this feature existed can get their space **without a
redeploy or a stream restart**. This touches only the Genie space and its ledger row,
never the job, stream, Kafka topic, notebooks or Lakebase resources.

```
POST /api/admin/pipelines/{setup_id}/genie/{mode}
```

- `{setup_id}` accepts either `pipeline_setup.id` or the mode's `pipeline_deployment.id`. It must be a UUID.
- Admin-gated: uses the same `app_permissions.is_admin` check as `POST …/deploy/{mode}`.
- Idempotent: the first call creates the space and writes the ledger row, and later calls update it in
  place. It runs the intelligence-table precheck and has a 60 s timeout.

Response `200`:
```json
{"space_id": "01ef…", "created": true, "warnings": []}
```
`created` is `false` when an existing space was updated. `warnings` is non-empty only when the recorded
space had disappeared and a new one was created.

| Status | Meaning |
|---|---|
| 403 | not an admin |
| 404 | non-UUID/unknown id, deployment id of another mode, or no deployment for `{mode}` |
| 409 | deployment not in a deployed state (needs a `job_id`; status not pending/deploying/failed), or a precondition failed (for example, missing intelligence tables: run `POST /api/admin/intelligence/sync`, then call again) |
| 422 | unknown mode, or the setup's industry is not one the app offers |
| 503 | app DB unavailable |
| 504 | provisioning timed out (60 s) |
| 502 | Genie SDK failure (sanitized) |

Example (from a machine authenticated to the app as an admin):
```bash
curl -X POST "https://<app-url>/api/admin/pipelines/<setup_id-or-deployment_id>/genie/realtime" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"
```
