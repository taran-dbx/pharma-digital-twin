# Deploying with Declarative Automation Bundles (DABs)

> **WARNING: `prod` resolves to the live app name (`iot-digital-twin`).** Always
> `databricks bundle deployment bind` first, review `databricks bundle plan -t prod`,
> and never deploy without prod overrides
> (`.databricks/bundle/prod/variable-overrides.json`). Workspace variables have no
> defaults, so validation fails until they are supplied; do not work around that
> with placeholder values.

The app is deployed with the bundle in `databricks.yml` + `resources/app.yml`.
It replaces manual `databricks sync`, `.databricksignore` and static
`PGPASSWORD` logins.

## Layout

| File | Purpose |
| --- | --- |
| `databricks.yml` | Bundle name, variables, `sync` rules, `dev` / `prod` targets |
| `resources/app.yml` | The App: command, env, and bound resources (SQL warehouse, Genie space, Lakebase Postgres) |
| `app.yaml` | Placeholder-only file kept for non-bundle deploys (rendered by `scripts/render_app_yaml.py`); when the bundle sets `config`, the bundle wins |

## Commands

Never commit a workspace host. Pass the CLI profile with `--profile` (or
`DATABRICKS_CONFIG_PROFILE`).

```bash
make bundle-validate PROFILE=<profile>              # validate --strict, target dev
databricks bundle validate --strict -t dev --output json --profile <profile>   # inspect resolved app name
databricks bundle deploy -t dev --profile <profile>
databricks bundle run iot_twin -t dev --profile <profile>   # (re)deploys source + starts the app
databricks apps logs <app-name> --profile <profile>
databricks bundle destroy -t dev --profile <profile>        # dev only
```

Validation needs workspace auth, so it is **not** run in CI; run it locally
(`make bundle-validate`) before changing bundle files.

Things worth knowing about the workflow:

* `bundle deploy` uploads the files and applies the app's resources and config, but
  it does **not** start the app or deploy the source. Run `bundle run iot_twin` for
  that; it normally takes about 60 s to reach `RUNNING`.
* Deploys are idempotent. A repeated `bundle deploy` prints `Resources: 1 changed`,
  yet the deployment id and app state are unchanged.
* `databricks bundle summary` can show the app URL as `(not deployed)`. Use
  `databricks apps get <app-name> --profile <profile>` for the real URL.
* `validate` does not catch an env entry that resolves to an empty string; that
  only fails at `bundle run` ("Must specify environment variable source").
  `tests/test_bundle_config.py` guards this offline.

## Variables

Workspace-specific variables have **no default**; a missing value fails
validation. Copy `deploy/variable-overrides.json.example` (placeholders only) to the
gitignored `.databricks/bundle/<target>/variable-overrides.json` and fill in real
values (or use `--var name=value` / `BUNDLE_VAR_<name>`). `make bundle-validate`
stops with a clear message when the file for `BUNDLE_TARGET` is missing.
Variables with defaults are not workspace-specific: `app_name` (dev default
`iot-twin-dev`, fixed to `iot-digital-twin` in `prod`), `uc_schema`,
`lakebase_branch`, `lakebase_database`, `industries`, `admin_group` and
`zerobus_secret_key`. `uc_catalog` has **no** default.

### Required variables (no default)

Both `dev` and `prod` fail validation until every variable below has a value, so
supply them all up front instead of discovering them one error at a time. The list is
checked against `databricks.yml` by `tests/test_bundle_config.py`.

<!-- required-variables:start -->
| Variable | Feeds | Example (placeholder) |
| --- | --- | --- |
| `uc_catalog` | `UC_CATALOG` | `example-catalog` |
| `warehouse_id` | `sql-warehouse` resource (CAN_USE) | `0123456789abcdef` |
| `genie_space_id` | `genie-space` resource (CAN_VIEW) | 32-char hex space id |
| `lakebase_project` | `lakebase` resource, `LAKEBASE_PROJECT`, `LAKEBASE_ENDPOINT` | `iot-app-backend-example` |
| `pghost` | `PGHOST` | `<endpoint>.database.<region>.cloud.databricks.com` |
| `pipelines_lakebase_project` | `PIPELINES_LAKEBASE_PROJECT` | `iot-pipelines-example` |
| `msk_bootstrap_servers` | `MSK_BOOTSTRAP_SERVERS` | `b-1....:9198,b-2....:9198` |
| `zerobus_server_endpoint` | `ZEROBUS_SERVER_ENDPOINT` | `<id>.zerobus.<region>.cloud.databricks.com` |
| `zerobus_client_id` | `ZEROBUS_CLIENT_ID` | service principal client id |
| `uc_service_credential` | `UC_SERVICE_CREDENTIAL` | `iot-rtm-sc` |
<!-- required-variables:end -->

### All variables

| Variable | Env var / resource it feeds | Notes |
| --- | --- | --- |
| `app_name` | App name | Workspace-global, max 30 chars. Dev default is the fixed `iot-twin-dev`, so override it per developer (e.g. `iot-twin-dev-<initials>`, check `bundle validate --output json`); `prod` is fixed to `iot-digital-twin` |
| `uc_catalog`, `uc_schema` | `UC_CATALOG`, `UC_SCHEMA` | |
| `warehouse_id` | `sql-warehouse` resource (CAN_USE) → `DATABRICKS_SQL_WAREHOUSE_ID` via `value_from` | |
| `genie_space_id` | `genie-space` resource (CAN_VIEW) → `GENIE_SPACE_ID` via `value_from` | |
| `lakebase_project`, `lakebase_branch`, `lakebase_database` | `lakebase` Postgres resource (CAN_CONNECT_AND_CREATE), `LAKEBASE_PROJECT`, `LAKEBASE_ENDPOINT` | |
| `pghost` | `PGHOST` | Not injected by the Postgres resource; keep as a variable |
| `pipelines_lakebase_project` | `PIPELINES_LAKEBASE_PROJECT` | |
| `msk_bootstrap_servers` | `MSK_BOOTSTRAP_SERVERS` | |
| `zerobus_server_endpoint`, `zerobus_client_id` | `ZEROBUS_SERVER_ENDPOINT`, `ZEROBUS_CLIENT_ID` | |
| `uc_service_credential` | `UC_SERVICE_CREDENTIAL` | |
| `zerobus_secret_key` | `ZEROBUS_SECRET_KEY` | Default `zerobus-client-secret` (the runtime default). `app.yaml` emits it empty, but the Apps API rejects empty env values in a bundle `config` |
| `industries`, `admin_group` | `INDUSTRIES`, `ADMIN_GROUP` | Admin access is group based. The optional `ADMIN_EMAILS` (CSV) is **not** in the bundle; see below |
| `usage_tracking_enabled` | `USAGE_TRACKING_ENABLED` | Default `"true"`; set `"false"` to opt out of dbdemos-tracker usage tracking (see README) |

**Per-pipeline Genie spaces** need no extra bundle variable or app resource:
after each deploy the app creates one Genie space per `(pipeline, mode)` for every
industry, using its own service principal. That principal needs the permission to
create Genie spaces, `CAN_USE` on the `warehouse_id` warehouse (the `sql-warehouse`
resource already grants it) and `SELECT` on the industry and `intelligence_*` tables
in `uc_catalog`.`uc_schema` (granted by industry provisioning). Provisioning is
best-effort: a failure shows up as a warning on the deploy result, never as a failed
deploy. The optional `GENIE_SPACE_PARENT_PATH` (workspace folder for new spaces) is
not in the bundle for the same empty-value reason as below; add an `env` entry with a
real value to use it. See [genie.md](genie.md).

**Not in the bundle:** `ADMIN_EMAILS` (optional extra admin emails, still honoured by
`services/app_permissions.py`; an unset-by-default variable would render as an empty env value
and break `bundle run`), `ZEROBUS_TABLE_NAME`, `ZEROBUS_SECRET_SCOPE`, `UC_STORAGE_BASE`
and `UC_EXTERNAL_LOCATION` are empty in `app.yaml` (unset-equivalent at runtime),
and the Apps API rejects empty env values for bundle `config` (deploy fails with
"Must specify environment variable source"). They are therefore left unset, which
the runtime treats identically. To use one, add an `env` entry with a real value
in `resources/app.yml` and a matching variable with a non-empty value (and, for the
empty-in-`app.yaml` ones, update the drift test's `empty_only` set in
`tests/test_render_app_yaml.py`). `tests/test_bundle_config.py` fails if any env entry
resolves to an empty string.

`LAKETS_SQL_PATH` is no longer set: `lakets.sql` is resolved relative to the
package (`iot_twin.core.paths`). The env var still overrides it.

## What ships

`sync` in `databricks.yml` replaces `.databricksignore`. It ships `src/`,
`app.yaml`, `requirements.txt`, `pyproject.toml`, `lib/*.whl` and
`static/dist/` (the built SPA), and excludes `client/`, `docs/`, `tests/`,
`scripts/`, `deploy/` and tooling dirs.

* **Frontend:** `static/dist` is committed and CI fails if it is stale, so the
  bundle does not build it. Run `make frontend-build` and commit before deploying
  frontend changes.
* **Simulator wheel:** `lib/iot_data_simulator-*.whl` is a committed binary (its
  source is not in this repo) installed via `requirements.txt`; there is no
  artifact build for it.

## Dev vs prod

* `dev` (`mode: development`, default): creates its own app with a distinct
  name and a per-user workspace folder. Safe to `destroy`.
* `prod` (`mode: production`): explicit `root_path`, fixed app name
  `iot-digital-twin`, `CAN_MANAGE` for the deployer and `admin_group`.

## Prod must bind, not create

App names are workspace-global. The live app already exists, so
`databricks bundle deploy -t prod` would fail on the name conflict, or (if it
were deleted first) create a new app with a new service principal that has none
of the existing grants. Bind instead:

```bash
databricks bundle deployment bind iot_twin iot-digital-twin -t prod --profile <profile>
databricks bundle deploy -t prod --profile <profile>
```

After binding, the next deploy applies the bundle's `resources` and `config` to
the existing app, replacing its current resources and env. Review
`databricks bundle plan -t prod` first. `bundle destroy` on a bound `prod` app
deletes it; do not use it there.

## Fresh app prerequisites

A new app gets a new service principal (SP) with **no grants**. Until it is granted
access, the app starts and serves `/api/health`, but health shows
`db_available: false` (`permission denied for schema public`), DBSQL calls fail and
Genie provisioning is skipped with warnings. Replace the placeholders below; the SP
is identified by `service_principal_client_id` from
`databricks apps get <app-name> --profile <profile> -o json`.

**Lakebase.** The app connects to the `databricks_postgres` database as the Postgres
role named after the SP client id (`DATABRICKS_CLIENT_ID`, unless `PGUSER` is set),
using a short-lived OAuth credential for `LAKEBASE_ENDPOINT`
(`core/lakebase.py`). The `postgres` app resource creates the role; the role still
needs the schema privileges. Connect to the project's endpoint as an owner/admin
(for example with `psql`) and run:

```sql
GRANT USAGE, CREATE ON SCHEMA public TO "<sp-client-id>";
-- only if tables were pre-created by someone else:
GRANT ALL ON ALL TABLES IN SCHEMA public TO "<sp-client-id>";
```

Do not pre-create app tables; the app creates them on first start (see
*Troubleshooting*, `must be owner of table`).

**Unity Catalog** (used by DBSQL, industry provisioning, the `intelligence_*` tables
and per-pipeline Genie spaces):

```sql
GRANT USE CATALOG ON CATALOG `<uc_catalog>` TO `<sp-client-id>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA `<uc_catalog>`.`<uc_schema>` TO `<sp-client-id>`;
```

`SELECT` is what Genie needs on the industry and `intelligence_*` tables;
`CREATE TABLE`/`MODIFY` are needed because the app creates and syncs those tables.
Add the external location and service credential grants listed under
*Troubleshooting* if you provision catalogs or storage from the app.

**Genie.** Per-pipeline spaces also need the permission to create Genie spaces and
`CAN_USE` on the warehouse (already granted by the `sql-warehouse` resource). See
[genie.md](genie.md).

Workspace group membership in `admin_group` is for users, not the SP.

## Troubleshooting

> **Two storage variables:** `UC_STORAGE_BASE` sets where per-industry external tables live (`industry_catalog_service`); `UC_STORAGE_ROOT` sets the managed storage root used when creating a catalog (`routes/provisioning.py`). They are independent.

Each item below was checked against the current code.

| Symptom | Cause | Fix |
| --- | --- | --- |
| `/api/health` returns `db_available: false` with a `db_error` | The app's service principal has no Lakebase role or privileges yet (see *Fresh app prerequisites*) | Bind the `postgres` app resource, then grant the SP `USAGE`/`CREATE` on `public`. The app authenticates with SDK OAuth tokens; no static `PGPASSWORD` is needed |
| `InvalidPasswordError ... user '<uuid>'` | `PGUSER` is set to the wrong identity. The runtime `DATABRICKS_CLIENT_ID` is the app's `service_principal_client_id`, not its `oauth2_app_client_id` (`core/lakebase.py` falls back to `DATABRICKS_CLIENT_ID` when `PGUSER` is unset) | Leave `PGUSER` unset, or set it to `service_principal_client_id` from `databricks apps get <app>` |
| `must be owner of table ...` at startup | Tables were pre-created by a different Postgres user than the one the app connects as | Do not pre-create app tables. Let the app create them on first start, or drop them and redeploy so the SP owns them |
| Catalog creation fails with `403 Forbidden` from the cloud storage provider, or the "no default storage root" hint | The metastore default managed storage is not writable by the SP | Set a storage root: Settings > Unity Catalog, or the `UC_STORAGE_ROOT` env var. Catalogs are created at `<storage_root>/<catalog_name>` (`routes/provisioning.py`; precedence: request body, saved settings, env) |
| `User does not have CREATE MANAGED STORAGE on External Location` | The SP lacks grants on the external location | Grant the SP `CREATE_CATALOG` on the metastore, and on the external location `CREATE_MANAGED_STORAGE`, `READ_FILES`, `WRITE_FILES`, `BROWSE`, `CREATE_EXTERNAL_TABLE`. Grant these up front rather than one error at a time |
| `Catalog '<name>' already exists` on retry after a failed provision | A previous run created the catalog but failed later (for example on volume creation) | Fix the underlying permission and retry; catalog and schema creation treat `AlreadyExists` as success. If the catalog is half-built and unwanted, delete it (transfer ownership first if the SP owns it) |
| First bundle deploy hangs or fails right after the app is created | App compute takes a few minutes to reach `ACTIVE` | Wait for `compute_status.state == ACTIVE` (`databricks apps get <app>`), then deploy |
| App starts but the UI is missing or stale | `static/dist` is stale or was not synced | Run `make frontend-build`, commit `static/dist`, redeploy. The bundle `sync` includes `static/dist/**` and `lib/*.whl` |
