# IoT Digital Twin

A **Databricks App** that demonstrates three streaming-ingestion architectures for IoT
sensor data side by side, with a live per-pipeline dashboard (KPIs, 3D twin, latency
metrics, hot and cold history). The backend is FastAPI; the frontend is a React SPA.

The UI is **industry-first**: pick an industry (for example pharmaceutical or
manufacturing), then deploy one or more **pipeline flavors** against it. Each flavor is a
real Databricks deployment: a Kafka topic, a Spark or Zerobus job, a Lakebase branch and
Unity Catalog objects.

## Contents

- [Pipeline flavors](#pipeline-flavors)
- [Repository layout](#repository-layout)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Deploying](#deploying)
- [Testing and quality gates](#testing-and-quality-gates)
- [Documentation](#documentation)
- [Contributing](#contributing)

## Pipeline flavors

| Flavor | Path | Latency profile |
|---|---|---|
| **Real-time** | Kafka (MSK) → Spark Structured Streaming in Real-Time Mode → Lakebase LakeTS ChronoTable (hot) and the app's SSE stream | milliseconds |
| **Micro-batch** | Kafka → Spark micro-batch (`processingTime`) → Unity Catalog Delta table | seconds |
| **Zerobus** | Zerobus SDK → Unity Catalog Delta table (no Spark job) | direct ingest |

The dashboard reads three tiers: **SSE** for live values, **Lakebase** for recent
(hot) rows, and **DBSQL** for history that Lakebase CDF has synced into Delta.
See [docs/architecture.md](docs/architecture.md) for the data flow in detail.

## Repository layout

```
src/iot_twin/            Python package (run as iot_twin.app:app)
  app.py                 FastAPI application and startup/shutdown wiring
  core/                  config, auth, state, paths, Lakebase connection manager
  models/                typed data models and SQL DDL helpers
  routes/                HTTP API routers (one module per area)
  services/              business logic, grouped into subpackages
    alerts/ validation/ industry/ pipelines/ lakets/ deployment/ db/
  domain/                simulator domain catalog (also shipped to job clusters)
  scenario_engine/       scenario engine (also shipped to job clusters)
  assets/                packaged data files (lakets.sql)
client/                  React + Vite + TypeScript SPA source
static/dist/             built SPA served by the app (committed; CI checks it is fresh)
lib/                     simulator wheel installed by requirements.txt
tests/                   pytest suite (e2e/playwright = browser tests)
requirements.txt         runtime deps installed by Databricks Apps (dev extra: pyproject.toml)
scripts/                 ops and end-to-end tooling (profile-driven)
deploy/                  workspaces/*.profile.example + variable-overrides.json.example
databricks.yml           Declarative Automation Bundle (DABs) definition
resources/               bundle resource definitions (the app)
docs/                    architecture, deployment and API documentation
ops/                     operator tooling: the pipeline health-check notebook
usecases/                customer-facing KPI notebooks (manufacturing OEE, alarms, throughput)
```

## Quick start

Requirements: Python 3.11+, Node (see `client/.nvmrc`), and the Databricks CLI for deploys.

```bash
# 1. Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -e ".[dev]"

# 2. Frontend dependencies
cd client && npm ci && cd ..

# 3. Run the backend (works without a database; /api/health reports it as unavailable)
PYTHONPATH=src python -m uvicorn --app-dir src iot_twin.app:app --reload --port 8000

# 4. Run the frontend dev server (proxies /api to the backend)
cd client && npm run dev
```

Useful locally:

- `IOT_ALLOW_ANONYMOUS_IDENTITY=1` bypasses user-identity checks outside the Apps runtime.
- Set `PGHOST` and the other variables below to connect to a real Lakebase instance.

`make help` lists every shortcut (`install`, `lint`, `format`, `typecheck`, `test`,
`frontend-build`, `check`, `bundle-validate`).

## Configuration

The app is configured through environment variables, set in `app.yaml` (rendered per
workspace) or as bundle variables. Real workspace values are never committed: git holds
placeholders only. Main settings:

| Variable | Purpose |
|---|---|
| `UC_CATALOG`, `UC_SCHEMA` | Unity Catalog location for per-industry objects |
| `PGHOST`, `PGDATABASE`, `PGPORT`, `LAKEBASE_PROJECT`, `LAKEBASE_ENDPOINT` | App-backend Lakebase |
| `PIPELINES_LAKEBASE_PROJECT` | Lakebase project that holds per-industry streaming sinks |
| `DATABRICKS_SQL_WAREHOUSE_ID` | SQL warehouse for DBSQL queries (bound as an app resource) |
| `GENIE_SPACE_ID` | Genie space for the copilot (bound as an app resource) |
| `MSK_BOOTSTRAP_SERVERS` | Kafka brokers for real-time and micro-batch flavors |
| `ZEROBUS_SERVER_ENDPOINT`, `ZEROBUS_CLIENT_ID`, `ZEROBUS_SECRET_SCOPE`, `ZEROBUS_SECRET_KEY` | Zerobus ingest |
| `UC_SERVICE_CREDENTIAL`, `UC_STORAGE_BASE`, `UC_EXTERNAL_LOCATION` | Storage and credentials |
| `INDUSTRIES` | Industries offered in the UI |
| `ADMIN_GROUP` | Group whose members are app admins (optional `ADMIN_EMAILS` CSV adds extra admins; not set by the bundle) |
| `USAGE_TRACKING_ENABLED` | Usage tracking on/off (default `true`; `false`, `0`, `no` or `off` opts out) |
| `USAGE_TRACKING_APP_NAME`, `USAGE_TRACKING_DEMO_CATALOG_ID` | Optional tracker app name (default `iot-digital-twin`) and [demo catalog](https://go/demo-catalog) id |

### Usage tracking

The app reports usage to the dbdemos analytics service through the
[`dbdemos-tracker`](https://github.com/databricks-field-eng/dbdemos-tracker) package
(`src/iot_twin/services/usage_tracking.py`). Each event carries the workspace id, the app
name and a fixed event path. For `@databricks.com` users only, a SHA-256 hash of their
email is also sent (never the address itself; other users send no user identifier).
Tracked events: app start, SPA page loads (landing, industries, industry setup, mode dashboards, setup/validate, pipelines, settings), live dashboard
streams (`/api/events`), industry setup, validation start, pipeline create and pipeline
deploy. Ids are replaced with `{id}`; query strings and request bodies are never sent.

Tracking is fire-and-forget and fail-safe. If the package, the workspace id lookup or the
network is unavailable, tracking turns itself off and the app keeps running. Opt out with
`USAGE_TRACKING_ENABLED=false` (bundle variable `usage_tracking_enabled`, or the profile key
of the same name for `scripts/render_app_yaml.py`). The package is published on the
Databricks-internal PyPI proxy, which the Apps runtime cannot reach, so its wheel is
vendored in `lib/` and installed from `requirements.txt`.

Dev scripts under `scripts/` never default to a CLI profile: pass `--profile <name>` or
set `DATABRICKS_CONFIG_PROFILE`.

## Deploying

Deployment uses **Declarative Automation Bundles**. Quick version:

Workspace-specific values have no defaults. Copy `deploy/variable-overrides.json.example` to
`.databricks/bundle/<target>/variable-overrides.json` (gitignored) and fill it in first.

```bash
make bundle-validate PROFILE=<cli-profile>
databricks bundle deploy -t dev --profile <cli-profile>
databricks bundle run -t dev --profile <cli-profile>
```

Do not point `prod` at an existing app without binding it first
(`databricks bundle deployment bind`); see [docs/deployment.md](docs/deployment.md) for
variables, targets, binding, prerequisites (service-principal grants) and troubleshooting.

## Testing and quality gates

```bash
make check          # everything CI runs
make test           # pytest only
cd client && npm run lint && npm run typecheck && npm run build
cd tests/e2e/playwright && npm ci && npx playwright install chromium && npm test
```

CI (`.github/workflows/ci.yml`) runs ruff (lint and format), mypy on `core` and `models`,
pytest, the frontend lint/typecheck/build with a check that `static/dist` is up to date,
gitleaks, and an identifier check that blocks workspace-specific values. The same checks
run locally through `pre-commit`.

## Documentation

| Document | What it covers |
|---|---|
| [docs/README.md](docs/README.md) | Index of the documentation |
| [docs/architecture.md](docs/architecture.md) | Components, data flow, alerts, Genie copilot, auth, storage |
| [docs/deployment.md](docs/deployment.md) | Bundle deploys, variables, binding, prerequisites, troubleshooting |
| [docs/genie.md](docs/genie.md) | Shared copilot space, per-pipeline Genie spaces, permissions, API |
| [docs/api.md](docs/api.md) | API routes by router; live OpenAPI at `/docs` and `/openapi.json` |
| [ops/README.md](ops/README.md) | Pipeline health-check notebook (read-only, inline report) |
| [usecases/README.md](usecases/README.md) | Manufacturing KPI notebook: OEE, downtime and alarms, sensor health, throughput |
| [tests/e2e/playwright/README.md](tests/e2e/playwright/README.md) | Browser QA |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Workflow, standards, commit conventions |
| [CHANGELOG.md](CHANGELOG.md) | Notable changes |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). In short: branch from the default branch, run
`make check`, use Conventional Commits, and open a pull request.
