# API overview

The backend is one FastAPI application (`iot_twin.app:app`). Every router is registered in
one place, `routes.create_api_router()` in `src/iot_twin/routes/__init__.py`. Almost all
endpoints live under `/api`; the exceptions are `/metrics`, `/ws` and the SPA routes.

## Live OpenAPI

FastAPI's defaults are in effect (no `docs_url` / `openapi_url` overrides in `app.py`), so
these are served by the running app:

| URL | Content |
|---|---|
| `/docs` | Swagger UI |
| `/redoc` | ReDoc |
| `/openapi.json` | Machine-readable OpenAPI schema |

They are registered before the SPA catch-all (`/{path:path}`), so they are not swallowed by
the frontend. The OpenAPI schema is the authoritative reference for request and response
bodies; this page only lists routes.

## Conventions

* Identity comes from the `x-forwarded-access-token` header added by the Databricks Apps
  runtime. Admin endpoints (`/api/admin/*`) require an admin (see
  [architecture.md](architecture.md#authentication-and-authorization)).
* Live data is pushed over Server-Sent Events at `GET /api/events`.
* Errors use FastAPI's standard JSON error body (`{"detail": ...}`).

## Routes by router module

Generated from the routers registered in the app (132 HTTP routes in the routers, 1
WebSocket, plus the SPA catch-all in `app.py`). Regenerate by listing `router.routes` for
each module in `routes/__init__.py`.

### `routes/alerts.py`

Alert list, history, acknowledge, responses, evaluate.

| Method | Path |
|---|---|
| GET | `/api/alerts` |
| GET | `/api/alerts/history` |
| GET | `/api/alerts/{alert_id}` |
| POST | `/api/alerts/{alert_id}/acknowledge` |
| GET | `/api/alerts/{alert_id}/responses` |
| POST | `/api/alerts/{alert_id}/responses` |
| POST | `/api/alerts/evaluate` |

### `routes/cluster.py`

Cluster helpers and source test.

| Method | Path |
|---|---|
| GET | `/api/cluster/node-types` |
| GET | `/api/cluster/spark-versions` |
| POST | `/api/cluster/provision` |
| GET | `/api/cluster/{cluster_id}/status` |
| POST | `/api/cluster/{cluster_id}/start` |
| POST | `/api/test-source` |

### `routes/deployment.py`

Connectivity test, wheel upload, job deployment.

| Method | Path |
|---|---|
| POST | `/api/test-connectivity/upload-wheel` |
| POST | `/api/test-connectivity/submit` |
| GET | `/api/test-connectivity/{run_id}/status` |
| POST | `/api/cluster/{cluster_id}/validate` |
| POST | `/api/cluster/deploy-job` |

### `routes/genie.py`

Genie copilot conversations.

| Method | Path |
|---|---|
| GET | `/api/genie/status` |
| POST | `/api/genie/conversations` |
| POST | `/api/genie/conversations/{conversation_id}/messages` |
| GET | `/api/genie/conversations/{conversation_id}/messages/{message_id}` |

### `routes/genie_pipelines.py`

Per-pipeline Genie copilot (one space per pipeline and mode, every industry). See
[genie.md](genie.md).

| Method | Path |
|---|---|
| GET | `/api/genie/pipelines/{setup_id}/{mode}/status` |
| GET | `/api/genie/pipelines/{setup_id}/{mode}/starters` |
| POST | `/api/genie/pipelines/{setup_id}/{mode}/conversations` |
| POST | `/api/genie/pipelines/{setup_id}/{mode}/conversations/{conversation_id}/messages` |
| GET | `/api/genie/pipelines/{setup_id}/{mode}/conversations/{conversation_id}/messages/{message_id}` |
| GET | `/api/genie/pipelines/{setup_id}/{mode}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/result` |

### `routes/health.py`

Health, app defaults, Prometheus-style metrics.

| Method | Path |
|---|---|
| GET | `/api/health` |
| GET | `/api/app-defaults` |
| GET | `/metrics` |

### `routes/industries.py`

Industry catalog, assets, reference and mode start/stop/config.

| Method | Path |
|---|---|
| GET | `/api/industries` |
| GET | `/api/industries/{name}/sensors` |
| POST | `/api/generate-sample` |
| GET | `/api/industries/catalog` |
| GET | `/api/industries/{industry}/assets` |
| GET | `/api/industries/{industry}/reference` |
| POST | `/api/industries/{industry}/modes/{mode}/start` |
| POST | `/api/industries/{industry}/modes/{mode}/stop` |
| POST | `/api/industries/{industry}/modes/{mode}/config` |

### `routes/industries_admin.py`

Admin pipeline setup, provision, health and delete.

| Method | Path |
|---|---|
| POST | `/api/admin/industries/{industry}/setup` |
| PATCH | `/api/admin/pipelines/{setup_id}/modes/{mode}/config` |
| GET | `/api/admin/industries/{industry}/setup` |
| PATCH | `/api/admin/pipelines/{setup_id}/modes` |
| POST | `/api/admin/industries/{industry}/provision` |
| GET | `/api/industries/runtime` |
| GET | `/api/admin/pipelines` |
| GET | `/api/admin/pipelines/{setup_id}/health` |
| DELETE | `/api/admin/pipelines/{setup_id}` |

### `routes/intelligence.py`

Admin intelligence catalog status and sync.

| Method | Path |
|---|---|
| GET | `/api/admin/intelligence/status` |
| POST | `/api/admin/intelligence/sync` |

### `routes/jobs.py`

Job status and control.

| Method | Path |
|---|---|
| GET | `/api/job/{job_id}/status` |
| POST | `/api/job/{job_id}/stop` |
| POST | `/api/job/{job_id}/restart` |

### `routes/lakets.py`

LakeTS sync check, CDF status, dashboard, retention.

| Method | Path |
|---|---|
| POST | `/api/lakets/sync/check` |
| GET | `/api/pipelines/{pipeline_id}/cdf-status` |
| GET | `/api/pipelines/{pipeline_id}/lakets/dashboard` |
| POST | `/api/lakets/retention/deploy` |

### `routes/pipeline_deploy.py`

Admin per-mode deploy, start, stop, status, CDF reconcile, Genie space backfill.

| Method | Path |
|---|---|
| POST | `/api/admin/pipelines/{setup_id}/deploy/{mode}` |
| POST | `/api/admin/pipelines/{setup_id}/{mode}/start` |
| POST | `/api/admin/pipelines/{setup_id}/realtime/reconcile-cdf` |
| POST | `/api/admin/pipelines/{setup_id}/{mode}/stop` |
| GET | `/api/admin/pipelines/{setup_id}/{mode}/status` |
| POST | `/api/admin/pipelines/{setup_id}/genie/{mode}` |

### `routes/pipelines.py`

Pipeline CRUD, reset, clone, health.

| Method | Path |
|---|---|
| GET | `/api/pipelines` |
| POST | `/api/pipelines` |
| POST | `/api/pipelines/check-source` |
| GET | `/api/pipelines/{pipeline_id}` |
| PUT | `/api/pipelines/{pipeline_id}` |
| DELETE | `/api/pipelines/{pipeline_id}` |
| POST | `/api/pipelines/{pipeline_id}/reset` |
| POST | `/api/pipelines/{pipeline_id}/heal-lakebase` |
| POST | `/api/pipelines/{pipeline_id}/clone` |
| GET | `/api/pipelines/{pipeline_id}/lakebase-health` |
| GET | `/api/pipeline/{pipeline_id}/history` |

### `routes/producers.py`

Kafka producer control (global and per pipeline).

| Method | Path |
|---|---|
| GET | `/api/pipelines/{pipeline_id}/producer/status` |
| POST | `/api/pipelines/{pipeline_id}/producer/start` |
| POST | `/api/pipelines/{pipeline_id}/producer/stop` |
| GET | `/api/kafka-producer/status` |
| POST | `/api/kafka-producer/start` |
| POST | `/api/kafka-producer/stop` |

### `routes/production.py`

Cell production state, stop and resume.

| Method | Path |
|---|---|
| GET | `/api/industries/{industry}/production` |
| GET | `/api/industries/{industry}/production/events` |
| POST | `/api/industries/{industry}/production` |

### `routes/provisioning.py`

Unity Catalog, secrets and Lakebase provisioning.

| Method | Path |
|---|---|
| POST | `/api/provision/uc` |
| POST | `/api/provision/pipeline-catalog` |
| POST | `/api/pipelines/{pipeline_id}/secrets` |
| GET | `/api/pipelines/{pipeline_id}/secrets/keys` |
| POST | `/api/provision/lakebase-table` |

### `routes/sensor_data.py`

Ingest, readings, aggregates, per-pipeline stats and metrics.

| Method | Path |
|---|---|
| POST | `/api/sss/ingest` |
| GET | `/api/sss/aggregates` |
| GET | `/api/timeseries` |
| GET | `/api/sensors` |
| GET | `/api/data` |
| GET | `/api/aggregates` |
| GET | `/api/latest` |
| GET | `/api/pipelines/{pipeline_id}/stats` |
| GET | `/api/pipelines/{pipeline_id}/timeseries` |
| GET | `/api/pipeline-stats` |
| GET | `/api/pipelines/{pipeline_id}/lakebase-perf` |
| GET | `/api/pipelines/{pipeline_id}/lakets/gapfill` |
| GET | `/api/pipelines/{pipeline_id}/lakets/rate` |
| GET | `/api/pipelines/{pipeline_id}/lakets/histogram` |
| GET | `/api/pipeline/{pipeline_id}/streaming-metrics` |
| GET | `/api/pipelines/{pipeline_id}/zerobus-metrics` |
| GET | `/api/pipeline/{pipeline_id}/dbsql-timeseries` |

### `routes/settings.py`

Workspace settings validation and provisioning.

| Method | Path |
|---|---|
| POST | `/api/settings/validate` |
| GET | `/api/settings/status` |
| POST | `/api/settings/provision` |
| POST | `/api/settings/provision/uc` |
| POST | `/api/settings/provision/secrets` |
| POST | `/api/settings/provision/lakebase` |

### `routes/setup.py`

Pipeline Lakebase setup.

| Method | Path |
|---|---|
| POST | `/api/setup/pipeline-lakebase` |

### `routes/simulator.py`

Built-in simulator.

| Method | Path |
|---|---|
| GET | `/api/simulator/status` |
| POST | `/api/simulator/restart` |

### `routes/sources.py`

Streaming source registry and config.

| Method | Path |
|---|---|
| GET | `/api/sources` |
| PUT | `/api/sources/{name}/config` |

### `routes/static_files.py`

SPA entry point.

| Method | Path |
|---|---|
| GET | `/` |

### `routes/streaming_latency.py`

Streaming and Zerobus latency KPIs.

| Method | Path |
|---|---|
| GET | `/api/latency/streaming` |
| GET | `/api/latency/zerobus` |

### `routes/user.py`

Current user and user settings.

| Method | Path |
|---|---|
| GET | `/api/me` |
| GET | `/api/settings` |
| PUT | `/api/settings` |

### `routes/validation_admin.py`

Admin pre-deploy validation steps.

| Method | Path |
|---|---|
| POST | `/api/admin/industries/{industry}/validate/start` |
| GET | `/api/admin/industries/{industry}/validate/state` |
| POST | `/api/admin/industries/{industry}/validate/retry/{step}` |
| POST | `/api/admin/industries/{industry}/validate/cdf-enabled` |
| GET | `/api/admin/industries/{industry}/validate/can-deploy` |

### `routes/warehouses.py`

SQL warehouse listing.

| Method | Path |
|---|---|
| GET | `/api/warehouses` |
| POST | `/api/warehouses` |

### `routes/websocket.py`

SSE event stream, SSE alert ingest.

| Method | Path |
|---|---|
| GET | `/api/events` |
| WS | `/ws` |
| POST | `/api/sss/alerts` |
