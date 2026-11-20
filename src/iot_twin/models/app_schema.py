"""App-backend data-model DDL for the ``iot-app-backend`` Lakebase project.

Implements the parent/child + normalized-ledger model from the admin pipeline
design spec §2.3 (Option B):

    pipeline_setup       — one row per industry (UNIQUE industry)
    pipeline_deployment  — one row per (setup, mode); FK setup_id CASCADE,
                           UNIQUE(setup_id, mode)
    provisioned_resource — normalized teardown ledger; FK setup_id CASCADE +
                           nullable deployment_id, ordered by teardown_order

These tables live ONLY in the new ``iot-app-backend`` project — they are
additive and do not touch the legacy ``pipelines`` / ``user_settings`` /
``source_bindings`` wiring on ``iot-lakets-demo``.

The ``ensure_*`` creators are idempotent (``CREATE TABLE IF NOT EXISTS`` +
``CREATE INDEX IF NOT EXISTS``) and accept any async DB executor exposing an
``execute(query, *args)`` coroutine — the ``LakebaseConnectionManager``, a
``PipelineLakebaseHandle``, or a raw ``asyncpg.Connection``.
"""

from __future__ import annotations

from typing import Protocol


class _Executor(Protocol):
    async def execute(self, query: str, *args): ...


# ---------------------------------------------------------------------------
# DDL constants
# ---------------------------------------------------------------------------
PIPELINE_SETUP_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_setup (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    industry TEXT NOT NULL UNIQUE,
    flavor TEXT,
    source_type TEXT,
    bootstrap_servers TEXT,
    derived JSONB DEFAULT '{}'::jsonb,
    service_credential TEXT,
    enabled_modes JSONB DEFAULT '[]'::jsonb,
    mode_configs JSONB DEFAULT '{}'::jsonb,
    cluster_spec JSONB DEFAULT '{}'::jsonb,
    warehouse_id TEXT,
    validation_state JSONB DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'draft',
    user_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

PIPELINE_DEPLOYMENT_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_deployment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    setup_id UUID NOT NULL
        REFERENCES pipeline_setup(id) ON DELETE CASCADE,
    mode TEXT NOT NULL
        CHECK (mode IN ('realtime', 'microbatch', 'zerobus')),
    status TEXT NOT NULL DEFAULT 'pending',
    partition_count INT,
    cdf_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    metrics_config JSONB DEFAULT '{}'::jsonb,
    lakebase_branch TEXT,
    lakebase_endpoints JSONB DEFAULT '{}'::jsonb,
    delta_table TEXT,
    checkpoint_path TEXT,
    job_id TEXT,
    notebook_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_deployment_setup_mode UNIQUE (setup_id, mode)
)
"""

PROVISIONED_RESOURCE_DDL = """
CREATE TABLE IF NOT EXISTS provisioned_resource (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    setup_id UUID NOT NULL
        REFERENCES pipeline_setup(id) ON DELETE CASCADE,
    deployment_id UUID
        REFERENCES pipeline_deployment(id) ON DELETE CASCADE,
    resource_type TEXT NOT NULL,
    identifier TEXT NOT NULL,
    parent_ref TEXT,
    teardown_order INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

PROVISIONED_RESOURCE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_provisioned_resource_setup "
    "ON provisioned_resource (setup_id, teardown_order DESC)",
    "CREATE INDEX IF NOT EXISTS idx_provisioned_resource_deployment "
    "ON provisioned_resource (deployment_id)",
)

ALERT_DDL = """
CREATE TABLE IF NOT EXISTS alert (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint TEXT NOT NULL UNIQUE,
    industry TEXT,
    pipeline_id TEXT,
    rule_id TEXT NOT NULL,
    sensor_id TEXT,
    sensor_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
    severity TEXT NOT NULL
        CHECK (severity IN ('info', 'warn', 'crit')),
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'acknowledged', 'cleared')),
    fault_code TEXT,
    value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    cleared_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'::jsonb,
    version INTEGER NOT NULL DEFAULT 0
)
"""

ALERT_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS alert_event (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL
        REFERENCES alert(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('opened', 'updated', 'acknowledged', 'cleared')),
    fingerprint TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

ALERT_RESPONSE_DDL = """
CREATE TABLE IF NOT EXISTS alert_response (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL
        REFERENCES alert(id) ON DELETE CASCADE,
    episode_opened_at TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL
        CHECK (action IN ('hold_batch', 'send_to_inspect', 'request_maintenance')),
    recorded_by TEXT NOT NULL,
    reference JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (alert_id, episode_opened_at, action)
)
"""

PRODUCTION_STATE_DDL = """
CREATE TABLE IF NOT EXISTS production_state (
    industry TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('running', 'stopped')),
    assets JSONB NOT NULL DEFAULT '[]'::jsonb,
    reason TEXT,
    alert_id UUID,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

PRODUCTION_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS production_event (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    industry TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running', 'stopped')),
    assets JSONB NOT NULL DEFAULT '[]'::jsonb,
    reason TEXT,
    alert_id UUID,
    changed_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

PRODUCTION_EVENT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_production_event_industry_created "
    "ON production_event (industry, created_at DESC)",
)

ALERT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_alert_status_updated ON alert (status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_alert_industry ON alert (industry)",
    "CREATE INDEX IF NOT EXISTS idx_alert_pipeline ON alert (pipeline_id)",
)

ALERT_EVENT_INDEXES = (
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_event_fingerprint ON alert_event (fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_alert_event_alert_created "
    "ON alert_event (alert_id, created_at)",
)


# ---------------------------------------------------------------------------
# Idempotent creators
# ---------------------------------------------------------------------------
async def ensure_pipeline_setup_table(db: _Executor) -> None:
    """Create or additively upgrade the ``pipeline_setup`` table."""
    await db.execute(PIPELINE_SETUP_DDL)
    await db.execute(
        "ALTER TABLE pipeline_setup ADD COLUMN IF NOT EXISTS mode_configs JSONB DEFAULT '{}'::jsonb"
    )


async def ensure_pipeline_deployment_table(db: _Executor) -> None:
    """Create the ``pipeline_deployment`` table if it does not exist."""
    await db.execute(PIPELINE_DEPLOYMENT_DDL)


async def ensure_provisioned_resource_table(db: _Executor) -> None:
    """Create the ``provisioned_resource`` ledger table + indexes.

    ``CREATE INDEX IF NOT EXISTS`` performs a Postgres **ownership check before**
    the IF-NOT-EXISTS skip, so if the table was created by a different identity
    (e.g. seeded by a human before the app SP first ran), the index DDL raises
    ``InsufficientPrivilegeError`` ("must be owner of table ...") even though the
    index already exists — which previously failed the whole app-backend init and
    503'd every admin route. The index is a non-critical perf optimization, so
    tolerate that specific case (re-raise anything else) to keep boot resilient.
    """
    await db.execute(PROVISIONED_RESOURCE_DDL)
    for index_ddl in PROVISIONED_RESOURCE_INDEXES:
        try:
            await db.execute(index_ddl)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "must be owner" in msg or "insufficientprivilege" in type(exc).__name__.lower():
                continue
            raise


async def _ensure_indexes(db: _Executor, statements: tuple[str, ...]) -> None:
    """Create indexes, tolerating ownership errors on already-existing objects."""
    for index_ddl in statements:
        try:
            await db.execute(index_ddl)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "must be owner" in msg or "insufficientprivilege" in type(exc).__name__.lower():
                continue
            raise


async def ensure_alert_table(db: _Executor) -> None:
    """Create the current-state ``alert`` table + indexes."""
    await db.execute(ALERT_DDL)
    await db.execute(
        "ALTER TABLE alert ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 0"
    )
    await _ensure_indexes(db, ALERT_INDEXES)


async def ensure_alert_event_table(db: _Executor) -> None:
    """Create the append-only ``alert_event`` table + idempotent indexes."""
    await db.execute(ALERT_EVENT_DDL)
    await _ensure_indexes(db, ALERT_EVENT_INDEXES)


async def ensure_alert_response_table(db: _Executor) -> None:
    """Create the ``alert_response`` table for recorded operator decisions."""
    await db.execute(ALERT_RESPONSE_DDL)


async def ensure_production_tables(db: _Executor) -> None:
    """Create the cell ``production_state`` row table and its ``production_event`` log."""
    await db.execute(PRODUCTION_STATE_DDL)
    await db.execute(PRODUCTION_EVENT_DDL)
    await _ensure_indexes(db, PRODUCTION_EVENT_INDEXES)


async def ensure_app_schema(db: _Executor) -> None:
    """Create all app-backend tables in FK-dependency order.

    Order matters: ``pipeline_deployment`` and ``provisioned_resource``
    reference ``pipeline_setup``; ``provisioned_resource`` also references
    ``pipeline_deployment``; ``alert_event`` and ``alert_response`` reference
    ``alert``.
    """
    await ensure_pipeline_setup_table(db)
    await ensure_pipeline_deployment_table(db)
    await ensure_provisioned_resource_table(db)
    await ensure_alert_table(db)
    await ensure_alert_event_table(db)
    await ensure_alert_response_table(db)
    await ensure_production_tables(db)
