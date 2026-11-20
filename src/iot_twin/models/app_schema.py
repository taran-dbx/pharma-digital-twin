"""App-backend data-model DDL for the ``<prefix>-backend`` Lakebase project.

Implements the parent/child + normalized-ledger model from the admin pipeline
design spec §2.3 (Option B):

    pipeline_setup       — one row per industry (UNIQUE industry)
    pipeline_deployment  — one row per (setup, mode); FK setup_id CASCADE,
                           UNIQUE(setup_id, mode)
    provisioned_resource — normalized teardown ledger; FK setup_id CASCADE +
                           nullable deployment_id, ordered by teardown_order

These tables live ONLY in the new ``<prefix>-backend`` project — they are
additive and do not touch the legacy ``pipelines`` / ``user_settings`` /
``source_bindings`` wiring on ``<prefix>-lakets-demo``.

The ``ensure_*`` creators are idempotent (``CREATE TABLE IF NOT EXISTS`` +
``CREATE INDEX IF NOT EXISTS``) and accept any async DB executor exposing an
``execute(query, *args)`` coroutine — the ``LakebaseConnectionManager``, a
``PipelineLakebaseHandle``, or a raw ``asyncpg.Connection``.
"""

from __future__ import annotations

import logging
from typing import Protocol

logger = logging.getLogger(__name__)


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

# Operator decisions on an alert episode. The first four are the medical-device
# quality workflow; the legacy CNC-cell values stay accepted so rows written by
# older builds remain valid.
ALERT_RESPONSE_ACTIONS: tuple[str, ...] = (
    "quarantine_lot",
    "open_ncr",
    "request_cmm_inspection",
    "request_maintenance",
)
LEGACY_ALERT_RESPONSE_ACTIONS: tuple[str, ...] = ("hold_batch", "send_to_inspect")
ALL_ALERT_RESPONSE_ACTIONS: tuple[str, ...] = (
    *ALERT_RESPONSE_ACTIONS,
    *LEGACY_ALERT_RESPONSE_ACTIONS,
)
ALERT_RESPONSE_ACTION_CHECK = "alert_response_action_check"
ALERT_RESPONSE_ALERT_FK = "alert_response_alert_id_fkey"
_ACTION_LIST_SQL = ", ".join(f"'{action}'" for action in ALL_ALERT_RESPONSE_ACTIONS)

# Part 11-style audit fields; actor and timestamp are recorded_by / created_at.
ALERT_RESPONSE_AUDIT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("reason", "TEXT"),
    ("comment", "TEXT"),
    ("signature_meaning", "TEXT"),
    ("signed_at", "TIMESTAMPTZ"),
)

ALERT_RESPONSE_DDL = f"""
CREATE TABLE IF NOT EXISTS alert_response (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL
        CONSTRAINT {ALERT_RESPONSE_ALERT_FK} REFERENCES alert(id) ON DELETE RESTRICT,
    episode_opened_at TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL
        CONSTRAINT {ALERT_RESPONSE_ACTION_CHECK} CHECK (action IN ({_ACTION_LIST_SQL})),
    recorded_by TEXT NOT NULL,
    reference JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    reason TEXT,
    comment TEXT,
    signature_meaning TEXT,
    signed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (alert_id, episode_opened_at, action)
)
"""

_AUDIT_COLUMN_STEPS = "\n".join(
    f"""    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'alert_response' AND column_name = '{column}'
    ) THEN
        ALTER TABLE alert_response ADD COLUMN IF NOT EXISTS {column} {sql_type};
    END IF;"""
    for column, sql_type in ALERT_RESPONSE_AUDIT_COLUMNS
)

# Only ALTERs when a column is missing: Postgres checks table ownership before
# IF NOT EXISTS, so an unconditional ALTER fails for a non-owner on every boot.
ALERT_RESPONSE_AUDIT_MIGRATION = f"""
DO $$
BEGIN
{_AUDIT_COLUMN_STEPS}
END
$$
"""

_MISSING_ACTION_TESTS = " OR ".join(
    f"position('''{action}''' IN current_def) = 0" for action in ALL_ALERT_RESPONSE_ACTIONS
)

# Rebuilds the action CHECK only when it lacks a current value. The DO block is
# one transaction, so the drop and re-add are atomic; every legacy value stays
# in the list, so existing rows validate and nothing is rewritten or lost.
ALERT_RESPONSE_ACTION_MIGRATION = f"""
DO $$
DECLARE
    current_def TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO current_def
    FROM pg_constraint
    WHERE conrelid = 'alert_response'::regclass
      AND conname = '{ALERT_RESPONSE_ACTION_CHECK}';
    IF current_def IS NULL OR {_MISSING_ACTION_TESTS} THEN
        ALTER TABLE alert_response DROP CONSTRAINT IF EXISTS {ALERT_RESPONSE_ACTION_CHECK};
        ALTER TABLE alert_response ADD CONSTRAINT {ALERT_RESPONSE_ACTION_CHECK}
            CHECK (action IN ({_ACTION_LIST_SQL}));
    END IF;
END
$$
"""

# Signed decisions must outlive their alert row: an alert delete is refused
# instead of cascading. Rebuilt only while the FK still cascades.
ALERT_RESPONSE_FK_MIGRATION = f"""
DO $$
DECLARE
    fk_name TEXT;
BEGIN
    SELECT conname INTO fk_name
    FROM pg_constraint
    WHERE conrelid = 'alert_response'::regclass
      AND contype = 'f'
      AND confrelid = 'alert'::regclass
      AND confdeltype = 'c'
    LIMIT 1;
    IF fk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE alert_response DROP CONSTRAINT %I', fk_name);
        ALTER TABLE alert_response ADD CONSTRAINT {ALERT_RESPONSE_ALERT_FK}
            FOREIGN KEY (alert_id) REFERENCES alert(id) ON DELETE RESTRICT;
    END IF;
END
$$
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

# Part 11-style trace of every AI action plan: who asked, when, which endpoint and
# model answered, the exact prompt (template version + hash), the exact rows it was
# given (grounding_rows, with citation ids), the answer and its grounding check.
# Like alert_response, the FK is ON DELETE RESTRICT: an alert with audited AI
# plans cannot be deleted, so the trace always outlives (and points at) its alert.
GENAI_AUDIT_ALERT_FK = "genai_action_plan_audit_alert_id_fkey"
GENAI_ACTION_PLAN_AUDIT_DDL = f"""
CREATE TABLE IF NOT EXISTS genai_action_plan_audit (
    id UUID PRIMARY KEY,
    alert_id UUID NOT NULL
        CONSTRAINT {GENAI_AUDIT_ALERT_FK} REFERENCES alert(id) ON DELETE RESTRICT,
    industry TEXT,
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    endpoint TEXT NOT NULL,
    model TEXT,
    model_version TEXT,
    prompt_template_version TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    grounding_hash TEXT NOT NULL,
    grounding_rows JSONB NOT NULL DEFAULT '[]'::jsonb,
    response JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    grounding_check TEXT NOT NULL CHECK (grounding_check IN ('pass', 'fail')),
    grounding_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('generated', 'rejected', 'error')),
    error TEXT,
    latency_ms INTEGER,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    applied_by TEXT,
    applied_at TIMESTAMPTZ,
    applied_response_id UUID
        REFERENCES alert_response(id) ON DELETE RESTRICT,
    applied_override_reason TEXT
)
"""

GENAI_AUDIT_APPLIED_COLUMNS: tuple[str, ...] = (
    "applied_by",
    "applied_at",
    "applied_response_id",
    "applied_override_reason",
)
GENAI_AUDIT_TRIGGER = "genai_action_plan_audit_append_only"
_GENAI_MUTABLE = " ".join(f"- '{column}'" for column in GENAI_AUDIT_APPLIED_COLUMNS)

# Added after the first build of this table; guarded so a non-owner boot is a no-op.
GENAI_AUDIT_COLUMN_MIGRATION = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'genai_action_plan_audit'
          AND column_name = 'applied_override_reason'
    ) THEN
        ALTER TABLE genai_action_plan_audit ADD COLUMN IF NOT EXISTS applied_override_reason TEXT;
    END IF;
END
$$
"""

# Append-only: DELETE and TRUNCATE are refused, and an UPDATE may only set the
# applied_* columns, once (while applied_at is NULL). Created only when missing,
# so boots by a non-owner role never need ownership.
GENAI_AUDIT_APPEND_ONLY_MIGRATION = f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = '{GENAI_AUDIT_TRIGGER}' AND n.nspname = current_schema()
    ) THEN
        EXECUTE $create$
        CREATE FUNCTION {GENAI_AUDIT_TRIGGER}() RETURNS trigger LANGUAGE plpgsql AS $fn$
        BEGIN
            IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
                RAISE EXCEPTION 'genai_action_plan_audit is append-only (% refused)', TG_OP;
            END IF;
            IF OLD.applied_at IS NOT NULL THEN
                RAISE EXCEPTION 'genai_action_plan_audit row % is already applied', OLD.id;
            END IF;
            IF (to_jsonb(NEW) {_GENAI_MUTABLE}) IS DISTINCT FROM (to_jsonb(OLD) {_GENAI_MUTABLE}) THEN
                RAISE EXCEPTION 'genai_action_plan_audit is append-only: only applied_* may be set';
            END IF;
            RETURN NEW;
        END
        $fn$
        $create$;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'genai_action_plan_audit'::regclass AND tgname = '{GENAI_AUDIT_TRIGGER}'
    ) THEN
        CREATE TRIGGER {GENAI_AUDIT_TRIGGER} BEFORE UPDATE OR DELETE ON genai_action_plan_audit
            FOR EACH ROW EXECUTE FUNCTION {GENAI_AUDIT_TRIGGER}();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'genai_action_plan_audit'::regclass
          AND tgname = '{GENAI_AUDIT_TRIGGER}_truncate'
    ) THEN
        CREATE TRIGGER {GENAI_AUDIT_TRIGGER}_truncate BEFORE TRUNCATE ON genai_action_plan_audit
            FOR EACH STATEMENT EXECUTE FUNCTION {GENAI_AUDIT_TRIGGER}();
    END IF;
END
$$
"""

# App-generated GenAI settings, e.g. the pseudonym HMAC key used when no
# GENAI_PSEUDONYM_SALT secret is bound (services/genai/redact.py).
GENAI_SETTINGS_DDL = """
CREATE TABLE IF NOT EXISTS genai_settings (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

GENAI_ACTION_PLAN_AUDIT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_genai_audit_alert_requested "
    "ON genai_action_plan_audit (alert_id, requested_at DESC)",
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


def _is_ownership_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "must be owner" in msg or "insufficientprivilege" in type(exc).__name__.lower()


async def ensure_alert_response_table(db: _Executor) -> None:
    """Create or upgrade ``alert_response`` for the medical-device decision workflow.

    Both upgrades are idempotent and no-ops on a current table. If the table is
    owned by another identity and still needs an upgrade, boot continues with a
    warning; the decision store then falls back to the legacy column set.
    """
    await db.execute(ALERT_RESPONSE_DDL)
    for step, sql in (
        ("audit columns", ALERT_RESPONSE_AUDIT_MIGRATION),
        ("action check", ALERT_RESPONSE_ACTION_MIGRATION),
        ("delete rule", ALERT_RESPONSE_FK_MIGRATION),
    ):
        try:
            await db.execute(sql)
        except Exception as exc:  # noqa: BLE001
            if not _is_ownership_error(exc):
                raise
            logger.warning(
                "alert_response %s not upgraded: the table is owned by another role (%s); "
                "ask its owner to run ensure_alert_response_table",
                step,
                exc,
            )


async def ensure_production_tables(db: _Executor) -> None:
    """Create the cell ``production_state`` row table and its ``production_event`` log."""
    await db.execute(PRODUCTION_STATE_DDL)
    await db.execute(PRODUCTION_EVENT_DDL)
    await _ensure_indexes(db, PRODUCTION_EVENT_INDEXES)


async def ensure_genai_action_plan_audit_table(db: _Executor) -> None:
    """Create the AI action-plan audit table, index, late column and append-only trigger.

    ``CREATE TABLE IF NOT EXISTS`` is a no-op for a non-owner when the table
    exists; the index step tolerates the ownership error Postgres raises before
    its IF-NOT-EXISTS skip (see :func:`ensure_provisioned_resource_table`).
    """
    await db.execute(GENAI_ACTION_PLAN_AUDIT_DDL)
    await _ensure_indexes(db, GENAI_ACTION_PLAN_AUDIT_INDEXES)
    for step, sql in (
        ("override column", GENAI_AUDIT_COLUMN_MIGRATION),
        ("append-only trigger", GENAI_AUDIT_APPEND_ONLY_MIGRATION),
    ):
        try:
            await db.execute(sql)
        except Exception as exc:  # noqa: BLE001
            if not _is_ownership_error(exc):
                raise
            logger.warning(
                "genai_action_plan_audit %s not applied: the table is owned by another role (%s)",
                step,
                exc,
            )


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
    await ensure_genai_action_plan_audit_table(db)
    await db.execute(GENAI_SETTINGS_DDL)
