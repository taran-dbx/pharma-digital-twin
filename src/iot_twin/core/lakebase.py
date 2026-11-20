"""Lakebase connection manager for the FastAPI application.

For Lakebase Autoscaling, the app generates short-lived OAuth tokens via the
Databricks SDK and refreshes them periodically.  The PGHOST and LAKEBASE_ENDPOINT
env vars must be set (injected by app.yaml).

When PGUSER is auto-injected (e.g. by a Lakebase resource), the manager uses
that directly.  Otherwise it falls back to SDK-based credential generation.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

logger = logging.getLogger("iot_digital_twin.backend")

# Bounded pool waits so a hung Lakebase endpoint cannot pin app workers forever
# (proxy upstream timeouts were ~10m with no asyncpg command/connect limits).
LAKEBASE_CONNECT_TIMEOUT_SECONDS = float(os.getenv("LAKEBASE_CONNECT_TIMEOUT_SECONDS", "10"))
# DDL/CDF migrations can legitimately exceed dashboard probe budgets. Routes
# apply shorter per-request wait_for deadlines; this pool-level ceiling is the
# final guard for long-running provisioning statements.
LAKEBASE_COMMAND_TIMEOUT_SECONDS = float(os.getenv("LAKEBASE_COMMAND_TIMEOUT_SECONDS", "120"))
LAKEBASE_SDK_TIMEOUT_SECONDS = float(os.getenv("LAKEBASE_SDK_TIMEOUT_SECONDS", "8"))


def _get_lakebase_credentials(endpoint_name: str | None = None) -> tuple[str, str]:
    """Generate a short-lived OAuth token via the Databricks SDK.

    ``endpoint_name`` overrides the ``LAKEBASE_ENDPOINT`` env var — used by
    per-pipeline Lakebase pools that target a different project than the
    app backend.

    Returns (pg_user, pg_password).
    """
    w = WorkspaceClient(
        config=Config(
            http_timeout_seconds=LAKEBASE_SDK_TIMEOUT_SECONDS,
            retry_timeout_seconds=LAKEBASE_SDK_TIMEOUT_SECONDS,
        )
    )
    endpoint_name = endpoint_name or os.getenv("LAKEBASE_ENDPOINT")
    if not endpoint_name:
        raise RuntimeError("LAKEBASE_ENDPOINT env var not set")

    pg_user = (
        os.getenv("PGUSER") or os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
    )
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    return pg_user, cred.token


class LakebaseConnectionManager:
    """Manages an asyncpg pool to Lakebase with OAuth token refresh."""

    def __init__(
        self,
        pg_host: str | None = None,
        pg_database: str | None = None,
        pg_port: int = 5432,
        pool_min: int = 2,
        pool_max: int = 8,
        endpoint_name: str | None = None,
    ) -> None:
        self._pg_host = pg_host or os.getenv("PGHOST")
        self._pg_database = pg_database or os.getenv("PGDATABASE", "databricks_postgres")
        self._pg_port = pg_port if pg_port != 5432 else int(os.getenv("PGPORT", "5432"))
        self._pool_min = pool_min
        self._pool_max = pool_max
        self._endpoint_name = endpoint_name
        self._pool: asyncpg.Pool | None = None
        self._refresh_task: asyncio.Task | None = None
        self._pg_user: str | None = None
        self._pg_password: str | None = None
        self._uses_static_creds = False

    async def initialize(self) -> None:
        """Generate credentials and create the connection pool.

        Credential resolution order:
        1. PGUSER + PGPASSWORD both set → static credentials (no refresh).
        2. Otherwise → SDK-based OAuth token generation (auto-refresh).
           PGUSER is still respected as the username if set.
        """
        if not self._pg_host:
            raise RuntimeError("Lakebase host not configured. Set PGHOST env var.")

        if os.getenv("PGUSER") and os.getenv("PGPASSWORD"):
            self._pg_user = os.getenv("PGUSER")
            self._pg_password = os.getenv("PGPASSWORD")
            self._uses_static_creds = True
        else:
            self._pg_user, self._pg_password = await asyncio.to_thread(
                _get_lakebase_credentials, self._endpoint_name
            )
            self._uses_static_creds = False

        ssl_mode = os.getenv("PGSSLMODE", "require")
        self._pool = await asyncpg.create_pool(
            host=self._pg_host,
            database=self._pg_database,
            user=self._pg_user,
            password=self._pg_password,
            port=self._pg_port,
            min_size=self._pool_min,
            max_size=self._pool_max,
            ssl=ssl_mode,
            # Pool acquire + connect wait (asyncpg pops ``timeout`` for both).
            timeout=LAKEBASE_CONNECT_TIMEOUT_SECONDS,
            command_timeout=LAKEBASE_COMMAND_TIMEOUT_SECONDS,
        )
        logger.info(
            "Lakebase pool created — host=%s db=%s user=%s static=%s",
            self._pg_host,
            self._pg_database,
            self._pg_user,
            self._uses_static_creds,
        )

        if not self._uses_static_creds:
            self._refresh_task = asyncio.create_task(self._token_refresh_loop())

    async def _token_refresh_loop(self) -> None:
        """Periodically refresh the OAuth token and recreate the pool."""
        while True:
            await asyncio.sleep(50 * 60)  # 50 minutes
            try:
                self._pg_user, self._pg_password = await asyncio.to_thread(
                    _get_lakebase_credentials, self._endpoint_name
                )
                old_pool = self._pool
                ssl_mode = os.getenv("PGSSLMODE", "require")
                self._pool = await asyncpg.create_pool(
                    host=self._pg_host,
                    database=self._pg_database,
                    user=self._pg_user,
                    password=self._pg_password,
                    port=self._pg_port,
                    min_size=self._pool_min,
                    max_size=self._pool_max,
                    ssl=ssl_mode,
                    timeout=LAKEBASE_CONNECT_TIMEOUT_SECONDS,
                    command_timeout=LAKEBASE_COMMAND_TIMEOUT_SECONDS,
                )
                if old_pool:
                    await old_pool.close()
                logger.info("Lakebase token refreshed and pool recreated")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Token refresh failed: %s", exc)

    async def close(self) -> None:
        """Cancel refresh task and close the pool."""
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        if self._pool:
            await self._pool.close()
            self._pool = None
        logger.info("Lakebase pool closed")

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a connection from the pool."""
        if self._pool is None:
            raise RuntimeError("Pool not initialized — call initialize() first")
        async with self._pool.acquire() as conn:
            yield conn

    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        """Execute a query and return all rows."""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> asyncpg.Record | None:
        """Execute a query and return a single row."""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        """Execute a query and return the first column of the first row (or None)."""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args) -> str:
        """Execute a statement (INSERT, UPDATE, DDL, etc.)."""
        async with self.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args: list) -> int:
        """Batch-execute a statement with multiple tuples of parameters.

        Wraps the call in an explicit transaction so rows actually commit.
        asyncpg's ``executemany`` runs inside an implicit transaction that
        is rolled back when the connection is released to the pool unless
        explicitly committed — leading to silent data loss.

        Returns the COUNT(*) seen on this same connection immediately after
        insert, inside the same transaction, for verification.
        """
        async with self.acquire() as conn, conn.transaction():
            await conn.executemany(query, args)
            # In-transaction sanity check. If this returns 0 after an INSERT,
            # the INSERT statement is a no-op (e.g., WHERE clause false,
            # wrong table, trigger drops rows).
            try:
                # Match the INSERT target — assumes INSERT INTO sensor_readings
                if "sensor_readings" in query.lower():
                    n = await conn.fetchval("SELECT COUNT(*) FROM sensor_readings")
                    return int(n or 0)
            except Exception:
                pass
            return -1

    async def ensure_pipelines_table(self) -> None:
        """Create the pipelines table if it does not exist.

        If the table already exists but is owned by another role (e.g.
        after migrating from static-password auth to OAuth SP auth),
        the DDL will raise InsufficientPrivilegeError.  We catch that
        and continue — DML still works via GRANT / role membership.
        """
        try:
            ddl = """
            CREATE TABLE IF NOT EXISTS pipelines (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_email TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                industry TEXT,
                sensor_config JSONB DEFAULT '[]'::jsonb,
                source_type TEXT,
                source_config JSONB DEFAULT '{}'::jsonb,
                sink_config JSONB DEFAULT '{}'::jsonb,
                resources JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
            await self.execute(ddl)
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipelines_user ON pipelines (user_email)"
            )
        except asyncpg.InsufficientPrivilegeError:
            logger.info("pipelines table exists (owned by another role) — skipping DDL, DML OK")
        logger.info("Ensured pipelines table exists")

    async def ensure_user_settings_table(self) -> None:
        """Create the user_settings table if it does not exist.

        See ensure_pipelines_table for ownership notes.
        """
        try:
            ddl = """
            CREATE TABLE IF NOT EXISTS user_settings (
                user_email TEXT PRIMARY KEY,
                settings JSONB DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
            await self.execute(ddl)
        except asyncpg.InsufficientPrivilegeError:
            logger.info("user_settings table exists (owned by another role) — skipping DDL, DML OK")
        logger.info("Ensured user_settings table exists")

    async def ensure_source_bindings_table(self) -> None:
        """Create the source_bindings table if it does not exist.

        This backs the source-claim feature (one streaming source ↔ one active
        pipeline).  Without it, ``check_source_available`` / ``claim_source``
        raise UndefinedTableError on every POST that carries a source_type,
        surfacing as an unhandled 500.  See ensure_pipelines_table for
        ownership notes.
        """
        from iot_twin.models.sql_ddl import SOURCE_BINDINGS_DDL

        try:
            await self.execute(SOURCE_BINDINGS_DDL)
        except asyncpg.InsufficientPrivilegeError:
            logger.info(
                "source_bindings table exists (owned by another role) — skipping DDL, DML OK"
            )
        logger.info("Ensured source_bindings table exists")
