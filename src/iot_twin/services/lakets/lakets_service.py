"""LakeTS bootstrap — install toolkit into a Lakebase instance and probe version.

LakeTS is a PL/pgSQL-only toolkit; no Postgres extension is required. Installing
it is a single `psql -f dist/lakets.sql` equivalent against the Lakebase pool.
The SQL registers its own version in ``lakets._version``.

Source resolution precedence (in order):
1. explicit ``sql_path`` argument
2. ``LAKETS_SQL_PATH`` env var — absolute path to ``lakets.sql``
3. ``LAKETS_GIT_URL`` env var — git repo to shallow-clone at provision time
4. the ``lakets.sql`` packaged with ``iot_twin`` (resolved relative to the package)
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from iot_twin.core.paths import LAKETS_SQL_PATH as PACKAGED_LAKETS_SQL
from iot_twin.models.sql_ddl import LAKETS_VERSION_CHECK_SQL

logger = logging.getLogger("iot_digital_twin.services.lakets")


_CLONE_TIMEOUT_SEC = 60
_DIST_RELPATH = Path("dist") / "lakets.sql"


async def get_installed_version(pool: Any) -> str | None:
    """Return the latest installed LakeTS version, or None if not installed."""
    try:
        return await pool.fetchval(LAKETS_VERSION_CHECK_SQL)
    except Exception as exc:
        logger.debug("LakeTS version probe failed (not installed?): %s", exc)
        return None


def resolve_lakets_source(explicit_path: str | None = None) -> Path:
    """Resolve a path to ``lakets.sql`` from arg, env, or git clone.

    Raises:
        FileNotFoundError: explicit path was supplied but does not exist.
        RuntimeError: no source is configured at all.
    """
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"LakeTS SQL not found at {path}")
        return path

    env_path = os.getenv("LAKETS_SQL_PATH")
    if env_path:
        path = Path(env_path)
        if not path.exists():
            raise FileNotFoundError(f"LakeTS SQL not found at {path} (LAKETS_SQL_PATH)")
        return path

    git_url = os.getenv("LAKETS_GIT_URL")
    if git_url:
        clone_dir = Path(tempfile.mkdtemp(prefix="lakets-"))
        try:
            _git_clone(git_url, clone_dir)
        except Exception:
            shutil.rmtree(clone_dir, ignore_errors=True)
            raise
        resolved = clone_dir / _DIST_RELPATH
        if not resolved.exists():
            raise FileNotFoundError(f"Cloned repo {git_url} does not contain {_DIST_RELPATH}")
        return resolved

    if PACKAGED_LAKETS_SQL.exists():
        return PACKAGED_LAKETS_SQL

    raise RuntimeError("LakeTS source not configured — set LAKETS_SQL_PATH or LAKETS_GIT_URL")


def _git_clone(url: str, dest: Path) -> None:
    """Shallow-clone ``url`` into ``dest``. Kept as a separate function for mocking."""
    subprocess.run(
        ["git", "clone", "--depth", "1", url, str(dest)],
        check=True,
        timeout=_CLONE_TIMEOUT_SEC,
        capture_output=True,
    )


async def install_lakets(
    pool: Any,
    sql_path: Path,
    *,
    skip_if_installed: bool = False,
) -> str:
    """Execute ``lakets.sql`` against the pool and return the installed version.

    Args:
        pool: asyncpg pool or anything exposing ``.execute`` / ``.fetchval`` coroutines.
        sql_path: path to ``dist/lakets.sql``.
        skip_if_installed: if True and LakeTS is already present, skip execution
            and just return the existing version (idempotent fast path).

    Raises:
        FileNotFoundError: sql_path does not exist.
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"LakeTS SQL not found at {sql_path}")

    if skip_if_installed:
        existing = await get_installed_version(pool)
        if existing:
            logger.info("LakeTS %s already installed — skipping execution", existing)
            return existing

    sql_text = sql_path.read_text(encoding="utf-8")
    logger.info("Installing LakeTS from %s (%d bytes)", sql_path, len(sql_text))
    await pool.execute(sql_text)

    version = await get_installed_version(pool)
    if not version:
        raise RuntimeError(
            "LakeTS install completed but lakets._version is empty — check for SQL errors"
        )
    logger.info("LakeTS %s installed", version)
    return version


# ---------------------------------------------------------------------------
# Per-endpoint idempotent install (used by provisioning + heal endpoints)
# ---------------------------------------------------------------------------


async def install_lakets_if_missing(
    endpoint_name: str,
    database: str = "databricks_postgres",
) -> dict:
    """Install LakeTS on the given Lakebase endpoint if not already present.

    Generates an OAuth credential for ``endpoint_name`` via the Databricks
    SDK, opens an asyncpg connection to ``database``, and checks
    ``lakets._version``.  If the toolkit is already installed, returns
    ``{"status": "already_exists", "version": "..."}``.  Otherwise reads
    the SQL from ``LAKETS_SQL_PATH`` (or ``LAKETS_GIT_URL``), executes it,
    and re-verifies the version.

    Returns a single result dict compatible with the other provisioning
    helpers:
        {"resource": "lakets extension", "status": "...", "version": "...", "error": "..."}

    Never raises — all failures are surfaced in the ``error`` field.  Safe
    to call multiple times.
    """
    resource = f"LakeTS on {endpoint_name}"
    result: dict = {"resource": resource, "status": "failed"}

    try:
        import asyncpg
        from databricks.sdk import WorkspaceClient
    except Exception as exc:
        result["error"] = f"import failure: {exc}"[:200]
        return result

    try:
        w = WorkspaceClient()
        # Host resolution with REST fallback for older SDKs.
        try:
            endpoint = w.postgres.get_endpoint(name=endpoint_name)
            pg_host = endpoint.status.hosts.host
        except AttributeError:
            resp = w.api_client.do("GET", f"/api/2.0/{endpoint_name}")
            pg_host = resp["status"]["hosts"]["host"]

        # Credential with REST fallback.
        try:
            cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
            pg_token = cred.token
        except AttributeError:
            cred_resp = w.api_client.do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": endpoint_name},
            )
            pg_token = cred_resp["token"]

        pg_user = w.current_user.me().user_name
    except Exception as exc:
        result["error"] = f"credential resolution failed: {exc}"[:200]
        return result

    try:
        conn = await asyncpg.connect(
            host=pg_host,
            database=database,
            user=pg_user,
            password=pg_token,
            port=5432,
            ssl="require",
        )
    except Exception as exc:
        result["error"] = f"connect failed: {exc}"[:200]
        return result

    try:
        # Step 1: probe the version.  ``get_installed_version`` returns None
        # if the table doesn't exist yet.
        existing = await get_installed_version(conn)
        if existing:
            return {
                "resource": resource,
                "status": "already_exists",
                "version": existing,
            }

        # Step 2: resolve the source SQL — prefers LAKETS_SQL_PATH, then
        # LAKETS_GIT_URL.  Errors bubble up as a "failed" result.
        try:
            sql_path = resolve_lakets_source(None)
        except Exception as exc:
            return {
                "resource": resource,
                "status": "failed",
                "error": f"cannot resolve LakeTS SQL source: {exc}"[:200],
            }

        # Step 3: execute the SQL.
        try:
            sql_text = sql_path.read_text(encoding="utf-8")
            await conn.execute(sql_text)
        except Exception as exc:
            return {
                "resource": resource,
                "status": "failed",
                "error": f"SQL execution failed: {exc}"[:200],
            }

        # Step 4: re-verify.
        version = await get_installed_version(conn)
        if not version:
            return {
                "resource": resource,
                "status": "failed",
                "error": "LakeTS install completed but _version is empty",
            }
        return {
            "resource": resource,
            "status": "created",
            "version": version,
        }
    finally:
        try:
            await conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# ChronoTable + tier-policy helpers (Phase 2)
# ---------------------------------------------------------------------------
_CHRONOTABLE_PROBE_SQL = (
    "SELECT id FROM lakets._chronotable_registry WHERE schema_name = $1 AND table_name = $2"
)
_CHRONOTABLE_CREATE_SQL = "SELECT lakets.create_chronotable($1, $2, $3::interval, $4)"
_POLICY_PROBE_SQL = (
    "SELECT p.id FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
    "WHERE c.schema_name = $1 AND c.table_name = $2 "
    "  AND p.policy_type = $3"
)
_TIERED_RETENTION_APPLY_SQL = (
    "SELECT lakets.add_tiered_retention_policy($1, $2::interval, $3::interval, $4)"
)


async def create_chronotable(
    pool: Any,
    *,
    table: str,
    time_col: str,
    chunk: str,
    schema: str = "public",
) -> bool:
    """Register ``table`` as a LakeTS ChronoTable. Idempotent.

    Returns ``True`` if the table was already a ChronoTable (no-op),
    ``False`` if it was created.
    """
    existing_id = await pool.fetchval(_CHRONOTABLE_PROBE_SQL, schema, table)
    if existing_id is not None:
        logger.info("ChronoTable %s.%s already registered (id=%s)", schema, table, existing_id)
        return True

    new_id = await pool.fetchval(
        _CHRONOTABLE_CREATE_SQL,
        table,
        time_col,
        _interval_to_timedelta(chunk),
        schema,
    )
    logger.info(
        "ChronoTable %s.%s created (id=%s, time=%s, chunk=%s)",
        schema,
        table,
        new_id,
        time_col,
        chunk,
    )
    return False


async def apply_tier_policy(
    pool: Any,
    *,
    table: str,
    hot: str,
    drop: str,
    schema: str = "public",
) -> bool:
    """Apply a tiered-retention policy. Data tiers from hot (Postgres) to
    cold (Delta) after ``hot`` elapses; Postgres copy is dropped after ``drop``.

    Both ``hot`` and ``drop`` are Postgres interval strings (e.g. ``"1 hour"``,
    ``"7 days"``).  Raises ``ValueError`` if ``hot >= drop`` (redundant with
    LakeTS' own server-side check, surfaced early with a clearer message).

    Returns ``True`` if a policy already existed (no-op), ``False`` if applied.
    """
    if hot == drop or not _interval_lt(hot, drop):
        raise ValueError(f"hot ({hot}) must be less than drop ({drop})")

    existing_id = await pool.fetchval(
        _POLICY_PROBE_SQL,
        schema,
        table,
        "tiered_retention",
    )
    if existing_id is not None:
        logger.info(
            "Tiered retention policy on %s.%s already exists (id=%s)",
            schema,
            table,
            existing_id,
        )
        return True

    new_id = await pool.fetchval(
        _TIERED_RETENTION_APPLY_SQL,
        table,
        _interval_to_timedelta(hot),
        _interval_to_timedelta(drop),
        schema,
    )
    logger.info(
        "Tiered retention policy applied on %s.%s (id=%s, hot=%s, drop=%s)",
        schema,
        table,
        new_id,
        hot,
        drop,
    )
    return False


# ---------------------------------------------------------------------------
# RollUp creation (Phase 3) — incrementally-maintained time-bucketed aggregates
# over a ChronoTable.  Wraps the vendored SQL:
#   lakets.create_rollup(p_name, p_query, p_bucket_interval,
#                        p_source_table, p_source_schema, p_depends_on)
# The toolkit takes a full SELECT in ``p_query``; this helper builds that query
# from a high-level ``aggregates`` spec so callers don't hand-roll SQL.
# ---------------------------------------------------------------------------
_ROLLUP_PROBE_SQL = "SELECT id, query_text FROM lakets._rollup_registry WHERE name = $1"
_ROLLUP_CREATE_SQL = "SELECT lakets.create_rollup($1, $2, $3::interval, $4, $5)"


# Aggregate function names the rollup editor offers as bare strings. A bare
# name (e.g. "avg") means "apply this function to the metric column", which the
# UI does not spell out — it sends just ["avg", "min", "max"]. Without this
# mapping those land in the SELECT verbatim ("…, avg, min, max …") and Postgres
# fails with `column "avg" does not exist`.
_KNOWN_AGG_FNS = {"avg", "min", "max", "sum", "count", "stddev_samp"}
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_sql_identifier(identifier: str) -> str:
    if not isinstance(identifier, str) or not _SQL_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return identifier


def _aggregate_to_sql(agg: Any, value_column: str = "value") -> str:
    """Map one aggregate definition to a SQL select-list fragment.

    Accepts:
      * a dict ``{"fn": "avg", "column": "value", "as": "avg_v"}`` (the ``as``
        alias is optional and defaults to ``<fn>_<column>``);
      * a bare aggregate function name from the UI (``"avg"``, ``"min"``,
        ``"max"``, ``"sum"``, ``"count"``), expanded to
        ``<fn>(<value_column>) AS <fn>_<value_column>`` (``count`` →
        ``count(*) AS count_<value_column>``);
      * unknown strings are rejected. A top-level raw SQL select-list remains
        available through ``create_rollup`` for trusted internal callers.
    """
    if isinstance(agg, str):
        token = agg.strip()
        fn = token.lower()
        if fn in _KNOWN_AGG_FNS:
            if fn == "count":
                return f"count(*) AS count_{value_column}"
            return f"{fn}({value_column}) AS {fn}_{value_column}"
        raise ValueError(f"unsupported aggregate function: {token!r}")
    if not isinstance(agg, dict):
        raise ValueError(f"unsupported aggregate spec: {agg!r}")
    fn = agg.get("fn")
    column = agg.get("column")
    if not fn or not column:
        raise ValueError(f"aggregate spec needs 'fn' and 'column': {agg!r}")
    fn = _validate_sql_identifier(fn).lower()
    if fn not in _KNOWN_AGG_FNS:
        raise ValueError(f"unsupported aggregate function: {fn!r}")
    column = _validate_sql_identifier(column)
    alias = agg.get("as") or f"{fn}_{column}"
    alias = _validate_sql_identifier(alias)
    if fn == "count":
        return f"count(*) AS {alias}"
    return f"{fn}({column}) AS {alias}"


def _build_rollup_query(
    *,
    table: str,
    schema: str,
    bucket: str,
    time_col: str,
    aggregates: Any,
    value_column: str = "value",
    group_columns: list[str] | None = None,
) -> str:
    """Compose the time-bucketed aggregation SELECT for ``create_rollup``.

    ``aggregates`` is either a list of aggregate specs (dict/bare-fn/raw-str)
    or a single raw SQL select-list string.  The first column is always the
    time bucket.
    """
    if isinstance(aggregates, str):
        agg_sql = aggregates
    else:
        agg_sql = ", ".join(_aggregate_to_sql(a, value_column) for a in aggregates)

    dimensions = [_validate_sql_identifier(column) for column in (group_columns or [])]
    bucket_expr = f"lakets.time_bucket('{bucket}'::interval, {time_col}) AS bucket"
    select_list = ", ".join([bucket_expr, *dimensions, agg_sql])
    group_by = ", ".join(["bucket", *dimensions])
    return f"SELECT {select_list} FROM {schema}.{table} GROUP BY {group_by}"


def _normalize_sql(sql: str) -> str:
    normalized = re.sub(r"\s+", " ", sql.strip().lower())
    return re.sub(r"\s*([(),])\s*", r"\1", normalized)


def _rollup_definition_matches(expected: str, existing: str) -> bool:
    return _normalize_sql(expected) == _normalize_sql(existing)


def _rollup_mismatch_result(resource: str, name: str) -> dict:
    return {
        "resource": resource,
        "status": "failed",
        "error": (f"existing RollUp {name} definition mismatch; drop or migrate it explicitly"),
    }


async def create_rollup(
    pool: Any,
    *,
    table: str,
    name: str,
    bucket: str,
    aggregates: Any,
    schema: str = "public",
    time_col: str = "event_time",
    value_column: str = "value",
    group_columns: list[str] | None = None,
) -> dict:
    """Create a LakeTS RollUp over ``schema.table``. Idempotent. Never raises.

    Args:
        table: source ChronoTable name.
        name: rollup name (also the registry key).
        bucket: Postgres interval string for the time bucket (e.g. ``"1 hour"``).
        aggregates: either a list of aggregate specs — each a dict like
            ``{"fn": "avg", "column": "value", "as": "avg_v"}`` (``as`` optional)
            — or a raw SQL select-list string (e.g. ``"avg(value) AS v"``).
            Raw select lists are a trust boundary for internal configuration
            only; list entries are restricted to the aggregate allowlist.
        schema: source schema (default ``public``); passed as ``p_source_schema``.
        time_col: time column on the source table used for bucketing.

    Returns ``{"resource", "status"}`` where status is
    ``created`` | ``already_exists`` | ``failed`` (with ``error`` on failure).
    """
    resource = f"LakeTS rollup {name}"
    try:
        _validate_sql_identifier(table)
        _validate_sql_identifier(schema)
        _validate_sql_identifier(time_col)
        _validate_sql_identifier(value_column)
        _interval_seconds(bucket)
        query = _build_rollup_query(
            table=table,
            schema=schema,
            bucket=bucket,
            time_col=time_col,
            aggregates=aggregates,
            value_column=value_column,
            group_columns=group_columns,
        )
        existing = await pool.fetchrow(_ROLLUP_PROBE_SQL, name)
        if existing is not None:
            if not _rollup_definition_matches(query, existing["query_text"]):
                return _rollup_mismatch_result(resource, name)
            logger.info("RollUp %s already exists (id=%s)", name, existing["id"])
            return {"resource": resource, "status": "already_exists"}

        rollup_id = await pool.fetchval(
            _ROLLUP_CREATE_SQL,
            name,
            query,
            _interval_to_timedelta(bucket),
            table,
            schema,
        )
        logger.info("RollUp %s created (id=%s)", name, rollup_id)
        return {"resource": resource, "status": "created"}
    except Exception as exc:
        if any(hint in str(exc).lower() for hint in ("already exists", "duplicate", "unique")):
            try:
                existing = await pool.fetchrow(_ROLLUP_PROBE_SQL, name)
                if existing is not None and _rollup_definition_matches(
                    query, existing["query_text"]
                ):
                    return {"resource": resource, "status": "already_exists"}
                if existing is not None:
                    return _rollup_mismatch_result(resource, name)
            except Exception:
                pass
        logger.warning("create_rollup %s failed: %s", name, exc)
        return {"resource": resource, "status": "failed", "error": str(exc)[:200]}


_ROLLUP_INVALIDATION_PROBE_SQL = """
SELECT bool_and(trigger_exists)
FROM (
    SELECT trigger_name,
           EXISTS (
               SELECT 1
               FROM pg_trigger t
               JOIN pg_class rel ON rel.oid = t.tgrelid
               JOIN pg_namespace ns ON ns.oid = rel.relnamespace
               WHERE ns.nspname = c.schema_name
                 AND rel.relname = c.table_name
                 AND t.tgname = trigger_name
                 AND NOT t.tgisinternal
           ) AS trigger_exists
    FROM lakets._rollup_registry r
    JOIN lakets._chronotable_registry c ON c.id = r.source_chronotable_id
    CROSS JOIN (
        VALUES
            ('trg_lakets_rollup_invalidation'),
            ('trg_lakets_rollup_bulk_invalidation')
    ) AS expected(trigger_name)
    WHERE r.name = $1
) probes
"""
_ENABLE_ROLLUP_INVALIDATION_SQL = "SELECT lakets.enable_rollup_invalidation($1)"
_LVC_PROBE_SQL = """
SELECT l.id, l.key_columns, l.value_columns, l.enabled
FROM lakets._lvc_registry l
JOIN lakets._chronotable_registry c ON c.id = l.chronotable_id
WHERE c.schema_name = $1 AND c.table_name = $2
"""
_ENABLE_LVC_SQL = "SELECT lakets.enable_lvc($1, $2::text[], $3::text[], $4)"


async def enable_rollup_invalidation(pool: Any, name: str) -> dict:
    """Enable invalidation triggers for a registered RollUp. Never raises."""
    resource = f"LakeTS invalidation {name}"
    try:
        already_enabled = bool(await pool.fetchval(_ROLLUP_INVALIDATION_PROBE_SQL, name))
        # The vendored function uses CREATE OR REPLACE for parent invalidation
        # triggers and every active partition's touch trigger. Calling it on
        # every setup also repairs partitions created after the previous run.
        await pool.fetchval(_ENABLE_ROLLUP_INVALIDATION_SQL, name)
        return {
            "resource": resource,
            "status": "already_exists" if already_enabled else "created",
        }
    except Exception as exc:
        logger.warning("RollUp invalidation setup failed for %s: %s", name, exc)
        return {"resource": resource, "status": "failed", "error": str(exc)[:200]}


async def enable_lvc(
    pool: Any,
    *,
    table: str,
    key_columns: list[str],
    value_columns: list[str],
    schema: str = "public",
) -> dict:
    """Enable Last Value Cache for a registered ChronoTable. Never raises."""
    resource = f"LakeTS LVC {schema}.{table}"
    try:
        _validate_sql_identifier(schema)
        _validate_sql_identifier(table)
        validated_keys = [_validate_sql_identifier(column) for column in key_columns]
        validated_values = [_validate_sql_identifier(column) for column in value_columns]
        if not validated_keys:
            raise ValueError("key_columns must not be empty")

        existing = await pool.fetchrow(_LVC_PROBE_SQL, schema, table)
        if existing is not None:
            existing_keys = list(existing["key_columns"] or [])
            existing_values = list(existing["value_columns"] or [])
            if (
                existing_keys != validated_keys
                or existing_values != validated_values
                or not existing["enabled"]
            ):
                return {
                    "resource": resource,
                    "status": "failed",
                    "error": (
                        "existing LVC definition mismatch: "
                        f"enabled={bool(existing['enabled'])}, "
                        f"keys={existing_keys}, values={existing_values}; "
                        f"requested keys={validated_keys}, values={validated_values}"
                    )[:200],
                }
            return {"resource": resource, "status": "already_exists"}

        await pool.fetchval(
            _ENABLE_LVC_SQL,
            table,
            validated_keys,
            validated_values,
            schema,
        )
        return {"resource": resource, "status": "created"}
    except Exception as exc:
        if any(hint in str(exc).lower() for hint in ("already enabled", "duplicate", "unique")):
            try:
                existing = await pool.fetchrow(_LVC_PROBE_SQL, schema, table)
                if existing is not None:
                    existing_keys = list(existing["key_columns"] or [])
                    existing_values = list(existing["value_columns"] or [])
                    if (
                        existing["enabled"]
                        and existing_keys == validated_keys
                        and existing_values == validated_values
                    ):
                        return {"resource": resource, "status": "already_exists"}
                    return {
                        "resource": resource,
                        "status": "failed",
                        "error": (
                            "existing LVC definition mismatch after concurrent setup: "
                            f"enabled={bool(existing['enabled'])}, "
                            f"keys={existing_keys}, values={existing_values}"
                        )[:200],
                    }
            except Exception:
                pass
        logger.warning("LVC setup failed for %s.%s: %s", schema, table, exc)
        return {"resource": resource, "status": "failed", "error": str(exc)[:200]}


# Approximate ordering of Postgres interval strings for client-side validation.
# LakeTS enforces the same rule server-side — this just fails fast with a clearer error.
_UNIT_SECONDS = {
    "second": 1,
    "seconds": 1,
    "sec": 1,
    "minute": 60,
    "minutes": 60,
    "min": 60,
    "hour": 3600,
    "hours": 3600,
    "day": 86400,
    "days": 86400,
    "week": 604800,
    "weeks": 604800,
    "month": 2592000,
    "months": 2592000,  # approximate — 30 days
    "year": 31536000,
    "years": 31536000,  # approximate — 365 days
}


def _interval_lt(a: str, b: str) -> bool:
    """Return True iff Postgres interval string ``a`` is strictly less than ``b``."""
    return _interval_seconds(a) < _interval_seconds(b)


def _interval_seconds(s: str) -> float:
    """Rough conversion of a Postgres interval string to seconds."""
    parts = s.strip().split()
    if len(parts) != 2:
        raise ValueError(f"expected '<n> <unit>', got {s!r}")
    n, unit = parts
    seconds_per_unit = _UNIT_SECONDS.get(unit.lower())
    if seconds_per_unit is None:
        raise ValueError(f"unknown interval unit: {unit!r}")
    return float(n) * seconds_per_unit


# ---------------------------------------------------------------------------
# Shadow-sync enablement (enable_sync) — prepares the unpartitioned shadow in
# the lakets_cdf schema (REPLICA IDENTITY FULL) + mirror trigger, REQUIRED
# before Lakebase CDF / cold-tiering can stream.  The managed sync itself is
# still enabled once via the Databricks UI on the lakets_cdf schema; this only
# prepares the shadow.  Signature mirrors the vendored SQL:
#   lakets.enable_sync(p_table_name TEXT, p_schema_name TEXT DEFAULT 'public')
# ---------------------------------------------------------------------------
_SYNC_STATE_PROBE_SQL = (
    "SELECT id, sync_enabled FROM lakets._chronotable_registry "
    "WHERE schema_name = $2 AND table_name = $1"
)
_ENABLE_SYNC_SQL = "SELECT lakets.enable_sync($1, $2)"
_SHADOW_STATE_SQL = (
    "SELECT shadow_table_name, sync_enabled "
    "FROM lakets._chronotable_registry "
    "WHERE schema_name = $1 AND table_name = $2"
)
_LEGACY_SENSOR_SHADOW = "_shadow_sensor_readings"
_CDF_BACKFILL_MARKER = "lakets-cdf-backfill-complete:v1"
_LEGACY_SHADOW_EXISTS_SQL = "SELECT to_regclass('lakets_cdf._shadow_sensor_readings') IS NOT NULL"
_TARGET_MIGRATION_STATE_SQL = """
SELECT
    obj_description(to_regclass($1), 'pg_class') = $2 AS backfill_complete,
    EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class index_class ON index_class.oid = i.indexrelid
        WHERE i.indrelid = to_regclass($1)
          AND index_class.relname = $3
          AND i.indisunique
          AND i.indisvalid
          AND i.indisready
          AND i.indnkeyatts = 1
          AND i.indnatts = 1
          AND (
              SELECT a.attname
              FROM pg_attribute a
              WHERE a.attrelid = i.indrelid
                AND a.attnum = i.indkey[0]
          ) = 'id'
    ) AS index_valid
"""
_SENSOR_ID_INVARIANT_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'sensor_readings'
      AND a.attname = 'id'
      AND a.atttypid = 'bigint'::regtype
      AND a.attnotnull
      AND NOT a.attisdropped
)
"""
_LEGACY_SCHEMA_MATCH_SQL = """
WITH columns AS (
    SELECT n.nspname,
           c.relname,
           array_agg(
               ROW(a.attname, a.atttypid, a.atttypmod, a.attnotnull)
               ORDER BY a.attnum
           ) AS signature
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND (
          (n.nspname = 'public' AND c.relname = 'sensor_readings')
          OR
          (n.nspname = 'lakets_cdf' AND c.relname = '_shadow_sensor_readings')
      )
    GROUP BY n.nspname, c.relname
)
SELECT (
    SELECT signature FROM columns
    WHERE nspname = 'public' AND relname = 'sensor_readings'
) = (
    SELECT signature FROM columns
    WHERE nspname = 'lakets_cdf' AND relname = '_shadow_sensor_readings'
)
"""

# Substrings that mean the shadow / sync is already in place — treated as
# success (already_exists), never as a failure.
_ALREADY_SYNCED_HINTS = ("already", "exists", "duplicate")


async def enable_sync(pool: Any, table: str, schema: str = "public") -> dict:
    """Prepare the LakeTS shadow table + mirror trigger for ``schema.table``.

    Runs ``SELECT lakets.enable_sync(table, schema)``, which creates the
    unpartitioned shadow in the ``lakets_cdf`` schema (REPLICA IDENTITY FULL)
    and the mirror trigger on the source.  Idempotent: if the ChronoTable's
    registry row already has ``sync_enabled = TRUE`` the call is skipped, and
    any "already exists" race from the SQL is swallowed.

    Never raises — all outcomes are returned as a result dict:
        {"resource": "...", "status": "created|already_exists|failed", "error": ...}
    """
    resource = f"LakeTS sync on {schema}.{table}"

    # Idempotency fast-path: if the registry already records sync_enabled,
    # there's nothing to do.
    try:
        row = await pool.fetchrow(_SYNC_STATE_PROBE_SQL, table, schema)
    except Exception as exc:
        logger.debug("enable_sync registry probe failed for %s.%s: %s", schema, table, exc)
        row = None

    if row is not None and row.get("sync_enabled"):
        logger.info("LakeTS sync on %s.%s already enabled", schema, table)
        return {"resource": resource, "status": "already_exists"}

    try:
        await pool.fetchval(_ENABLE_SYNC_SQL, table, schema)
    except Exception as exc:
        msg = str(exc)
        if any(hint in msg.lower() for hint in _ALREADY_SYNCED_HINTS):
            logger.info("LakeTS sync on %s.%s already present (%s)", schema, table, msg[:80])
            return {"resource": resource, "status": "already_exists"}
        logger.warning("LakeTS enable_sync on %s.%s failed: %s", schema, table, exc)
        return {"resource": resource, "status": "failed", "error": msg[:200]}

    logger.info("LakeTS sync enabled on %s.%s", schema, table)
    return {"resource": resource, "status": "created"}


async def reconcile_chronotable_shadow(
    pool: Any,
    *,
    shadow_table: str,
    table: str = "sensor_readings",
    schema: str = "public",
    shadow_schema: str = "lakets_cdf",
) -> dict:
    """Online-cut over append-only ``public.sensor_readings`` to a unique shadow.

    ``sensor_readings.id`` is a monotonically increasing, immutable SSOT key.
    A short writer lock switches the trigger before any history copy. New rows
    then flow directly to the target while conflict-protected copies merge the
    legacy shadow and current source without blocking writers. Generic user
    UPDATE/DELETE history during legacy backfill is unsupported; validation
    rows are excluded from the legacy copy so deleted test data is not revived.

    A durable table marker is written only after both copies complete. Retries
    after an interrupted cutover repeat the idempotent copies, while completed
    migrations return without writes or locks.
    """
    resource = f"LakeTS sync on {schema}.{table}"
    try:
        for identifier in (shadow_table, table, schema, shadow_schema):
            _validate_sql_identifier(identifier)
        if schema != "public" or table != "sensor_readings" or shadow_schema != "lakets_cdf":
            raise RuntimeError(
                "online CDF migration only supports append-only public.sensor_readings"
            )
        state = await pool.fetchrow(_SHADOW_STATE_SQL, schema, table)
        if state is None:
            raise RuntimeError(f"{schema}.{table} is not a registered ChronoTable")
        previous_shadow = state.get("shadow_table_name")
        if previous_shadow not in (None, _LEGACY_SENSOR_SHADOW, shadow_table):
            raise RuntimeError(
                f"{schema}.{table} is already bound to unexpected shadow {previous_shadow!r}"
            )

        target_relation = f"{shadow_schema}.{shadow_table}"
        target_index = f"{shadow_table}_id_uidx"
        if previous_shadow == shadow_table and state.get("sync_enabled"):
            migration_state = await pool.fetchrow(
                _TARGET_MIGRATION_STATE_SQL,
                target_relation,
                _CDF_BACKFILL_MARKER,
                target_index,
            )
            if (
                migration_state
                and migration_state.get("backfill_complete")
                and migration_state.get("index_valid")
            ):
                return {
                    "resource": resource,
                    "status": "already_exists",
                    "shadow_table": target_relation,
                    "previous_shadow": previous_shadow,
                    "copy_sources": [],
                }

        if not await pool.fetchval(_SENSOR_ID_INVARIANT_SQL):
            raise RuntimeError("public.sensor_readings must have an immutable, non-null BIGINT id")
        legacy_exists = bool(await pool.fetchval(_LEGACY_SHADOW_EXISTS_SQL))
        if legacy_exists and not await pool.fetchval(_LEGACY_SCHEMA_MATCH_SQL):
            raise RuntimeError("legacy CDF shadow columns do not match public.sensor_readings")
        copy_sources = []
        if legacy_exists:
            copy_sources.append(f"{shadow_schema}.{_LEGACY_SENSOR_SHADOW}")
        copy_sources.append(f"{schema}.{table}")
        await pool.execute(
            f"SELECT lakets._build_shadow_table('{schema}', '{table}', '{shadow_table}')"
        )
        await pool.execute(f"COMMENT ON TABLE {target_relation} IS NULL")
        migration_state = await pool.fetchrow(
            _TARGET_MIGRATION_STATE_SQL,
            target_relation,
            _CDF_BACKFILL_MARKER,
            target_index,
        )
        if not migration_state or not migration_state.get("index_valid"):
            await pool.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {shadow_schema}.{target_index}")
            await pool.execute(
                f"CREATE UNIQUE INDEX CONCURRENTLY {target_index} ON {target_relation} (id)"
            )
            migration_state = await pool.fetchrow(
                _TARGET_MIGRATION_STATE_SQL,
                target_relation,
                _CDF_BACKFILL_MARKER,
                target_index,
            )
            if not migration_state or not migration_state.get("index_valid"):
                raise RuntimeError("unique CDF shadow id index is not valid")
        cutover_sql = f"""
DO $reconcile$
DECLARE
    current_shadow TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('lakets-cdf:{schema}.{table}', 0)
    );
    SELECT shadow_table_name
      INTO current_shadow
      FROM lakets._chronotable_registry
     WHERE schema_name = '{schema}' AND table_name = '{table}'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION '{schema}.{table} is not a registered ChronoTable';
    END IF;
    IF current_shadow IS NOT NULL
       AND current_shadow NOT IN ('{_LEGACY_SENSOR_SHADOW}', '{shadow_table}') THEN
        RAISE EXCEPTION '{schema}.{table} is bound to unexpected shadow %',
            current_shadow;
    END IF;

    LOCK TABLE {schema}.{table} IN SHARE ROW EXCLUSIVE MODE;
    UPDATE lakets._chronotable_registry
       SET shadow_table_name = '{shadow_table}', sync_enabled = TRUE
     WHERE schema_name = '{schema}' AND table_name = '{table}';

    CREATE OR REPLACE TRIGGER trg_lakets_sync
        AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table}
        FOR EACH ROW EXECUTE FUNCTION lakets._sync_trigger_fn();
END
$reconcile$;
"""
        await pool.execute(cutover_sql)
        current = await pool.fetchrow(_SHADOW_STATE_SQL, schema, table)
        if (
            current is None
            or current.get("shadow_table_name") != shadow_table
            or not current.get("sync_enabled")
        ):
            raise RuntimeError("ChronoTable registry did not retain the unique shadow")

        if legacy_exists:
            await pool.execute(
                f"""
INSERT INTO {target_relation}
SELECT source_rows.*
FROM {shadow_schema}.{_LEGACY_SENSOR_SHADOW} AS source_rows
WHERE source_rows.source IS DISTINCT FROM 'validation'
ON CONFLICT (id) DO NOTHING
"""
            )
        await pool.execute(
            f"""
INSERT INTO {target_relation}
SELECT source_rows.*
FROM {schema}.{table} AS source_rows
ON CONFLICT (id) DO NOTHING
"""
        )
        await pool.execute(f"COMMENT ON TABLE {target_relation} IS '{_CDF_BACKFILL_MARKER}'")
        completed = await pool.fetchrow(
            _TARGET_MIGRATION_STATE_SQL,
            target_relation,
            _CDF_BACKFILL_MARKER,
            target_index,
        )
        if (
            not completed
            or not completed.get("backfill_complete")
            or not completed.get("index_valid")
        ):
            raise RuntimeError("CDF shadow backfill marker was not persisted")
        return {
            "resource": resource,
            "status": "migrated",
            "shadow_table": target_relation,
            "previous_shadow": previous_shadow,
            "copy_sources": copy_sources,
        }
    except Exception as exc:
        logger.warning("CDF shadow reconciliation failed for %s.%s: %s", schema, table, exc)
        return {
            "resource": resource,
            "status": "failed",
            "shadow_table": f"{shadow_schema}.{shadow_table}",
            "error": str(exc)[:300],
        }


# ---------------------------------------------------------------------------
# Lakehouse Sync readiness (Phase 5 preflight, surfaced to the UI)
# ---------------------------------------------------------------------------
_REQUIRED_DATABASE = "databricks_postgres"

_CURRENT_DATABASE_SQL = "SELECT current_database()"
_WAL2DELTA_AVAILABLE_SQL = (
    "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'wal2delta')"
)
_WAL2DELTA_INSTALLED_SQL = "SELECT extversion FROM pg_extension WHERE extname = 'wal2delta'"
_LAKETS_SYNC_PRESENT_SQL = (
    "SELECT EXISTS ("
    " SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
    " WHERE n.nspname = 'lakets' AND p.proname = 'enable_sync'"
    ")"
)


async def check_lakehouse_sync_readiness(pool: Any) -> dict:
    """Inspect the connected database for Lakehouse Sync prerequisites.

    Returns a dict with booleans for each gate plus a human-readable ``reason``
    when ``ready_to_enable`` is False.  Never raises — all pool errors are
    caught and surfaced as ``error``.
    """
    try:
        current_db = await pool.fetchval(_CURRENT_DATABASE_SQL)
        wal2delta_available = bool(await pool.fetchval(_WAL2DELTA_AVAILABLE_SQL))
        wal2delta_version = await pool.fetchval(_WAL2DELTA_INSTALLED_SQL)
        lakets_sync = bool(await pool.fetchval(_LAKETS_SYNC_PRESENT_SQL))
    except Exception as exc:
        logger.warning("Lakehouse Sync readiness probe failed: %s", exc)
        return {
            "database_compatible": False,
            "wal2delta_available": False,
            "wal2delta_installed": False,
            "lakets_sync_functions_present": False,
            "ready_to_enable": False,
            "error": str(exc)[:200],
        }

    database_compatible = current_db == _REQUIRED_DATABASE
    wal2delta_installed = wal2delta_version is not None

    # Lakehouse Sync is a Databricks-managed feature — the wal2delta extension
    # is wired up by the platform, not by the user.  Keep the probe for info
    # only; do NOT gate readiness on it.
    # See https://docs.databricks.com/aws/en/oltp/projects/lakehouse-sync.
    reasons: list[str] = []
    if not database_compatible:
        reasons.append(
            f"Lakehouse Sync currently requires the `{_REQUIRED_DATABASE}` database "
            f"(connected to `{current_db}`)."
        )
    if not lakets_sync:
        reasons.append("LakeTS shadow-sync functions not found — install LakeTS first.")

    return {
        "database_compatible": database_compatible,
        "current_database": current_db,
        "wal2delta_available": wal2delta_available,
        "wal2delta_installed": wal2delta_installed,
        "wal2delta_version": wal2delta_version,
        "lakets_sync_functions_present": lakets_sync,
        "ready_to_enable": not reasons,
        "reason": "  ".join(reasons) if reasons else None,
    }


# ---------------------------------------------------------------------------
# Consolidated observability dashboard
# ---------------------------------------------------------------------------
DASHBOARD_SECTIONS = (
    "partitions",
    "current_partition",
    "tiering",
    "rollup",
    "query_comparison",
    "data_quality",
    "lvc",
    "cdf",
)

_PARTITIONS_DASHBOARD_SQL = """
WITH chunks AS (
    SELECT cm.chunk_name AS name,
           cm.range_start,
           cm.range_end,
           CASE
               WHEN cm.range_end <= now() THEN 'past'
               WHEN cm.range_start <= now() AND now() < cm.range_end THEN 'current'
               ELSE 'future'
           END AS phase,
           cm.status,
           cm.row_count AS recorded_row_count,
           cm.size_bytes AS recorded_size_bytes,
           count(*) FILTER (WHERE cm.status = 'active') OVER () AS active_count,
           count(*) FILTER (WHERE cm.status = 'tiered') OVER () AS tiered_count,
           count(*) FILTER (WHERE cm.status = 'dropped') OVER () AS dropped_count,
           count(*) OVER () AS total_count,
           (cm.range_start <= now() AND now() < cm.range_end) AS is_current,
           CASE WHEN cm.range_start <= now() AND now() < cm.range_end
                THEN (
                    SELECT count(*)
                    FROM public.sensor_readings readings
                    WHERE readings.event_time >= cm.range_start
                      AND readings.event_time < cm.range_end
                )
           END AS live_row_count,
           CASE WHEN cm.range_start <= now() AND now() < cm.range_end
                THEN pg_total_relation_size(to_regclass(cm.chunk_name))
           END AS live_size_bytes,
           CASE WHEN cm.range_start <= now() AND now() < cm.range_end
                THEN extract(epoch FROM (now() - cm.range_start))
           END AS age_seconds
    FROM lakets._chunk_metadata cm
    JOIN lakets._chronotable_registry c ON c.id = cm.chronotable_id
    WHERE c.schema_name = 'public' AND c.table_name = 'sensor_readings'
),
ranked AS (
    SELECT chunks.*,
           row_number() OVER (
               PARTITION BY phase
               ORDER BY
                   CASE WHEN phase = 'past' THEN range_start END DESC,
                   range_start ASC
           ) AS phase_rank
    FROM chunks
),
selected AS (
    SELECT *
    FROM ranked
    ORDER BY phase_rank,
             CASE phase WHEN 'current' THEN 0 WHEN 'past' THEN 1 ELSE 2 END
    LIMIT 500
)
SELECT *
FROM selected
ORDER BY range_start
"""

_TIERING_DASHBOARD_SQL = """
WITH registry AS (
    SELECT c.id,
           p.policy_type,
           p.config,
           p.enabled,
           p.last_run_at,
           CASE p.policy_type
               WHEN 'tiering' THEN p.config->>'after'
               WHEN 'tiered_retention' THEN p.config->>'tier_after'
           END AS tier_interval,
           lakets._cdf_committed_lsn(c.shadow_table_name) AS committed_lsn
    FROM lakets._chronotable_registry c
    LEFT JOIN LATERAL (
        SELECT policy_type, config, enabled, last_run_at
        FROM lakets._policy_registry
        WHERE chronotable_id = c.id
          AND policy_type IN ('tiering', 'tiered_retention')
        ORDER BY enabled DESC NULLS LAST, id DESC
        LIMIT 1
    ) p ON TRUE
    WHERE c.schema_name = 'public' AND c.table_name = 'sensor_readings'
)
SELECT count(*) FILTER (WHERE cm.status = 'active') AS active,
       count(*) FILTER (
           WHERE cm.status = 'active'
             AND r.policy_type IS NOT NULL
             AND r.tier_interval IS NOT NULL
             AND cm.range_end <= now() - r.tier_interval::interval
       ) AS age_eligible,
       count(*) FILTER (
           WHERE cm.status = 'active'
             AND r.policy_type IS NOT NULL
             AND r.tier_interval IS NOT NULL
             AND cm.range_end <= now() - r.tier_interval::interval
             AND cm.last_write_lsn IS NOT NULL
             AND r.committed_lsn >= cm.last_write_lsn
       ) AS durable_eligible,
       count(*) FILTER (WHERE cm.status = 'tiered') AS tiered,
       count(*) FILTER (WHERE cm.status = 'dropped') AS dropped,
       r.policy_type,
       r.config AS policy,
       r.enabled,
       r.last_run_at,
       r.committed_lsn
FROM registry r
LEFT JOIN lakets._chunk_metadata cm ON cm.chronotable_id = r.id
GROUP BY r.id, r.policy_type, r.config, r.enabled, r.last_run_at,
         r.tier_interval, r.committed_lsn
"""

_ROLLUP_DASHBOARD_SQL = """
SELECT r.name,
       r.rollup_table,
       r.bucket_interval::text AS bucket,
       r.watermark,
       r.last_refreshed_at,
       extract(epoch FROM (now() - r.last_refreshed_at)) AS refresh_age_seconds,
       count(i.id) AS pending_invalidations
FROM lakets._rollup_registry r
LEFT JOIN lakets._rollup_invalidation_log i ON i.rollup_id = r.id
WHERE r.name = 'sensor_1m'
GROUP BY r.id
"""

_ROLLUP_SERIES_DASHBOARD_SQL = """
SELECT bucket,
       sensor_name,
       sample_count,
       avg_value,
       min_value,
       max_value,
       stddev_value
FROM public._rollup_sensor_1m
WHERE bucket >= date_trunc('minute', now()) - interval '29 minutes'
  AND bucket < date_trunc('minute', now()) + interval '1 minute'
ORDER BY bucket, sensor_name
LIMIT 5000
"""

_RAW_AGGREGATE_COUNT_SQL = """
SELECT count(*) AS aggregate_rows,
       coalesce(sum(sample_count), 0) AS source_rows
FROM (
    SELECT lakets.time_bucket('1 minute'::interval, event_time) AS bucket,
           sensor_name,
           count(*) AS sample_count,
           avg(value) AS avg_value,
           min(value) AS min_value,
           max(value) AS max_value,
           stddev_samp(value) AS stddev_value
    FROM public.sensor_readings
    WHERE event_time >= date_trunc('minute', now()) - interval '29 minutes'
      AND event_time < date_trunc('minute', now()) + interval '1 minute'
    GROUP BY bucket, sensor_name
) raw_aggregate
"""
_ROLLUP_AGGREGATE_COUNT_SQL = """
SELECT count(*) AS aggregate_rows,
       coalesce(sum(sample_count), 0) AS source_rows
FROM (
    SELECT bucket, sensor_name, sample_count, avg_value, min_value,
           max_value, stddev_value
    FROM public._rollup_sensor_1m
    WHERE bucket >= date_trunc('minute', now()) - interval '29 minutes'
      AND bucket < date_trunc('minute', now()) + interval '1 minute'
) rollup_aggregate
"""

_DATA_QUALITY_DASHBOARD_SQL = """
WITH bounds AS (
    SELECT date_trunc('minute', now()) - interval '29 minutes' AS window_start,
           date_trunc('minute', now()) AS window_end
),
sensors AS (
    SELECT DISTINCT readings.sensor_name
    FROM public.sensor_readings
    CROSS JOIN bounds
    WHERE readings.event_time >= bounds.window_start
      AND readings.event_time < bounds.window_end + interval '1 minute'
),
slots AS (
    SELECT generate_series(
        bounds.window_start,
        bounds.window_end,
        interval '1 minute'
    ) AS bucket
    FROM bounds
),
expected AS (
    SELECT sensors.sensor_name, slots.bucket
    FROM sensors
    CROSS JOIN slots
),
observed AS (
    SELECT readings.sensor_name,
           lakets.time_bucket(
               '1 minute'::interval, readings.event_time
           ) AS bucket
    FROM public.sensor_readings readings
    CROSS JOIN bounds
    WHERE readings.event_time >= bounds.window_start
      AND readings.event_time < bounds.window_end + interval '1 minute'
    GROUP BY readings.sensor_name, bucket
)
SELECT (SELECT count(*) FROM sensors) AS sensor_count,
       count(*) AS expected_bucket_count,
       count(observed.bucket) AS observed_bucket_count,
       count(*) FILTER (WHERE observed.bucket IS NULL) AS missing_bucket_count,
       CASE WHEN count(*) > 0
            THEN round(
                count(*) FILTER (WHERE observed.bucket IS NULL)::numeric
                * 100 / count(*),
                2
            )
       END AS missing_bucket_percentage
FROM expected
LEFT JOIN observed USING (sensor_name, bucket)
"""

_LVC_REGISTRY_DASHBOARD_SQL = """
SELECT l.enabled,
       l.cache_table_name,
       l.key_columns,
       l.value_columns
FROM lakets._lvc_registry l
JOIN lakets._chronotable_registry c ON c.id = l.chronotable_id
WHERE c.schema_name = 'public' AND c.table_name = 'sensor_readings'
"""
_LVC_CACHE_DASHBOARD_SQL = """
SELECT (SELECT count(*) FROM public._lvc_sensor_readings) AS cached_series,
       (SELECT max(event_time) FROM public.sensor_readings) AS source_event_time,
       (SELECT max(event_time) FROM public._lvc_sensor_readings) AS cache_event_time,
       (SELECT max(last_updated) FROM public._lvc_sensor_readings) AS cache_updated_at,
       extract(epoch FROM (
           (SELECT max(event_time) FROM public.sensor_readings)
           - (SELECT max(event_time) FROM public._lvc_sensor_readings)
       )) AS source_to_cache_event_lag_seconds,
       extract(epoch FROM (
           now()
           - (SELECT max(event_time) FROM public.sensor_readings)
       )) AS source_event_age_seconds,
       extract(epoch FROM (
           now()
           - (SELECT max(last_updated) FROM public._lvc_sensor_readings)
       )) AS cache_update_age_seconds
"""

_CDF_STATE_DASHBOARD_SQL = """
WITH cdf_state AS (
    SELECT c.id,
           c.sync_enabled,
           c.shadow_table_name,
           lakets._cdf_committed_lsn(c.shadow_table_name) AS committed_lsn
    FROM lakets._chronotable_registry c
    WHERE c.schema_name = 'public' AND c.table_name = 'sensor_readings'
)
SELECT (SELECT max(event_time) FROM public.sensor_readings) AS hot_latest,
       c.sync_enabled,
       c.shadow_table_name,
       c.committed_lsn,
       CASE
           WHEN c.committed_lsn IS NOT NULL
           THEN greatest(
               pg_wal_lsn_diff(pg_current_wal_lsn(), c.committed_lsn),
               0
           )::bigint
       END AS wal_lag_bytes
FROM cdf_state c
"""


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, _dt.timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, Mapping):
        return {str(key): _json_safe(nested) for key, nested in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(nested) for nested in value]
    return str(value)


def _dashboard_value(value: Any, reason: str | None = None) -> dict:
    return {"value": _json_safe(value), "reason": reason}


def _dashboard_error(section: str, exc: Exception) -> dict:
    logger.warning("LakeTS %s dashboard probe failed: %s", section, exc, exc_info=True)
    return _dashboard_value(None, f"{section} telemetry unavailable")


async def _partition_dashboard(pool: Any) -> tuple[dict, dict]:
    try:
        rows = await pool.fetch(_PARTITIONS_DASHBOARD_SQL)
    except Exception as exc:
        unavailable = _dashboard_error("Partition", exc)
        return unavailable, unavailable.copy()

    if not rows:
        reason = "public.sensor_readings is not registered as a ChronoTable"
        return _dashboard_value(None, reason), _dashboard_value(None, reason)

    timeline = [
        {
            "name": row["name"],
            "range_start": row["range_start"],
            "range_end": row["range_end"],
            "phase": row["phase"],
            "status": row["status"],
            "row_count": row["recorded_row_count"],
            "size_bytes": row["recorded_size_bytes"],
        }
        for row in rows
    ]
    first = rows[0]
    partitions = _dashboard_value(
        {
            "timeline": timeline,
            "counts": {
                "active": int(first["active_count"] or 0),
                "tiered": int(first["tiered_count"] or 0),
                "dropped": int(first["dropped_count"] or 0),
                "total": int(first["total_count"]),
            },
        }
    )

    current = next((row for row in rows if row["is_current"]), None)
    if current is None:
        return partitions, _dashboard_value(None, "No current partition covers now()")
    return partitions, _dashboard_value(
        {
            "name": current["name"],
            "range_start": current["range_start"],
            "range_end": current["range_end"],
            "phase": current["phase"],
            "status": current["status"],
            "row_count": current["live_row_count"],
            "size_bytes": current["live_size_bytes"],
            "age_seconds": current["age_seconds"],
        }
    )


async def _tiering_dashboard(pool: Any) -> dict:
    try:
        row = await pool.fetchrow(_TIERING_DASHBOARD_SQL)
    except Exception as exc:
        return _dashboard_error("Tiering", exc)
    if row is None:
        return _dashboard_value(None, "No ChronoTable registry entry")
    tiering = {
        "active": int(row["active"] or 0),
        "tiered": int(row["tiered"] or 0),
        "dropped": int(row["dropped"] or 0),
        "eligible": None,
        "age_eligible": None,
        "durable_eligible": None,
        "policy": None,
    }
    if row["policy_type"] is None:
        return _dashboard_value(
            tiering,
            "No tiering policy configured; eligibility is unavailable",
        )

    tiering["policy"] = {
        "type": row["policy_type"],
        "config": row["policy"],
        "enabled": row["enabled"],
        "last_run_at": row["last_run_at"],
    }
    if not row["enabled"]:
        return _dashboard_value(
            tiering,
            "Tiering policy is configured but disabled; eligibility is unavailable",
        )

    tiering["age_eligible"] = int(row["age_eligible"] or 0)
    if row["committed_lsn"] is None:
        return _dashboard_value(
            tiering,
            "CDF durability position is unavailable; durable eligibility cannot be proven",
        )
    tiering["durable_eligible"] = int(row["durable_eligible"] or 0)
    tiering["eligible"] = tiering["durable_eligible"]
    return _dashboard_value(tiering)


async def _rollup_dashboard(pool: Any) -> dict:
    try:
        row, series = await asyncio.gather(
            pool.fetchrow(_ROLLUP_DASHBOARD_SQL),
            pool.fetch(_ROLLUP_SERIES_DASHBOARD_SQL),
        )
    except Exception as exc:
        return _dashboard_error("Rollup", exc)
    if row is None:
        return _dashboard_value(None, "RollUp sensor_1m is not registered")
    return _dashboard_value(
        {
            "name": row["name"],
            "table": row["rollup_table"],
            "bucket": row["bucket"],
            "watermark": row["watermark"],
            "last_refreshed_at": row["last_refreshed_at"],
            "refresh_age_seconds": row["refresh_age_seconds"],
            "pending_invalidations": int(row["pending_invalidations"] or 0),
            "series": [
                {
                    "bucket": sample["bucket"],
                    "sensor_name": sample["sensor_name"],
                    "sample_count": int(sample["sample_count"] or 0),
                    "avg_value": sample["avg_value"],
                    "min_value": sample["min_value"],
                    "max_value": sample["max_value"],
                    "stddev_value": sample["stddev_value"],
                }
                for sample in series
            ],
        }
    )


async def _timed_aggregate_count(
    pool: Any, sql: str, label: str
) -> tuple[dict[str, int] | None, float, str | None]:
    started = time.perf_counter()
    try:
        row = await pool.fetchrow(sql)
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000
        logger.warning(
            "LakeTS %s aggregate timing probe failed: %s",
            label,
            exc,
            exc_info=True,
        )
        return None, elapsed_ms, f"{label} aggregate query unavailable"
    elapsed_ms = (time.perf_counter() - started) * 1000
    if row is None:
        return None, elapsed_ms, "Aggregate query returned no result"
    return (
        {
            "aggregate_rows": int(row["aggregate_rows"]),
            "source_rows": int(row["source_rows"]),
        },
        elapsed_ms,
        None,
    )


async def _query_comparison_dashboard(pool: Any) -> dict:
    raw_probe, rollup_probe = await asyncio.gather(
        _timed_aggregate_count(pool, _RAW_AGGREGATE_COUNT_SQL, "Raw"),
        _timed_aggregate_count(pool, _ROLLUP_AGGREGATE_COUNT_SQL, "Rollup"),
    )
    raw_stats, raw_ms, raw_error = raw_probe
    rollup_stats, rollup_ms, rollup_error = rollup_probe
    raw_aggregate_rows = raw_stats["aggregate_rows"] if raw_stats is not None else None
    raw_source_rows = raw_stats["source_rows"] if raw_stats is not None else None
    rollup_rows = rollup_stats["aggregate_rows"] if rollup_stats is not None else None
    reduction = None
    if raw_source_rows and rollup_rows is not None:
        reduction = round((1 - (rollup_rows / raw_source_rows)) * 100, 2)
    comparison = {
        "window_minutes": 30,
        "raw_rows": raw_source_rows,
        "raw_aggregate_rows": raw_aggregate_rows,
        "rollup_rows": rollup_rows,
        "raw_query_ms": round(raw_ms, 3),
        "rollup_query_ms": round(rollup_ms, 3),
        "source_event_rows": raw_source_rows,
        "persisted_rollup_rows": rollup_rows,
        "event_to_rollup_row_reduction_percentage": reduction,
        "row_reduction_percentage": reduction,
        "raw_error": raw_error,
        "rollup_error": rollup_error,
    }
    reasons = [reason for reason in (raw_error, rollup_error) if reason]
    return _dashboard_value(comparison, "; ".join(reasons) if reasons else None)


async def _data_quality_dashboard(pool: Any) -> dict:
    try:
        row = await pool.fetchrow(_DATA_QUALITY_DASHBOARD_SQL)
    except Exception as exc:
        return _dashboard_error("Data quality", exc)
    if row is None or not row["sensor_count"]:
        return _dashboard_value(None, "No readings in the 30-minute lookback window")
    return _dashboard_value(
        {
            "window_minutes": 30,
            "sensor_count": int(row["sensor_count"]),
            "expected_bucket_count": int(row["expected_bucket_count"]),
            "observed_bucket_count": int(row["observed_bucket_count"]),
            "observed_buckets": int(row["observed_bucket_count"]),
            "missing_bucket_count": int(row["missing_bucket_count"]),
            "missing_bucket_percentage": float(row["missing_bucket_percentage"]),
            "gapfilled_count": None,
            "gapfilled_reason": "No LOCF or gapfill query was executed",
            "missing_sample_percentage": None,
            "missing_sample_reason": ("Expected sensor sampling rate is not configured"),
        }
    )


async def _lvc_dashboard(pool: Any) -> dict:
    try:
        registry = await pool.fetchrow(_LVC_REGISTRY_DASHBOARD_SQL)
        if registry is None:
            return _dashboard_value(None, "LVC is not enabled for public.sensor_readings")
        if registry["cache_table_name"] != "_lvc_sensor_readings":
            return _dashboard_value(
                None,
                "Registered LVC cache table is not _lvc_sensor_readings",
            )
        cache = await pool.fetchrow(_LVC_CACHE_DASHBOARD_SQL)
        if cache is None:
            return _dashboard_value(None, "LVC cache statistics are unavailable")
        return _dashboard_value(
            {
                "enabled": bool(registry["enabled"]),
                "cache_table": registry["cache_table_name"],
                "key_columns": list(registry["key_columns"] or []),
                "value_columns": list(registry["value_columns"] or []),
                "cached_series": (
                    int(cache["cached_series"]) if cache["cached_series"] is not None else None
                ),
                "source_event_time": cache["source_event_time"],
                "cache_event_time": cache["cache_event_time"],
                "cache_updated_at": cache["cache_updated_at"],
                "source_to_cache_event_lag_seconds": cache["source_to_cache_event_lag_seconds"],
                "cache_update_age_seconds": cache["cache_update_age_seconds"],
            }
        )
    except Exception as exc:
        return _dashboard_error("LVC", exc)


async def _cdf_dashboard(pool: Any) -> dict:
    try:
        row = await pool.fetchrow(_CDF_STATE_DASHBOARD_SQL)
    except Exception as exc:
        return _dashboard_error("CDF", exc)
    if row is None:
        return _dashboard_value(None, "No ChronoTable registry entry")

    reasons: list[str] = []
    shadow_latest = None
    shadow_table = row["shadow_table_name"]
    if shadow_table:
        try:
            shadow_table = _validate_sql_identifier(shadow_table)
            shadow_latest = await pool.fetchval(
                f"SELECT max(event_time) FROM lakets_cdf.{shadow_table}"
            )
        except Exception as exc:
            logger.warning("LakeTS CDF shadow probe failed: %s", exc, exc_info=True)
            reasons.append("CDF shadow latest telemetry unavailable")
    else:
        reasons.append("No CDF shadow is registered")

    hot_to_shadow_lag = None
    if isinstance(row["hot_latest"], _dt.datetime) and isinstance(shadow_latest, _dt.datetime):
        hot_to_shadow_lag = (row["hot_latest"] - shadow_latest).total_seconds()
    value = {
        "hot_latest": row["hot_latest"],
        "shadow_latest": shadow_latest,
        "hot_to_shadow_event_lag_seconds": hot_to_shadow_lag,
        "sync_enabled": bool(row["sync_enabled"]),
        "shadow_table": row["shadow_table_name"],
        "wal_lag_bytes": (int(row["wal_lag_bytes"]) if row["wal_lag_bytes"] is not None else None),
    }
    if row["committed_lsn"] is None:
        reasons.append("CDF streaming WAL position is unavailable")
    return _dashboard_value(value, "; ".join(reasons) if reasons else None)


async def get_dashboard(pool: Any) -> dict:
    """Return independent LakeTS observability sections for sensor_readings."""
    probe_slots = asyncio.Semaphore(3)

    async def safe_probe(
        probe: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        async with probe_slots:
            try:
                return await probe(pool)
            except Exception as exc:
                return _dashboard_error("LakeTS", exc)

    (
        partition_sections,
        tiering,
        rollup,
        query_comparison,
        data_quality,
        lvc,
        cdf,
    ) = await asyncio.gather(
        safe_probe(_partition_dashboard),
        safe_probe(_tiering_dashboard),
        safe_probe(_rollup_dashboard),
        safe_probe(_query_comparison_dashboard),
        safe_probe(_data_quality_dashboard),
        safe_probe(_lvc_dashboard),
        safe_probe(_cdf_dashboard),
    )
    if isinstance(partition_sections, tuple):
        partitions, current_partition = partition_sections
    else:
        partitions = partition_sections
        current_partition = partition_sections.copy()

    return _json_safe(
        {
            "partitions": partitions,
            "current_partition": current_partition,
            "tiering": tiering,
            "rollup": rollup,
            "query_comparison": query_comparison,
            "data_quality": data_quality,
            "lvc": lvc,
            "cdf": cdf,
        }
    )


# ---------------------------------------------------------------------------
# Retention runner (Phase 4.1a — scheduled job entry point)
# ---------------------------------------------------------------------------
_RETENTION_TABLES_SQL = (
    "SELECT DISTINCT c.schema_name, c.table_name "
    "FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
    "WHERE p.policy_type IN ('retention', 'tiered_retention') "
    "  AND p.enabled = TRUE"
)
_EXECUTE_RETENTION_SQL = "SELECT lakets.execute_retention($1, $2)"


async def run_retention(pool: Any) -> dict:
    """Run LakeTS retention for every ChronoTable that has a retention policy.

    Returns ``{f"{schema}.{table}": rows_dropped_or_error_message}``.  A failure
    on one table does not abort the rest — errors are captured per-table.
    """
    rows = await pool.fetch(_RETENTION_TABLES_SQL)
    results: dict = {}
    for row in rows:
        schema = row["schema_name"]
        table = row["table_name"]
        key = f"{schema}.{table}"
        try:
            dropped = await pool.fetchval(_EXECUTE_RETENTION_SQL, table, schema)
            results[key] = dropped
            logger.info("Retention on %s dropped %s chunk(s)", key, dropped)
        except Exception as exc:
            logger.warning("Retention on %s failed: %s", key, exc)
            results[key] = f"error: {exc}"
    return results


def _interval_to_timedelta(s: str) -> _dt.timedelta:
    """Convert a Postgres interval string to a Python timedelta.

    asyncpg encodes timedeltas as INTERVAL on the wire; raw strings fail with
    ``'str' object has no attribute 'days'``.  Month/year are approximated as
    30 / 365 days — callers wanting calendar-aware semantics should inline the
    interval literal in SQL instead.
    """
    return _dt.timedelta(seconds=_interval_seconds(s))
