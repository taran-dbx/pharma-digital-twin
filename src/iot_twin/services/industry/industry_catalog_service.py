"""Canonical per-industry UC provisioning + grants + secrets (spec §2.5–2.7, §1.11).

Creates the canonical ``iot_<industry>`` Unity Catalog structure and the shared
identity/secret plumbing for one industry pipeline, recording every resource in
the ``provisioned_resource`` ledger with a sensible ``teardown_order`` +
``parent_ref`` so delete can walk them in reverse-dependency order.

Layout (spec §2.5)::

    iot_<industry>/
    ├── realtime/     sensor_readings              Delta   (CDF-synced historical mirror)
    ├── microbatch/   sensor_data                  Delta   (micro-batch bronze)
    ├── zerobus/      iot_sensor_data              Delta   (zerobus ingest target)
    ├── metrics/      streaming_metrics_realtime   Delta   EXTERNAL, delta-rs writes
    │                 streaming_metrics_microbatch Delta   EXTERNAL, delta-rs writes
    │                 (zerobus ingest is not a Spark stream → no metrics table)
    └── checkpoints/  stream_checkpoints           Volume  (SS checkpoints)

Provisioning notes (spike-verified 2026-07-05):
- This metastore has **no default storage root**, so ``CREATE CATALOG`` must
  supply an explicit ``MANAGED LOCATION`` — ``s3://example-shared-bucket/<catalog>``
  (external location ``example-shared-bucket``, ``us-east-1``).
- Each mode gets its own EXTERNAL ``metrics.streaming_metrics_<mode>`` table
  (delta-rs writes via UC temp-table-credentials) → any writer SP (streaming SP
  for the streaming query listener, app SP for the app-hosted listener) needs
  ``EXTERNAL USE SCHEMA`` + ``MODIFY`` on the metrics schema.

This module is **additive** — it does not touch ``app.yaml``, ``core/lakebase.py`` live
wiring, or the legacy ``iot-lakets-demo`` project.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from iot_twin.core.workspace_config import APP_SP_OAUTH_SECRET_KEY, get_secret_scope
from iot_twin.models.metrics_schema import metrics_managed_table_ddl
from iot_twin.models.sensor_schema import delta_table_ddl
from iot_twin.services.dbsql_service import execute_dbsql
from iot_twin.services.resource_ledger import record_resource
from iot_twin.services.uc_ident import sql_fqn, sql_ident

logger = logging.getLogger("iot_digital_twin.services.industry_catalog")

# --- Constants -------------------------------------------------------------
# Canonical schema set + checkpoint volume (spec §2.5).
CANONICAL_SCHEMAS = ("realtime", "microbatch", "zerobus", "metrics", "checkpoints")
CHECKPOINT_SCHEMA = "checkpoints"
CHECKPOINT_VOLUME = "stream_checkpoints"


# --- Env-resolved workspace values -----------------------------------------
# Read at call time (not import time) so tests can monkeypatch freely.


def _storage_base() -> str:
    """S3 base URI for external metrics tables (UC_STORAGE_BASE env var).

    Returns an empty string when not configured — callers treat empty as
    "use metastore-managed storage" and omit the MANAGED LOCATION clause.
    """
    return os.getenv("UC_STORAGE_BASE", "")


def _external_location_name() -> str:
    """UC external location name matching the S3 storage base (UC_EXTERNAL_LOCATION).

    Returns an empty string when not configured — the pre-flight check is
    skipped gracefully when no external location is needed.
    """
    return os.getenv("UC_EXTERNAL_LOCATION", "")


# Ledger teardown_order. Higher = considered earlier during teardown. Catalog
# and schema rows are inventory only: per-pipeline deletion always preserves
# those shared namespace containers.
TEARDOWN_ORDER_SP = 1  # shared — never torn down per-industry
TEARDOWN_ORDER_SECRET_SCOPE = 2  # shared — never torn down per-industry
TEARDOWN_ORDER_SECRET = 3  # shared — never torn down per-industry
TEARDOWN_ORDER_CATALOG = 10
TEARDOWN_ORDER_SCHEMA = 15
TEARDOWN_ORDER_VOLUME = 18
TEARDOWN_ORDER_METRICS_TABLE = 20
TEARDOWN_ORDER_GRANT = 45  # revoke first (harmless once objects dropped)

_CATALOG_SAFE_RE = re.compile(r"[^a-z0-9_]+")


# --- Name helpers ----------------------------------------------------------
def catalog_name(industry: str) -> str:
    """Return the canonical catalog name ``iot_<industry>`` for ``industry``.

    Lower-cases and collapses any non ``[a-z0-9_]`` run to a single underscore
    so an arbitrary industry label produces a valid UC identifier.
    """
    safe = _CATALOG_SAFE_RE.sub("_", (industry or "").strip().lower()).strip("_")
    if not safe:
        raise ValueError(f"industry {industry!r} has no valid characters for a catalog name")
    return f"iot_{safe}"


def catalog_managed_location(catalog: str) -> str:
    """Return the MANAGED LOCATION S3 URI for ``catalog``.

    Returns an empty string when ``UC_STORAGE_BASE`` is not configured, which
    signals callers to use metastore-managed storage instead.
    """
    base = _storage_base()
    return f"{base}/{catalog}" if base else ""


# --- One-schema / industry-prefixed managed-table layout (Task 3) ----------
# The workspace catalog PRE-EXISTS (owned by another SP), so provisioning never
# creates it.  All industry tables are MANAGED and live in the single
# ``{UC_CATALOG}.{UC_SCHEMA}`` schema, distinguished by an ``{industry}_{kind}``
# name.  Config values are read at call time (never import-time literals) so
# tests can monkeypatch the environment.
TABLE_KINDS: tuple[str, ...] = ("realtime", "microbatch", "zerobus", "metrics")

# The single UC secret holding the app SP's own OAuth client-secret, read by the
# Zerobus producer notebooks at runtime (they only need READ SECRET).
APP_SP_OAUTH_SECRET = APP_SP_OAUTH_SECRET_KEY


def _uc_catalog() -> str:
    """Return the pre-existing UC catalog to provision into (``UC_CATALOG``)."""
    catalog = os.getenv("UC_CATALOG", "")
    if not catalog:
        raise RuntimeError(
            "UC_CATALOG must be set — it names the pre-existing catalog to "
            "provision the industry schema/tables into."
        )
    return catalog


def _uc_schema() -> str:
    """Return the single UC schema all industry tables live in (``UC_SCHEMA``)."""
    schema = os.getenv("UC_SCHEMA", "")
    if not schema:
        raise RuntimeError("UC_SCHEMA must be set — it names the single schema to provision.")
    return schema


def uc_catalog_name() -> str:
    """Return the configured pre-existing one-schema catalog."""
    return _uc_catalog()


def uc_schema_name() -> str:
    """Return the configured schema containing industry-prefixed tables."""
    return _uc_schema()


def _sanitize_industry(industry: str) -> str:
    """Lower-case + collapse non ``[a-z0-9_]`` runs so ``industry`` is a valid identifier prefix."""
    safe = _CATALOG_SAFE_RE.sub("_", (industry or "").strip().lower()).strip("_")
    if not safe:
        raise ValueError(f"industry {industry!r} has no valid characters for a table name")
    return safe


def industry_table(industry: str, kind: str) -> str:
    """Return the FQN of an industry's managed table: ``{catalog}.{schema}.{industry}_{kind}``.

    ``kind`` must be one of :data:`TABLE_KINDS` (realtime/microbatch/zerobus/metrics).
    This is the single place a table FQN is built for the one-schema layout.
    """
    if kind not in TABLE_KINDS:
        raise ValueError(f"unknown table kind {kind!r}; expected one of {TABLE_KINDS}")
    return f"{_uc_catalog()}.{_uc_schema()}.{_sanitize_industry(industry)}_{kind}"


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _is_already_exists(exc: Exception) -> bool:
    return "already exists" in str(exc).lower()


def _is_not_found(exc: Exception) -> bool:
    s = str(exc).lower()
    return "not_found" in s or "not found" in s or "does not exist" in s


def _is_permission_denied(msg: str) -> bool:
    s = msg.lower()
    return "permission_denied" in s or "permission denied" in s


# --- Warehouse resolution --------------------------------------------------
def resolve_warehouse_id(ws: Any, warehouse_id: str | None = None) -> str:
    """Return a usable SQL warehouse id (explicit arg wins, else first listed)."""
    if warehouse_id:
        return warehouse_id
    warehouses = list(ws.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse available for catalog SQL/DDL")
    return warehouses[0].id


# --- Pre-flight ------------------------------------------------------------
def preflight_checks(*, ws: Any | None = None, warehouse_id: str | None = None) -> list[dict]:
    """Run pre-flight checks for industry catalog provisioning.

    Returns one ``{check, status, detail}`` per gate (``status`` ∈
    ``pass``/``fail``).  The orchestrator raises with a clear message when any
    check fails.  Never raises itself.
    """
    ws = ws or _new_ws()
    checks: list[dict] = []

    # 1. External location the MANAGED LOCATION lives under must exist (only
    #    checked when UC_EXTERNAL_LOCATION is configured; otherwise the workspace
    #    uses metastore-managed storage and no external location is required).
    ext_loc = _external_location_name()
    storage_base = _storage_base()
    if ext_loc:
        try:
            ws.external_locations.get(name=ext_loc)
            checks.append(
                {
                    "check": f"External location '{ext_loc}'",
                    "status": "pass",
                    "detail": "present",
                }
            )
        except Exception as exc:
            checks.append(
                {
                    "check": f"External location '{ext_loc}'",
                    "status": "fail",
                    "detail": (
                        f"Not found ({str(exc)[:120]}). CREATE CATALOG needs an explicit "
                        f"MANAGED LOCATION under {storage_base}; the external "
                        f"location '{ext_loc}' must exist and be writable."
                    ),
                }
            )
    else:
        checks.append(
            {
                "check": "External location",
                "status": "pass",
                "detail": "UC_EXTERNAL_LOCATION not configured — using metastore-managed storage",
            }
        )

    # 2. A SQL warehouse must be available for external-table DDL + grants.
    try:
        wh = resolve_warehouse_id(ws, warehouse_id)
        checks.append(
            {
                "check": "SQL warehouse",
                "status": "pass",
                "detail": f"using warehouse {wh}",
            }
        )
    except Exception as exc:
        checks.append(
            {
                "check": "SQL warehouse",
                "status": "fail",
                "detail": str(exc)[:160],
            }
        )

    return checks


def raise_on_failed_preflight(checks: list[dict]) -> None:
    """Raise ``RuntimeError`` with a clear message if any pre-flight check failed."""
    failed = [c for c in checks if c.get("status") == "fail"]
    if failed:
        detail = "; ".join(f"{c['check']}: {c['detail']}" for c in failed)
        raise RuntimeError(f"Pre-flight checks failed — {detail}")


# --- SQL helper ------------------------------------------------------------
async def _run_sql(sql: str, warehouse_id: str) -> dict:
    """Execute one SQL statement via DBSQL, returning a result/status dict."""
    try:
        await execute_dbsql(sql, warehouse_id)
        return {"status": "ok"}
    except Exception as exc:
        if _is_already_exists(exc):
            return {"status": "already_exists"}
        return {"status": "failed", "error": str(exc)[:200]}


# --- Catalog / schemas / volume / metrics table ----------------------------
async def provision_industry_catalog(
    industry: str,
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Provision one industry into the PRE-EXISTING ``{UC_CATALOG}.{UC_SCHEMA}``.

    The catalog already exists and is owned by another SP, so this NEVER issues
    ``CREATE CATALOG``.  It runs ``CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}``
    then creates the MANAGED, industry-prefixed Delta tables (no ``LOCATION``):

    - ``{industry}_zerobus``  — the Zerobus ingest target, pre-created with the
      canonical SensorRecord/SSOT schema so the Zerobus SDK ingests into an
      existing table.
    - ``{industry}_realtime`` / ``{industry}_microbatch`` — same SensorRecord
      schema (created for completeness; not exercised in this plan).
    - ``{industry}_metrics`` — the managed streaming-metrics table with a leading
      ``mode`` column.

    Records the schema + each table in the ledger.  Returns
    ``{catalog, schema, results, tables, metrics_table}`` where ``tables`` is a
    ``{kind: fqn}`` dict.
    """
    ws = ws or _new_ws()
    catalog = _uc_catalog()
    schema = _uc_schema()
    schema_fqn = f"{catalog}.{schema}"
    sql_schema_fqn = sql_fqn(catalog, schema)
    wh = resolve_warehouse_id(ws, warehouse_id)
    results: list[dict] = []
    tables: dict[str, str] = {}

    # 1. Schema — the catalog pre-exists (owned by another SP), so only ever
    #    create the schema.  Managed tables need no external storage.
    res = await _run_sql(f"CREATE SCHEMA IF NOT EXISTS {sql_schema_fqn}", wh)
    results.append(
        {
            "resource": f"Schema '{schema_fqn}'",
            "status": "created" if res["status"] == "ok" else res["status"],
            **({"error": res["error"]} if res.get("error") else {}),
        }
    )
    if res["status"] != "failed":
        await _record(
            db,
            setup_id,
            deployment_id,
            resource_type="uc_schema",
            identifier=schema_fqn,
            parent_ref=catalog,
            teardown_order=TEARDOWN_ORDER_SCHEMA,
        )
    else:
        # Schema is the root dependency for the tables — abort if it failed.
        return {
            "catalog": catalog,
            "schema": schema,
            "results": results,
            "tables": tables,
            "metrics_table": None,
        }

    # 2. Managed, industry-prefixed Delta tables (no LOCATION).
    for kind in TABLE_KINDS:
        fqn = industry_table(industry, kind)
        sql_fqn_table = sql_fqn(*fqn.split("."))
        ddl = (
            metrics_managed_table_ddl(sql_fqn_table)
            if kind == "metrics"
            else delta_table_ddl(sql_fqn_table)
        )
        res = await _run_sql(ddl, wh)
        results.append(
            {
                "resource": f"Table '{fqn}'",
                "status": "created" if res["status"] == "ok" else res["status"],
                **({"error": res["error"]} if res.get("error") else {}),
            }
        )
        if res["status"] != "failed":
            tables[kind] = fqn
            await _record(
                db,
                setup_id,
                deployment_id,
                resource_type="uc_table",
                identifier=fqn,
                parent_ref=schema_fqn,
                teardown_order=TEARDOWN_ORDER_METRICS_TABLE,
                metadata={"managed": True, "kind": kind},
            )

    return {
        "catalog": catalog,
        "schema": schema,
        "results": results,
        "tables": tables,
        "metrics_table": tables.get("metrics"),
    }


# --- Grants ----------------------------------------------------------------
def resolve_streaming_sp_principal(ws: Any) -> str:
    """Return the current app SP used by the single-principal deployment."""
    return resolve_app_principal(ws) or ""


def resolve_app_principal(ws: Any) -> str | None:
    """Return the single app SP's application id — the identity used for ALL
    auth (DBSQL reads, health checks, streaming writes, Zerobus ingest).  In the
    deployed app this is ``DATABRICKS_CLIENT_ID``; falls back to the current
    principal.  ``None`` if it can't be resolved (grant then skipped, non-fatal)."""
    cid = os.getenv("DATABRICKS_CLIENT_ID")
    if cid:
        return cid
    try:
        return ws.current_user.me().user_name
    except Exception as exc:  # pragma: no cover - offline / no creds
        logger.warning("Could not resolve app principal (non-fatal): %s", exc)
        return None


def _app_sp_grant_statements(
    catalog: str,
    schema: str,
    industry: str,
    principal: str,
) -> list[tuple[str, str]]:
    """Return ``(label, sql)`` GRANT statements for the SINGLE app SP.

    The one app SP does everything, so it gets USE CATALOG on the catalog;
    USE SCHEMA + CREATE TABLE + MODIFY + SELECT on the schema (CREATE TABLE so
    the Zerobus SDK can ensure its target table; MODIFY/SELECT for ingest +
    reads); and READ SECRET on the schema so it can read the ``app_sp_oauth``
    UC secret the Zerobus notebooks authenticate with.
    """
    q = f"`{principal}`"
    sql_catalog = sql_ident(catalog)
    sql_schema_fqn = sql_fqn(catalog, schema)
    sql_metrics_table = sql_fqn(catalog, schema, f"{_sanitize_industry(industry)}_metrics")
    return [
        ("USE CATALOG", f"GRANT USE CATALOG ON CATALOG {sql_catalog} TO {q}"),
        (
            "USE SCHEMA, EXTERNAL USE SCHEMA, CREATE TABLE, MODIFY, SELECT on schema",
            f"GRANT USE SCHEMA, EXTERNAL USE SCHEMA, CREATE TABLE, MODIFY, SELECT "
            f"ON SCHEMA {sql_schema_fqn} TO {q}",
        ),
        (
            "explicit SELECT, MODIFY on metrics table for Zerobus",
            f"GRANT SELECT, MODIFY ON TABLE {sql_metrics_table} TO {q}",
        ),
        ("READ SECRET on schema", f"GRANT READ SECRET ON SCHEMA {sql_schema_fqn} TO {q}"),
    ]


async def grant_app_sp(
    industry: str,
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    principal: str | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Grant the SINGLE app SP everything it needs on ``{catalog}.{schema}``.

    Idempotent; records each grant in the ledger.  ``industry`` is accepted for
    call-site symmetry (the grants are schema-scoped, not per-industry).
    Returns ``{catalog, schema, principal, results}``.
    """
    ws = ws or _new_ws()
    catalog = _uc_catalog()
    schema = _uc_schema()
    principal = principal or resolve_app_principal(ws)
    if not principal:
        return {
            "catalog": catalog,
            "schema": schema,
            "principal": None,
            "results": [
                {
                    "resource": "app SP grant",
                    "status": "skipped",
                    "error": "app SP principal unresolved",
                }
            ],
        }
    wh = resolve_warehouse_id(ws, warehouse_id)
    results: list[dict] = []
    schema_fqn = f"{catalog}.{schema}"
    for label, sql in _app_sp_grant_statements(catalog, schema, industry, principal):
        res = await _run_sql(sql, wh)
        # The catalog-level USE CATALOG grant fails with PERMISSION_DENIED when the
        # deployer SP does not own the catalog; the deployer applies it out-of-band.
        # Record it as skipped (not failed) so it never blocks provisioning.
        if (
            label == "USE CATALOG"
            and res.get("status") == "failed"
            and _is_permission_denied(res.get("error", ""))
        ):
            res = {
                "status": "skipped",
                "reason": "catalog-level grant is applied out-of-band by the deployer",
            }
        results.append(
            {
                "resource": f"GRANT {label} → {principal}",
                "status": res["status"],
                **({"error": res["error"]} if res.get("error") else {}),
                **({"reason": res["reason"]} if res.get("reason") else {}),
            }
        )
        if res["status"] != "failed":
            await _record(
                db,
                setup_id,
                deployment_id,
                resource_type="grant",
                identifier=f"{schema_fqn}:{label}:{principal}",
                parent_ref=schema_fqn,
                teardown_order=TEARDOWN_ORDER_GRANT,
                metadata={"sql": sql},
            )
    return {"catalog": catalog, "schema": schema, "principal": principal, "results": results}


# --- Secrets + SP ----------------------------------------------------------
async def ensure_secret_scope(
    scope: str | None = None,
    *,
    ws: Any | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
) -> dict:
    """Ensure the shared secret scope exists (idempotent), recording it (shared).

    ``scope`` is resolved at call time from ``workspace_config.get_secret_scope()``
    when not provided explicitly.  The scope + SP are shared across all industries,
    so the ledger row is flagged ``shared`` and must never be torn down per-industry.
    """
    scope = scope or get_secret_scope()
    ws = ws or _new_ws()
    status: str
    try:
        ws.secrets.create_scope(scope=scope)
        status = "created"
    except Exception as exc:
        if _is_already_exists(exc):
            status = "already_exists"
        else:
            return {
                "resource": f"Secret scope '{scope}'",
                "status": "failed",
                "error": str(exc)[:200],
            }

    await _record(
        db,
        setup_id,
        None,
        resource_type="secret_scope",
        identifier=scope,
        teardown_order=TEARDOWN_ORDER_SECRET_SCOPE,
        metadata={"shared": True},
    )
    return {"resource": f"Secret scope '{scope}'", "status": status}


# --- Ledger helper ---------------------------------------------------------
async def _record(
    db: Any | None,
    setup_id: Any | None,
    deployment_id: Any | None,
    *,
    resource_type: str,
    identifier: str,
    parent_ref: str | None = None,
    teardown_order: int = 0,
    metadata: dict | None = None,
) -> None:
    """Record a resource in the ledger when a db + setup_id are provided."""
    if db is None or setup_id is None:
        return
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type=resource_type,
        identifier=identifier,
        parent_ref=parent_ref,
        teardown_order=teardown_order,
        metadata=metadata,
    )
