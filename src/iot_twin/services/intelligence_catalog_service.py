"""Shared Unity Catalog Delta reference tables for the Genie copilot.

Provisions and syncs domain catalog fixtures into ``{UC_CATALOG}.{UC_SCHEMA}``
so Genie can query a governed allowlist. Catalog and schema always come from
workspace config — never hardcoded.

Tables:
- intelligence_assets
- intelligence_sensor_bindings
- intelligence_maintenance_records
- intelligence_alert_rules
- intelligence_alert_snapshot  (read-only snapshot contract; Genie cannot mutate alerts)
- intelligence_genai_audit     (read-only mirror of the AI action-plan audit trail)

The snapshot carries the thermal-risk model's output on predicted alerts
(``alert_kind``, ``risk_score``, ``model_name``, ``model_version``,
``top_features``); tables created before those columns existed are migrated
with ``ALTER TABLE ... ADD COLUMNS`` on provision. See docs/ml-risk-model.md.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from typing import Any

from databricks.sdk import WorkspaceClient

from iot_twin.core.workspace_config import get_uc_catalog, get_uc_schema
from iot_twin.domain.catalog import CURATED_DOMAIN_INDUSTRIES, get_domain
from iot_twin.ml.risk_contract import alert_kind_for, risk_score_of
from iot_twin.services.dbsql_service import execute_dbsql
from iot_twin.services.industry.industry_catalog_service import resolve_warehouse_id
from iot_twin.services.uc_ident import sql_fqn

logger = logging.getLogger("pharma_twin.services.intelligence_catalog")

TABLE_NAMES: tuple[str, ...] = (
    "intelligence_assets",
    "intelligence_sensor_bindings",
    "intelligence_maintenance_records",
    "intelligence_alert_rules",
    "intelligence_alert_snapshot",
    "intelligence_genai_audit",
)

# Snapshot columns, in insert order. The first eleven are the original contract;
# MODEL_SNAPSHOT_COLUMNS were added with the thermal-risk model.
ALERT_SNAPSHOT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("alert_id", "STRING"),
    ("industry", "STRING"),
    ("rule_id", "STRING"),
    ("sensor_id", "STRING"),
    ("status", "STRING"),
    ("severity", "STRING"),
    ("fault_code", "STRING"),
    ("value", "DOUBLE"),
    ("threshold", "DOUBLE"),
    ("opened_at", "STRING"),
    ("updated_at", "STRING"),
    ("alert_kind", "STRING"),
    ("risk_score", "DOUBLE"),
    ("model_name", "STRING"),
    ("model_version", "STRING"),
    ("top_features", "STRING"),
    ("metadata", "STRING"),
)
MODEL_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    "alert_kind",
    "risk_score",
    "model_name",
    "model_version",
    "top_features",
    "metadata",
)
ALERT_SNAPSHOT_COLUMN_LIST = ", ".join(name for name, _ in ALERT_SNAPSHOT_COLUMNS)

_TABLE_DDL: dict[str, str] = {
    "intelligence_assets": """
industry STRING,
site_id STRING,
asset_id STRING,
name STRING
""",
    "intelligence_sensor_bindings": """
industry STRING,
site_id STRING,
asset_id STRING,
component_id STRING,
sensor_id STRING,
sensor_name STRING,
cadence_hz DOUBLE
""",
    "intelligence_maintenance_records": """
industry STRING,
event_id STRING,
asset_id STRING,
component_id STRING,
kind STRING,
started_at STRING,
completed_at STRING,
notes STRING
""",
    "intelligence_alert_rules": """
industry STRING,
rule_id STRING,
sensor_id STRING,
operator STRING,
threshold DOUBLE,
severity STRING,
fault_code STRING
""",
    "intelligence_alert_snapshot": ",\n".join(
        f"{name} {sql_type}" for name, sql_type in ALERT_SNAPSHOT_COLUMNS
    ),
    "intelligence_genai_audit": """
plan_id STRING,
alert_id STRING,
industry STRING,
requested_by STRING,
requested_at STRING,
endpoint STRING,
model STRING,
model_version STRING,
prompt_template_version STRING,
prompt_hash STRING,
grounding_hash STRING,
grounding_check STRING,
grounding_reasons STRING,
status STRING,
citation_ids STRING,
grounding_rows STRING,
plan_summary STRING,
plan STRING,
risk_score DOUBLE,
risk_model STRING,
risk_model_version STRING,
latency_ms BIGINT,
total_tokens BIGINT,
applied_by STRING,
applied_at STRING
""",
}

_KIND_BY_NAME: dict[str, str] = {
    "intelligence_assets": "assets",
    "intelligence_sensor_bindings": "sensor_bindings",
    "intelligence_maintenance_records": "maintenance_records",
    "intelligence_alert_rules": "alert_rules",
    "intelligence_alert_snapshot": "alert_snapshot",
    "intelligence_genai_audit": "genai_audit",
}


def _catalog() -> str:
    catalog = get_uc_catalog()
    if not catalog:
        raise RuntimeError("UC_CATALOG must be set to provision intelligence tables")
    return catalog


def _schema() -> str:
    schema = get_uc_schema()
    if not schema:
        raise RuntimeError("UC_SCHEMA must be set to provision intelligence tables")
    return schema


def reference_tables() -> dict[str, str]:
    """Return ``{kind: catalog.schema.table}`` for the shared reference set."""
    catalog = _catalog()
    schema = _schema()
    return {_KIND_BY_NAME[name]: f"{catalog}.{schema}.{name}" for name in TABLE_NAMES}


def alert_snapshot_contract(syncer: Any | None = None) -> dict[str, Any]:
    freshness = "admin_sync"
    last_sync_at: str | None = None
    last_error: str | None = None
    interval_s = 60
    dirty = False
    catalog = get_uc_catalog()
    schema = get_uc_schema()
    uc_ready = bool(catalog and schema)
    if syncer is not None:
        last_sync_at = getattr(syncer, "last_sync_at", None)
        last_error = getattr(syncer, "last_error", None)
        interval_s = int(getattr(syncer, "interval_s", 60) or 60)
        dirty = bool(getattr(syncer, "dirty", False))
        if not getattr(syncer, "enabled", True) or not uc_ready:
            freshness = "disabled"
        elif getattr(syncer, "retrying", False):
            freshness = "retry"
        elif last_error:
            freshness = "error"
        elif last_sync_at:
            freshness = "periodic"
        else:
            freshness = "pending"
    elif not uc_ready:
        freshness = "disabled"
        last_error = "Missing UC_CATALOG, UC_SCHEMA"
    table = reference_tables()["alert_snapshot"] if uc_ready else ""
    return {
        "table": table,
        "access": "read_only",
        "freshness": freshness,
        "last_sync_at": last_sync_at,
        "last_error": last_error,
        "interval_s": interval_s,
        "dirty": dirty,
        "notes": (
            "Read-only alert snapshot for Genie, refreshed by a bounded "
            "background copy of Lakebase/in-memory alerts (and by admin "
            "full sync). Copilot may explain and recommend but cannot mutate "
            "alerts or write this table. UC grants are not tightened here; "
            "treat access as a prompt/API contract, not an enforced Delta "
            "permission."
        ),
    }


def _sql_str(value: Any) -> str:
    """A Databricks SQL string literal for ``value``.

    Databricks SQL treats ``\\`` as the escape character inside string literals, so
    both backslashes and single quotes are backslash-escaped. Doubling quotes
    alone is not enough: a value ending in ``\\`` or containing ``\\'`` would
    end the literal early and break (or inject into) the statement.
    """
    if value is None:
        return "NULL"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _sql_num(value: Any) -> str:
    if value is None:
        return "NULL"
    return str(float(value))


def _warehouse(ws: Any | None, warehouse_id: str | None) -> str:
    if warehouse_id:
        return warehouse_id
    env = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if env:
        return env
    client = ws or WorkspaceClient()
    return resolve_warehouse_id(client, None)


async def provision_reference_tables(
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    catalog = _catalog()
    schema = _schema()
    wh = _warehouse(ws, warehouse_id)
    results: list[dict[str, Any]] = []
    tables: dict[str, str] = {}
    for name in TABLE_NAMES:
        fqn = f"{catalog}.{schema}.{name}"
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {sql_fqn(catalog, schema, name)} (\n"
            f"{_TABLE_DDL[name].strip()}\n) USING DELTA"
        )
        await execute_dbsql(ddl, wh)
        if name == "intelligence_alert_snapshot":
            await _ensure_snapshot_columns(sql_fqn(catalog, schema, name), wh)
        results.append({"resource": f"Table '{fqn}'", "status": "created"})
        tables[_KIND_BY_NAME[name]] = fqn
    return {"catalog": catalog, "schema": schema, "results": results, "tables": tables}


async def _ensure_snapshot_columns(table_sql: str, warehouse_id: str) -> list[str]:
    """Add the model columns to a snapshot table created before they existed."""
    described = await execute_dbsql(f"DESCRIBE TABLE {table_sql}", warehouse_id)
    existing = {
        str(row.get("col_name") or "").strip().lower()
        for row in described.get("data") or []
        if isinstance(row, dict)
    }
    if not existing:
        return []
    types = dict(ALERT_SNAPSHOT_COLUMNS)
    missing = [name for name in MODEL_SNAPSHOT_COLUMNS if name not in existing]
    if missing:
        columns = ", ".join(f"{name} {types[name]}" for name in missing)
        await execute_dbsql(f"ALTER TABLE {table_sql} ADD COLUMNS ({columns})", warehouse_id)
        logger.info("Added model columns to %s: %s", table_sql, missing)
    return missing


def _asset_rows() -> list[str]:
    rows: list[str] = []
    for industry in CURATED_DOMAIN_INDUSTRIES:
        domain = get_domain(industry)
        for site in domain.sites:
            for asset in site.assets:
                rows.append(
                    f"({_sql_str(industry)}, {_sql_str(site.site_id)}, "
                    f"{_sql_str(asset.asset_id)}, {_sql_str(asset.name)})"
                )
    return rows


def _sensor_rows() -> list[str]:
    rows: list[str] = []
    for industry in CURATED_DOMAIN_INDUSTRIES:
        domain = get_domain(industry)
        for site in domain.sites:
            for asset in site.assets:
                for component in asset.components:
                    for sensor in component.sensors:
                        rows.append(
                            f"({_sql_str(industry)}, {_sql_str(site.site_id)}, "
                            f"{_sql_str(asset.asset_id)}, {_sql_str(component.component_id)}, "
                            f"{_sql_str(sensor.sensor_id)}, {_sql_str(sensor.name)}, "
                            f"{_sql_num(sensor.cadence_hz)})"
                        )
    return rows


def _maintenance_rows() -> list[str]:
    rows: list[str] = []
    for industry in CURATED_DOMAIN_INDUSTRIES:
        domain = get_domain(industry)
        for event in domain.maintenance_history:
            rows.append(
                f"({_sql_str(industry)}, {_sql_str(event.event_id)}, "
                f"{_sql_str(event.asset_id)}, {_sql_str(event.component_id)}, "
                f"{_sql_str(event.kind)}, {_sql_str(event.started_at)}, "
                f"{_sql_str(event.completed_at)}, {_sql_str(event.notes)})"
            )
    return rows


def _alert_rule_rows() -> list[str]:
    rows: list[str] = []
    for industry in CURATED_DOMAIN_INDUSTRIES:
        domain = get_domain(industry)
        for rule in domain.alert_rules:
            rows.append(
                f"({_sql_str(industry)}, {_sql_str(rule.rule_id)}, "
                f"{_sql_str(rule.sensor_id)}, {_sql_str(rule.operator)}, "
                f"{_sql_num(rule.threshold)}, {_sql_str(rule.severity)}, "
                f"{_sql_str(rule.fault_code)})"
            )
    return rows


def _sql_json(value: Any) -> str:
    if value in (None, "", [], {}):
        return "NULL"
    return _sql_str(json.dumps(value, default=str, sort_keys=True))


def _model_snapshot_values(alert: dict[str, Any]) -> list[str]:
    metadata = alert.get("metadata") if isinstance(alert.get("metadata"), dict) else {}
    rule_id = alert.get("rule_id")
    kind = alert.get("alert_kind") or alert_kind_for(rule_id, metadata)
    score = alert.get("risk_score")
    if score is None:
        score = risk_score_of(rule_id, metadata)
    return [
        _sql_str(kind),
        _sql_num(score),
        _sql_str(metadata.get("model_name")),
        _sql_str(metadata.get("model_version")),
        _sql_json(metadata.get("top_features")),
        _sql_json(metadata),
    ]


def _alert_snapshot_rows(alerts: Iterable[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for alert in alerts:
        values = [
            _sql_str(alert.get("id")),
            _sql_str(alert.get("industry")),
            _sql_str(alert.get("rule_id")),
            _sql_str(alert.get("sensor_id")),
            _sql_str(alert.get("status")),
            _sql_str(alert.get("severity")),
            _sql_str(alert.get("fault_code")),
            _sql_num(alert.get("value")),
            _sql_num(alert.get("threshold")),
            _sql_str(alert.get("opened_at")),
            _sql_str(alert.get("updated_at")),
            *_model_snapshot_values(alert),
        ]
        rows.append(f"({', '.join(values)})")
    return rows


def _quoted_table(name: str) -> str:
    return sql_fqn(_catalog(), _schema(), name)


_VALUES_BATCH = 100


async def _replace_table(table_sql: str, warehouse_id: str, values: list[str], columns: str) -> int:
    """Replace table contents. INSERT OVERWRITE is atomic on Delta; empty stays DELETE."""
    if not values:
        await execute_dbsql(f"DELETE FROM {table_sql}", warehouse_id)
        return 0
    first = values[:_VALUES_BATCH]
    await execute_dbsql(
        f"INSERT OVERWRITE TABLE {table_sql} ({columns}) VALUES {', '.join(first)}",
        warehouse_id,
    )
    for start in range(_VALUES_BATCH, len(values), _VALUES_BATCH):
        chunk = values[start : start + _VALUES_BATCH]
        await execute_dbsql(
            f"INSERT INTO {table_sql} ({columns}) VALUES {', '.join(chunk)}",
            warehouse_id,
        )
    return len(values)


async def sync_reference_data(
    *,
    alerts: Iterable[dict[str, Any]] | None = None,
    ws: Any | None = None,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    catalog = _catalog()
    schema = _schema()
    wh = _warehouse(ws, warehouse_id)
    tables = reference_tables()
    counts = {
        "assets": await _replace_table(
            _quoted_table("intelligence_assets"),
            wh,
            _asset_rows(),
            "industry, site_id, asset_id, name",
        ),
        "sensor_bindings": await _replace_table(
            _quoted_table("intelligence_sensor_bindings"),
            wh,
            _sensor_rows(),
            "industry, site_id, asset_id, component_id, sensor_id, sensor_name, cadence_hz",
        ),
        "maintenance_records": await _replace_table(
            _quoted_table("intelligence_maintenance_records"),
            wh,
            _maintenance_rows(),
            "industry, event_id, asset_id, component_id, kind, started_at, completed_at, notes",
        ),
        "alert_rules": await _replace_table(
            _quoted_table("intelligence_alert_rules"),
            wh,
            _alert_rule_rows(),
            "industry, rule_id, sensor_id, operator, threshold, severity, fault_code",
        ),
        "alert_snapshot": await _replace_table(
            _quoted_table("intelligence_alert_snapshot"),
            wh,
            _alert_snapshot_rows(alerts or ()),
            ALERT_SNAPSHOT_COLUMN_LIST,
        ),
    }
    logger.info("Intelligence catalog sync complete: %s", counts)
    return {"catalog": catalog, "schema": schema, "tables": tables, "rows": counts}
