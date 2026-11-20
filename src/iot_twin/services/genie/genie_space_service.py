"""Per-pipeline Genie space lifecycle, for every industry the app offers.

One Genie space per ``(setup_id, mode)``, auto-created (or refreshed) after a
successful deploy.  The space contains only tables that exist for the mode:

- the mode's telemetry table (``{catalog}.{schema}.{industry}_{mode}``, or the
  CDF shadow history table for realtime),
- the shared streaming metrics table ``{industry}_metrics`` (modes that write
  one — not zerobus),
- the dedicated ``intelligence_*`` reference tables.

Wording and examples are generated from the industry catalog
(:mod:`genie_industry_profile`); KPIs (downtime / availability / health index
or OEE / anomalies) are answered through Genie text instructions, example SQL
and SQL snippets — no new tables.

**Persistence**: the space id lives in the ``provisioned_resource`` ledger
(``resource_type='genie_space'``, ``metadata.mode``).  The ledger is already
the per-setup source of truth for teardown, so no schema migration is needed;
:func:`find_space_id` is the ``(setup_id, mode)`` lookup.

``serialized_space`` follows the Genie v2 format: every id is a 32-char
lowercase hex string, text fields are string arrays, id-keyed arrays are sorted
by ``id`` and tables by ``identifier``.  Ids are derived deterministically so a
redeploy produces an identical payload.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import uuid
from typing import Any

from databricks.sdk.errors import NotFound

from iot_twin.services import resource_ledger
from iot_twin.services.cdf_reconciliation_service import delta_table_fqn
from iot_twin.services.db.app_repo_common import DBExecutor
from iot_twin.services.deployment import deployment_repo
from iot_twin.services.genie import genie_space_content
from iot_twin.services.genie.genie_industry_profile import (
    is_known_industry,
    mode_has_metrics,
    profile_for,
)
from iot_twin.services.genie.genie_space_content import (
    column_configs,
    example_sqls,
    sql_snippets,
    text_instructions,
)
from iot_twin.services.genie_service import invalidate_allowlist_cache
from iot_twin.services.industry.industry_catalog_service import industry_table
from iot_twin.services.intelligence_catalog_service import reference_tables
from iot_twin.services.pipelines import pipeline_setup_repo

logger = logging.getLogger("iot_digital_twin.services.genie_space")

RESOURCE_TYPE = "genie_space"
# Higher teardown_order is deleted first: the space references the tables
# (20-35) and is independent of the jobs (40), so trash it before both.
TEARDOWN_ORDER_GENIE_SPACE = 45
PARENT_PATH_ENV = "GENIE_SPACE_PARENT_PATH"
PIPELINE_MODES: tuple[str, ...] = ("realtime", "microbatch", "zerobus")
MAX_RESULT_ROWS = 500
SERIALIZED_VERSION = 2
INTELLIGENCE_SYNC_HINT = "run intelligence sync (POST /api/admin/intelligence/sync) then redeploy"

__all__ = [
    "GenieSpaceError",
    "MAX_RESULT_ROWS",
    "PARENT_PATH_ENV",
    "RESOURCE_TYPE",
    "build_serialized_space",
    "ensure_pipeline_space",
    "expected_tables",
    "find_pipeline_space",
    "find_space_id",
    "make_genie_space_deleter",
    "query_result_payload",
    "starter_questions",
    "industry_supported",
    "metrics_table",
    "telemetry_table",
]


class GenieSpaceError(RuntimeError):
    """Raised when a per-pipeline Genie space cannot be provisioned."""


class PipelineNotFound(LookupError):
    """The pipeline id is not a UUID / unknown setup / deployment of another mode."""


def _canonical_uuid(raw: object) -> str:
    try:
        return str(uuid.UUID(str(raw)))
    except (ValueError, AttributeError, TypeError) as exc:
        raise PipelineNotFound("unknown pipeline") from exc


async def resolve_pipeline_setup(db: DBExecutor, pipeline_id: object, mode: str) -> dict:
    """Resolve a ``pipeline_setup.id`` or a ``pipeline_deployment.id`` of ``mode``.

    Single source for the Genie routes and the admin provisioning endpoint.
    Invalid UUIDs never reach the database.  Raises :class:`PipelineNotFound`.
    """
    canonical = _canonical_uuid(pipeline_id)
    setup = await pipeline_setup_repo.get_setup(db, canonical)
    if setup is not None:
        return setup
    deployment = await deployment_repo.get_deployment(db, canonical)
    if deployment is None or deployment.get("mode") != mode or not deployment.get("setup_id"):
        raise PipelineNotFound("unknown pipeline")
    setup = await pipeline_setup_repo.get_setup(db, deployment["setup_id"])
    if setup is None:
        raise PipelineNotFound("unknown pipeline")
    return setup


# ---------------------------------------------------------------------------
# Scope helpers
# ---------------------------------------------------------------------------
def industry_supported(industry: str | None) -> bool:
    """Return True for every industry the app offers (curated template exists)."""
    return is_known_industry(industry)


def starter_questions(industry: str, mode: str) -> tuple[str, ...]:
    """Starter questions for ``(industry, mode)`` — industry-aware."""
    _require_mode(mode)
    return genie_space_content.starter_questions(mode, profile_for(industry))


def _require_mode(mode: str) -> str:
    if mode not in PIPELINE_MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {PIPELINE_MODES}")
    return mode


def telemetry_table(industry: str, mode: str) -> str:
    """FQN of the UC table that actually holds ``mode``'s sensor readings.

    The realtime (RTM) stream writes Lakebase; Lakehouse Sync mirrors it into
    the CDF shadow history table, so ``{industry}_realtime`` stays empty.
    Micro-batch and zerobus write ``{industry}_{mode}`` directly.
    """
    _require_mode(mode)
    if mode == "realtime":
        return delta_table_fqn(industry)
    return industry_table(industry, mode)


def metrics_table(industry: str, mode: str) -> str | None:
    """FQN of the shared streaming-metrics table, or None when the mode has none."""
    _require_mode(mode)
    return industry_table(industry, "metrics") if mode_has_metrics(mode) else None


def expected_tables(industry: str, mode: str) -> tuple[str, ...]:
    """Return the sorted FQN set a pipeline space must contain — no more, no less.

    Only tables that exist for the mode are listed (zerobus has no metrics table).
    """
    _require_mode(mode)
    metrics = metrics_table(industry, mode)
    tables = {
        telemetry_table(industry, mode),
        *([metrics] if metrics else []),
        *reference_tables().values(),
    }
    return tuple(sorted(tables))


def _stable_id(*parts: str) -> str:
    """Deterministic 32-hex id (Genie requires lowercase hex without hyphens)."""
    return hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()[:32]


def _parent_path() -> str | None:
    value = (os.getenv(PARENT_PATH_ENV) or "").strip()
    return value or None


def space_title(industry: str, mode: str, setup_id: Any) -> str:
    return f"{profile_for(industry).name} {mode} pipeline ({setup_id})"


def space_description(industry: str, mode: str) -> str:
    profile = profile_for(industry)
    metrics = " streaming metrics and" if mode_has_metrics(mode) else ""
    return (
        f"{profile.name} {mode} telemetry,{metrics} intelligence "
        f"reference tables. Answers {profile.health_term}, downtime, anomaly, fault and "
        "maintenance questions for this pipeline."
    )


# ---------------------------------------------------------------------------
# serialized_space builder
# ---------------------------------------------------------------------------
def _table_entries(industry: str, mode: str) -> list[dict[str, Any]]:
    profile = profile_for(industry)
    telemetry = telemetry_table(industry, mode)
    entries = []
    for fqn in expected_tables(industry, mode):
        entry: dict[str, Any] = {"identifier": fqn}
        if fqn == telemetry:
            entry["column_configs"] = sorted(
                column_configs(profile, cdc=mode == "realtime"), key=lambda c: c["column_name"]
            )
        entries.append(entry)
    return sorted(entries, key=lambda e: e["identifier"])


def _with_ids(kind: str, mode: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keyed = [{"id": _stable_id(kind, mode, str(i)), **item} for i, item in enumerate(items)]
    return sorted(keyed, key=lambda item: item["id"])


def _sample_questions(industry: str, mode: str) -> list[dict[str, Any]]:
    return _with_ids("sample", mode, [{"question": [q]} for q in starter_questions(industry, mode)])


def _instructions(industry: str, mode: str) -> dict[str, Any]:
    telemetry = telemetry_table(industry, mode)
    metrics = metrics_table(industry, mode)
    refs = reference_tables()
    profile = profile_for(industry)
    snippets = sql_snippets(telemetry, mode, profile)
    return {
        "text_instructions": _with_ids(
            "text", mode, [{"content": text_instructions(telemetry, metrics, mode, profile)}]
        ),
        "example_question_sqls": _with_ids(
            "example",
            mode,
            [
                {"question": [question], "sql": [sql]}
                for question, sql in example_sqls(telemetry, metrics, refs, mode, profile)
            ],
        ),
        "sql_snippets": {
            key: _with_ids(f"snippet-{key}", mode, list(items)) for key, items in snippets.items()
        },
    }


def build_serialized_space(industry: str, mode: str) -> str:
    """Return the Genie v2 ``serialized_space`` JSON string for a pipeline."""
    _require_mode(mode)
    payload = {
        "version": SERIALIZED_VERSION,
        "config": {"sample_questions": _sample_questions(industry, mode)},
        "data_sources": {"tables": _table_entries(industry, mode)},
        "instructions": _instructions(industry, mode),
    }
    return json.dumps(payload, sort_keys=True)


# ---------------------------------------------------------------------------
# Ledger lookup
# ---------------------------------------------------------------------------
def _is_active_space(row: dict, mode: str) -> bool:
    metadata = row.get("metadata") or {}
    return (
        row.get("resource_type") == RESOURCE_TYPE
        and (row.get("status") or "active") == "active"
        and metadata.get("mode") == mode
        and bool(row.get("identifier"))
    )


async def find_pipeline_space(db: DBExecutor, setup_id: Any, mode: str) -> dict | None:
    """Return the newest active ``genie_space`` ledger row for ``(setup_id, mode)``."""
    rows = await resource_ledger.list_resources(db, setup_id)
    matches = [row for row in rows if row and _is_active_space(row, mode)]
    return matches[-1] if matches else None


async def find_space_id(db: DBExecutor, setup_id: Any, mode: str) -> str | None:
    row = await find_pipeline_space(db, setup_id, mode)
    return str(row["identifier"]) if row else None


# ---------------------------------------------------------------------------
# Create / update
# ---------------------------------------------------------------------------
def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _space_kwargs(mode: str, setup_id: Any, industry: str, warehouse_id: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "warehouse_id": warehouse_id,
        "serialized_space": build_serialized_space(industry, mode),
        "title": space_title(industry, mode, setup_id),
        "description": space_description(industry, mode),
    }
    parent = _parent_path()
    if parent:
        kwargs["parent_path"] = parent
    return kwargs


async def _create_space(w: Any, kwargs: dict[str, Any]) -> str:
    space = await asyncio.to_thread(lambda: w.genie.create_space(**kwargs))
    space_id = getattr(space, "space_id", None)
    if not space_id:
        raise GenieSpaceError("Genie create_space returned no space_id")
    return str(space_id)


async def _update_space(w: Any, space_id: str, kwargs: dict[str, Any]) -> bool:
    """Update in place; return False when the recorded space no longer exists."""
    update = {key: value for key, value in kwargs.items() if key != "parent_path"}
    try:
        await asyncio.to_thread(lambda: w.genie.update_space(space_id, **update))
    except NotFound:
        logger.warning("Recorded Genie space %s is gone — recreating", space_id)
        return False
    return True


async def _record_space(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    space_id: str,
    industry: str,
    mode: str,
    kwargs: dict[str, Any],
) -> None:
    await resource_ledger.record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type=RESOURCE_TYPE,
        identifier=space_id,
        teardown_order=TEARDOWN_ORDER_GENIE_SPACE,
        metadata={
            "mode": mode,
            "industry": industry,
            "title": kwargs["title"],
            "parent_path": kwargs.get("parent_path"),
            "tables": list(expected_tables(industry, mode)),
        },
    )


async def genie_warehouse(db: DBExecutor, setup: dict) -> str:
    """Setup-pinned warehouse first, else the app-global DBSQL resolution."""
    from iot_twin.services.dbsql_service import resolve_warehouse_id

    pinned = str(setup.get("warehouse_id") or "").strip()
    return pinned or await resolve_warehouse_id(db, True, "")


def _table_exists(w: Any, fqn: str) -> bool | None:
    """True/False from UC; None when existence cannot be determined."""
    try:
        response = w.tables.exists(fqn)
    except Exception as exc:
        logger.info("Could not check table %s before Genie create: %s", fqn, exc)
        return None
    value = getattr(response, "table_exists", None)
    return None if value is None else bool(value)


async def _require_intelligence_tables(w: Any) -> None:
    """Fail with a sync hint when intelligence_* tables are known to be missing."""
    names = sorted(reference_tables().values())
    found = await asyncio.to_thread(lambda: [(n, _table_exists(w, n)) for n in names])
    missing = [name for name, exists in found if exists is False]
    if missing:
        raise GenieSpaceError(
            f"intelligence tables missing: {', '.join(missing)}; {INTELLIGENCE_SYNC_HINT}"
        )


# Strong refs to in-flight create+record tasks so a task outliving an outer
# timeout is neither garbage-collected nor left with an unretrieved exception.
_BACKGROUND_CREATES: set[asyncio.Task] = set()


def _log_create_outcome(task: asyncio.Task, setup_id: Any, mode: str) -> None:
    _BACKGROUND_CREATES.discard(task)
    if task.cancelled():
        logger.warning("Genie space create for setup %s mode %s was cancelled", setup_id, mode)
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "Genie space create for setup %s mode %s failed: %s",
            setup_id,
            mode,
            exc,
            exc_info=exc,
        )
        return
    logger.info(
        "Genie space %s created and recorded for setup %s mode %s",
        task.result(),
        setup_id,
        mode,
    )


def _track(task: asyncio.Task, setup_id: Any, mode: str) -> asyncio.Task:
    _BACKGROUND_CREATES.add(task)
    task.add_done_callback(lambda done: _log_create_outcome(done, setup_id, mode))
    return task


async def _create_and_record(
    db: DBExecutor,
    client: Any,
    kwargs: dict[str, Any],
    *,
    setup_id: Any,
    deployment_id: Any,
    industry: str,
    mode: str,
) -> str:
    space_id = await _create_space(client, kwargs)
    await _record_space(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        space_id=space_id,
        industry=industry,
        mode=mode,
        kwargs=kwargs,
    )
    return space_id


def _validate_inputs(industry: str, mode: str, warehouse_id: str | None) -> None:
    if not industry_supported(industry):
        raise GenieSpaceError(f"unknown industry {industry!r}: no Genie space can be built")
    _require_mode(mode)
    if not (warehouse_id or "").strip():
        raise GenieSpaceError("no SQL warehouse configured for the Genie space")


async def ensure_pipeline_space(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    industry: str,
    mode: str,
    warehouse_id: str | None,
    w: Any | None = None,
) -> dict[str, Any]:
    """Create the pipeline's Genie space, or update it in place on redeploy.

    Returns ``{"space_id", "action": created|updated|recreated, "tables"}``.
    Raises :class:`GenieSpaceError` (or the SDK error) on failure — the deploy
    hook turns any failure into a warning.
    """
    _validate_inputs(industry, mode, warehouse_id)
    industry_key = industry.strip().lower()
    client = w or _new_ws()
    kwargs = _space_kwargs(mode, setup_id, industry_key, str(warehouse_id).strip())
    tables = list(expected_tables(industry_key, mode))

    existing = await find_pipeline_space(db, setup_id, mode)
    if existing is not None:
        space_id = str(existing["identifier"])
        invalidate_allowlist_cache(space_id)
        if await _update_space(client, space_id, kwargs):
            # A concurrent check may have re-cached the pre-update tables.
            invalidate_allowlist_cache(space_id)
            return {"space_id": space_id, "action": "updated", "tables": tables}
        await resource_ledger.mark_resource_status(db, existing.get("id"), "deleted")

    await _require_intelligence_tables(client)
    # Shielded: if an outer timeout fires mid-create, the space still gets its
    # ledger row (so teardown can trash it) instead of being orphaned.
    space_id = await asyncio.shield(
        _track(
            asyncio.create_task(
                _create_and_record(
                    db,
                    client,
                    kwargs,
                    setup_id=setup_id,
                    deployment_id=deployment_id,
                    industry=industry_key,
                    mode=mode,
                )
            ),
            setup_id,
            mode,
        )
    )
    action = "recreated" if existing is not None else "created"
    logger.info("Genie space %s %s for setup %s mode %s", space_id, action, setup_id, mode)
    return {"space_id": space_id, "action": action, "tables": tables}


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
def make_genie_space_deleter(w: Any | None = None):
    """Return an async teardown deleter for ``resource_type='genie_space'`` rows."""
    box: dict[str, Any] = {"ws": w}

    async def _delete(resource_row: dict) -> None:
        space_id = resource_row.get("identifier")
        if not space_id:
            return
        if box["ws"] is None:
            box["ws"] = _new_ws()
        client = box["ws"]
        try:
            await asyncio.to_thread(client.genie.trash_space, str(space_id))
        except NotFound:
            logger.info("Genie space %s already absent during teardown", space_id)
            return
        logger.info("Trashed Genie space %s during teardown", space_id)

    return _delete


# ---------------------------------------------------------------------------
# Attachment query-result shaping
# ---------------------------------------------------------------------------
_EMPTY_RESULT: dict[str, Any] = {"columns": [], "rows": [], "truncated": False}


def _column_type(column: Any) -> str | None:
    type_text = getattr(column, "type_text", None)
    if type_text:
        return str(type_text)
    type_name = getattr(column, "type_name", None)
    value = getattr(type_name, "value", None)
    if value:
        return str(value)
    return str(type_name) if type_name is not None else None


def _columns(manifest: Any) -> list[dict[str, Any]]:
    schema = getattr(manifest, "schema", None)
    columns = getattr(schema, "columns", None) or []
    return [{"name": getattr(c, "name", None), "type": _column_type(c)} for c in columns]


def _is_truncated(manifest: Any, total_rows: int, max_rows: int) -> bool:
    if total_rows > max_rows or bool(getattr(manifest, "truncated", False)):
        return True
    reported = getattr(manifest, "total_row_count", None)
    return isinstance(reported, int) and reported > min(total_rows, max_rows)


def query_result_payload(response: Any, max_rows: int = MAX_RESULT_ROWS) -> dict[str, Any]:
    """Shape ``get_message_attachment_query_result`` into ``{columns, rows, truncated}``."""
    statement = getattr(response, "statement_response", None)
    if statement is None:
        return dict(_EMPTY_RESULT)
    manifest = getattr(statement, "manifest", None)
    data = getattr(getattr(statement, "result", None), "data_array", None) or []
    rows = [list(row) for row in data[:max_rows]]
    return {
        "columns": _columns(manifest),
        "rows": rows,
        "truncated": _is_truncated(manifest, len(data), max_rows),
    }
