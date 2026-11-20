"""Concrete per-``resource_type`` teardown deleters (spec §2.4, §1.8, §1.11).

``make_all_deleters()`` builds a registry mapping every ``resource_type`` the
provisioning services record (see ``services/resource_ledger.RESOURCE_TYPES``
plus ``lakebase_project``) to an async deleter, so the teardown orchestrator can
remove uniquely owned cloud resources instead of silently ``skip``-ing them and
then FK-cascade-dropping the ledger row. Shared UC catalogs and schemas are
explicit preservation-only entries and are never removed by this registry.

Deleter contract (see ``iot_twin.services.teardown_service``):
- return normally  → the orchestrator records the row ``deleted``;
- raise ``SkipResource`` → recorded ``skipped`` (intentional, not a failure);
- raise anything else → recorded ``failed`` (surfaced; setup NOT cascaded).

Guard rails:
- **Shared** resources (``metadata.shared`` — the shared SP, ``iot-secrets``
  scope, and the shared ``iot-pipelines`` project) are skipped both by the
  orchestrator and, defensively, by their own deleter.
- **Kafka topics** are skipped with a clear note — ``kafka-cluster:DeleteTopic``
  is denied on this cluster (spec §1.8); topics are cleaned up manually on MSK.

External S3 objects behind the metrics table are deleted with **UC
temporary-path-credentials → boto3** (stock runtime boto3), mirroring the
metrics-writer's temp-credential pattern (spec §1.11).

Injectable seams (``ws`` / ``warehouse_id`` / ``run_sql`` / ``delete_s3_prefix``)
keep every deleter unit-testable without touching a live workspace.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from databricks.sdk.errors import NotFound

from iot_twin.services import cdf_config_service
from iot_twin.services.deployment.deployment_service import make_job_deleter
from iot_twin.services.genie.genie_space_service import make_genie_space_deleter
from iot_twin.services.teardown_service import Deleter, SkipResource

logger = logging.getLogger("iot_digital_twin.services.resource_deleters")

_S3_REGION = "us-east-1"
_TEMP_PATH_CREDS_ENDPOINT = "/api/2.1/unity-catalog/temporary-path-credentials"


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _is_shared(res: dict) -> bool:
    return bool((res.get("metadata") or {}).get("shared"))


# ---------------------------------------------------------------------------
# External S3 prefix deletion (UC temporary-path-credentials + boto3)
# ---------------------------------------------------------------------------
def _parse_s3_url(location: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key/prefix`` URL into ``(bucket, prefix)``."""
    if not location or not location.startswith("s3://"):
        raise ValueError(f"not an s3:// URL: {location!r}")
    rest = location[len("s3://") :]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def _generate_temp_path_credentials(ws: Any, url: str) -> dict:
    """Mint short-lived AWS creds for ``PATH_DELETE`` on ``url`` via UC.

    Returns ``{"access_key_id", "secret_access_key", "session_token"}``.
    """
    resp = ws.api_client.do(
        "POST",
        _TEMP_PATH_CREDS_ENDPOINT,
        body={"url": url, "operation": "PATH_READ_WRITE"},
    )
    creds = (resp or {}).get("aws_temp_credentials") or {}
    return {
        "access_key_id": creds.get("access_key_id"),
        "secret_access_key": creds.get("secret_access_key"),
        "session_token": creds.get("session_token"),
    }


def _delete_s3_prefix_sync(ws: Any, location: str) -> int:
    """Delete every object under ``location`` (blocking).  Returns count deleted.

    Uses the **stock runtime boto3** (do NOT upgrade it — breaks the DBR kernel,
    spec §1.8) with UC-minted temporary path credentials.
    """
    import boto3

    bucket, prefix = _parse_s3_url(location)
    creds = _generate_temp_path_credentials(ws, location)
    session = boto3.session.Session(
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        aws_session_token=creds["session_token"],
        region_name=_S3_REGION,
    )
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    batch: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:  # S3 delete_objects caps at 1000 keys
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted += len(batch)
                batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)
    logger.info("teardown: deleted %d S3 object(s) under %s", deleted, location)
    return deleted


async def _delete_s3_prefix(ws: Any, location: str) -> int:
    """Async wrapper around :func:`_delete_s3_prefix_sync` (off the event loop)."""
    import asyncio

    return await asyncio.to_thread(_delete_s3_prefix_sync, ws, location)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
def make_all_deleters(
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    run_sql: Callable[[str], Awaitable[Any]] | None = None,
    delete_s3_prefix: Callable[[str], Awaitable[Any]] | None = None,
) -> dict[str, Deleter]:
    """Build the full ``resource_type`` → deleter registry.

    Args:
        ws: WorkspaceClient (created lazily on first use when omitted).
        warehouse_id: SQL warehouse for UC DDL (resolved from ``ws`` when None).
        run_sql: async ``(sql) -> result`` override (defaults to DBSQL execution).
        delete_s3_prefix: async ``(location) -> None`` override for S3 cleanup.
    """
    ws_box: dict[str, Any] = {"ws": ws}

    def _ws() -> Any:
        if ws_box["ws"] is None:
            ws_box["ws"] = _new_ws()
        return ws_box["ws"]

    _run_sql = run_sql or _default_run_sql(_ws, warehouse_id)
    _del_s3 = delete_s3_prefix or (lambda loc: _delete_s3_prefix(_ws(), loc))

    # --- UC ---------------------------------------------------------------
    async def preserve_uc_namespace(res: dict) -> None:
        # Catalogs and schemas are shared containers. Their lifecycle requires a
        # separate admin workflow with ownership and emptiness checks; a pipeline
        # delete must never issue namespace DDL.
        raise SkipResource(
            "catalog/schema is shared infrastructure — pipeline teardown preserves it"
        )

    async def del_uc_volume(res: dict) -> None:
        await _run_sql(f"DROP VOLUME IF EXISTS {res['identifier']}")

    async def del_uc_table(res: dict) -> None:
        # The only ``uc_table`` recorded today is a Postgres ChronoTable living
        # inside the Lakebase branch (metadata.chronotable) — it is removed when
        # the branch is deleted and is not reachable via DBSQL, so skip it.
        if (res.get("metadata") or {}).get("chronotable"):
            raise SkipResource(
                "ChronoTable lives inside the Lakebase branch — removed when the "
                "branch is deleted (not a UC table)"
            )
        if not res.get("_ownership_verified"):
            raise SkipResource("UC table pipeline ownership is unverified — preserving table")
        try:
            _ws().tables.delete(full_name=res["identifier"])
        except NotFound:
            logger.info("UC table %s already absent during teardown", res["identifier"])

    async def del_metrics_table(res: dict) -> None:
        # EXTERNAL Delta table: drop the UC object AND delete its S3 objects
        # (otherwise the data files are orphaned in the shared bucket).
        if not res.get("_ownership_verified"):
            raise SkipResource("metrics table pipeline ownership is unverified — preserving table")
        try:
            _ws().tables.delete(full_name=res["identifier"])
        except NotFound:
            logger.info("Metrics table %s already absent during teardown", res["identifier"])
        location = (res.get("metadata") or {}).get("location")
        if location:
            try:
                await _del_s3(location)
            except (ImportError, ModuleNotFoundError) as exc:
                # The deployed Databricks App runtime has no ``boto3`` (S3
                # cleanup is designed for the DBR kernel).  The UC table is
                # already dropped; degrade to a logged warning so a missing
                # boto3 never fails teardown and blocks the cascade delete of
                # the setup row.  The S3 data files are left orphaned.
                logger.warning(
                    "teardown: boto3 unavailable (%s) — dropped UC table '%s' but "
                    "could not delete S3 objects under %s (left orphaned)",
                    exc,
                    res.get("identifier"),
                    location,
                )
        else:
            logger.warning(
                "teardown: metrics_table '%s' has no S3 location in metadata — "
                "S3 objects may be orphaned",
                res.get("identifier"),
            )

    async def del_grant(res: dict) -> None:
        # Best-effort revoke: harmless (and expected to fail) once the object is
        # dropped, so never let it fail the teardown.
        sql = (res.get("metadata") or {}).get("sql")
        if not sql:
            return
        revoke = _grant_to_revoke(sql)
        try:
            await _run_sql(revoke)
        except Exception as exc:  # best-effort — object likely already dropped
            logger.info("teardown: best-effort revoke skipped (%s): %s", exc, revoke)

    # --- Lakebase ----------------------------------------------------------
    async def del_lakebase_branch(res: dict) -> None:
        _ws().postgres.delete_branch(name=res["identifier"])
        logger.info("teardown: deleted Lakebase branch %s", res["identifier"])

    async def del_lakebase_endpoint(res: dict) -> None:
        # The branch's auto-created primary (READ_WRITE) endpoint cannot be
        # deleted on its own — it is removed when the branch is deleted.
        md = res.get("metadata") or {}
        name = res.get("identifier") or ""
        if md.get("endpoint_type") == "READ_WRITE" or name.endswith("/primary"):
            raise SkipResource("primary endpoint is auto-removed when its branch is deleted")
        _ws().postgres.delete_endpoint(name=name)
        logger.info("teardown: deleted Lakebase endpoint %s", name)

    async def del_lakebase_project(res: dict) -> None:
        raise SkipResource("shared iot-pipelines project — never torn down per-industry")

    # --- Identity / secrets ------------------------------------------------
    async def del_service_principal(res: dict) -> None:
        if _is_shared(res):
            raise SkipResource("shared streaming SP — never torn down per-industry")
        sp_id = (res.get("metadata") or {}).get("id") or res.get("identifier")
        _ws().service_principals.delete(id=sp_id)
        logger.info("teardown: deleted service principal %s", sp_id)

    async def del_secret_scope(res: dict) -> None:
        if _is_shared(res):
            raise SkipResource("shared secret scope — never torn down per-industry")
        _ws().secrets.delete_scope(scope=res["identifier"])
        logger.info("teardown: deleted secret scope %s", res["identifier"])

    async def del_secret(res: dict) -> None:
        if _is_shared(res):
            raise SkipResource("shared secret — never torn down per-industry")
        md = res.get("metadata") or {}
        scope = md.get("scope")
        key = md.get("key") or res.get("identifier")
        _ws().secrets.delete_secret(scope=scope, key=key)
        logger.info("teardown: deleted secret %s/%s", scope, key)

    # --- Kafka / CDF -------------------------------------------------------
    async def del_kafka_topic(res: dict) -> None:
        raise SkipResource(
            "kafka-cluster:DeleteTopic denied on this cluster (spec §1.8) — "
            "topic left in place; delete manually on the MSK cluster"
        )

    async def del_cdf_sync(res: dict) -> None:
        metadata = res.get("metadata") or {}
        parent = metadata.get("parent")
        config_id = metadata.get("cdf_config_id")
        if not parent or not config_id:
            raise SkipResource(
                "CDF sync metadata is incomplete — preserving config because "
                "its parent or config id is unknown"
            )
        deleted = await asyncio.to_thread(
            cdf_config_service.delete_cdf_config,
            parent,
            config_id,
            force=bool(metadata.get("force_delete_tables", False)),
            ws=_ws(),
        )
        logger.info(
            "teardown: CDF config %s (%s)",
            res.get("identifier"),
            "deleted" if deleted else "already absent",
        )

    return {
        "uc_catalog": preserve_uc_namespace,
        "uc_schema": preserve_uc_namespace,
        "uc_volume": del_uc_volume,
        "uc_table": del_uc_table,
        "metrics_table": del_metrics_table,
        "grant": del_grant,
        "job": make_job_deleter(ws),
        "lakebase_branch": del_lakebase_branch,
        "lakebase_endpoint": del_lakebase_endpoint,
        "lakebase_read_replica": del_lakebase_endpoint,
        "lakebase_project": del_lakebase_project,
        "service_principal": del_service_principal,
        "secret_scope": del_secret_scope,
        "secret": del_secret,
        "kafka_topic": del_kafka_topic,
        "cdf_sync": del_cdf_sync,
        "genie_space": make_genie_space_deleter(ws),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _default_run_sql(
    ws_getter: Callable[[], Any], warehouse_id: str | None
) -> Callable[[str], Awaitable[Any]]:
    """Return an async ``(sql) -> result`` that runs DDL via DBSQL.

    Resolves the warehouse lazily (explicit arg wins, else first listed) so the
    registry can be built before a warehouse is known.
    """
    wh_box: dict[str, str | None] = {"wh": warehouse_id}

    async def _run(sql: str) -> Any:
        from iot_twin.services.dbsql_service import execute_dbsql
        from iot_twin.services.industry.industry_catalog_service import resolve_warehouse_id

        if not wh_box["wh"]:
            wh_box["wh"] = resolve_warehouse_id(ws_getter(), None)
        return await execute_dbsql(sql, wh_box["wh"])

    return _run


def _grant_to_revoke(grant_sql: str) -> str:
    """Turn a recorded ``GRANT ... TO <p>`` statement into its ``REVOKE`` form."""
    revoke = grant_sql.strip()
    if revoke.upper().startswith("GRANT "):
        revoke = "REVOKE " + revoke[len("GRANT ") :]
    # ``GRANT <privs> ON <obj> TO <p>`` → ``REVOKE <privs> ON <obj> FROM <p>``.
    # Replace the last " TO " so object names containing "TO" are unaffected.
    idx = revoke.rfind(" TO ")
    if idx != -1:
        revoke = revoke[:idx] + " FROM " + revoke[idx + len(" TO ") :]
    return revoke
