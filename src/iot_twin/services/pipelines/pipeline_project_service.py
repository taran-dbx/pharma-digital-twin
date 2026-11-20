"""Shared ``iot-pipelines`` Lakebase project provisioning (spec §2.1–2.2).

Real-time streaming data for **all** real-time industry pipelines lives in a
single shared Lakebase project, ``iot-pipelines``.  Each real-time industry
pipeline gets **one never-expiring branch** (branch name = industry) with:

- a **primary ``READ_WRITE`` endpoint** — the streaming job writes here.  The
  branch **auto-provisions** its own ``primary`` endpoint, so we never create
  it; we only *resize* its autoscaling CU via ``update_endpoint`` + ``FieldMask``.
- **one read replica** (``READ_ONLY`` endpoint id ``replica``) — the app and
  dashboard read here (~6s replication lag; writes are correctly rejected).

Every provisioned resource (project / branch / primary / replica) is recorded
in the ``provisioned_resource`` ledger so teardown can walk them in reverse
dependency order.  The shared project is flagged ``metadata.shared`` so a
per-industry teardown never deletes it.

Reads resolve to the replica endpoint (``get_read_conn``) and writes to the
primary endpoint (``get_write_conn``), reusing ``core.lakebase.LakebaseConnectionManager``
for OAuth-refreshing asyncpg pools.

This module is **additive** — it does not touch ``app.yaml`` wiring,
``core/lakebase.py`` live wiring, or the legacy ``iot-lakets-demo`` project.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from iot_twin.core.lakebase import LakebaseConnectionManager
from iot_twin.core.workspace_config import get_pipelines_project
from iot_twin.services.resource_ledger import record_resource

logger = logging.getLogger("iot_digital_twin.services.pipelines_project")

# --- Constants -------------------------------------------------------------
# PIPELINES_PROJECT has been removed as an import-time constant to prevent
# workspace literals being locked in at module load.  Use get_pipelines_project()
# at call time, or pass project_id explicitly to any function below.
PG_DATABASE = "databricks_postgres"

PRIMARY_ENDPOINT_ID = "primary"
REPLICA_ENDPOINT_ID = "replica"

# Autoscaling CU limits.  Primary carries the streaming write load; the replica
# serves lighter dashboard reads.
PRIMARY_MIN_CU = 0.5
PRIMARY_MAX_CU = 4.0
REPLICA_MIN_CU = 0.5
REPLICA_MAX_CU = 2.0

# Ledger teardown_order for Lakebase resources.  Higher = deleted earlier
# (reverse dependency order).  Deleting the branch removes its auto-created
# primary endpoint, so replica must be deleted first, then branch.  The shared
# project is flagged ``shared`` and is never torn down per-industry.
TEARDOWN_ORDER_PROJECT = 5
TEARDOWN_ORDER_BRANCH = 20
TEARDOWN_ORDER_PRIMARY = 25
TEARDOWN_ORDER_REPLICA = 30
# Branch-scoped Postgres role — dropped implicitly when the branch is deleted, so
# it is recorded (for visibility) but torn down earliest and needs no deleter.
TEARDOWN_ORDER_ROLE = 35

# Endpoint readiness polling.
_WAIT_TIMEOUT_S = 120
_POLL_INTERVAL_S = 5
_ACTIVE_STATES = {"ACTIVE", "IDLE"}  # IDLE = autoscaled-to-zero but usable


def _new_ws() -> Any:
    """Return a default WorkspaceClient (matches the app-wide convention)."""
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


# --- Resource-name helpers -------------------------------------------------
def project_resource_name(project_id: str | None = None) -> str:
    return f"projects/{project_id or get_pipelines_project()}"


def branch_resource_name(branch: str, project_id: str | None = None) -> str:
    return f"projects/{project_id or get_pipelines_project()}/branches/{branch}"


def endpoint_resource_name(branch: str, endpoint_id: str, project_id: str | None = None) -> str:
    return f"projects/{project_id or get_pipelines_project()}/branches/{branch}/endpoints/{endpoint_id}"


# --- Error classification --------------------------------------------------
def _is_not_found(exc: Exception) -> bool:
    s = str(exc).lower()
    return "not_found" in s or "not found" in s or "does not exist" in s


def _is_already_exists(exc: Exception) -> bool:
    return "already exists" in str(exc).lower()


# --- Project ---------------------------------------------------------------
def ensure_pipelines_project(*, ws: Any | None = None, project_id: str | None = None) -> dict:
    """Ensure the shared ``iot-pipelines`` project exists (create if missing).

    Returns ``{"project", "name", "status"}`` where status is
    ``"existing"`` or ``"created"``.
    """
    from databricks.sdk.service.postgres import Project

    project_id = project_id or get_pipelines_project()
    ws = ws or _new_ws()
    name = project_resource_name(project_id)
    try:
        ws.postgres.get_project(name=name)
        logger.info("Lakebase project %s already exists", project_id)
        return {"project": project_id, "name": name, "status": "existing"}
    except Exception as exc:
        if not _is_not_found(exc):
            raise

    try:
        ws.postgres.create_project(project=Project(), project_id=project_id)
        logger.info("Created Lakebase project %s", project_id)
        return {"project": project_id, "name": name, "status": "created"}
    except Exception as exc:
        if _is_already_exists(exc):
            return {"project": project_id, "name": name, "status": "existing"}
        raise


# --- Endpoint readiness ----------------------------------------------------
def _endpoint_state(endpoint: Any) -> str | None:
    status = getattr(endpoint, "status", None)
    state = getattr(status, "current_state", None) if status else None
    if state is None:
        return None
    return getattr(state, "value", None) or getattr(state, "name", None) or str(state)


def _endpoint_host(endpoint: Any) -> str | None:
    status = getattr(endpoint, "status", None)
    hosts = getattr(status, "hosts", None) if status else None
    return getattr(hosts, "host", None) if hosts else None


async def _wait_endpoint_active(
    ws: Any,
    name: str,
    *,
    timeout_s: int = _WAIT_TIMEOUT_S,
    poll_interval_s: int = _POLL_INTERVAL_S,
) -> Any:
    """Poll ``get_endpoint(name)`` until it reports an active state or times out.

    Returns the last-seen endpoint (even on timeout) so the caller can still
    read the host if one is available.
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    endpoint = None
    while True:
        try:
            endpoint = ws.postgres.get_endpoint(name=name)
            state = _endpoint_state(endpoint)
            if state in _ACTIVE_STATES:
                return endpoint
        except Exception as exc:
            if not _is_not_found(exc):
                raise
        if asyncio.get_event_loop().time() >= deadline:
            logger.warning("Endpoint %s not active within %ss", name, timeout_s)
            return endpoint
        await asyncio.sleep(poll_interval_s)


# --- Branch + endpoints ----------------------------------------------------
async def provision_industry_branch(
    industry: str,
    *,
    ws: Any | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
    project_id: str | None = None,
    primary_min_cu: float = PRIMARY_MIN_CU,
    primary_max_cu: float = PRIMARY_MAX_CU,
    replica_min_cu: float = REPLICA_MIN_CU,
    replica_max_cu: float = REPLICA_MAX_CU,
    wait_timeout_s: int = _WAIT_TIMEOUT_S,
    poll_interval_s: int = _POLL_INTERVAL_S,
) -> dict:
    """Provision a never-expiring branch for ``industry`` on ``iot-pipelines``.

    Idempotent: an existing branch / replica is reused.  Steps:
      1. Ensure the shared project exists.
      2. Create the never-expiring branch (branch name = industry).
      3. Resize the branch's **auto-created** primary endpoint's autoscaling CU.
      4. Create one ``replica`` READ_ONLY endpoint; wait for both to be active.
      5. Record project / branch / primary / replica in the ledger (if ``db`` and
         ``setup_id`` are provided).

    Returns ``{project, branch, primary_endpoint, replica_endpoint,
    primary_host, replica_host}``.
    """
    from databricks.sdk.service.postgres import (
        Branch,
        BranchSpec,
        Endpoint,
        EndpointSpec,
        EndpointType,
        FieldMask,
    )

    project_id = project_id or get_pipelines_project()
    ws = ws or _new_ws()
    branch = industry
    parent_project = project_resource_name(project_id)
    branch_name = branch_resource_name(branch, project_id)
    primary_name = endpoint_resource_name(branch, PRIMARY_ENDPOINT_ID, project_id)
    replica_name = endpoint_resource_name(branch, REPLICA_ENDPOINT_ID, project_id)

    # 1. Shared project.
    ensure_pipelines_project(ws=ws, project_id=project_id)

    # 2. Never-expiring branch (idempotent).
    branch_created = False
    try:
        ws.postgres.get_branch(name=branch_name)
        logger.info("Lakebase branch %s already exists", branch)
    except Exception as exc:
        if not _is_not_found(exc):
            raise
        try:
            ws.postgres.create_branch(
                parent=parent_project,
                branch=Branch(spec=BranchSpec(no_expiry=True)),
                branch_id=branch,
            )
            branch_created = True
            logger.info("Created never-expiring Lakebase branch %s", branch)
        except Exception as create_exc:
            if not _is_already_exists(create_exc):
                raise

    # 3. Resize the auto-created primary endpoint (never create it).
    #    Wait for it to exist/activate first (it appears shortly after the
    #    branch is created).
    await _wait_endpoint_active(
        ws, primary_name, timeout_s=wait_timeout_s, poll_interval_s=poll_interval_s
    )
    try:
        ws.postgres.update_endpoint(
            name=primary_name,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=primary_min_cu,
                    autoscaling_limit_max_cu=primary_max_cu,
                )
            ),
            # Only the CU fields are in the mask — endpoint_type is required by
            # the EndpointSpec constructor but is not applied.
            update_mask=FieldMask(
                [
                    "spec.autoscaling_limit_min_cu",
                    "spec.autoscaling_limit_max_cu",
                ]
            ),
        )
        logger.info(
            "Resized primary endpoint %s to CU [%s, %s]",
            primary_name,
            primary_min_cu,
            primary_max_cu,
        )
    except Exception as exc:
        logger.warning("Primary endpoint resize skipped for %s: %s", primary_name, exc)

    # 4. Read replica (idempotent).  Hold onto the endpoint object from
    #    whichever path produced it — get_endpoint (exists) or create(...).wait()
    #    (created) — so we never re-poll a just-created endpoint.
    replica_created = False
    replica_ep = None
    try:
        replica_ep = ws.postgres.get_endpoint(name=replica_name)
        logger.info("Read replica %s already exists", replica_name)
    except Exception as exc:
        if not _is_not_found(exc):
            raise
        try:
            replica_ep = ws.postgres.create_endpoint(
                parent=branch_name,
                endpoint=Endpoint(
                    spec=EndpointSpec(
                        endpoint_type=EndpointType.ENDPOINT_TYPE_READ_ONLY,
                        autoscaling_limit_min_cu=replica_min_cu,
                        autoscaling_limit_max_cu=replica_max_cu,
                    )
                ),
                endpoint_id=REPLICA_ENDPOINT_ID,
            ).wait()
            replica_created = True
            logger.info("Created read replica %s", replica_name)
        except Exception as create_exc:
            if not _is_already_exists(create_exc):
                raise

    # 5. Ensure both endpoints are active + resolve hosts.  Poll the primary
    #    (auto-created — we hold no handle); poll the replica only if it was
    #    pre-existing without a resolvable host.
    primary_ep = await _wait_endpoint_active(
        ws, primary_name, timeout_s=wait_timeout_s, poll_interval_s=poll_interval_s
    )
    if _endpoint_host(replica_ep) is None:
        replica_ep = await _wait_endpoint_active(
            ws, replica_name, timeout_s=wait_timeout_s, poll_interval_s=poll_interval_s
        )
    primary_host = _endpoint_host(primary_ep)
    replica_host = _endpoint_host(replica_ep)

    result = {
        "project": project_id,
        "branch": branch,
        "primary_endpoint": primary_name,
        "replica_endpoint": replica_name,
        "primary_host": primary_host,
        "replica_host": replica_host,
    }

    # 5.5 Register the app's OWN service principal as a Postgres role on THIS
    #     branch.  Lakebase roles are BRANCH-scoped, so without this the app's
    #     read/write path (get_read_conn / get_write_conn) fails with "password
    #     authentication failed" on every freshly provisioned industry branch.
    result["app_sp_role"] = await _ensure_app_sp_branch_role(
        ws,
        project_id,
        branch,
        db=db,
        setup_id=setup_id,
        deployment_id=deployment_id,
    )

    # 6. Ledger.
    if db is not None and setup_id is not None:
        await _record_ledger(
            db,
            setup_id=setup_id,
            deployment_id=deployment_id,
            project_id=project_id,
            branch=branch,
            branch_name=branch_name,
            primary_name=primary_name,
            replica_name=replica_name,
            primary_host=primary_host,
            replica_host=replica_host,
            branch_created=branch_created,
            replica_created=replica_created,
        )

    return result


def _app_sp_client_id(ws: Any) -> str | None:
    """Resolve the app's own service-principal client id (its connect identity).

    Prefer the injected ``DATABRICKS_CLIENT_ID`` (set for the deployed app SP);
    fall back to the caller identity (``current_user.me().user_name`` is the SP's
    client id when the app runs as its own SP).
    """
    import os

    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    if client_id:
        return client_id
    try:
        return ws.current_user.me().user_name
    except Exception:  # pragma: no cover - defensive
        return None


async def _ensure_app_sp_branch_role(
    ws: Any,
    project_id: str,
    branch: str,
    *,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Register the app's own SP as an OAuth Postgres role on ``branch``.

    Roles are branch-scoped: this is what lets the app read the branch replica /
    write the primary without a "password authentication failed" error.  Mirrors
    the app-backend grant (``LAKEBASE_OAUTH_V1`` / ``SERVICE_PRINCIPAL`` /
    ``DATABRICKS_SUPERUSER``).  Best-effort — never raises — and recorded in the
    ledger for visibility.
    """
    from iot_twin.services import provisioning_service

    client_id = _app_sp_client_id(ws)
    if not client_id:
        logger.warning("Could not resolve app SP client id — skipping branch role for %s", branch)
        return {"status": "skipped", "reason": "unresolved app SP client id"}

    try:
        role_results = await provisioning_service.provision_lakebase_role(
            sp_client_id=client_id,
            project_id=project_id,
            branch_id=branch,
            ws=ws,
        )
    except Exception as exc:  # pragma: no cover - provision_lakebase_role is best-effort
        logger.warning("App SP branch-role registration failed for %s (non-fatal): %s", branch, exc)
        role_results = [{"status": "failed", "error": str(exc)[:200]}]

    if db is not None and setup_id is not None:
        await record_resource(
            db,
            setup_id=setup_id,
            deployment_id=deployment_id,
            resource_type="lakebase_role",
            identifier=f"{branch}:{client_id}",
            parent_ref=branch_resource_name(branch, project_id),
            teardown_order=TEARDOWN_ORDER_ROLE,
            metadata={
                "branch_scoped": True,
                "sp_client_id": client_id,
                "results": role_results,
            },
        )
    return {"client_id": client_id, "results": role_results}


async def _record_ledger(
    db: Any,
    *,
    setup_id: Any,
    deployment_id: Any | None,
    project_id: str,
    branch: str,
    branch_name: str,
    primary_name: str,
    replica_name: str,
    primary_host: str | None,
    replica_host: str | None,
    branch_created: bool,
    replica_created: bool,
) -> None:
    """Record the four Lakebase resources in the provisioned_resource ledger."""
    # Shared project — flagged so per-industry teardown never deletes it.
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type="lakebase_project",
        identifier=project_resource_name(project_id),
        teardown_order=TEARDOWN_ORDER_PROJECT,
        metadata={"shared": True, "project": project_id},
    )
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type="lakebase_branch",
        identifier=branch_name,
        parent_ref=project_resource_name(project_id),
        teardown_order=TEARDOWN_ORDER_BRANCH,
        metadata={"branch": branch, "no_expiry": True, "created": branch_created},
    )
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type="lakebase_endpoint",
        identifier=primary_name,
        parent_ref=branch_name,
        teardown_order=TEARDOWN_ORDER_PRIMARY,
        metadata={"endpoint_type": "READ_WRITE", "host": primary_host},
    )
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type="lakebase_read_replica",
        identifier=replica_name,
        parent_ref=branch_name,
        teardown_order=TEARDOWN_ORDER_REPLICA,
        metadata={
            "endpoint_type": "READ_ONLY",
            "host": replica_host,
            "created": replica_created,
        },
    )


# --- Connections -----------------------------------------------------------
def _resolve_endpoint_host(ws: Any, name: str) -> str | None:
    endpoint = ws.postgres.get_endpoint(name=name)
    return _endpoint_host(endpoint)


async def _open_conn(
    branch: str, endpoint_id: str, *, ws: Any | None, project_id: str
) -> LakebaseConnectionManager:
    ws = ws or _new_ws()
    name = endpoint_resource_name(branch, endpoint_id, project_id)
    host = _resolve_endpoint_host(ws, name)
    if not host:
        raise RuntimeError(f"Could not resolve host for endpoint {name}")
    manager = LakebaseConnectionManager(
        pg_host=host,
        pg_database=PG_DATABASE,
        endpoint_name=name,
    )
    await manager.initialize()
    return manager


async def get_write_conn(
    branch: str, *, ws: Any | None = None, project_id: str | None = None
) -> LakebaseConnectionManager:
    """Return an initialized connection manager for the branch's PRIMARY (write)."""
    return await _open_conn(
        branch, PRIMARY_ENDPOINT_ID, ws=ws, project_id=project_id or get_pipelines_project()
    )


async def get_read_conn(
    branch: str, *, ws: Any | None = None, project_id: str | None = None
) -> LakebaseConnectionManager:
    """Return an initialized connection manager for the branch's REPLICA (read)."""
    return await _open_conn(
        branch, REPLICA_ENDPOINT_ID, ws=ws, project_id=project_id or get_pipelines_project()
    )
