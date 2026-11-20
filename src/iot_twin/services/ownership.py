"""Ownership checks for resource-ledger rows before anything is torn down.

The ledger (``provisioned_resource``) is read back from a database. Before a
destructive path acts on it, every row that names a workspace resource must
name one owned by this deployment (see :func:`iot_twin.core.config.assert_owned_name`).
"""

from __future__ import annotations

import os
from typing import Any

from iot_twin.core.config import NotOwnedError, assert_owned_name

# Types whose identifier is a dotted UC name: catalog[.schema[.object]].
_UC_TYPES = frozenset({"uc_catalog", "uc_schema", "uc_table", "uc_volume"})
# Types whose identifier is itself a resource name.
_NAMED_TYPES = frozenset({"secret_scope", "kafka_topic"})


def _uc_parts(identifier: str) -> list[str]:
    return [part.strip("`") for part in identifier.split(".")]


def assert_owned_uc(
    catalog: str,
    schema: str | None = None,
    *,
    shared_catalog: str = "",
    what: str = "UC target",
) -> None:
    """Raise :class:`NotOwnedError` unless this app owns the UC ``catalog``[.``schema``].

    The catalog must be owned. The one exception is the configured shared
    catalog (``UC_CATALOG``, deliberately reused): there the schema must be
    owned. The shared catalog on its own is never owned.
    """
    catalog = (catalog or "").strip().strip("`")
    if shared_catalog and catalog == shared_catalog:
        if not schema:
            raise NotOwnedError(f"{what} {catalog!r} is the shared catalog, not owned by this app")
        assert_owned_name(schema.strip().strip("`"), what=f"{what} schema")
        return
    assert_owned_name(catalog, what=f"{what} catalog")


def assert_owned_ledger_row(row: dict[str, Any], *, shared_catalog: str = "") -> None:
    """Raise :class:`NotOwnedError` if ``row`` names a resource this app does not own.

    - ``projects/...`` identifiers (Lakebase project / branch / endpoint): the
      project segment must be owned.
    - ``uc_catalog`` / ``uc_schema`` / ``uc_table`` / ``uc_volume``: the catalog
      segment must be owned. Or, when it is the configured shared catalog
      (``UC_CATALOG``, deliberately reused), the schema segment must be owned.
    - ``secret_scope`` / ``kafka_topic``: the identifier must be owned.
    - ``service_principal``: only the shared app SP (``metadata.shared``) may
      appear; it is never torn down. Any other SP must carry an owned
      ``metadata.display_name``.
    - Any other row flagged ``metadata.shared`` is exempt: teardown skips shared
      rows and never deletes them.
    """
    rtype = str(row.get("resource_type") or "")
    ident = str(row.get("identifier") or "").strip()
    metadata = row.get("metadata") or {}
    what = f"ledger {rtype or 'resource'}"
    if metadata.get("shared") and rtype != "service_principal":
        return  # shared rows (scope, SP, pipelines project) are never torn down

    if ident.startswith("projects/"):
        assert_owned_name(ident, what=what)
        return
    if rtype in _UC_TYPES:
        parts = _uc_parts(ident)
        assert_owned_uc(
            parts[0], parts[1] if len(parts) > 1 else None, shared_catalog=shared_catalog, what=what
        )
        return
    if rtype in _NAMED_TYPES:
        assert_owned_name(ident, what=what)
        return
    if rtype == "service_principal":
        if metadata.get("shared"):
            return
        assert_owned_name(str(metadata.get("display_name") or ""), what=f"{what} display_name")


def assert_owned_ledger(rows: list[dict[str, Any]], *, shared_catalog: str = "") -> None:
    """Check every ledger row; raise on the first one that is not owned."""
    for row in rows:
        assert_owned_ledger_row(row, shared_catalog=shared_catalog)


# --- Direct-mode Lakebase ----------------------------------------------------------
ALLOW_DIRECT_LAKEBASE_ENV = "ALLOW_DIRECT_LAKEBASE"
_TRUTHY = frozenset({"1", "true", "yes", "on"})


def direct_lakebase_enabled() -> bool:
    """True only when ``ALLOW_DIRECT_LAKEBASE`` is explicitly enabled (default off)."""
    return os.getenv(ALLOW_DIRECT_LAKEBASE_ENV, "false").strip().lower() in _TRUTHY


def owned_endpoint_hosts(
    project_id: str, branch_id: str = "production", *, ws: Any = None
) -> set[str]:
    """Return the endpoint hosts of an OWNED Lakebase branch (raises if not owned)."""
    project_id = assert_owned_name(project_id, what="Lakebase project_id")
    if ws is None:
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()
    parent = f"projects/{project_id}/branches/{branch_id or 'production'}"
    hosts: set[str] = set()
    for endpoint in ws.postgres.list_endpoints(parent=parent):
        detail = ws.postgres.get_endpoint(name=endpoint.name)
        host = getattr(getattr(getattr(detail, "status", None), "hosts", None), "host", None)
        if host:
            hosts.add(host)
    return hosts


def assert_direct_lakebase(lb: dict, *, internal: bool = False, ws: Any = None) -> None:
    """Gate a direct-mode (host/user/password) Lakebase connection.

    A host name does not reveal its project, so direct mode could otherwise
    reach any Postgres, including the upstream app's. Direct mode therefore
    needs ``ALLOW_DIRECT_LAKEBASE`` enabled (``internal=True`` is reserved for
    app code that resolved the host from an owned endpoint itself). In every
    case the host must be one of the endpoint hosts of an owned ``project_id``
    / ``branch_id`` given in ``lb``. Raises :class:`NotOwnedError`.
    """
    if not internal and not direct_lakebase_enabled():
        raise NotOwnedError(
            f"direct-mode Lakebase is disabled ({ALLOW_DIRECT_LAKEBASE_ENV}=false); use autoscale"
        )
    host = str(lb.get("host") or "").strip()
    project_id = str(lb.get("project_id") or "").strip()
    if not host or not project_id:
        raise NotOwnedError("direct-mode Lakebase needs host and an owned project_id")
    if host not in owned_endpoint_hosts(project_id, str(lb.get("branch_id") or ""), ws=ws):
        raise NotOwnedError(
            f"direct-mode host {host!r} is not an endpoint of owned project {project_id!r}"
        )
