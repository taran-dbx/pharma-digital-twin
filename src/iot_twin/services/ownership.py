"""Ownership checks for resource-ledger rows before anything is torn down.

The ledger (``provisioned_resource``) is read back from a database. Before a
destructive path acts on it, every row that names a workspace resource must
name one owned by this deployment (see :func:`iot_twin.core.config.assert_owned_name`).
"""

from __future__ import annotations

from typing import Any

from iot_twin.core.config import NotOwnedError, assert_owned_name

# Types whose identifier is a dotted UC name: catalog[.schema[.object]].
_UC_TYPES = frozenset({"uc_catalog", "uc_schema", "uc_table", "uc_volume"})
# Types whose identifier is itself a resource name.
_NAMED_TYPES = frozenset({"secret_scope", "kafka_topic"})


def _uc_parts(identifier: str) -> list[str]:
    return [part.strip("`") for part in identifier.split(".")]


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
    """
    rtype = str(row.get("resource_type") or "")
    ident = str(row.get("identifier") or "").strip()
    metadata = row.get("metadata") or {}
    what = f"ledger {rtype or 'resource'}"

    if ident.startswith("projects/"):
        assert_owned_name(ident, what=what)
        return
    if rtype in _UC_TYPES:
        parts = _uc_parts(ident)
        if shared_catalog and parts[0] == shared_catalog:
            if len(parts) < 2:
                raise NotOwnedError(f"{what} {ident!r} is the shared catalog itself")
            assert_owned_name(parts[1], what=f"{what} schema")
        else:
            assert_owned_name(parts[0], what=f"{what} catalog")
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
