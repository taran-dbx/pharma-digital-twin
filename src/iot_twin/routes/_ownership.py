"""Request-boundary ownership checks shared by the routes.

Any route that accepts a resource name in its body (Lakebase project ids
in particular) checks it here first, so a request can never make the app touch
a resource outside this deployment's ``RESOURCE_PREFIX`` namespace. A rejected
name is a client error (403), not a server failure.
"""

from __future__ import annotations

from fastapi.responses import JSONResponse

from iot_twin.core.config import IsolationMisconfiguredError, NotOwnedError, assert_owned_name

FORBIDDEN = 403
MISCONFIGURED = 500


def misconfigured_response(exc: IsolationMisconfiguredError) -> JSONResponse:
    """Return the 500 body for a non-owned name in the app's own configuration."""
    return JSONResponse(
        status_code=MISCONFIGURED,
        content={"error": f"isolation misconfiguration: {exc}"},
    )


def not_owned_response(exc: NotOwnedError) -> JSONResponse:
    """Return 403 for a rejected request name, 500 for a misconfigured env name."""
    if isinstance(exc, IsolationMisconfiguredError):
        return misconfigured_response(exc)
    return JSONResponse(status_code=FORBIDDEN, content={"error": str(exc)})


def reject_foreign_name(name: str | None, *, what: str) -> JSONResponse | None:
    """Return a 403 response when ``name`` is set but not owned, else ``None``."""
    if not name:
        return None
    try:
        assert_owned_name(name, what=what)
    except NotOwnedError as exc:
        return not_owned_response(exc)
    return None


def reject_foreign_project(project_id: str | None) -> JSONResponse | None:
    """Return a 403 response when a Lakebase ``project_id`` is set but not owned."""
    return reject_foreign_name(project_id, what="Lakebase project_id")


def reject_foreign_uc(catalog: str, schemas: list[str] | None = None) -> JSONResponse | None:
    """Return a 403 unless ``catalog`` (and each schema in it) is owned.

    Same rule as the ledger check: an owned catalog, or the shared ``UC_CATALOG``
    with owned schemas.
    """
    from iot_twin.core.workspace_config import get_uc_catalog
    from iot_twin.services.ownership import assert_owned_uc

    try:
        for schema in schemas or [None]:
            assert_owned_uc(catalog, schema, shared_catalog=get_uc_catalog())
    except NotOwnedError as exc:
        return not_owned_response(exc)
    return None


def reject_foreign_uc_settings(uc: dict) -> JSONResponse | None:
    """Return a 403 unless a UC-settings body (catalog_pattern + schemas) is owned."""
    from iot_twin.services.provisioning_service import uc_settings_target

    catalog, schemas = uc_settings_target(uc)
    return reject_foreign_uc(catalog, schemas)


def reject_direct_lakebase(lb: dict) -> JSONResponse | None:
    """Return a 403 unless a request-supplied direct-mode Lakebase target is allowed.

    Needs ALLOW_DIRECT_LAKEBASE enabled and a host that is an endpoint of an
    owned ``project_id`` (see ``services.ownership.assert_direct_lakebase``).
    """
    from iot_twin.services.ownership import assert_direct_lakebase

    try:
        assert_direct_lakebase(lb)
    except NotOwnedError as exc:
        return not_owned_response(exc)
    return None


def reject_foreign_scope(scope: str | None) -> JSONResponse | None:
    """Return a 403 response when a secret ``scope`` is set but not owned."""
    return reject_foreign_name(scope, what="secret scope")
