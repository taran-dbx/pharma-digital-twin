"""Databricks App permission helpers.

A deployed Databricks App authenticates incoming requests against its own
access-control list. When the streaming pipeline mints a *new* service
principal (SP) for the Lakebase sink, that SP has no permission on the app, so
its OAuth token is rejected with a 401 the moment it tries to call back into
the app. The fix is to grant the SP ``CAN_USE`` on the app via the apps
permissions API.

``grant_app_can_use`` uses *merge* semantics (``update_permissions``, not
``set_permissions``) so existing grants — including the app owner — are left
untouched. The call is best-effort: a failure here must never break the
provisioning / heal flow, so we log and swallow.

This module also gates admin-only actions (add industry, provisioning
internals) via ``is_admin``. Identity is resolved the same way
``iot_twin.core.auth.get_current_user`` does: Databricks Apps injects the caller's
OAuth token in the ``x-forwarded-access-token`` header, and its ``sub`` claim
is the user's email — decoded locally, no network call needed since the
platform proxy already validated the token. Group membership is a network
call (SDK, scoped to the caller's own forwarded token) and is therefore kept
in a separate, patchable ``_user_groups`` function.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from iot_twin.core import resource_names
from iot_twin.core.config import owned_env

logger = logging.getLogger("pharma_twin.services.app_permissions")

# Import-time snapshots of the prefix-derived defaults (kept for callers that
# import them); the getters below resolve the prefix on every call.
DEFAULT_APP_NAME = resource_names.app_name()
DEFAULT_ADMIN_GROUP = resource_names.admin_group()


def _app_name() -> str:
    """Return the Databricks App name from env, falling back to the constant."""
    name = (os.getenv("DATABRICKS_APP_NAME") or "").strip()
    return name or resource_names.app_name()


def _admin_group() -> str:
    """Return the admin group display name from env, falling back to the default."""
    name = owned_env("ADMIN_GROUP", what="ADMIN_GROUP")
    return name or resource_names.admin_group()


def _forwarded_token(request: Any) -> str | None:
    """Read the Databricks Apps forwarded OAuth token off the request, if any."""
    try:
        return request.headers.get("x-forwarded-access-token")
    except Exception:
        return None


_ME_CACHE_ATTR = "_pharma_twin_verified_me"


def _verified_me(request: Any) -> Any | None:
    """Return ``current_user.me()`` for the caller's OWN forwarded token, or ``None``.

    The Databricks API verifies the token, so the identity (``user_name``,
    ``groups``) comes from the workspace, not from client-controlled bytes. The
    result is cached on ``request.state`` so one request costs one API call.
    Defensive: any failure returns ``None``.
    """
    state = getattr(request, "state", None)
    cached = getattr(state, _ME_CACHE_ATTR, None) if state is not None else None
    if cached is not None:
        return cached or None  # ``False`` caches a failed lookup
    token = _forwarded_token(request)
    me: Any = None
    if token:
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.config import Config

            host = (os.getenv("DATABRICKS_HOST") or "").strip() or None
            # auth_type="pat": inside a Databricks App the SP's OAuth env vars are always
            # set too, and without an explicit preference the SDK refuses two methods.
            config = Config(host=host, token=token, auth_type="pat")
            me = WorkspaceClient(config=config).current_user.me()
        except Exception as exc:
            logger.warning("Failed to verify caller identity (treating as unknown): %s", exc)
            me = None
    if state is not None:
        try:
            setattr(state, _ME_CACHE_ATTR, me if me is not None else False)
        except Exception:
            pass
    return me


def _caller_email(request: Any) -> str | None:
    """Resolve the caller's email from a VERIFIED identity.

    With a forwarded OAuth token, the email is ``current_user.me().user_name``
    as returned by the workspace for that token. The token is never decoded
    locally: an unverified JWT payload is client-controlled. Without a token,
    falls back to the ``x-forwarded-email`` header that the Databricks Apps proxy
    injects for authenticated browser requests. Returns ``None`` on any failure.
    """
    if _forwarded_token(request):
        me = _verified_me(request)
        user_name = getattr(me, "user_name", None) if me is not None else None
        return user_name or None
    try:
        return request.headers.get("x-forwarded-email") or None
    except Exception:
        return None


def _user_groups(request: Any) -> set[str]:
    """Resolve the caller's Databricks group display names from the verified identity.

    Uses the same cached ``current_user.me()`` call as :func:`_caller_email`
    (scoped to the caller's own forwarded token). Any failure returns an empty set.
    """
    me = _verified_me(request)
    groups = getattr(me, "groups", None) or [] if me is not None else []
    try:
        return {g.display for g in groups if getattr(g, "display", None)}
    except Exception as exc:
        logger.warning("Failed to resolve user groups (defaulting to none): %s", exc)
        return set()


def _is_app_owner(request: Any) -> bool:
    """Best-effort check: is the caller the Databricks App's creator/owner?

    Defensive: returns False on any error (missing token, no SDK access,
    app lookup failure) so a failed owner lookup never grants admin by
    accident.
    """
    caller_email = _caller_email(request)
    if not caller_email:
        return False
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        app = w.apps.get(_app_name())
        owner_email = getattr(app, "creator", None)
        return bool(owner_email) and owner_email == caller_email
    except Exception as exc:
        logger.warning("Failed to resolve app-owner fallback (defaulting to non-owner): %s", exc)
        return False


def _admin_emails() -> set[str]:
    """Lowercased set of explicit admin emails from ``ADMIN_EMAILS`` (CSV)."""
    raw = os.getenv("ADMIN_EMAILS", "") or ""
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def is_admin(request: Any) -> bool:
    """True if the caller is an explicit admin email, in the admin group, or the app owner.

    Resolution order (any one grants admin): the caller's email is listed in
    ``ADMIN_EMAILS`` (CSV); membership in ``ADMIN_GROUP`` (default
    ``"<prefix>-admins"``) via ``_user_groups``; or the app-owner fallback.
    Defensive throughout: any failure denies rather than raises.
    """
    try:
        caller = _caller_email(request)
        if caller and caller.strip().lower() in _admin_emails():
            return True
    except Exception as exc:
        logger.warning("is_admin: email allowlist check failed (denying): %s", exc)

    try:
        if _admin_group() in _user_groups(request):
            return True
    except Exception as exc:
        logger.warning("is_admin: group check failed (denying): %s", exc)

    try:
        return _is_app_owner(request)
    except Exception as exc:
        logger.warning("is_admin: app-owner check failed (denying): %s", exc)
        return False


def grant_app_can_use(w: Any, app_name: str, sp_application_id: str) -> None:
    """Grant ``CAN_USE`` on the app to a service principal (merge semantics).

    Non-fatal: any failure is logged and swallowed so callers (provisioning,
    heal) keep running. ``sp_application_id`` is the SP's *application id*
    (a.k.a. client id), which is what the apps ACL keys on.
    """
    if not app_name or not sp_application_id:
        logger.warning(
            "grant_app_can_use: missing app_name=%r or sp_application_id=%r — skipping",
            app_name,
            sp_application_id,
        )
        return

    try:
        from databricks.sdk.service.apps import (
            AppAccessControlRequest,
            AppPermissionLevel,
        )

        w.apps.update_permissions(
            app_name=app_name,
            access_control_list=[
                AppAccessControlRequest(
                    service_principal_name=sp_application_id,
                    permission_level=AppPermissionLevel.CAN_USE,
                )
            ],
        )
        logger.info(
            "Granted CAN_USE on app '%s' to service principal '%s'",
            app_name,
            sp_application_id,
        )
    except Exception as exc:
        logger.warning(
            "Failed to grant CAN_USE on app '%s' to SP '%s' (non-fatal): %s",
            app_name,
            sp_application_id,
            exc,
        )
