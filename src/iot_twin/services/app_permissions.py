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

import base64
import json
import logging
import os
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.app_permissions")

DEFAULT_APP_NAME = "iot-digital-twin"
DEFAULT_ADMIN_GROUP = "iot-admins"


def _app_name() -> str:
    """Return the Databricks App name from env, falling back to the constant."""
    name = (os.getenv("DATABRICKS_APP_NAME") or "").strip()
    return name or DEFAULT_APP_NAME


def _admin_group() -> str:
    """Return the admin group display name from env, falling back to the default."""
    name = (os.getenv("ADMIN_GROUP") or "").strip()
    return name or DEFAULT_ADMIN_GROUP


def _forwarded_token(request: Any) -> str | None:
    """Read the Databricks Apps forwarded OAuth token off the request, if any."""
    try:
        return request.headers.get("x-forwarded-access-token")
    except Exception:
        return None


def _caller_email(request: Any) -> str | None:
    """Resolve the caller's email.

    Prefers the ``x-forwarded-email`` header that Databricks Apps inject for
    authenticated browser requests (the authoritative email), falling back to
    the ``sub`` claim of the forwarded OAuth token. Returns ``None`` on any
    failure rather than raising.
    """
    try:
        header_email = request.headers.get("x-forwarded-email")
        if header_email:
            return header_email
    except Exception:
        pass
    token = _forwarded_token(request)
    if not token:
        return None
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        # Prefer email-shaped claims; ``sub`` is often a UUID on Databricks.
        for claim in ("email", "upn", "preferred_username", "unique_name", "sub"):
            value = payload.get(claim)
            if value and "@" in str(value):
                return value
        return payload.get("sub") or None
    except Exception as exc:
        logger.warning("Failed to decode caller email from forwarded token: %s", exc)
        return None


def _user_groups(request: Any) -> set[str]:
    """Resolve the caller's Databricks group display names via the SDK.

    Builds a ``WorkspaceClient`` scoped to the caller's own forwarded OAuth
    token (the same header ``iot_twin.core.auth.get_current_user`` reads) and asks
    for ``current_user.me()``, whose ``groups`` field lists the identities
    the caller belongs to.

    Defensive: any failure (missing token, SDK error, network issue) returns
    an empty set rather than raising, so callers default to "no groups"
    instead of blowing up.
    """
    token = _forwarded_token(request)
    if not token:
        return set()
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config

        host = (os.getenv("DATABRICKS_HOST") or "").strip() or None
        w = WorkspaceClient(config=Config(host=host, token=token))
        me = w.current_user.me()
        groups = getattr(me, "groups", None) or []
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
    ``"iot-admins"``) via ``_user_groups``; or the app-owner fallback.
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
