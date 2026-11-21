"""Admin gate for workspace-writing routes.

Routes that create or modify workspace resources from a request body (settings
provisioning, ``/api/provision/*``, LakeTS, one-click setup, pipeline secrets)
are admin-only. They use the same check as the industry admin API
(``services.app_permissions.is_admin``: ``ADMIN_EMAILS``, ``ADMIN_GROUP``
membership or the app owner).
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request

from iot_twin.services import app_permissions


def require_admin(request: Request) -> None:
    """FastAPI dependency: 403 unless the caller is an app admin."""
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")


ADMIN_ONLY = [Depends(require_admin)]
