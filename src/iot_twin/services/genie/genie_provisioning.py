"""Provision the Genie space for an ALREADY-deployed pipeline of any industry.

Backs ``POST /api/admin/pipelines/{setup_id}/genie/{mode}`` so pipelines
deployed before per-pipeline Genie existed can get a space without a redeploy.
Only the Genie space and its ``provisioned_resource`` ledger row are touched —
never the job, stream, Kafka topic, notebooks or Lakebase resources.

Errors carry an HTTP status (:class:`GenieProvisionError.status_code`) so the
route stays a thin mapping.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from iot_twin.services.db.app_repo_common import DBExecutor
from iot_twin.services.deployment import deployment_repo
from iot_twin.services.genie import genie_space_service
from iot_twin.services.pipelines import pipeline_view

logger = logging.getLogger("iot_digital_twin.services.genie_provisioning")

GENIE_SPACE_TIMEOUT_S = 60.0


class GenieProvisionError(RuntimeError):
    """Provisioning refused/failed; ``status_code`` is the HTTP mapping."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


async def _require_deployed(db: DBExecutor, setup_id: str, mode: str) -> dict:
    deployment = await deployment_repo.get_deployment_by_mode(db, setup_id, mode)
    if deployment is None:
        raise GenieProvisionError(404, f"no {mode} deployment for this pipeline")
    if not pipeline_view.is_deployed(deployment):
        raise GenieProvisionError(
            409, f"{mode} deployment is not deployed (status={deployment.get('status')!r})"
        )
    return deployment


def _response(space: dict[str, Any]) -> dict[str, Any]:
    action = space.get("action")
    warnings = (
        ["recorded Genie space no longer existed; recreated a new space"]
        if action == "recreated"
        else []
    )
    return {
        "space_id": space["space_id"],
        "created": action in {"created", "recreated"},
        "warnings": warnings,
    }


async def _ensure(
    db: DBExecutor, setup: dict, setup_id: str, deployment: dict, mode: str, w: Any | None
) -> dict[str, Any]:
    warehouse_id = await genie_space_service.genie_warehouse(db, setup)
    try:
        return await asyncio.wait_for(
            genie_space_service.ensure_pipeline_space(
                db,
                setup_id=setup_id,
                deployment_id=deployment.get("id"),
                industry=str(setup.get("industry")),
                mode=mode,
                warehouse_id=warehouse_id,
                w=w,
            ),
            timeout=GENIE_SPACE_TIMEOUT_S,
        )
    except TimeoutError as exc:
        raise GenieProvisionError(
            504, f"Genie space provisioning timed out after {GENIE_SPACE_TIMEOUT_S:g}s"
        ) from exc
    except genie_space_service.GenieSpaceError as exc:
        raise GenieProvisionError(409, str(exc)) from exc
    except Exception as exc:
        logger.exception("Genie space provisioning failed for setup %s mode %s", setup_id, mode)
        raise GenieProvisionError(502, "Genie space provisioning failed") from exc


async def provision_existing_pipeline_space(
    db: DBExecutor, pipeline_id: str, mode: str, *, w: Any | None = None
) -> dict[str, Any]:
    """Create-or-update the Genie space of a deployed pipeline mode (any industry).

    Returns ``{"space_id", "created", "warnings"}``.  Raises
    :class:`GenieProvisionError` (404 unknown id/deployment, 409 not deployed or
    precondition such as missing intelligence tables, 422 unknown industry,
    504 timeout, 502 SDK failure).
    """
    if mode not in genie_space_service.PIPELINE_MODES:
        raise GenieProvisionError(422, f"unknown mode {mode!r}")
    try:
        setup = await genie_space_service.resolve_pipeline_setup(db, pipeline_id, mode)
    except genie_space_service.PipelineNotFound as exc:
        raise GenieProvisionError(404, "unknown pipeline") from exc
    if not genie_space_service.industry_supported(setup.get("industry")):
        raise GenieProvisionError(
            422, f"per-pipeline Genie is not available for industry {setup.get('industry')!r}"
        )
    setup_id = str(setup.get("id"))
    deployment = await _require_deployed(db, setup_id, mode)
    space = await _ensure(db, setup, setup_id, deployment, mode, w)
    return _response(space)
