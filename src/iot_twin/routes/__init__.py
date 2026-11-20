"""API route modules — one file per domain."""

from __future__ import annotations

from fastapi import APIRouter

from iot_twin.routes.alerts import router as alerts_router
from iot_twin.routes.cluster import router as cluster_router
from iot_twin.routes.deployment import router as deployment_router
from iot_twin.routes.genie import router as genie_router
from iot_twin.routes.genie_pipelines import router as genie_pipelines_router
from iot_twin.routes.health import router as health_router
from iot_twin.routes.industries import router as industries_router
from iot_twin.routes.industries_admin import router as industries_admin_router
from iot_twin.routes.intelligence import router as intelligence_router
from iot_twin.routes.jobs import router as jobs_router
from iot_twin.routes.lakets import router as lakets_router
from iot_twin.routes.pipeline_deploy import router as pipeline_deploy_router
from iot_twin.routes.pipelines import router as pipelines_router
from iot_twin.routes.producers import router as producers_router
from iot_twin.routes.production import router as production_router
from iot_twin.routes.provisioning import router as provisioning_router
from iot_twin.routes.sensor_data import router as sensor_data_router
from iot_twin.routes.settings import router as settings_router
from iot_twin.routes.setup import router as setup_router
from iot_twin.routes.simulator import router as simulator_router
from iot_twin.routes.sources import router as sources_router
from iot_twin.routes.static_files import router as static_files_router
from iot_twin.routes.streaming_latency import router as streaming_latency_router
from iot_twin.routes.user import router as user_router
from iot_twin.routes.validation_admin import router as validation_admin_router
from iot_twin.routes.warehouses import router as warehouses_router
from iot_twin.routes.websocket import router as websocket_router


def create_api_router() -> APIRouter:
    """Combine every router into a single top-level router.

    Registration order is significant (first match wins) and mirrors the
    historical order: API routers, one-click setup, admin management,
    validation, then the SPA root.
    """
    api = APIRouter()
    api.include_router(health_router)
    api.include_router(websocket_router)
    api.include_router(sources_router)
    api.include_router(sensor_data_router)
    api.include_router(simulator_router)
    api.include_router(producers_router)
    api.include_router(provisioning_router)
    api.include_router(cluster_router)
    api.include_router(deployment_router)
    api.include_router(jobs_router)
    api.include_router(industries_router)
    api.include_router(user_router)
    api.include_router(settings_router)
    api.include_router(warehouses_router)
    api.include_router(pipelines_router)
    api.include_router(lakets_router)
    api.include_router(pipeline_deploy_router)
    api.include_router(streaming_latency_router)
    api.include_router(alerts_router)
    api.include_router(production_router)
    api.include_router(genie_router)
    api.include_router(genie_pipelines_router)
    api.include_router(intelligence_router)
    # One-click Lakebase + LakeTS pipeline setup (POST /api/setup/...)
    api.include_router(setup_router)
    # Admin pipeline management (/api/admin/industries/..., /api/admin/pipelines/...)
    api.include_router(industries_admin_router)
    api.include_router(validation_admin_router)
    # SPA root (/); the catch-all lives in app.py and must stay last
    api.include_router(static_files_router)
    return api
