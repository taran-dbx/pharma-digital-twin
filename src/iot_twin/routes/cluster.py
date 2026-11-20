"""Cluster management endpoints — node types, spark versions, provisioning."""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from iot_twin.core.state import AppState, get_app_state
from iot_twin.services.databricks_sdk import detect_cloud
from iot_twin.services.pipelines.pipeline_service import track_resource

logger = logging.getLogger("iot_digital_twin.routes.cluster")

router = APIRouter(tags=["cluster"])


@router.get("/api/cluster/node-types")
async def list_node_types():
    """Return available cluster node types with core/memory info.

    Uses the Databricks SDK to fetch live data from the workspace and
    detects the cloud provider from ``DATABRICKS_HOST``.
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        resp = w.clusters.list_node_types()

        node_types = []
        for nt in resp.node_types or []:
            if getattr(nt, "is_deprecated", False):
                continue

            nit = getattr(nt, "node_instance_type", None)
            local_disks = (getattr(nit, "local_disks", 0) or 0) if nit else 0
            local_disk_size_gb = (getattr(nit, "local_disk_size_gb", 0) or 0) if nit else 0

            if local_disks > 0 and local_disk_size_gb > 0:
                storage_label = f"{local_disks}x {local_disk_size_gb} GB NVMe SSD"
            else:
                storage_label = "EBS-only"

            node_types.append(
                {
                    "node_type_id": nt.node_type_id,
                    "num_cores": getattr(nt, "num_cores", 0) or 0,
                    "memory_mb": getattr(nt, "memory_mb", 0) or 0,
                    "num_gpus": getattr(nt, "num_gpus", 0) or 0,
                    "category": getattr(nt, "category", "General Purpose") or "General Purpose",
                    "description": getattr(nt, "description", nt.node_type_id) or nt.node_type_id,
                    "is_deprecated": False,
                    "storage_label": storage_label,
                }
            )

        node_types.sort(key=lambda n: (n["category"], n["num_cores"], n["memory_mb"]))

        return {"cloud": detect_cloud(), "node_types": node_types}

    except Exception as exc:
        logger.error("Failed to list node types: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/cluster/spark-versions")
async def list_spark_versions():
    """Return the 2 most recent standard (non-Photon, non-ML) runtimes."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        resp = w.clusters.spark_versions()

        versions = []
        for sv in resp.versions or []:
            key_lower = (sv.key or "").lower()
            if "photon" in key_lower or "-ml-" in key_lower or "cpu-ml" in key_lower:
                continue
            versions.append(
                {
                    "key": sv.key,
                    "name": sv.name,
                }
            )

        versions.sort(key=lambda v: v["key"], reverse=True)
        versions = versions[:2]

        return {"spark_versions": versions}

    except Exception as exc:
        logger.error("Failed to list spark versions: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/cluster/provision")
async def provision_cluster(request: Request, state: AppState = Depends(get_app_state)):
    """Create an all-purpose compute cluster and return its ID.

    The cluster starts asynchronously; poll ``/api/cluster/{id}/status``
    to track progress.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    num_workers = int(body.get("num_workers", 2))
    worker_node_type = body.get("worker_node_type", "")
    spark_version = body.get("spark_version", "")
    auto_terminate_min = int(body.get("auto_terminate_min", 30))
    spark_conf = body.get("spark_conf", {})
    spark_conf["spark.databricks.streaming.realTimeMode.enabled"] = "true"
    cluster_name = body.get("cluster_name", f"iot-digital-twin-{int(time.time())}")

    if not worker_node_type or not spark_version:
        return JSONResponse(
            status_code=422,
            content={"error": "'worker_node_type' and 'spark_version' are required"},
        )

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.compute import (
            AwsAttributes,
            AzureAttributes,
            DataSecurityMode,
            EbsVolumeType,
        )

        cloud = detect_cloud()
        no_spot_kwargs: dict[str, Any] = {}
        if cloud == "aws":
            no_spot_kwargs["aws_attributes"] = AwsAttributes(
                first_on_demand=num_workers + 1,
                ebs_volume_count=1,
                ebs_volume_size=100,
                ebs_volume_type=EbsVolumeType.GENERAL_PURPOSE_SSD,
            )
        elif cloud == "azure":
            no_spot_kwargs["azure_attributes"] = AzureAttributes(first_on_demand=num_workers + 1)

        w = WorkspaceClient()
        resp = w.clusters.create(
            cluster_name=cluster_name,
            spark_version=spark_version,
            node_type_id=worker_node_type,
            driver_node_type_id=worker_node_type,
            num_workers=num_workers,
            autotermination_minutes=auto_terminate_min,
            spark_conf=spark_conf,
            data_security_mode=DataSecurityMode.SINGLE_USER,
            **no_spot_kwargs,
        )
        cluster_id = resp.cluster_id
        logger.info("Cluster created: %s (id=%s)", cluster_name, cluster_id)

        pipeline_id = body.get("pipeline_id")
        if pipeline_id:
            await track_resource(
                state.db, state.db_available, pipeline_id, "cluster_id", cluster_id
            )

        return {"cluster_id": cluster_id, "state": "PENDING"}

    except Exception as exc:
        logger.error("Cluster provisioning failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/cluster/{cluster_id}/status")
async def cluster_status(cluster_id: str):
    """Return the current state of a cluster."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        info = w.clusters.get(cluster_id=cluster_id)

        return {
            "cluster_id": cluster_id,
            "state": info.state.value if info.state else "UNKNOWN",
            "state_message": getattr(info, "state_message", "") or "",
        }

    except Exception as exc:
        logger.error("Failed to get cluster status: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/cluster/{cluster_id}/start")
async def start_cluster(cluster_id: str):
    """Restart a terminated cluster."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        w.clusters.start(cluster_id=cluster_id)
        return {"cluster_id": cluster_id, "state": "STARTING"}

    except Exception as exc:
        logger.error("Failed to start cluster: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/test-source")
async def test_source_connectivity(request: Request):
    """Test connectivity to an external data source using the iot_simulator sinks.

    Currently supports Kafka via KafkaSink.  For sources that only support
    UC service credentials (no plain-text creds), returns a guidance message
    to use "Run on Cluster" instead.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    source_type = body.get("source_type", "")
    connection = body.get("connection", {})
    num_records = int(body.get("num_records", 5))

    if source_type != "kafka":
        return JSONResponse(
            status_code=422,
            content={
                "error": f"Direct connectivity testing is only supported for Kafka. "
                f"For {source_type}, use 'Run on Cluster' to validate connectivity "
                f"via UC service credentials.",
                "suggestion": "run_on_cluster",
            },
        )

    bootstrap = connection.get("bootstrap_servers", "")
    topic = connection.get("topic", "")
    security_protocol = connection.get("security_protocol", "PLAINTEXT")
    sasl_username = connection.get("sasl_username")
    sasl_password = connection.get("sasl_password")

    if not bootstrap or not topic:
        return JSONResponse(
            status_code=422, content={"error": "bootstrap_servers and topic are required"}
        )

    # If only UC service credential is provided (no plain-text creds for SASL),
    # we cannot resolve the credential outside of Spark
    if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and not sasl_username:
        service_cred = connection.get("service_credential", "")
        if service_cred:
            return {
                "success": False,
                "error": "UC service credentials can only be resolved on a Spark cluster. "
                "Use 'Run on Cluster' to test connectivity with UC service credentials.",
                "suggestion": "run_on_cluster",
            }
        return JSONResponse(
            status_code=422,
            content={
                "error": "SASL authentication requires sasl_username and sasl_password, "
                "or a UC service credential (testable only via 'Run on Cluster')."
            },
        )

    try:
        from iot_simulator.models import SensorRecord
        from iot_simulator.sinks.kafka import KafkaSink

        sink = KafkaSink(
            bootstrap_servers=bootstrap,
            topic=topic,
            security_protocol=security_protocol,
            sasl_mechanism="PLAIN" if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") else None,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            compression=None,
            acks=1,
            batch_size=num_records,
        )

        await sink.connect()

        records = [
            SensorRecord(
                sensor_name=f"test_sensor_{i}",
                industry="connectivity_test",
                value=float(i),
                unit="test",
                sensor_type="gauge",
                timestamp=time.time(),
                min_value=0.0,
                max_value=100.0,
                nominal_value=50.0,
                fault_active=False,
            )
            for i in range(num_records)
        ]

        await sink.write(records)
        await sink.flush()
        await sink.close()

        return {
            "success": True,
            "message": f"Successfully produced {num_records} test records to topic '{topic}'",
            "records_sent": num_records,
        }

    except ImportError:
        return JSONResponse(
            status_code=500,
            content={
                "error": "iot_simulator[kafka] is not installed. Install with: pip install iot-data-simulator[kafka]"
            },
        )
    except Exception as exc:
        logger.error("Source connectivity test failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})
