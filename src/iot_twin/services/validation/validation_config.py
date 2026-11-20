"""Verified compute / Kafka / partition configuration for the validation harness.

Pure, side-effect-free builders used by ``services/validation/validation_steps.py`` (the
effectful executors) and unit-tested in isolation.  Everything here encodes the
configs the Phase-0/Phase-1 spikes VERIFIED on ``example-us-east`` (spec
§1.3–1.11):

- Interactive validation cluster: **DBR 18.2** (``18.2.x-scala2.13``),
  **single-user / dedicated**, **Photon OFF, autoscaling OFF, spot OFF**, RTM
  Spark conf, 90-minute auto-terminate (floor).
- MSK ``example`` reached via the **public ``:9198``** bootstrap (private
  ``:9098`` is probed but unreachable — no VPC peering); ``iot-rtm-sc`` UC
  service credential handles MSK-IAM.
- Worker sizing (M1): ``clamp(ceil(partitions / cores_per_worker), 1, 8)``.
- Partition pre-create horizon: **24h / 96 chunks**.
"""

from __future__ import annotations

import math
import os
import re
from typing import Any

__all__ = [
    "VALIDATION_DBR",
    "SERVICE_CREDENTIAL",
    "DEFAULT_REGION",
    "MSK_PUBLIC_PORT",
    "MSK_PRIVATE_PORT",
    "DEFAULT_AUTO_TERMINATE_MIN",
    "MIN_AUTO_TERMINATE_MIN",
    "MIN_WORKERS",
    "MAX_WORKERS",
    "PARTITION_HORIZON_HOURS",
    "PARTITION_CHUNK_COUNT",
    "RTM_BASE_CONF",
    "RTM_STREAM_JOIN_CONF",
    "worker_count",
    "worker_sizing",
    "rtm_spark_conf",
    "build_validation_cluster_spec",
    "topic_name",
    "replication_factor",
    "region_from_bootstrap",
]

# --- Verified constants ----------------------------------------------------
# Fallback DBR only: the interactive cluster resolves the workspace's latest LTS
# runtime dynamically (validation_steps._latest_lts_spark_version) and drops back
# to this pinned DBR 18.2 GA (Spark 4.1.0; verified on service creds + single-user)
# only if that lookup fails.
VALIDATION_DBR = "18.2.x-scala2.13"
SERVICE_CREDENTIAL: str = os.getenv("UC_SERVICE_CREDENTIAL", "")
DEFAULT_REGION = "us-east-1"

MSK_PUBLIC_PORT = 9198  # reachable from example-us-east
MSK_PRIVATE_PORT = 9098  # IAM/private — probed but unreachable (no VPC peering)

# 90-minute inactivity auto-terminate: default AND floor (admin may raise only).
DEFAULT_AUTO_TERMINATE_MIN = 90
MIN_AUTO_TERMINATE_MIN = 90

# Worker sizing clamp bounds (M1).
MIN_WORKERS = 1
MAX_WORKERS = 8

# Partition pre-create forward window (Approach A).
PARTITION_HORIZON_HOURS = 24
PARTITION_CHUNK_COUNT = 96  # 24h / 15-min chunks

# --- RTM Spark conf (verified) ---------------------------------------------
RTM_BASE_CONF: dict[str, str] = {
    "spark.databricks.streaming.realTimeMode.enabled": "true",
}

# Only needed when the query does stream-stream joins (our pipelines do not, but
# exposed so the harness can opt in without editing the base conf).
RTM_STREAM_JOIN_CONF: dict[str, str] = {
    "spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled": "true",
    "spark.sql.streaming.join.stateFormatVersion": "4",
    "spark.sql.streaming.join.stateFormatV4.enabled": "true",
    "spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion": "2",
    "spark.databricks.streaming.realTimeMode.controlMessage.enabled": "true",
}

_TOPIC_SAFE_RE = re.compile(r"[^a-z0-9]+")


# --- Worker sizing (M1) ----------------------------------------------------
def worker_count(
    num_partitions: int,
    cores_per_worker: int,
    *,
    min_workers: int = MIN_WORKERS,
    max_workers: int = MAX_WORKERS,
) -> int:
    """Return ``clamp(ceil(num_partitions / cores_per_worker), min, max)``.

    Guarantees ``workers * cores_per_worker >= num_partitions`` up to the cap,
    so every Kafka partition has a reader (unless the partition count exceeds
    ``max_workers * cores_per_worker`` — see :func:`worker_sizing`).
    """
    if cores_per_worker < 1:
        raise ValueError("cores_per_worker must be >= 1")
    if num_partitions < 1:
        raise ValueError("num_partitions must be >= 1")
    needed = math.ceil(num_partitions / cores_per_worker)
    return max(min_workers, min(max_workers, needed))


def worker_sizing(
    num_partitions: int,
    cores_per_worker: int,
    *,
    min_workers: int = MIN_WORKERS,
    max_workers: int = MAX_WORKERS,
) -> dict:
    """Return the worker count plus an ``over_cap`` warning flag.

    ``over_cap`` is True when ``num_partitions > max_workers * cores_per_worker``
    — the harness cannot give every partition a dedicated core, so the admin is
    warned rather than silently under-provisioned (M1).
    """
    workers = worker_count(
        num_partitions,
        cores_per_worker,
        min_workers=min_workers,
        max_workers=max_workers,
    )
    over_cap = num_partitions > max_workers * cores_per_worker
    return {
        "workers": workers,
        "num_partitions": num_partitions,
        "cores_per_worker": cores_per_worker,
        "over_cap": over_cap,
        "reason": (
            f"clamp(ceil({num_partitions}/{cores_per_worker}), "
            f"{min_workers}, {max_workers}) = {workers}"
            + (" (WARN: partitions exceed max task slots)" if over_cap else "")
        ),
    }


# --- RTM Spark conf --------------------------------------------------------
def rtm_spark_conf(*, stream_stream_join: bool = False) -> dict[str, str]:
    """Return the verified RTM Spark conf (immutable copy)."""
    conf = dict(RTM_BASE_CONF)
    if stream_stream_join:
        conf.update(RTM_STREAM_JOIN_CONF)
    return conf


# --- Validation cluster spec (verified) ------------------------------------
def build_validation_cluster_spec(
    *,
    node_type_id: str,
    num_workers: int,
    single_user_name: str,
    spark_version: str = VALIDATION_DBR,
    auto_terminate_min: int = DEFAULT_AUTO_TERMINATE_MIN,
    stream_stream_join: bool = False,
    cloud: str = "aws",
    cluster_name: str | None = None,
) -> dict[str, Any]:
    """Build the ``clusters.create`` kwargs for the interactive validation cluster.

    Encodes the verified config: DBR 18.2, **single-user**, **Photon OFF**
    (``runtime_engine=STANDARD``), **autoscaling OFF** (fixed ``num_workers``, no
    autoscale block), **spot OFF** (all nodes on-demand), 90-min auto-terminate
    floor, RTM Spark conf, and driver == worker node type.
    """
    if auto_terminate_min < MIN_AUTO_TERMINATE_MIN:
        auto_terminate_min = MIN_AUTO_TERMINATE_MIN  # floor — admin may raise only

    spec: dict[str, Any] = {
        "cluster_name": cluster_name or "iot-validation-harness",
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "driver_node_type_id": node_type_id,  # driver == worker (§1.3)
        "num_workers": num_workers,  # fixed → autoscaling OFF
        "autotermination_minutes": auto_terminate_min,
        "spark_conf": rtm_spark_conf(stream_stream_join=stream_stream_join),
        "data_security_mode": "SINGLE_USER",  # dedicated / single-user
        "single_user_name": single_user_name,
        "runtime_engine": "STANDARD",  # Photon OFF
    }

    # Spot OFF: every node on-demand (first_on_demand covers driver + workers).
    if cloud == "aws":
        spec["aws_attributes"] = {
            "availability": "ON_DEMAND",
            "first_on_demand": num_workers + 1,
            "ebs_volume_count": 1,
            "ebs_volume_size": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        }
    elif cloud == "azure":
        spec["azure_attributes"] = {
            "availability": "ON_DEMAND_AZURE",
            "first_on_demand": num_workers + 1,
        }
    return spec


# --- Kafka topic + MSK helpers ---------------------------------------------
def topic_name(industry: str, mode: str) -> str:
    """Return the dotted topic name ``iot.<industry>.<mode>`` (spec §1.14).

    Only ``realtime`` and ``microbatch`` have topics; ``zerobus`` is non-Kafka.
    """
    if mode not in ("realtime", "microbatch"):
        raise ValueError(f"mode {mode!r} has no Kafka topic (Kafka modes only)")
    safe = _TOPIC_SAFE_RE.sub("_", (industry or "").strip().lower()).strip("_")
    if not safe:
        raise ValueError(f"industry {industry!r} has no valid characters for a topic")
    return f"iot.{safe}.{mode}"


def replication_factor(broker_count: int) -> int:
    """Return ``RF = min(3, broker_count)`` (spec §1.8)."""
    if broker_count < 1:
        raise ValueError("broker_count must be >= 1")
    return min(3, broker_count)


def region_from_bootstrap(bootstrap_servers: str) -> str:
    """Derive the AWS region from an MSK bootstrap hostname.

    MSK public bootstrap hosts look like
    ``b-1-public.example.abc123.c2.kafka.us-east-1.amazonaws.com:9198``.
    Falls back to :data:`DEFAULT_REGION` when no region token is found.
    """
    match = re.search(r"\.kafka\.([a-z]{2}-[a-z]+-\d+)\.amazonaws\.com", bootstrap_servers or "")
    return match.group(1) if match else DEFAULT_REGION
