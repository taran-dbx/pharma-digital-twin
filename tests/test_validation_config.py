"""Unit tests for the verified validation-harness config builders (spec §1.3–1.11)."""

from __future__ import annotations

import pytest

from iot_twin.services.validation import validation_config as vc


# --- Worker sizing (M1) ----------------------------------------------------
@pytest.mark.parametrize(
    "partitions,cores,expected",
    [
        (1, 4, 1),  # clamps up to min 1
        (4, 4, 1),
        (5, 4, 2),  # ceil(5/4)=2
        (12, 4, 3),
        (96, 4, 8),  # ceil(96/4)=24 → clamps to max 8
        (100, 4, 8),
    ],
)
def test_worker_count_clamp(partitions, cores, expected):
    assert vc.worker_count(partitions, cores) == expected


def test_worker_sizing_over_cap_flag():
    # 96 partitions / 4 cores = 24 workers needed, but capped at 8 → over_cap.
    sizing = vc.worker_sizing(96, 4)
    assert sizing["workers"] == 8
    assert sizing["over_cap"] is True
    sizing2 = vc.worker_sizing(24, 4)
    assert sizing2["workers"] == 6
    assert sizing2["over_cap"] is False


def test_worker_count_rejects_bad_input():
    with pytest.raises(ValueError):
        vc.worker_count(4, 0)
    with pytest.raises(ValueError):
        vc.worker_count(0, 4)


# --- RTM Spark conf --------------------------------------------------------
def test_rtm_conf_base():
    conf = vc.rtm_spark_conf()
    assert conf["spark.databricks.streaming.realTimeMode.enabled"] == "true"
    assert "spark.sql.streaming.join.stateFormatVersion" not in conf


def test_rtm_conf_stream_join_opt_in():
    conf = vc.rtm_spark_conf(stream_stream_join=True)
    assert conf["spark.sql.streaming.join.stateFormatVersion"] == "4"
    # Base conf not mutated by opt-in.
    assert "spark.sql.streaming.join.stateFormatVersion" not in vc.RTM_BASE_CONF


# --- Validation cluster spec ----------------------------------------------
def test_cluster_spec_verified_config():
    spec = vc.build_validation_cluster_spec(
        node_type_id="m5d.xlarge",
        num_workers=3,
        single_user_name="a@b.com",
    )
    assert spec["spark_version"] == vc.VALIDATION_DBR == "18.2.x-scala2.13"
    assert spec["data_security_mode"] == "SINGLE_USER"
    assert spec["single_user_name"] == "a@b.com"
    assert spec["runtime_engine"] == "STANDARD"  # Photon OFF
    assert spec["driver_node_type_id"] == "m5d.xlarge"  # driver == worker
    assert spec["num_workers"] == 3  # autoscaling OFF (fixed)
    assert "autoscale" not in spec
    assert spec["autotermination_minutes"] == 90
    # Spot OFF — all nodes on-demand.
    assert spec["aws_attributes"]["availability"] == "ON_DEMAND"
    assert spec["aws_attributes"]["first_on_demand"] == 4


def test_cluster_spec_auto_terminate_floor():
    # Admin may raise but not lower below the 90-min floor.
    spec = vc.build_validation_cluster_spec(
        node_type_id="m5d.xlarge",
        num_workers=1,
        single_user_name="a@b.com",
        auto_terminate_min=30,
    )
    assert spec["autotermination_minutes"] == 90
    spec2 = vc.build_validation_cluster_spec(
        node_type_id="m5d.xlarge",
        num_workers=1,
        single_user_name="a@b.com",
        auto_terminate_min=120,
    )
    assert spec2["autotermination_minutes"] == 120


# --- Kafka topic + MSK helpers --------------------------------------------
def test_topic_name_dotted():
    assert vc.topic_name("pharmaceutical", "realtime") == "iot.pharmaceutical.realtime"
    assert vc.topic_name("Oil & Gas", "microbatch") == "iot.oil_gas.microbatch"


def test_topic_name_rejects_non_kafka_mode():
    with pytest.raises(ValueError):
        vc.topic_name("pharmaceutical", "zerobus")


def test_replication_factor():
    assert vc.replication_factor(1) == 1
    assert vc.replication_factor(3) == 3
    assert vc.replication_factor(6) == 3
    with pytest.raises(ValueError):
        vc.replication_factor(0)


def test_region_from_bootstrap():
    host = (
        "b-1-public.example.abc123.c2.kafka.us-east-1.amazonaws.com:9198,"
        "b-2-public.example.abc123.c2.kafka.us-east-1.amazonaws.com:9198"
    )
    assert vc.region_from_bootstrap(host) == "us-east-1"
    assert vc.region_from_bootstrap("garbage") == "us-east-1"  # fallback
    assert vc.region_from_bootstrap("b-1.x.y.kafka.eu-west-2.amazonaws.com:9198") == "eu-west-2"
