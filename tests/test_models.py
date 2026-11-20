"""Tests for iot_twin.models.producer_state and iot_twin.models.sql_ddl."""

from __future__ import annotations

from iot_twin.models import sql_ddl
from iot_twin.models.producer_state import new_producer_state, source_label


class TestNewProducerState:
    """Verify new_producer_state returns the correct idle structure."""

    def test_returns_dict(self) -> None:
        state = new_producer_state()
        assert isinstance(state, dict)

    def test_running_is_false(self) -> None:
        state = new_producer_state()
        assert state["running"] is False

    def test_source_type_is_none(self) -> None:
        state = new_producer_state()
        assert state["source_type"] is None

    def test_source_label_is_none(self) -> None:
        state = new_producer_state()
        assert state["source_label"] is None

    def test_destination_is_none(self) -> None:
        state = new_producer_state()
        assert state["destination"] is None

    def test_run_id_is_none(self) -> None:
        state = new_producer_state()
        assert state["run_id"] is None

    def test_state_is_idle(self) -> None:
        state = new_producer_state()
        assert state["state"] == "IDLE"

    def test_error_is_none(self) -> None:
        state = new_producer_state()
        assert state["error"] is None

    def test_has_exactly_seven_keys(self) -> None:
        state = new_producer_state()
        assert len(state) == 7

    def test_returns_new_dict_each_call(self) -> None:
        """Ensure the factory does not reuse the same dict object."""
        a = new_producer_state()
        b = new_producer_state()
        assert a is not b
        a["running"] = True
        assert b["running"] is False


class TestSourceLabel:
    """Verify source_label maps known streaming source types."""

    def test_kafka(self) -> None:
        assert source_label("kafka") == "Kafka"

    def test_kinesis(self) -> None:
        assert source_label("kinesis") == "Kinesis"

    def test_event_hub(self) -> None:
        assert source_label("event_hub") == "Event Hub"

    def test_rabbitmq(self) -> None:
        assert source_label("rabbitmq") == "RabbitMQ"

    def test_pubsub(self) -> None:
        assert source_label("pubsub") == "Pub/Sub"

    def test_unknown_returns_input(self) -> None:
        assert source_label("custom_source") == "custom_source"

    def test_empty_string_returns_empty(self) -> None:
        assert source_label("") == ""


class TestSqlDdlConstants:
    """Verify SQL DDL constants are defined and contain expected fragments."""

    def test_sensor_readings_ddl_exists(self) -> None:
        assert isinstance(sql_ddl.SENSOR_READINGS_DDL, str)
        assert len(sql_ddl.SENSOR_READINGS_DDL) > 0

    def test_sensor_readings_ddl_contains_table_name_placeholder(self) -> None:
        assert "{table_name}" in sql_ddl.SENSOR_READINGS_DDL

    def test_sensor_readings_ddl_contains_expected_columns(self) -> None:
        ddl = sql_ddl.SENSOR_READINGS_DDL
        assert "event_time" in ddl
        assert "sensor_name" in ddl
        assert "value" in ddl
        assert "fault_active" in ddl

    def test_matview_1m_ddl_references_sensor_readings(self) -> None:
        assert "sensor_readings" in sql_ddl.MATVIEW_1M_DDL

    def test_matview_1m_ddl_contains_sensor_1m(self) -> None:
        assert "sensor_1m" in sql_ddl.MATVIEW_1M_DDL

    def test_matview_5m_ddl_references_sensor_readings(self) -> None:
        assert "sensor_readings" in sql_ddl.MATVIEW_5M_DDL

    def test_matview_5m_ddl_contains_sensor_5m(self) -> None:
        assert "sensor_5m" in sql_ddl.MATVIEW_5M_DDL

    def test_matview_index_ddl_is_list(self) -> None:
        assert isinstance(sql_ddl.MATVIEW_INDEX_DDL, list)
        assert len(sql_ddl.MATVIEW_INDEX_DDL) == 2

    def test_matview_indexes_contain_expected_names(self) -> None:
        joined = " ".join(sql_ddl.MATVIEW_INDEX_DDL)
        assert "sensor_1m_pk" in joined
        assert "sensor_5m_pk" in joined

    def test_lakebase_role_name_constant(self) -> None:
        assert sql_ddl.LAKEBASE_ROLE_NAME == "iot_app_user"

    def test_lakebase_role_id_constant(self) -> None:
        assert sql_ddl.LAKEBASE_ROLE_ID == "iot-app-user"
