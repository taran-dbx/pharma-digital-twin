"""Guard tests for the industry-first Kafka producer generator.

The producer notebook is generated client-side in TypeScript
(``client/src/lib/data-producer-notebook.ts``). As with the consumer guards,
these tests read the template source and assert the spike-VERIFIED patterns
(spec §1.8, §1.15) are present:

  * kafka-python producer with ``security_protocol="SASL_SSL"`` /
    ``sasl_mechanism="OAUTHBEARER"``.
  * MSK-IAM token provider subclassing ``AbstractTokenProvider`` ->
    ``MSKAuthTokenProvider.generate_auth_token(...)``.
  * every MSK token loads renewable credentials from the UC service credential
    through the signer's custom ``CredentialProvider`` integration.
  * realtime uses asynchronous native Kafka sends without per-record flushes.
  * requested throughput is records/sec, not simulator ticks/sec.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PRODUCER_TS = (
    Path(__file__).resolve().parent.parent / "client" / "src" / "lib" / "data-producer-notebook.ts"
)

pytestmark = pytest.mark.skipif(
    not PRODUCER_TS.exists(),
    reason="producer template not present (client/ gitignored)",
)


def _producer_section() -> str:
    """Isolate the industry Kafka producer section from the legacy generators."""
    text = PRODUCER_TS.read_text()
    m = re.search(
        r"// Industry-first Kafka producer.*?// Zerobus producer",
        text,
        re.DOTALL,
    )
    assert m is not None, "could not locate industry Kafka producer section"
    return m.group(0)


def _deployed_iam_kafka_cell() -> str:
    """Isolate the generated IAM cell used by deployed industry pipelines."""
    text = PRODUCER_TS.read_text()
    m = re.search(
        r"if \(hasCred\).*?mainCell = `(?P<cell>.*?)`\.trim\(\);"
        r"\s*} else if \(hasInlineCreds\)",
        text,
        re.DOTALL,
    )
    assert m is not None, "could not locate deployed IAM Kafka producer cell"
    return m.group("cell")


class TestOAuthBearerAuth:
    def test_sasl_ssl_oauthbearer(self) -> None:
        section = _producer_section()
        assert 'security_protocol="SASL_SSL"' in section
        assert 'sasl_mechanism="OAUTHBEARER"' in section

    def test_msk_iam_token_provider(self) -> None:
        section = _producer_section()
        assert "AbstractTokenProvider" in section
        assert "MSKAuthTokenProvider.generate_auth_token" in section
        assert "sasl_oauth_token_provider=MSKTokenProvider()" in section

    def test_renewable_credentials_from_service_credential(self) -> None:
        section = _producer_section()
        assert "getServiceCredentialsProvider" in section
        assert "class UCServiceCredentialProvider(CredentialProvider)" in section
        assert "return provider.get_credentials()" in section
        assert "generate_auth_token_from_credentials_provider" in section
        assert "get_frozen_credentials()" not in section

    def test_does_not_upgrade_boto3(self) -> None:
        section = _producer_section()
        assert "--upgrade boto3" not in section
        assert "aws-msk-iam-sasl-signer-python" in section


class TestGenerationSemantics:
    def test_realtime_uses_zero_linger_microbatch_is_batched(self) -> None:
        section = _producer_section()
        # Zero linger starts transmission immediately; microbatch may wait.
        assert "const lingerMs = isRealtime ? 0 : 100;" in section

    def test_realtime_send_is_async_and_flushes_only_at_shutdown(self) -> None:
        section = _producer_section()
        match = re.search(
            r"const sendLoop = isRealtime\s+\? `(?P<body>.*?)`\s+: `",
            section,
            re.DOTALL,
        )
        assert match is not None
        realtime_loop = match.group("body")
        assert "producer.send(" in realtime_loop
        assert "producer.flush()" not in realtime_loop

    def test_uses_simulator_and_records_per_sec(self) -> None:
        section = _producer_section()
        assert "from iot_simulator import Simulator" in section
        assert "rec/s" in section
        assert "TARGET_RECORDS_PER_SEC / sensor_count" in section
        assert "update_rate_hz=SIM_TICK_HZ" in section

    def test_wires_scenario_engine_and_fails_loud_c7314_regression(self) -> None:
        """[C7314][regression] Industry producer wraps ticks and fails if scenario_engine is missing."""
        section = _producer_section()
        assert "from scenario_engine import ScenarioEngine" in section
        assert "wrap_records" in section
        assert "electric_power" in section
        assert "emitting unwrapped simulator ticks" not in section
        assert "scenario_engine is not importable" not in section

    def test_zerobus_producer_maps_energy_and_wraps_c7314_defect(self) -> None:
        """[C7314][defect] Zerobus producers alias energy, wrap records, and fail loud on missing engine."""
        text = PRODUCER_TS.read_text()
        assert "function generateZerobusProducer" in text
        assert 'SIMULATOR_INDUSTRY = "electric_power" if INDUSTRY == "energy" else INDUSTRY' in text
        assert "wrap_records(records)" in text
        assert "Simulator(industries=[SIMULATOR_INDUSTRY]" in text
        assert "emitting unwrapped simulator ticks" not in text

    def test_does_not_create_topic(self) -> None:
        # Topic is created up-front by the provisioning engine (§1.8).
        section = _producer_section()
        assert "create_topics" not in section
        assert "NewTopic" not in section


class TestDeployedIndustryProducer:
    """Regression coverage for [C7250][defect][regression]."""

    def test_iam_path_uses_native_async_kafka_producer(self) -> None:
        section = _deployed_iam_kafka_cell()
        assert "KafkaProducer(" in section
        assert "Using native async Kafka producer" in section
        assert "linger_ms=0" in section
        assert "writeStream" not in section
        assert '.write.format("kafka")' not in section

    def test_target_records_per_second_is_divided_by_sensor_count(self) -> None:
        section = _deployed_iam_kafka_cell()
        assert "TARGET_RECORDS_PER_SEC" in section
        assert "TARGET_RECORDS_PER_SEC / sensor_count" in section
        assert "update_rate_hz=SIM_TICK_HZ" in section

    def test_callback_does_not_flush_each_record(self) -> None:
        section = _deployed_iam_kafka_cell()
        match = re.search(
            r"def on_records\(records\):(?P<body>.*?)\n\n\ndef _heartbeat",
            section,
            re.DOTALL,
        )
        assert match is not None, "could not isolate IAM on_records callback"
        callback = match.group("body")
        assert "producer.send(" in callback
        assert "producer.flush()" not in callback

    def test_msk_tokens_refresh_from_uc_service_credential(self) -> None:
        section = _deployed_iam_kafka_cell()
        assert "class UCServiceCredentialProvider(CredentialProvider)" in section
        assert "return provider.get_credentials()" in section
        assert "MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(" in section
        assert "get_frozen_credentials()" not in section
        assert 'os.environ["AWS_ACCESS_KEY_ID"]' not in section

    def test_sustained_send_failures_stop_the_silent_producer(self) -> None:
        section = _deployed_iam_kafka_cell()
        assert "MAX_CONSECUTIVE_SEND_ERRORS" in section
        assert "SEND_STALL_TIMEOUT_S" in section
        assert "_fatal_send_error.set()" in section
        assert "_thread.interrupt_main()" in section
        assert "if _fatal_send_error.is_set():" in section
        assert "except KeyboardInterrupt:" in section
        assert "raise RuntimeError(" in section
        assert "producer.flush(timeout=30)" in section
        assert "producer.close(timeout=30)" in section
