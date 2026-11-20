"""Tests for ``services/zerobus_validation.py`` (a REAL Zerobus producer for
the pipeline validation flow, replacing the no-op stubs in
``services/validation/validation_snippets.py``/``services/validation/validation_steps.py``).

``zerobus_produce_snippet`` is a pure string builder (content tests, mirroring
``tests/test_validation_snippets.py``'s style) — no cluster / network needed.
``auth_probe`` runs on the app driver; its network calls are mocked so this
file has no live dependencies.
"""

from __future__ import annotations

import ast
from unittest.mock import MagicMock, patch

from iot_twin.services import zerobus_validation as zbv
from tests._fakes import fake_credential

# Secret scope / key *names* passed to the snippet builder (identifiers, not secrets).
CUSTOM_SCOPE_NAME = "custom-scope"
CUSTOM_KEY_NAME = "custom-secret-key"


# ---------------------------------------------------------------------------
# zerobus_produce_snippet — content / syntax tests
# ---------------------------------------------------------------------------
def _snippet(**overrides) -> str:
    kwargs = dict(
        industry="pharmaceutical",
        catalog="iot_pharmaceutical",
        table_fqn="iot_pharmaceutical.zerobus.iot_sensor_data",
        workspace_url="https://example-us-east.cloud.databricks.com",
        server_endpoint="111111111111111.zerobus.us-east-1.cloud.databricks.com",
        num_records=25,
        source="validation",
    )
    kwargs.update(overrides)
    return zbv.zerobus_produce_snippet(**kwargs)


def test_snippet_is_valid_python_syntax():
    ast.parse(_snippet())


def test_snippet_uses_zerobus_sdk_not_a_stub():
    code = _snippet()
    assert "ZerobusSdk" in code
    assert "create_stream(" in code
    assert "ingest_record(" in code
    assert "wait_for_ack(" in code
    assert "stream.close()" in code
    # Not the old no-op stub which only prints the intended config.
    assert "dbutils.secrets.get" in code


def test_snippet_carries_ok_fail_sentinels():
    code = _snippet()
    assert zbv.OK_SENTINEL in code
    assert zbv.FAIL_SENTINEL in code
    assert "VALIDATION_OK" in code
    assert "VALIDATION_FAIL" in code


def test_snippet_record_uses_delta_column_names_not_record_field_names():
    """The Delta table (iot_twin.models.sensor_schema.delta_table_ddl) uses column
    names — 'event_time', not the raw SensorRecord field 'timestamp' that
    zerobus_record_keys()/ZerobusSink emit. Sending the wrong key silently
    drops the column (see module docstring point 1)."""
    code = _snippet()
    for column in (
        "sensor_name",
        "industry",
        "value",
        "unit",
        "sensor_type",
        "event_time",
        "min_value",
        "max_value",
        "nominal_value",
        "fault_active",
        "metadata",
    ):
        assert f"'{column}'" in code, f"missing SSOT column {column!r} in generated record"
    # The raw record-field name for the timestamp must NOT appear as a dict key.
    assert "'timestamp':" not in code


def test_snippet_timestamp_is_epoch_micros_int_not_iso_string():
    """LIVE-VERIFIED: an ISO-8601 string fails ingestion with INVALID_ARGUMENT
    'invalid digit found in string' — the server expects a numeric epoch."""
    code = _snippet()
    assert "int(time.time() * 1_000_000)" in code
    assert "isoformat()" not in code


def test_snippet_workspace_url_gets_https_prefix_guard():
    code = _snippet(workspace_url="example-us-east.cloud.databricks.com")
    assert 'WORKSPACE_URL.startswith("http")' in code


def test_snippet_embeds_table_and_endpoint_and_num_records():
    code = _snippet(
        table_fqn="iot_energy.zerobus.iot_sensor_data",
        server_endpoint="123.zerobus.us-west-2.cloud.databricks.com",
        num_records=42,
    )
    assert "iot_energy.zerobus.iot_sensor_data" in code
    assert "123.zerobus.us-west-2.cloud.databricks.com" in code
    assert "range(42)" in code


def test_snippet_uses_secret_scope_and_keys(monkeypatch):
    monkeypatch.setenv("ZEROBUS_SECRET_SCOPE", "iot-secrets")
    code = _snippet()
    assert "'iot-secrets'" in code
    assert "'zerobus-client-id'" in code
    assert "'zerobus-client-secret'" in code
    code2 = _snippet()
    code2 = zbv.zerobus_produce_snippet(
        industry="pharmaceutical",
        catalog="iot_pharmaceutical",
        table_fqn="iot_pharmaceutical.zerobus.iot_sensor_data",
        workspace_url="https://example-us-east.cloud.databricks.com",
        server_endpoint="111111111111111.zerobus.us-east-1.cloud.databricks.com",
        secret_scope=CUSTOM_SCOPE_NAME,
        client_id_key="custom-id-key",
        client_secret_key=CUSTOM_KEY_NAME,
    )
    assert repr(CUSTOM_SCOPE_NAME) in code2
    assert "'custom-id-key'" in code2
    assert repr(CUSTOM_KEY_NAME) in code2


# ---------------------------------------------------------------------------
# _resolve_zerobus_creds — unified credential resolver
# ---------------------------------------------------------------------------
def test_resolve_creds_uses_databricks_env_when_only_app_sp_set(monkeypatch):
    """With only DATABRICKS_CLIENT_ID/CLIENT_SECRET in env (the single-SP
    Databricks Apps deployment model), the resolver must return those values —
    not an error — so both auth_probe and produce succeed."""
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", fake_credential("app-sp"))
    # No ZEROBUS_* vars, no UC scope.
    resolved_id, resolved_secret, error = zbv._resolve_zerobus_creds(None, None, None, None)
    assert resolved_id == "app-sp-client-id"
    assert resolved_secret == fake_credential("app-sp")
    assert error is None


def test_resolve_creds_explicit_arg_beats_databricks_env(monkeypatch):
    """Explicit client_id/client_secret arguments take priority over all env vars."""
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", fake_credential("app-sp"))
    resolved_id, resolved_secret, error = zbv._resolve_zerobus_creds(
        "explicit-id", fake_credential("explicit"), None, None
    )
    assert resolved_id == "explicit-id"
    assert resolved_secret == fake_credential("explicit")
    assert error is None


def test_resolve_creds_databricks_env_beats_zerobus_env(monkeypatch):
    """DATABRICKS_* env takes precedence over the legacy ZEROBUS_* env vars."""
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "db-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", fake_credential("db"))
    monkeypatch.setenv("ZEROBUS_CLIENT_ID", "zb-id")
    monkeypatch.setenv("ZEROBUS_CLIENT_SECRET", fake_credential("zb"))
    resolved_id, resolved_secret, error = zbv._resolve_zerobus_creds(None, None, None, None)
    assert resolved_id == "db-id"
    assert resolved_secret == fake_credential("db")
    assert error is None


def test_resolve_creds_falls_back_to_zerobus_env_when_no_databricks(monkeypatch):
    """Legacy ZEROBUS_* env vars still work when DATABRICKS_* are absent."""
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    monkeypatch.setenv("ZEROBUS_CLIENT_ID", "zb-id")
    monkeypatch.setenv("ZEROBUS_CLIENT_SECRET", fake_credential("zb"))
    resolved_id, resolved_secret, error = zbv._resolve_zerobus_creds(None, None, None, None)
    assert resolved_id == "zb-id"
    assert resolved_secret == fake_credential("zb")
    assert error is None


def test_resolve_creds_returns_none_when_no_creds_set(monkeypatch):
    """When no env vars or explicit args are set, resolver returns (None, None, None)."""
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ZEROBUS_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZEROBUS_CLIENT_SECRET", raising=False)
    resolved_id, resolved_secret, error = zbv._resolve_zerobus_creds(None, None, None, None)
    assert resolved_id is None
    assert resolved_secret is None
    assert error is None


# ---------------------------------------------------------------------------
# auth_probe — driver-side reachability/credential probe (mocked network)
# ---------------------------------------------------------------------------
def test_auth_probe_empty_endpoint_fails_fast():
    ok, detail = zbv.auth_probe("https://x.cloud.databricks.com", "", "cat.sch.tbl")
    assert ok is False
    assert "error" in detail


def test_auth_probe_dns_failure():
    with patch("socket.gethostbyname", side_effect=OSError("nodename nor servname provided")):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com", "nonexistent.zerobus.example.com", "cat.sch.tbl"
        )
    assert ok is False
    assert detail["dns"] is False
    assert "dns_error" in detail


def test_auth_probe_tcp_failure():
    with (
        patch("socket.gethostbyname", return_value="10.0.0.1"),
        patch("socket.create_connection", side_effect=OSError("connection refused")),
    ):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com", "host.zerobus.example.com", "cat.sch.tbl"
        )
    assert ok is False
    assert detail["dns"] is True
    assert detail["tcp"] is False
    assert "tcp_error" in detail


def test_auth_probe_reachable_no_credentials_skips_token_check():
    with (
        patch("socket.gethostbyname", return_value="10.0.0.1"),
        patch("socket.create_connection", MagicMock()),
        patch.dict("os.environ", {}, clear=True),
    ):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "cat.sch.tbl",
        )
    assert ok is True
    assert detail["tcp"] is True
    assert detail["token_ok"] is None
    assert "token_skipped" in detail
    # Error message must mention the DATABRICKS_* vars (now the primary path).
    assert "DATABRICKS_CLIENT_ID" in detail["token_skipped"]


def test_auth_probe_token_exchange_success():
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {"access_token": fake_credential("access")}
    with (
        patch("socket.gethostbyname", return_value="10.0.0.1"),
        patch("socket.create_connection", MagicMock()),
        patch("requests.post", return_value=fake_resp) as mock_post,
    ):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "iot_pharmaceutical.zerobus.iot_sensor_data",
            client_id="cid",
            client_secret=fake_credential("sp"),
        )
    assert ok is True
    assert detail["token_ok"] is True
    kwargs = mock_post.call_args.kwargs
    assert kwargs["auth"] == ("cid", fake_credential("sp"))
    assert "authorization_details" in kwargs["data"]
    assert "iot_pharmaceutical" in kwargs["data"]["authorization_details"]


def test_auth_probe_token_exchange_missing_grants_fails_clearly():
    """LIVE-VERIFIED failure mode: SP without USE_CATALOG/USE_SCHEMA/SELECT/
    MODIFY grants gets a 401 invalid_authorization_details."""
    fake_resp = MagicMock()
    fake_resp.status_code = 401
    fake_resp.text = (
        '{"error":"invalid_authorization_details","error_description":"User is not authorized"}'
    )
    fake_resp.json.return_value = {}
    with (
        patch("socket.gethostbyname", return_value="10.0.0.1"),
        patch("socket.create_connection", MagicMock()),
        patch("requests.post", return_value=fake_resp),
    ):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "iot_pharmaceutical.zerobus.iot_sensor_data",
            client_id="cid",
            client_secret=fake_credential("sp"),
        )
    assert ok is False
    assert detail["token_ok"] is False
    assert "invalid_authorization_details" in detail["token_error"]


def test_auth_probe_bad_table_fqn_fails_before_network_call():
    with (
        patch("socket.gethostbyname", return_value="10.0.0.1"),
        patch("socket.create_connection", MagicMock()),
        patch("requests.post") as mock_post,
    ):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "not_a_three_part_name",
            client_id="cid",
            client_secret=fake_credential("sp"),
        )
    assert ok is False
    assert "catalog.schema.table" in detail["token_error"]
    mock_post.assert_not_called()
