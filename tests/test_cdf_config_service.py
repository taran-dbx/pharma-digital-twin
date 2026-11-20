"""Unit tests for automated Lakebase CDF configuration [C7201][regression]."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from iot_twin.services import cdf_config_service as cdf


class NotFoundError(RuntimeError):
    error_code = "NOT_FOUND"


def test_database_parent_uses_api_resource_id_c7212() -> None:
    """The API resource id is hyphenated, unlike the SQL database name [C7212][defect]."""
    assert cdf.database_parent("pharmaceutical", project_id="iot-pipelines") == (
        "projects/iot-pipelines/branches/pharmaceutical/databases/databricks-postgres"
    )


def test_ensure_creates_missing_config_c7201() -> None:
    """Create the managed sync through the documented API [C7201][smoke]."""
    ws = MagicMock()
    ws.api_client.do.side_effect = [
        NotFoundError("missing config"),
        {"name": "projects/p/operations/op-1", "done": True, "response": {}},
        {
            "name": "projects/p/branches/b/databases/d/cdf-configs/lakets_cdf",
            "cdf_config_id": "lakets_cdf",
            "catalog": "main",
            "schema": "realtime",
            "postgres_schema": "lakets_cdf",
        },
    ]

    result = cdf.ensure_cdf_config(
        "projects/p/branches/b/databases/d",
        catalog="main",
        schema="realtime",
        ws=ws,
    )

    assert result["status"] == "created"
    assert result["cdf_config_id"] == "lakets_cdf"
    create = ws.api_client.do.call_args_list[1]
    assert create.args == (
        "POST",
        "/api/2.0/postgres/projects/p/branches/b/databases/d/cdf-configs",
    )
    assert create.kwargs["query"] == {"cdf_config_id": "lakets_cdf"}
    assert create.kwargs["body"] == {
        "catalog": "main",
        "schema": "realtime",
        "postgres_schema": "lakets_cdf",
    }


def test_ensure_reuses_identical_config_c7202() -> None:
    """Idempotent retries reuse an exact immutable config [C7202][regression]."""
    ws = MagicMock()
    ws.api_client.do.return_value = {
        "name": "projects/p/branches/b/databases/d/cdf-configs/lakets_cdf",
        "cdf_config_id": "lakets_cdf",
        "catalog": "main",
        "schema": "realtime",
        "postgres_schema": "lakets_cdf",
    }

    result = cdf.ensure_cdf_config(
        "projects/p/branches/b/databases/d",
        catalog="main",
        schema="realtime",
        ws=ws,
    )

    assert result["status"] == "existing"
    ws.api_client.do.assert_called_once()


def test_ensure_rejects_immutable_mismatch_c7203() -> None:
    """Never silently redirect an existing sync [C7203][regression]."""
    ws = MagicMock()
    ws.api_client.do.return_value = {
        "catalog": "other",
        "schema": "realtime",
        "postgres_schema": "lakets_cdf",
    }

    with pytest.raises(cdf.CdfConfigMismatch, match="immutable"):
        cdf.ensure_cdf_config(
            "projects/p/branches/b/databases/d",
            catalog="main",
            schema="realtime",
            ws=ws,
        )


def test_delete_is_idempotent_c7204() -> None:
    """Teardown treats an absent sync as already deleted [C7204][regression]."""
    ws = MagicMock()
    ws.api_client.do.side_effect = NotFoundError("missing")

    assert cdf.delete_cdf_config("projects/p/branches/b/databases/d", "lakets_cdf", ws=ws) is False


def test_create_waits_for_long_running_operation_c7208(monkeypatch) -> None:
    """Provisioning waits for the API operation to finish [C7208][regression]."""
    ws = MagicMock()
    ws.api_client.do.side_effect = [
        NotFoundError("missing config"),
        {"name": "projects/p/operations/op-1", "done": False},
        {"name": "projects/p/operations/op-1", "done": True, "response": {}},
        {
            "name": "projects/p/branches/b/databases/d/cdf-configs/lakets_cdf",
            "cdf_config_id": "lakets_cdf",
            "catalog": "main",
            "schema": "realtime",
            "postgres_schema": "lakets_cdf",
        },
    ]
    monkeypatch.setattr(cdf.time, "sleep", lambda _seconds: None)

    result = cdf.ensure_cdf_config(
        "projects/p/branches/b/databases/d",
        catalog="main",
        schema="realtime",
        ws=ws,
    )

    assert result["status"] == "created"
    operation_poll = ws.api_client.do.call_args_list[2]
    assert operation_poll.args == (
        "GET",
        "/api/2.0/postgres/projects/p/operations/op-1",
    )


def test_delete_waits_when_done_false_is_omitted_c7210(monkeypatch) -> None:
    """Proto-default done=false must still be polled [C7210][regression]."""
    ws = MagicMock()
    ws.api_client.do.side_effect = [
        {"name": "projects/p/operations/op-delete"},
        {
            "name": "projects/p/operations/op-delete",
            "done": True,
            "response": {},
        },
    ]
    monkeypatch.setattr(cdf.time, "sleep", lambda _seconds: None)

    deleted = cdf.delete_cdf_config("projects/p/branches/b/databases/d", "lakets_cdf", ws=ws)

    assert deleted is True
    assert ws.api_client.do.call_args_list[1].args == (
        "GET",
        "/api/2.0/postgres/projects/p/operations/op-delete",
    )


def test_generic_message_is_not_treated_as_not_found_c7249() -> None:
    """Only structured API codes authorize create/delete fallback [C7249][regression]."""
    assert cdf._is_not_found(RuntimeError("resource does not exist")) is False
    assert cdf._is_not_found(NotFoundError("opaque")) is True
