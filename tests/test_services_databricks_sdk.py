"""Tests for iot_twin.services.databricks_sdk — detect_cloud helper."""

from __future__ import annotations

from unittest.mock import patch

from iot_twin.services.databricks_sdk import detect_cloud


class TestDetectCloud:
    """Verify detect_cloud returns the correct provider string."""

    @patch.dict("os.environ", {"DATABRICKS_HOST": "https://adb-12345.azuredatabricks.net"})
    def test_returns_azure_for_azuredatabricks_host(self) -> None:
        assert detect_cloud() == "azure"

    @patch.dict("os.environ", {"DATABRICKS_HOST": "https://dbc-12345.cloud.databricks.com"})
    def test_returns_aws_for_standard_databricks_host(self) -> None:
        assert detect_cloud() == "aws"

    @patch.dict("os.environ", {"DATABRICKS_HOST": "https://12345.gcp.databricks.com"})
    def test_returns_gcp_for_gcp_databricks_host(self) -> None:
        assert detect_cloud() == "gcp"

    @patch.dict("os.environ", {"DATABRICKS_HOST": ""})
    def test_returns_aws_for_empty_host(self) -> None:
        assert detect_cloud() == "aws"

    @patch.dict("os.environ", {}, clear=True)
    def test_returns_aws_when_env_var_not_set(self) -> None:
        assert detect_cloud() == "aws"

    @patch.dict("os.environ", {"DATABRICKS_HOST": "https://custom.internal.corp"})
    def test_returns_aws_for_unknown_host(self) -> None:
        assert detect_cloud() == "aws"

    @patch.dict("os.environ", {"DATABRICKS_HOST": "https://workspace.azuredatabricks.net/path"})
    def test_azure_with_path_suffix(self) -> None:
        assert detect_cloud() == "azure"

    @patch.dict("os.environ", {"DATABRICKS_HOST": "https://workspace.gcp.databricks.com/api"})
    def test_gcp_with_path_suffix(self) -> None:
        assert detect_cloud() == "gcp"
