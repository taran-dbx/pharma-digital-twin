"""Shared Databricks SDK helpers."""

from __future__ import annotations

import os


def detect_cloud() -> str:
    """Detect the cloud provider from DATABRICKS_HOST."""
    host = os.getenv("DATABRICKS_HOST", "")
    if ".azuredatabricks.net" in host:
        return "azure"
    if ".gcp.databricks.com" in host:
        return "gcp"
    return "aws"
