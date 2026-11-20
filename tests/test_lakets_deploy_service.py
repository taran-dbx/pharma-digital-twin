"""Tests for iot_twin.services.lakets.lakets_deploy_service.deploy_retention_job."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from iot_twin.services.lakets import lakets_deploy_service


def _fake_workspace_client() -> MagicMock:
    w = MagicMock()
    w.current_user.me.return_value = MagicMock(user_name="user@example.com")
    w.workspace.mkdirs = MagicMock(return_value=None)
    w.workspace.import_ = MagicMock(return_value=None)
    w.jobs.list.return_value = []
    w.jobs.create.return_value = MagicMock(job_id=42)
    w.jobs.reset = MagicMock(return_value=None)
    w.config.host = "https://e2.cloud.databricks.com"
    return w


class TestDeployRetentionJob:
    def test_raises_on_missing_project_id(self) -> None:
        with pytest.raises(ValueError, match="project_id is required"):
            lakets_deploy_service.deploy_retention_job("")

    def test_creates_job_when_none_exists(self) -> None:
        w = _fake_workspace_client()
        with (
            patch("databricks.sdk.WorkspaceClient", return_value=w),
            patch.object(
                lakets_deploy_service,
                "_notebook_source_bytes",
                return_value=b"# Databricks notebook source\nprint('hi')\n",
            ),
        ):
            result = lakets_deploy_service.deploy_retention_job(
                project_id="iot-lakets-demo",
            )

        assert result["job_id"] == 42
        assert result["action"] == "created"
        assert result["notebook_path"].endswith("/retention")
        assert result["schedule"]["cron"].startswith("0 0 3")
        w.workspace.import_.assert_called_once()
        w.jobs.create.assert_called_once()
        w.jobs.reset.assert_not_called()

    def test_updates_job_when_exists(self) -> None:
        w = _fake_workspace_client()
        existing = MagicMock(
            job_id=99,
            settings=MagicMock(name="LakeTS Retention — iot-lakets-demo/production"),
        )
        # MagicMock intercepts .name attr access; set explicitly
        existing.settings.name = "LakeTS Retention — iot-lakets-demo/production"
        w.jobs.list.return_value = [existing]

        with (
            patch("databricks.sdk.WorkspaceClient", return_value=w),
            patch.object(
                lakets_deploy_service,
                "_notebook_source_bytes",
                return_value=b"# body\n",
            ),
        ):
            result = lakets_deploy_service.deploy_retention_job(
                project_id="iot-lakets-demo",
            )

        assert result["job_id"] == 99
        assert result["action"] == "updated"
        w.jobs.reset.assert_called_once()
        w.jobs.create.assert_not_called()

    def test_passes_params_to_notebook_task(self) -> None:
        w = _fake_workspace_client()
        with (
            patch("databricks.sdk.WorkspaceClient", return_value=w),
            patch.object(
                lakets_deploy_service,
                "_notebook_source_bytes",
                return_value=b"# body\n",
            ),
        ):
            lakets_deploy_service.deploy_retention_job(
                project_id="my-proj",
                branch_id="staging",
                database="custom_db",
            )

        _, kwargs = w.jobs.create.call_args
        task = kwargs["tasks"][0]
        assert task.notebook_task.base_parameters == {
            "project_id": "my-proj",
            "branch_id": "staging",
            "database": "custom_db",
        }


def _ops_workspace_client() -> MagicMock:
    """A WorkspaceClient mock suitable for the ops-job deployers (w passed in)."""
    w = MagicMock()
    w.current_user.me.return_value = MagicMock(user_name="user@example.com")
    w.workspace.mkdirs = MagicMock(return_value=None)
    w.workspace.import_ = MagicMock(return_value=None)
    w.jobs.list.return_value = []
    w.jobs.create.return_value = MagicMock(job_id=7)
    w.jobs.reset = MagicMock(return_value=None)
    w.config.host = "https://e2.cloud.databricks.com"
    return w


def _existing_job(name: str, job_id: int) -> MagicMock:
    existing = MagicMock(job_id=job_id, settings=MagicMock())
    existing.settings.name = name
    return existing


class TestDeployPartitionManagerJob:
    def test_raises_on_missing_project_id(self) -> None:
        w = _ops_workspace_client()
        with pytest.raises(ValueError, match="project_id is required"):
            lakets_deploy_service.deploy_partition_manager_job(w, "", "production")

    def test_creates_job_when_none_exists(self) -> None:
        w = _ops_workspace_client()
        result = lakets_deploy_service.deploy_partition_manager_job(
            w, "iot-lakets-demo", "production"
        )

        assert result["job_id"] == 7
        assert result["status"] == "created"
        assert result["job"] == "LakeTS Partition Manager — iot-lakets-demo/production"
        w.workspace.import_.assert_called_once()
        w.jobs.create.assert_called_once()
        w.jobs.reset.assert_not_called()

        _, kwargs = w.jobs.create.call_args
        # Daily — the manager now pre-creates a 48h forward window
        # (p_future_count := 192), so one run/day keeps coverage well ahead of
        # ingestion (far cheaper than every-10-min cluster spins).
        assert kwargs["schedule"].quartz_cron_expression == "0 0 2 * * ?"
        assert kwargs["max_concurrent_runs"] == 1

    def test_updates_job_when_exists(self) -> None:
        w = _ops_workspace_client()
        name = "LakeTS Partition Manager — iot-lakets-demo/production"
        w.jobs.list.return_value = [_existing_job(name, 55)]

        result = lakets_deploy_service.deploy_partition_manager_job(
            w, "iot-lakets-demo", "production"
        )

        assert result["job_id"] == 55
        assert result["status"] == "updated"
        w.jobs.reset.assert_called_once()
        w.jobs.create.assert_not_called()


class TestDeployRollupRefreshJob:
    def test_raises_on_missing_project_id(self) -> None:
        w = _ops_workspace_client()
        with pytest.raises(ValueError, match="project_id is required"):
            lakets_deploy_service.deploy_rollup_refresh_job(w, "", "production")

    def test_creates_job_when_none_exists(self) -> None:
        w = _ops_workspace_client()
        result = lakets_deploy_service.deploy_rollup_refresh_job(w, "iot-lakets-demo", "production")

        assert result["job_id"] == 7
        assert result["status"] == "created"
        assert result["job"] == "LakeTS Rollup Refresh — iot-lakets-demo/production"
        w.jobs.create.assert_called_once()
        w.jobs.reset.assert_not_called()

        _, kwargs = w.jobs.create.call_args
        assert kwargs["schedule"].quartz_cron_expression == "0 0/15 * * * ?"

    def test_updates_job_when_exists(self) -> None:
        w = _ops_workspace_client()
        name = "LakeTS Rollup Refresh — iot-lakets-demo/production"
        w.jobs.list.return_value = [_existing_job(name, 88)]

        result = lakets_deploy_service.deploy_rollup_refresh_job(w, "iot-lakets-demo", "production")

        assert result["job_id"] == 88
        assert result["status"] == "updated"
        w.jobs.reset.assert_called_once()
        w.jobs.create.assert_not_called()


class TestDeployTieringJob:
    def test_raises_on_missing_project_id(self) -> None:
        w = _ops_workspace_client()
        with pytest.raises(ValueError, match="project_id is required"):
            lakets_deploy_service.deploy_tiering_job(w, "", "production")

    def test_creates_job_when_none_exists(self) -> None:
        w = _ops_workspace_client()
        result = lakets_deploy_service.deploy_tiering_job(w, "iot-lakets-demo", "production")

        assert result["job_id"] == 7
        assert result["status"] == "created"
        assert result["job"] == "LakeTS Tiering — iot-lakets-demo/production"
        w.workspace.import_.assert_called_once()
        w.jobs.create.assert_called_once()
        w.jobs.reset.assert_not_called()

        _, kwargs = w.jobs.create.call_args
        assert kwargs["schedule"].quartz_cron_expression == "0 0/30 * * * ?"
        assert kwargs["max_concurrent_runs"] == 1

    def test_updates_job_when_exists(self) -> None:
        w = _ops_workspace_client()
        name = "LakeTS Tiering — iot-lakets-demo/production"
        w.jobs.list.return_value = [_existing_job(name, 77)]

        result = lakets_deploy_service.deploy_tiering_job(w, "iot-lakets-demo", "production")

        assert result["job_id"] == 77
        assert result["status"] == "updated"
        w.jobs.reset.assert_called_once()
        w.jobs.create.assert_not_called()

    def test_notebook_calls_tier_chunk(self) -> None:
        # The inlined notebook must drive cold-tiering via tier_chunk over the
        # chunks returned by _get_chunks_to_tier.
        src = lakets_deploy_service._TIERING_NOTEBOOK_SOURCE.decode("utf-8")
        assert "lakets.tier_chunk" in src
        assert "lakets._get_chunks_to_tier" in src
        assert "policy_type = 'tiering'" in src


class TestDeployLakeTSOpsJobs:
    def test_raises_on_missing_project_id(self) -> None:
        w = _ops_workspace_client()
        with pytest.raises(ValueError, match="project_id is required"):
            lakets_deploy_service.deploy_lakets_ops_jobs(w, "", "production")

    def test_deploys_all_by_default(self) -> None:
        w = _ops_workspace_client()
        with patch.object(
            lakets_deploy_service,
            "deploy_retention_job",
            return_value={"job_id": 1, "job_name": "ret", "action": "created"},
        ) as mock_ret:
            results = lakets_deploy_service.deploy_lakets_ops_jobs(
                w, "iot-lakets-demo", "production"
            )

        assert len(results) == 4
        jobs = {r["job"] for r in results}
        assert "LakeTS Partition Manager — iot-lakets-demo/production" in jobs
        assert "LakeTS Rollup Refresh — iot-lakets-demo/production" in jobs
        assert "LakeTS Tiering — iot-lakets-demo/production" in jobs
        mock_ret.assert_called_once()
        for r in results:
            assert set(r) == {"job", "job_id", "status"}
            assert r["status"] in {"created", "updated"}

    def test_tiering_entry_omitted_when_disabled(self) -> None:
        w = _ops_workspace_client()
        with patch.object(lakets_deploy_service, "deploy_retention_job"):
            results = lakets_deploy_service.deploy_lakets_ops_jobs(
                w,
                "iot-lakets-demo",
                "production",
                partition_manager=False,
                rollup=False,
                retention=False,
                tiering=True,
            )

        assert len(results) == 1
        assert results[0]["job"] == "LakeTS Tiering — iot-lakets-demo/production"

    def test_tiering_omitted_when_false(self) -> None:
        w = _ops_workspace_client()
        with patch.object(lakets_deploy_service, "deploy_retention_job"):
            results = lakets_deploy_service.deploy_lakets_ops_jobs(
                w,
                "iot-lakets-demo",
                "production",
                partition_manager=False,
                rollup=False,
                retention=False,
                tiering=False,
            )

        assert results == []

    def test_respects_flags(self) -> None:
        w = _ops_workspace_client()
        with patch.object(lakets_deploy_service, "deploy_retention_job") as mock_ret:
            results = lakets_deploy_service.deploy_lakets_ops_jobs(
                w,
                "iot-lakets-demo",
                "production",
                partition_manager=True,
                rollup=False,
                retention=False,
                tiering=False,
            )

        assert len(results) == 1
        assert results[0]["job"] == ("LakeTS Partition Manager — iot-lakets-demo/production")
        mock_ret.assert_not_called()

    def test_only_rollup(self) -> None:
        w = _ops_workspace_client()
        with patch.object(lakets_deploy_service, "deploy_retention_job"):
            results = lakets_deploy_service.deploy_lakets_ops_jobs(
                w,
                "iot-lakets-demo",
                "production",
                partition_manager=False,
                rollup=True,
                retention=False,
                tiering=False,
            )

        assert len(results) == 1
        assert results[0]["job"] == "LakeTS Rollup Refresh — iot-lakets-demo/production"


def test_all_lakets_notebooks_retry_postgres_recovery_with_backoff() -> None:
    """[C7233][defect][regression] Every LakeTS job retries transient Postgres."""
    sources = (
        lakets_deploy_service._RETENTION_NOTEBOOK_SOURCE,
        lakets_deploy_service._PARTITION_MANAGER_NOTEBOOK_SOURCE,
        lakets_deploy_service._ROLLUP_REFRESH_NOTEBOOK_SOURCE,
        lakets_deploy_service._TIERING_NOTEBOOK_SOURCE,
    )

    for source in sources:
        text = source.decode("utf-8")
        assert "MAX_CONNECT_ATTEMPTS = 12" in text
        assert "BACKOFF_BASE_S = 1.0" in text
        assert "BACKOFF_MAX_S = 30.0" in text
        assert "def connect_with_backoff():" in text
        assert "def run_with_backoff(sql, params=None, fetch=None):" in text
        operation_retry = text.split("def run_with_backoff(sql, params=None, fetch=None):", 1)[1]
        assert "conn = connect_once()" in operation_retry
        assert "conn = connect_with_backoff()" not in operation_retry
        assert "time.sleep(delay)" in text
        assert "except (psycopg2.OperationalError, psycopg2.InterfaceError):" in text


def test_lakets_tasks_have_job_level_retry_safety_net() -> None:
    """[C7235][defect][regression] Exhausted DB retries restart LakeTS tasks."""
    for deployer in (
        lakets_deploy_service.deploy_retention_job,
        lakets_deploy_service._deploy_ops_job,
    ):
        source = inspect.getsource(deployer)
        assert "max_retries=3" in source
        assert "min_retry_interval_millis=30_000" in source
        assert "retry_on_timeout=True" in source


def test_rollup_cleanup_preserves_exhausted_postgres_error() -> None:
    """[C7236][defect][regression] Cleanup never masks the retry root cause."""
    text = lakets_deploy_service._ROLLUP_REFRESH_NOTEBOOK_SOURCE.decode("utf-8")

    assert "finally:\n    if conn is not None:\n        conn.close()" in text
