"""Unit tests for individual health-check rules [pipeline-throughput].

The check functions are pulled out of their notebook cell with ``ast`` (so the cell's
``REPORT.run(...)`` calls do not execute) and run against stubbed data.
"""

from __future__ import annotations

import ast
import types

import nbformat
import pytest

from tests.test_pipeline_health_notebook import NOTEBOOK, _cell, _namespace

BASE_CELLS = ("params", "config", "redaction", "sql-guard", "render", "model")


@pytest.fixture(scope="module")
def notebook():
    return nbformat.read(NOTEBOOK, as_version=4)


def _functions(notebook, cell_id: str, ns: dict) -> dict:
    """Define only the functions/constants of one cell in ``ns`` (no top-level calls)."""
    tree = ast.parse(_cell(notebook, cell_id))
    keep = [
        n for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.Assign, ast.Import, ast.ImportFrom))
    ]  # fmt: skip
    exec(compile(ast.Module(body=keep, type_ignores=[]), f"<cell:{cell_id}>", "exec"), ns)
    return ns


def _ns(notebook, monkeypatch, cell_id: str, mode: str = "realtime") -> dict:
    ns = _namespace(notebook, BASE_CELLS, monkeypatch)
    _functions(notebook, cell_id, ns)
    ns["PARAMS"] = types.SimpleNamespace(**{**vars(ns["PARAMS"]), "mode": mode})
    ns["_hot"] = lambda: None
    return ns


# -- metrics_batch_duration -------------------------------------------------------------------
def _metric_rows(trigger_ms: float, processing_p99: float, n: int = 6) -> list[dict]:
    return [{"trigger_ms": trigger_ms, "processing_p99": processing_p99} for _ in range(n)]


def test_rtm_long_trigger_is_not_a_failure_c9301_regression(notebook, monkeypatch) -> None:
    """[C9301][regression] A 300 s RTM trigger with 1 ms processing p99 PASSes (was FAIL)."""
    ns = _ns(notebook, monkeypatch, "metrics", mode="realtime")
    ns["_metrics_rows"] = lambda: _metric_rows(311_100.0, 1.0)
    outcome = ns["check_batch_duration"]()
    assert outcome.status == ns["PASS"]
    assert "RTM" in outcome.value and "trigger p95" in outcome.value


@pytest.mark.parametrize(("p99", "level"), [(300.0, "WARN"), (1_500.0, "FAIL")])
def test_rtm_slow_processing_is_flagged_c9302_smoke(notebook, monkeypatch, p99, level) -> None:
    """[C9302][smoke] RTM is judged on processing p99 (WARN > 250 ms, FAIL > 1 s)."""
    ns = _ns(notebook, monkeypatch, "metrics", mode="realtime")
    ns["_metrics_rows"] = lambda: _metric_rows(300_000.0, p99)
    assert ns["check_batch_duration"]().status == ns[level]


def test_microbatch_still_judges_batch_duration_c9303_regression(notebook, monkeypatch) -> None:
    """[C9303][regression] Micro-batch keeps the trigger-duration rule (FAIL > 60 s)."""
    ns = _ns(notebook, monkeypatch, "metrics", mode="microbatch")
    ns["_metrics_rows"] = lambda: _metric_rows(90_000.0, 1.0)
    assert ns["check_batch_duration"]().status == ns["FAIL"]
    ns["_metrics_rows"] = lambda: _metric_rows(2_000.0, 1.0)
    assert ns["check_batch_duration"]().status == ns["PASS"]


# -- lakets_rollups ----------------------------------------------------------------------------
class _Data:
    def __init__(self, **answers) -> None:
        self.answers = answers

    def lb(self, name: str, **_kwargs):
        return self.answers[name]

    def lb_one(self, name: str, **_kwargs):
        return self.answers[name][0]


def _rollup(age_s: float, lag_s: float) -> list[dict]:
    return [{"name": "sensor_1m", "refresh_age_s": age_s, "refresh_lag_s": lag_s,
             "pending_invalidations": 0}]  # fmt: skip


@pytest.mark.parametrize(
    ("age_s", "lag_s", "level"),
    [
        (63.6 * 60, 3600, "PASS"),  # live 2026-11-21: hourly rollup, refreshed 64 min ago
        (75 * 60 + 601, 3600, "WARN"),  # 10 min overdue
        (75 * 60 + 3601, 3600, "FAIL"),  # 1 h overdue
        (61 * 60, 0, "WARN"),  # no refresh_lag: the old absolute rule applies
    ],
)
def test_rollup_age_is_judged_against_refresh_lag_c9304_regression(
    notebook, monkeypatch, age_s, lag_s, level
) -> None:
    """[C9304][regression] Overdue = age - refresh_lag - 15 min job interval."""
    ns = _ns(notebook, monkeypatch, "lakets")
    ns["DATA"] = _Data(lk_rollups=_rollup(age_s, lag_s))
    assert ns["check_lakets_rollups"]().status == ns[level]


# -- lakets_sync_slot / lakets_insert_triggers -------------------------------------------------
def _slot(active=True, retained_mb=20.0, write_age_s=10.0) -> _Data:
    return _Data(
        lk_slot=[{"slot_name": "wal2delta_main_slot_x", "active": active, "wal_status": "reserved",
                  "retained_bytes": retained_mb * 1024**2, "flush_lag_bytes": 0}],
        lk_wal2delta=[{"status": "STREAMING", "write_age_s": write_age_s}],
    )  # fmt: skip


@pytest.mark.parametrize(
    ("data", "level"),
    [
        (_slot(), "PASS"),
        (_slot(write_age_s=300), "WARN"),
        (_slot(write_age_s=8_760), "FAIL"),  # 146 min, stall 1
        (_slot(retained_mb=2_214), "FAIL"),  # 2.2 GB, the 2026-11-20 health check
        (_slot(active=False), "FAIL"),
    ],
)
def test_sync_slot_flags_a_stuck_sync_c9305_smoke(notebook, monkeypatch, data, level) -> None:
    """[C9305][smoke] Stalled writer, retained WAL and inactive slot are each detected."""
    ns = _ns(notebook, monkeypatch, "lakets")
    ns["DATA"] = data
    ns["CDF_SHADOW_TABLE"] = "_shadow_manufacturing_sensor_readings"
    assert ns["check_sync_slot"]().status == ns[level]


def test_missing_slot_is_a_failure_c9306_regression(notebook, monkeypatch) -> None:
    """[C9306][regression] No wal2delta slot at all means the sync is detached."""
    ns = _ns(notebook, monkeypatch, "lakets")
    ns["DATA"] = _Data(lk_slot=[])
    assert ns["check_sync_slot"]().status == ns["FAIL"]


def _triggers(*names_per_row, sync=True) -> _Data:
    rows = [{"tgname": name, "per_row": per_row} for name, per_row in names_per_row]
    return _Data(lk_insert_triggers=rows, lk_cdf=[{"sync_enabled": sync}])


@pytest.mark.parametrize(
    ("data", "level"),
    [
        # after this PR: one statement-level path
        (_triggers(("trg_lakets_lvc_insert", False), ("trg_lakets_sync_insert", False)), "PASS"),
        # LakeTS 0.1.0: one per-row path, slow
        (_triggers(("trg_lakets_lvc", True), ("trg_lakets_sync", True)), "WARN"),
        # both paths: every row would be copied twice
        (_triggers(("trg_lakets_sync", True), ("trg_lakets_sync_insert", False)), "FAIL"),
        # no path while sync is enabled: rows never reach the shadow
        (_triggers(("trg_lakets_lvc_insert", False)), "FAIL"),
        # sync disabled: no shadow path is expected
        (_triggers(("trg_lakets_lvc_insert", False), sync=False), "PASS"),
    ],
)
def test_exactly_one_shadow_insert_path_c9307_regression(
    notebook, monkeypatch, data, level
) -> None:
    """[C9307][regression] Sync enabled => exactly one INSERT path into the shadow (0 or 2 FAIL)."""
    ns = _ns(notebook, monkeypatch, "lakets")
    ns["DATA"] = data
    assert ns["check_insert_triggers"]().status == ns[level]


# -- history-table row filter --------------------------------------------------------------------
HISTORY = "`example-catalog`.`pharma_twin`.`lb__shadow_manufacturing_sensor_readings_history`"


@pytest.mark.parametrize(
    "name", ["uc_window", "uc_rate_hour", "uc_dupes", "uc_recon", "uc_total", "uc_nulls"]
)
def test_history_row_queries_read_only_row_images_c9310_regression(
    notebook, monkeypatch, name
) -> None:
    """[C9310][regression] Pruned-shadow `delete` change rows never count as data rows."""
    ns = _namespace(notebook, ("params", "config", "sql-guard", "queries"), monkeypatch)
    fields = {"table": HISTORY, "hours": 2, "qhours": 2, "t0": "2026-01-01 00:00:00",
              "t1": "2026-01-01 01:00:00"}  # fmt: skip
    sql = ns["UC_SQL"][name].format(**ns["cdc_row_fields"](name, fields))
    assert (
        f"(SELECT * FROM {HISTORY} WHERE _pg_change_type IN ('insert', 'update_postimage'))" in sql
    )
    ns["assert_read_only"](sql)


def test_other_tables_and_metadata_queries_are_unchanged_c9311_smoke(notebook, monkeypatch) -> None:
    """[C9311][smoke] Micro-batch/zerobus Delta tables and DESCRIBE queries keep the raw table."""
    ns = _namespace(notebook, ("params", "config", "sql-guard", "queries"), monkeypatch)
    plain = {"table": "`c`.`pharma_twin`.`manufacturing_microbatch`", "hours": 2}
    assert ns["cdc_row_fields"]("uc_window", plain) == plain
    history = {"table": HISTORY}
    assert ns["cdc_row_fields"]("uc_history", history) == history
    assert ns["cdc_row_fields"]("uc_detail", history) == history
