"""Tests for ops/notebooks/pipeline_health_check.ipynb.

* structure: valid nbformat 4, python3 kernelspec, unique cell ids;
* the redaction helper and the read-only SQL guard (executed straight from the notebook cells);
* the notebook re-executed offline in sample mode (fresh kernel) renders every section and no
  host/id/email patterns;
* the *committed* outputs are present, error-free, identifier-clean and in sync with the code.

Execution tests need the dev extras (``nbformat``, ``nbclient``, ``ipykernel``).
"""

from __future__ import annotations

import base64
import importlib.util
import json
import re
import string
import sys
import types
from pathlib import Path

import pytest

nbformat = pytest.importorskip("nbformat")
pytest.importorskip("nbclient")
pytest.importorskip("ipykernel")

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "ops" / "notebooks" / "pipeline_health_check.ipynb"

SECTION_TITLES = (
    "Scorecard",
    "1. Header",
    "2. Pipeline &amp; job health",
    "3. Lakebase hot tier",
    "4. LakeTS outcomes",
    "5. CDF / Unity Catalog cold tier",
    "6. Streaming metrics",
    "7. Cross-checks &amp; data quality",
    "8. Final scorecard",
)
# Fabricated identifiers embedded in the synthetic dataset; none may reach a redacted output.
SAMPLE_IDENTIFIERS = (
    "alex.morgan",
    "acme-industrial",
    "example-ws",
    "example-lakebase",
    "7c1e2f94-3b5a-4d68-9e0f-a1b2c3d4e5f6",
    "0d9f3a71",
    "884201937452610",
    "a1b2c3d4" + "e5f60718",
    "acme_catalog",
    "iot_lakehouse",
    "iot-streaming-scope",
    "123456789012",
)


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_refresher():
    spec = importlib.util.spec_from_file_location(
        "refresh_notebook_outputs", ROOT / "ops" / "refresh_notebook_outputs.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["refresh_notebook_outputs"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def checker():
    return _load_script("check_identifiers")


@pytest.fixture(scope="module")
def committed():
    return nbformat.read(NOTEBOOK, as_version=4)


@pytest.fixture(scope="module")
def executed():
    """Fresh sample-mode run of the notebook (default widgets, redaction on)."""
    return _load_refresher().execute_sample()


def _cell(notebook, cell_id: str) -> str:
    return next(c.source for c in notebook.cells if c.id == cell_id)


def _namespace(notebook, cell_ids: tuple[str, ...], monkeypatch) -> dict:
    """Execute selected notebook cells (in order) into one namespace."""
    monkeypatch.setenv("HEALTH_SAMPLE", "true")
    module = types.ModuleType("health_notebook")  # dataclasses resolve annotations via sys.modules
    monkeypatch.setitem(sys.modules, module.__name__, module)
    namespace = module.__dict__
    for cell_id in cell_ids:
        exec(compile(_cell(notebook, cell_id), f"<cell:{cell_id}>", "exec"), namespace)
    return namespace


def _outputs_blob(notebook) -> str:
    return json.dumps([o for c in notebook.cells for o in c.get("outputs", [])])


def _html(notebook, cell_id: str) -> str:
    cell = next(c for c in notebook.cells if c.id == cell_id)
    return "".join(
        "".join(o["data"].get("text/html", ""))
        for o in cell.outputs
        if o.output_type == "display_data"
    )


def _all_html(notebook) -> str:
    return "".join(_html(notebook, c.id) for c in notebook.cells if c.cell_type == "code")


def _check_names(notebook) -> set[str]:
    """Check names from the scorecard table (first column of rows that carry a status badge)."""
    html = _html(notebook, "scorecard")
    return set(re.findall(r"<td[^>]*>([a-z0-9_]+)</td><td[^>]*><span", html))


# ── structure ────────────────────────────────────────────────────────────────
def test_notebook_is_valid_nbformat4(committed):
    nbformat.validate(committed)
    assert committed.nbformat == 4
    assert committed.metadata["kernelspec"]["name"] == "python3"
    ids = [c.id for c in committed.cells]
    assert len(ids) == len(set(ids)), "cell ids must be unique"
    assert {"params", "config", "redaction", "sql-guard", "scorecard", "final"} <= set(ids)


def test_notebook_has_no_magics_or_unguarded_dbutils(committed):
    for cell in committed.cells:
        if cell.cell_type != "code":
            continue
        assert not re.search(r"^\s*[%!]", cell.source, re.M), f"magic/shell in cell {cell.id}"
    for cell in committed.cells:
        if cell.cell_type == "code" and cell.id != "params":
            assert "dbutils" not in re.sub(r"#.*", "", cell.source).replace("_DBUTILS", ""), cell.id


# ── redaction helper (adversarial) ───────────────────────────────────────────
# A 12-digit AWS account id, assembled from pieces so the file stays identifier-clean.
_ACCOUNT_ID = "2109" + "8765" + "4321"


def _b64url(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


# Token-shaped fakes, built at runtime so no full credential-shaped literal is in the source.
_JWT_HEADER = _b64url(json.dumps({"alg": "HS256"}, separators=(",", ":")))
_JWT_PAYLOAD = _b64url(json.dumps({"sub": "1234567890"}, separators=(",", ":")))
_JWT_SIGNATURE = "abc123" + "_-xyz"
_PAT_BODY = "0123456789abcdef" * 2
_FAKE_KV_VALUE = "hunter2" * 2
_BEARER_BODY = "abcdEFGH" + "1234567890" + "abcdEFGH"


def _adversarial() -> dict[str, str]:
    """Sensitive strings; assembled at runtime so this file itself stays identifier-clean."""
    return {
        "host": "dbc-" + "a1b2c3d4-e5f6" + ".cloud." + "databricks.com",
        "azure_host": "adb-" + "1234567890123456.7" + ".azure" + "databricks.net",
        "apps_host": "my-app-" + "1234567890" + ".aws.databricks" + "apps.com",
        "lakebase_host": "ep-"
        + "quiet-river-"
        + "a1b2c3d4"
        + ".database.us-east-1.cloud.databricks.com",
        "url": "https://" + "workspace-9999" + ".cloud.databricks.com/api/2.0/jobs/get?job_id=1",
        "uuid": "7c1e2f94-3b5a-4d68-9e0f-a1b2c3d4e5f6",
        "email": "jane.doe" + "@" + "corp-example.com",
        "arn": "arn:aws:iam::" + _ACCOUNT_ID + ":role/streaming-writer",
        "account": "aws_account_id: " + _ACCOUNT_ID,
        "jwt": ".".join((_JWT_HEADER, _JWT_PAYLOAD, _JWT_SIGNATURE)),
        "pat": "dapi" + _PAT_BODY,
        "kv_pair": "=".join(("password", _FAKE_KV_VALUE)),
        "bearer": "Authorization: Bearer " + _BEARER_BODY,
        "dsn": "postgresql://"
        + "svc_user:"
        + "s3cr3t"
        + "%21@"
        + "db.internal-example.com:5432/databricks_postgres",
        "bucket": "s3://" + "acme-prod-lakehouse-bucket" + "/iot/metrics",
        "user_path": "/Users/" + "jane.doe" + "@corp-example.com/notebooks/health",
        "cluster": "0123-" + "456789-abcd1234",
        "job_id": "job " + "884201937452610" + " failed",
    }


def _secret_fragments() -> dict[str, tuple[str, ...]]:
    return {
        "host": ("a1b2c3d4-e5f6",),
        "azure_host": ("1234567890123456",),
        "apps_host": ("my-app-1234567890",),
        "lakebase_host": ("quiet-river",),
        "url": ("workspace-9999",),
        "uuid": ("7c1e2f94",),
        "email": ("jane.doe", "corp-example"),
        "arn": (_ACCOUNT_ID,),
        "account": (_ACCOUNT_ID,),
        "jwt": (_JWT_HEADER[3:], _JWT_SIGNATURE),
        "pat": (_PAT_BODY,),
        "kv_pair": (_FAKE_KV_VALUE,),
        "bearer": (_BEARER_BODY,),
        "dsn": ("s3cr3t", "svc_user"),
        "bucket": ("acme-prod-lakehouse-bucket",),
        "user_path": ("jane.doe", "corp-example"),
        "cluster": ("456789-abcd1234",),
        "job_id": ("884201937452610",),
    }


@pytest.fixture
def redactor_ns(committed, monkeypatch):
    return _namespace(committed, ("redaction",), monkeypatch)


@pytest.mark.parametrize("key", sorted(_adversarial()))
def test_redaction_masks_every_identifier_kind(redactor_ns, key):
    redact = redactor_ns["Redactor"](enabled=True)
    text = f"prefix {_adversarial()[key]} suffix"
    cleaned = redact(text)
    for fragment in _secret_fragments()[key]:
        assert fragment not in cleaned, (key, cleaned)
    assert redact(cleaned) == cleaned, "redaction must be idempotent"


def test_redaction_leaves_ordinary_report_text_alone(redactor_ns):
    redact = redactor_ns["Redactor"](enabled=True)
    text = "1,547,424 rows in public.sensor_readings, p99 = 1.4 s, 3 forward partitions, lag 3.2s"
    assert redact(text) == text


def test_redaction_handles_nested_tracebacks(redactor_ns):
    redact = redactor_ns["Redactor"](enabled=True)
    adversarial = _adversarial()
    try:
        try:
            raise ConnectionError(
                f"could not connect to {adversarial['lakebase_host']} as {adversarial['email']}"
            )
        except ConnectionError as inner:
            raise RuntimeError(
                f"auth failed {adversarial['dsn']} token {adversarial['jwt']}"
            ) from inner
    except RuntimeError as outer:
        rendered = redact.exception(outer, with_trace=True)
        short = redact.exception(outer)
    for text in (rendered, short):
        for key in ("lakebase_host", "email", "dsn", "jwt"):
            for fragment in _secret_fragments()[key]:
                assert fragment not in text
        assert "RuntimeError" in text


def test_redaction_registered_values_and_user_names(redactor_ns):
    redact = redactor_ns["Redactor"](enabled=True)
    redact.register("jane.doe" + "@corp-example.com", "<user>")
    redact.register("Prod_Catalog_X", "<catalog>")
    cleaned = redact("owner jane.doe ran select * from prod_catalog_x.sch.t")
    assert "jane.doe" not in cleaned and "prod_catalog_x" not in cleaned.lower()
    assert "<catalog>" in cleaned


def test_secrets_are_masked_even_when_redaction_is_off(redactor_ns):
    redact = redactor_ns["Redactor"](enabled=False)
    adversarial = _adversarial()
    cleaned = redact(f"{adversarial['host']} {adversarial['jwt']} {adversarial['kv_pair']}")
    assert _FAKE_KV_VALUE not in cleaned and _JWT_SIGNATURE not in cleaned
    assert "a1b2c3d4-e5f6" in cleaned, "hosts stay visible when the owner turns redaction off"


# ── read-only SQL guard ──────────────────────────────────────────────────────
@pytest.fixture
def guard_ns(committed, monkeypatch):
    return _namespace(committed, ("sql-guard",), monkeypatch)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "SELECT 1; DROP TABLE t",
        "SELECT 1; SELECT 2",
        "DROP TABLE t",
        "CREATE TABLE t (a int)",
        "TRUNCATE t",
        "COPY t FROM STDIN",
        "VACUUM t",
        "OPTIMIZE t",
        "GRANT SELECT ON t TO u",
        "SET ROLE admin",
        "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
        "SELECT pg_terminate_backend(1)",
        "SELECT lakets.create_chronotable('t', 'c', '1 day', 'x')",
        "SELECT lakets.run_retention('x')",
        "SELECT nextval('s')",
        "/* hi */ DROP TABLE t",
        "",
        "   ",
    ],
)
def test_sql_guard_rejects_writes(guard_ns, sql):
    with pytest.raises(guard_ns["UnsafeSqlError"]):
        guard_ns["assert_read_only"](sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select count(*) from public.sensor_readings where event_time >= now()",
        "WITH w AS (SELECT 1) SELECT * FROM w",
        "DESCRIBE HISTORY `c`.`s`.`t` LIMIT 5",
        "DESCRIBE DETAIL t",
        "SHOW TABLES",
        "SELECT 'DROP TABLE t' AS note",  # keywords inside literals are data
        "SELECT 1 -- ; DROP TABLE t",  # keywords inside comments are ignored
        "SELECT last_updated, created_at FROM t",  # column names that merely contain keywords
        "SELECT lakets._cdf_committed_lsn('x'), lakets.time_bucket('1 minute'::interval, now())",
        "SELECT 1;",
    ],
)
def test_sql_guard_allows_reads(guard_ns, sql):
    assert guard_ns["assert_read_only"](sql) == sql


def test_every_registered_statement_is_read_only(committed, monkeypatch):
    ns = _namespace(committed, ("params", "config", "sql-guard", "queries"), monkeypatch)
    for name, (target, sql) in ns["LAKEBASE_SQL"].items():
        assert target in {"app", "data", "ctl"}, name
        ns["assert_read_only"](sql)
    formatter = string.Formatter()
    for template in ns["UC_SQL"].values():
        fields = {f: "x" for _, f, _, _ in formatter.parse(template) if f}
        ns["assert_read_only"](template.format(**fields))


def test_unity_catalog_identifiers_are_validated(committed, monkeypatch):
    """The live-backend helpers (cell 'live-backend') only need the stdlib to be imported."""
    cells = (
        "params",
        "config",
        "redaction",
        "sql-guard",
        "render",
        "model",
        "queries",
        "live-backend",
    )
    ns = _namespace(committed, cells, monkeypatch)
    assert ns["uc_fqn"]("my-catalog", "sch", "tbl") == "`my-catalog`.`sch`.`tbl`"
    for bad in ("a`b", "a.b", "a b", "x'; drop table t; --", ""):
        with pytest.raises(ValueError):
            ns["uc_ident"](bad)
        with pytest.raises(ValueError):
            ns["uc_lit"](bad)


# ── executed notebook (sample mode, offline) ─────────────────────────────────
def test_executed_notebook_renders_every_section(executed):
    html = _all_html(executed)
    for title in SECTION_TITLES:
        assert title in html, f"missing section {title!r}"
    assert "SAMPLE DATA" in html
    assert "Recommended next steps" in html
    assert re.search(r"Overall:.*?(PASS|WARN|FAIL)", _html(executed, "scorecard"), re.S)
    names = _check_names(executed)
    assert len(names) >= 45, sorted(names)
    for expected in ("control_plane_record", "lb_freshness", "lakets_tiering", "uc_freshness",
                     "metrics_e2e_latency", "xc_reconciliation"):  # fmt: skip
        assert expected in names


def test_executed_notebook_has_no_errors_or_stderr(executed):
    for cell in executed.cells:
        for output in cell.get("outputs", []):
            assert output.output_type != "error", (cell.id, output.get("evalue"))
            assert not (output.output_type == "stream" and output.name == "stderr"), cell.id


def test_executed_output_is_redacted_and_identifier_clean(executed, checker):
    blob = _outputs_blob(executed)
    for fragment in SAMPLE_IDENTIFIERS:
        assert fragment not in blob, f"sample identifier leaked into output: {fragment}"
    assert not checker.scan_text(blob), checker.scan_text(blob)
    assert "<host>" in blob and "<setup-id>" in blob


def test_scorecard_is_updated_in_place_after_the_run(executed):
    """The top-of-notebook scorecard holds the final table, not the 'generated at the end' stub."""
    assert "Generated at the end of the run" not in _html(executed, "scorecard")
    assert "control_plane_record" in _html(executed, "scorecard")


def test_redaction_can_be_turned_off_but_secrets_stay_masked():
    run = _load_refresher().execute_sample(overrides={"redact": "false"})
    blob = _outputs_blob(run)
    assert "alex.morgan" in blob, "redact=false must show the (fabricated) sample user"


@pytest.mark.parametrize("mode", ["microbatch", "zerobus"])
def test_other_modes_degrade_gracefully(mode):
    run = _load_refresher().execute_sample(overrides={"mode": mode, "industry": "energy"})
    html = _all_html(run)
    assert "NOT APPLICABLE" in html, "Lakebase/LakeTS checks must be marked not applicable"
    assert "SAMPLE DATA" in html
    for cell in run.cells:
        assert all(o.output_type != "error" for o in cell.get("outputs", []))
    if mode == "zerobus":
        assert "zerobus_ingest" in _check_names(run)


def test_invalid_widget_values_fall_back_with_a_note():
    run = _load_refresher().execute_sample(
        overrides={"industry": "nope", "lookback_hours": "banana", "mode": "x'; drop table t"}
    )
    html = _html(run, "header")
    assert "is not one of" in html and "lookback_hours is not a number" in html


# ── committed outputs ────────────────────────────────────────────────────────
def test_committed_outputs_are_present(committed):
    report_cells = [
        c
        for c in committed.cells
        if c.cell_type == "code" and "report" in c.metadata.get("tags", [])
    ]
    assert len(report_cells) >= 8
    for cell in report_cells:
        assert cell.outputs, (
            f"cell {cell.id} has no saved outputs (run ops/refresh_notebook_outputs.py)"
        )
        assert cell.execution_count, cell.id
    for cell in committed.cells:
        for output in cell.get("outputs", []):
            assert output.output_type != "error", cell.id
    html = _all_html(committed)
    for title in SECTION_TITLES:
        assert title in html
    assert "SAMPLE DATA" in html


def test_committed_outputs_pass_the_identifier_check(committed, checker):
    blob = _outputs_blob(committed)
    assert not checker.scan_text(blob)
    for fragment in SAMPLE_IDENTIFIERS:
        assert fragment not in blob
    assert not checker.scan_file(NOTEBOOK, str(NOTEBOOK)), "the whole .ipynb must pass the check"


def test_committed_outputs_have_no_secret_shaped_values(committed):
    blob = _outputs_blob(committed)
    for pattern in (
        r"eyJ[A-Za-z0-9_-]{8,}\.",
        r"\bdapi[a-z0-9]{16,}",
        r"(?i)password\s*[=:]\s*(?!<redacted>)\S",
    ):
        assert not re.search(pattern, blob), pattern


def test_committed_outputs_match_the_current_notebook_code(committed, executed):
    """Fails when the code changed but ops/refresh_notebook_outputs.py was not re-run."""
    assert _check_names(committed) == _check_names(executed)
    for title in SECTION_TITLES:
        assert (title in _all_html(committed)) == (title in _all_html(executed))
