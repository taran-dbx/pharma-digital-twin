# Live-run snapshots

Dated, **redacted** snapshots of `../pipeline_health_check.ipynb` executed for real
(`sample=false`, `redact=true`, `lookback_hours=24`) against a deployed pipeline. They keep the
cell outputs so the report renders on GitHub, and they are **not** regenerated automatically:
each file describes one pipeline at one point in time and goes stale. The committed sample
notebook (`../pipeline_health_check.ipynb`, synthetic data) is unchanged and remains the
reference for the report layout.

| File | Meaning |
|---|---|
| `<industry>_<mode>_<YYYY-MM-DD>.ipynb` | One run: first cell reads `LIVE RUN, redacted, <date>` |
| `<industry>_<mode>_<YYYY-MM-DD>.html` | Static HTML copy of the same run (`jupyter nbconvert --to html --no-input`), for browsers; commit it only if `scripts/check_identifiers.py` passes on it |

Every output carries `text/html` plus `text/markdown` and `text/plain` versions (tables as pipe
tables, PASS/WARN/FAIL as words, charts as a one-line summary plus `image/svg+xml`), so the
report stays readable in viewers that do not render HTML (untrusted notebooks, sanitising
previews, IDE file viewers). Databricks `displayHTML` exports only HTML; the converter derives
the text versions with `ops/html_fallback.py`.

## How they are produced

1. Import the notebook to a temporary workspace path under your own home folder.
2. Submit a one-time serverless run per deployed industry and mode (the base environment must
   include `databricks-sdk>=0.133.0` and `psycopg2-binary`):

   ```bash
   databricks jobs submit --no-wait -p <profile> --json '{
     "run_name": "health-check",
     "tasks": [{"task_key": "main",
       "notebook_task": {"notebook_path": "<temp path>",
         "base_parameters": {"industry": "manufacturing", "mode": "realtime",
           "lookback_hours": "24", "sample": "false", "redact": "true"}},
       "environment_key": "e"}],
     "environments": [{"environment_key": "e", "spec": {"client": "4",
       "dependencies": ["databricks-sdk>=0.133.0", "psycopg2-binary"]}}]}'
   ```

3. Export the finished task run and convert it, keeping the outputs:

   ```bash
   databricks jobs export-run <task_run_id> --views-to-export ALL -p <profile> -o json > export.json
   python ops/export_run_notebook.py export.json \
     ops/notebooks/runs/<industry>_<mode>_<YYYY-MM-DD>.ipynb --date <YYYY-MM-DD>
   ```

   The date is the day the run actually executed. The converter refuses runs where any cell errored or was skipped (for example when the
   Python kernel crashed): re-run instead of committing a partial snapshot.
4. Scan before committing, and delete the temporary workspace notebook afterwards:

   ```bash
   jupyter nbconvert --to html --no-input ops/notebooks/runs/<name>.ipynb   # optional static copy
   python scripts/check_identifiers.py ops/notebooks/runs/*.ipynb ops/notebooks/runs/*.html
   pytest tests/test_health_run_snapshots.py tests/test_notebook_text_fallbacks.py
   ```

## Rules

- `redact=true` is mandatory. A run with `redact=false` must never be committed.
- Tests (`tests/test_health_run_snapshots.py`) validate nbformat, the label, the kept outputs and
  the identifier / secret-shape scans over every file here. They do not re-execute anything.
- The pipeline (job) name `iot-<industry>-<mode>` is shown as-is: it is the app's fixed naming
  convention, not a workspace identifier.
- Delete old snapshots rather than editing them.
