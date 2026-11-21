# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Spindle 3 risk model: a measured early warning on a SIMULATED, noisy precursor.
  `scenario_engine/spindle_scenario.py` is now the single generator for the live
  producer (embedded in the notebook) and for training. Excursions have a random
  30–180 s vibration / power precursor (strong, weak or none), and the scenario adds
  false alarms and quiet cycles. Training uses `--data-source live_format`: the
  producer's own simulator path at the live ~25 Hz rate. Labels follow breach
  episodes. The cut-off and clear level are chosen on validation for the most
  breaches warned ≥ 30 s ahead within a false-alarm budget. New versions register
  as `challenger` and become `champion` only when they beat it on the same held-out
  rows. v6 is champion: on a 4 h held-out replay through the dev endpoint it warned
  34 / 38 breaches ≥ 30 s ahead (median 91 s) at 2.3 false alarms/h; v4 warned
  30 / 38 (median 49 s). This is a replay, not a live measurement; see
  docs/ml-risk-model.md.

### Added
- Grounded AI action plan for an alert episode (`POST /api/alerts/{alert_id}/action-plan`).
  It calls a Foundation Model endpoint (`GENAI_ENDPOINT`, default
  `databricks-claude-sonnet-5-5`, as a `CAN_QUERY` app resource). Every claim cites its
  rows, and a schema and grounding check validates citations, numbers and serials.
  Every attempt is audited in Lakebase `genai_action_plan_audit` and mirrored to UC
  `intelligence_genai_audit` for Genie. The incident panel has an "AI action plan" card
  whose apply action pre-fills the signed response form. See
  `docs/genai-action-plan.md`.
- Protections on the AI action plan:
  - Admission control: a concurrency cap, one generation per alert and a
    per-user cooldown. Requests over the limit get a 429.
  - When the grounding is unchanged, the latest plan is reused (`force: true`
    regenerates).
  - Endpoint calls are bounded: a dedicated SDK client and thread pool inside
    a request budget.
  - On the server, a plan can be applied only if it passed the grounding
    check; otherwise an override reason is required and recorded.
  - The signed decision and the plan link commit in one transaction.
  - The audit table is append-only, enforced by a trigger.
  - People appear as pseudonyms in the grounding and the UC mirror.
  - Untrusted text is NFKC-normalised, then defanged.
  - Numbers are checked against each claim's own citations.
- Second round of AI action-plan hardening:
  - Plan-linked signing works on un-upgraded `alert_response` tables. Each
    insert attempt runs in a savepoint inside the signing transaction.
  - New secret resource `genai-pseudonym-salt` (scope `pharma-twin-secrets`) for
    the pseudonym key. Without it, the app persists a random key in Lakebase
    `genai_settings`. There is never a constant key.
  - Pseudonyms are now 16 hex characters.
  - New `GET /api/genai/status` reports the key source, whether the audit is
    append-only, and the call limits.
  - Prompt template is now `v4`. The plan-reuse key includes the
    grounding-checker version.
  - Checker: durations between cited timestamps, numbers from structured text
    and model feature names, and hyphenated units are now handled. Step and
    version references are checked.
  - New `scripts/genai_recheck_evidence.py` re-checks every saved plan.
- Grounding checker v4: risk-model feature names (untrusted metadata) ground
  only a strict trailing window token, in its own unit, and never feed
  differences or percentages. Percentages must match a cited 0–1 value or
  `pct` field. Durations come only from named alert-row pairs and the cited
  readings span. Version references must come from a cited row.
  `GET /api/genai/status` is admin-only.

- Declarative Automation Bundle (`databricks.yml`, `resources/app.yml`) with `dev` and
  `prod` targets and variable-driven configuration.
- `client/` frontend source is tracked, so `static/dist` is reproducible.
- Engineering standards: `pyproject.toml`, ruff, mypy, ESLint, pre-commit, gitleaks,
  GitHub Actions CI, Dependabot, Makefile and `.editorconfig`.
- Identifier check (`scripts/check_identifiers.py`) that blocks workspace-specific values.
- Documentation: README, architecture, deployment and contributing guides.
- `ops/notebooks/pipeline_health_check.ipynb`: a read-only, inline-only Databricks notebook that
  scores one deployed pipeline end to end (jobs, Lakebase, LakeTS, CDF/Delta, metrics,
  cross-checks), with identifier redaction and a synthetic `sample` mode; see `ops/README.md`.
- Fail-safe usage tracking with `dbdemos-tracker` (vendored wheel in `lib/`) for app
  start, page loads, dashboard streams and pipeline setup/validate/deploy. Opt out with
  `USAGE_TRACKING_ENABLED=false`.

### Changed
- **Secret required before redeploying (upgrade step):** the bundle now always
  declares the `genai-pseudonym-salt` secret resource. Create
  `pharma-twin-secrets/genai-pseudonym-salt` before redeploying, or the deploy
  fails:

  ```bash
  databricks secrets create-scope pharma-twin-secrets
  databricks secrets put-secret pharma-twin-secrets genai-pseudonym-salt \
    --string-value "$(openssl rand -hex 32)"
  ```

  The fallback to a random key persisted in Lakebase applies only to local or
  non-bundle runs. See `docs/deployment.md`, *Fresh app prerequisites*.
- **Genie allowlist (upgrade step):** `intelligence_genai_audit` is now an
  expected table in every Genie space. Existing spaces without it stay
  authorized, report it in `missing_optional_tables` and log a warning.
  Re-provision them (`POST /api/admin/pipelines/{setup_id}/genie/{mode}`, or
  redeploy the pipeline) to enable the AI-audit questions.
- `intelligence_catalog_service._sql_str` now backslash-escapes backslashes and
  quotes, because Databricks SQL treats `\` as the escape character. This
  fixes UC writes of values that contain `\'`.
- Python code moved to the `src/iot_twin/` package; `services/` grouped into subpackages.
- App start command is `uvicorn --app-dir src iot_twin.app:app`.
- Workspace profiles are templates (`*.profile.example`); real profiles are gitignored.
- `LAKETS_SQL_PATH` is optional and falls back to the packaged `lakets.sql`.
- Dev scripts require an explicit CLI profile.
- Repository layout tidied with no behaviour change: `design-ref/` removed (kept locally, outside the repo),
  `config/workspaces/` and `variable-overrides.json.example` -> `deploy/`, `qa/playwright/` ->
  `tests/e2e/playwright/`, packaged `src/iot_twin/resources/` -> `src/iot_twin/assets/`, and
  `requirements-dev.txt` folded into the `dev` extra in `pyproject.toml`
  (`pip install -r requirements.txt -e ".[dev]"`).
- `docs/` now holds only current documentation: archived plans, the legacy deployment playbook
  (its valid parts are a Troubleshooting section in `deployment.md`) and the stale diagrams were
  removed; added `docs/api.md`, `docs/README.md`, an inline architecture diagram and sections for
  alerts, the Genie copilot, the intelligence catalog and production control.

### Removed
- Legacy static pages and `/legacy` routes, unused modules, the duplicate simulator wheel,
  `.databricksignore` (replaced by bundle `sync` rules) and committed agent/IDE files.
