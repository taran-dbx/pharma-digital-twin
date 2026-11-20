# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Grounded AI action plan for an alert episode (`POST /api/alerts/{alert_id}/action-plan`).
  It calls a Foundation Model endpoint (`GENAI_ENDPOINT`, default
  `databricks-claude-sonnet-5-5`, as a `CAN_QUERY` app resource). Every claim cites its
  rows, and a schema and grounding check validates citations, numbers and serials.
  Every attempt is audited in Lakebase `genai_action_plan_audit` and mirrored to UC
  `intelligence_genai_audit` for Genie. The incident panel has an "AI action plan" card
  whose apply action pre-fills the signed response form. See
  `docs/genai-action-plan.md`.
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
