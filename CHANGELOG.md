# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Spindle 3 risk model: an early warning on a SIMULATED, noisy precursor, measured
  offline on fresh simulated data, not live.
  - **Champion v13**, chosen by a pre-registered, neutral selection run. On 231
    pooled fresh cycles it warned 68.5 % [64.2, 72.2] of breaches ≥ 30 s ahead at
    1.35 [0.99, 1.71] false alarms/h. Its plan-mix-standardised discrimination is
    0.375 [0.291, 0.457] (block bootstrap [0.291, 0.460]); a plan-aware oracle
    reference scores 0.30, a clock about 0. Cycle permutation gives p ≤ 0.001, the
    floor of 1,000 permutations, and it is invariant to the maintenance age. v7
    fails that invariance check and is tagged `known_defect`.
  - **An earlier 4 h endpoint replay (v7)** gave v7 19 / 26 breaches warned at 5.0 false
    alarms/h. Its raw discrimination was 0.16 [0.04, 0.29], or 0.24 [0.14, 0.34]
    standardised to the plan mix. The gap is diagnosed: a spurious maintenance-age
    dependence at the clipped catalog value, plus an alarm-heavy sample.
  - **One generator.** `scenario_engine/spindle_scenario.py` feeds the live producer
    (embedded in the notebook) and training. The ramp start is jittered, precursors
    are strong, weak or none, and the scenario adds false alarms and quiet cycles.
    Vibration is capped under the vibration alert.
  - **Training.** The producer's simulator path at about 25 Hz, breach-episode
    labels, and the maintenance age pinned.
  - **Promotion gate.** It pools test and replay cycles with bootstrap CIs and
    needs: the clear level above calm running, ≤ 3 false alarms/h, a beaten clock,
    permutation p < 0.05, and a lower 95 % bound of standardised discrimination
    above 0. A challenger also needs a significant paired gain over the champion.
    See docs/ml-risk-model.md.
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

### Fixed
- `GET /api/ops/summary`: time to acknowledge, time to disposition and MTTR counted
  excursions that were never acknowledged, never contained or still open as 0 s
  (Postgres `GREATEST(NULL, 0)` is 0), which dragged p50 / p90 / mean toward zero. The
  statistics now cover only excursions whose end event exists, in the SQL totals and
  the Python episode path alike. Each one reports its coverage
  (`acknowledged_count` / `unacknowledged_count`, `dispositioned_count` /
  `undispositioned_count`, `cleared_count` / `open_count`, all of `episodes_total`) and
  the age of the oldest open excursion still waiting
  (`oldest_open_unacknowledged_age_s`, `oldest_open_undispositioned_age_s`). The
  operations view shows the coverage next to TTA, time to disposition and MTTR, and an
  empty state instead of "0 s" when nothing was acknowledged or contained. See
  `docs/value-model.md`.
- Ops summary follow-ups:
  - The waiting ages are capped at `max_open_excursion_s` (24 h, `open_age_cap_s`), with
    `open_unacknowledged_past_cap_count` / `open_undispositioned_past_cap_count` for
    open excursions past the cap.
  - Each duration statistic has a `clock_skew_count` (end event stamped before the
    start, clamped to 0 s).
  - The acknowledge chart's empty state says "None of the latest *n* excursions
    acknowledged" when only older ones were acknowledged. The tiles show the waiting
    note with partial coverage too.
  - `usecases/medtech/medtech_kpis.ipynb` notes where its TTA differs from the app.

### Removed
- Legacy static pages and `/legacy` routes, unused modules, the duplicate simulator wheel,
  `.databricksignore` (replaced by bundle `sync` rules) and committed agent/IDE files.
