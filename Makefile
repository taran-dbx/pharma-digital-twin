# Developer entry points. Override the interpreter with PYTHON=..., e.g.
#   make check PYTHON=.venv/bin/python
PYTHON ?= python3
DATABRICKS ?= databricks
BUNDLE_TARGET ?= dev
NPM ?= npm
CLIENT_DIR := client

.DEFAULT_GOAL := help
.PHONY: bundle-validate help install lint format typecheck test frontend-install frontend-lint frontend-build check

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

install: frontend-install ## Install Python dev deps, client deps and git hooks
	$(PYTHON) -m pip install -r requirements.txt -e ".[dev]"
	$(PYTHON) -m pre_commit install

frontend-install: ## Install client dependencies from the lockfile
	cd $(CLIENT_DIR) && PUPPETEER_SKIP_DOWNLOAD=1 $(NPM) ci --no-audit --no-fund

lint: ## Ruff lint, ruff format check and client eslint
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	cd $(CLIENT_DIR) && $(NPM) run lint

format: ## Apply ruff fixes and formatting
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

typecheck: ## Mypy (src/iot_twin/core, models) and client tsc
	$(PYTHON) -m mypy
	cd $(CLIENT_DIR) && $(NPM) run typecheck

test: ## Python test suite
	$(PYTHON) -m pytest -q

frontend-build: ## Rebuild the committed bundle in static/dist
	cd $(CLIENT_DIR) && $(NPM) run build

check: lint typecheck test frontend-build ## All gates, as CI runs them
	@git diff --quiet -- static/dist && test -z "$$(git status --porcelain -- static/dist)" \
		|| (echo "static/dist is stale; commit the rebuilt bundle" && exit 1)

bundle-validate: ## Validate the DAB (needs auth + overrides: make bundle-validate PROFILE=<cli-profile>)
	@test -f .databricks/bundle/$(BUNDLE_TARGET)/variable-overrides.json || { \
		echo "missing .databricks/bundle/$(BUNDLE_TARGET)/variable-overrides.json"; \
		echo "copy deploy/variable-overrides.json.example there and fill in your workspace values"; exit 1; }
	$(DATABRICKS) bundle validate --strict -t $(BUNDLE_TARGET) $(if $(PROFILE),--profile $(PROFILE))
