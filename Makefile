# Real-Time NYC Transit Lakehouse
#
# `make help` lists everything. Targets are grouped: setup, quality, local
# stack, benchmark, infra.

SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV        := .venv
PY          := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
PYTEST      := $(VENV)/bin/pytest
RUFF        := $(VENV)/bin/ruff
BLACK       := $(VENV)/bin/black
DBT         := $(VENV)/bin/dbt
COMPOSE     := docker compose -f docker/docker-compose.yml
DBT_DIR     := dbt/nyc_transit
LINT_PATHS  := src tests scripts airflow

export DBT_PROFILES_DIR := $(CURDIR)/$(DBT_DIR)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
.PHONY: venv
venv: ## Create the virtualenv and install everything (python3.12 required)
	python3.12 -m venv $(VENV) || python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements/dev.txt
	$(PIP) install -e . --no-deps
	@echo "venv ready: source $(VENV)/bin/activate"

.PHONY: deps
deps: ## Install dbt packages (dbt_utils)
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) deps

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
.PHONY: fmt
fmt: ## Format with black and apply ruff autofixes
	$(BLACK) $(LINT_PATHS)
	$(RUFF) check --fix $(LINT_PATHS)

.PHONY: lint
lint: ## Lint (ruff + black --check), exactly as CI runs it
	$(RUFF) check $(LINT_PATHS)
	$(BLACK) --check --diff $(LINT_PATHS)

.PHONY: typecheck
typecheck: ## mypy over the package
	$(VENV)/bin/mypy

.PHONY: test
test: ## Full pytest suite (includes Spark/Iceberg-marked tests)
	$(PYTEST) -v

.PHONY: test-fast
test-fast: ## Unit tests only; skips anything that boots a SparkSession
	$(PYTEST) -v -m "not spark and not iceberg and not dbt"

.PHONY: cov
cov: ## Tests with a coverage report
	$(PYTEST) --cov=nyc_transit --cov-report=term-missing --cov-report=html
	@echo "HTML report: htmlcov/index.html"

.PHONY: seeds
seeds: ## Regenerate the dbt seed CSVs from the fixture (deterministic)
	$(PY) scripts/generate_dbt_seeds.py
	@git diff --stat -- $(DBT_DIR)/seeds || true

.PHONY: dbt-build
dbt-build: ## dbt seed + build (all models, the snapshot and all tests) on DuckDB
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) seed && $(CURDIR)/$(DBT) build

.PHONY: dbt-test
dbt-test: ## dbt test only
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) test

.PHONY: dbt-count-tests
dbt-count-tests: ## Print the exact number of dbt tests (the README figure)
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) parse >/dev/null && $(CURDIR)/$(PY) ../../scripts/count_dbt_tests.py

.PHONY: dbt-docs
dbt-docs: ## Generate and serve the dbt docs site
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) docs generate && $(CURDIR)/$(DBT) docs serve

.PHONY: airflow-check
airflow-check: ## Import every DAG and assert the expected dag_ids (needs the airflow extras)
	$(PY) scripts/check_dags.py

.PHONY: check
check: lint test dbt-build ## Everything CI runs, locally

# ---------------------------------------------------------------------------
# Local stack
# ---------------------------------------------------------------------------
.PHONY: up
up: ## Start Kafka + MinIO + the streaming job
	$(COMPOSE) up -d
	@echo "Kafka: localhost:9092   MinIO console: http://localhost:9001 (minioadmin/minioadmin)"

.PHONY: up-airflow
up-airflow: ## Start the full stack including Airflow (http://localhost:8080, admin/admin)
	$(COMPOSE) --profile airflow up -d

.PHONY: up-tools
up-tools: ## Start Kafka UI as well (http://localhost:8085)
	$(COMPOSE) --profile tools up -d

.PHONY: down
down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

.PHONY: clean-stack
clean-stack: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail the streaming job's logs
	$(COMPOSE) logs -f spark-streaming

.PHONY: compose-config
compose-config: ## Validate docker-compose.yml and every profile
	$(COMPOSE) config -q
	@for p in airflow bench live tools; do $(COMPOSE) --profile $$p config -q && echo "profile $$p ok"; done

.PHONY: feeds
feeds: ## List the GTFS-RT feeds that would be polled
	$(PY) -m nyc_transit.producer.cli --dry-run

.PHONY: show-ddl
show-ddl: ## Print the Iceberg DDL the streaming job applies
	$(PY) -m nyc_transit.streaming.jobs --show-ddl

.PHONY: stream-local
stream-local: ## Run the streaming jobs against the local filesystem warehouse
	LAKEHOUSE_CATALOG_TYPE=hadoop \
	LAKEHOUSE_WAREHOUSE=$(CURDIR)/warehouse \
	LAKEHOUSE_CHECKPOINT_ROOT=$(CURDIR)/checkpoints \
	$(PY) -m nyc_transit.streaming.jobs --create-tables --once --local-threads 2

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
.PHONY: bench
bench: ## Throughput benchmark: 2k events/sec target, no broker needed
	mkdir -p bench-results
	$(PY) -m nyc_transit.producer.loadgen --target null --duration 30 \
		--target-eps 2000 --json-out bench-results/null-2000eps.json

.PHONY: bench-ceiling
bench-ceiling: ## Unthrottled: find this machine's events/sec ceiling
	mkdir -p bench-results
	$(PY) -m nyc_transit.producer.loadgen --target null --duration 30 \
		--target-eps 0 --json-out bench-results/null-ceiling.json

.PHONY: bench-kafka
bench-kafka: ## End-to-end 2k events/sec into the local Kafka broker (needs `make up`)
	mkdir -p bench-results
	$(PY) -m nyc_transit.producer.loadgen --target kafka --duration 60 \
		--target-eps 2000 --json-out bench-results/kafka-2000eps.json

.PHONY: bench-all
bench-all: bench bench-ceiling ## Both no-broker benchmarks

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
.PHONY: tf-fmt
tf-fmt: ## terraform fmt -recursive
	cd infra && terraform fmt -recursive

.PHONY: tf-validate
tf-validate: ## terraform fmt -check + init -backend=false + validate
	cd infra && terraform fmt -check -recursive -diff
	cd infra && terraform init -backend=false -input=false
	cd infra && terraform validate

.PHONY: tf-validate-modules
tf-validate-modules: ## Validate each Terraform module standalone
	@for m in infra/modules/*/; do \
		echo "--- $$m"; \
		terraform -chdir="$$m" init -backend=false -input=false >/dev/null && \
		terraform -chdir="$$m" validate; \
	done

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------
.PHONY: clean
clean: ## Remove build, test and local warehouse artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage coverage.xml
	rm -rf warehouse checkpoints spark-warehouse metastore_db derby.log
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type d -name '*.egg-info' -prune -exec rm -rf {} +
