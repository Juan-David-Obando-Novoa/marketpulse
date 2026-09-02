# ---------------------------------------------------------------------------
# MarketPulse developer entry points.
#
# The rule here is that anything an engineer does more than twice gets a
# target, and every target is safe to run twice. `make help` is the index.
# ---------------------------------------------------------------------------
.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE       := docker compose
CORE          := --profile core
PROCESSING    := --profile core --profile processing
EVERYTHING    := --profile core --profile processing --profile ingestion --profile orchestration --profile serving --profile observability
DBT_DIR       := dbt/marketpulse
PYTHON        := python3

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
.PHONY: init
init: .env ## Create a virtualenv, install the package and the git hooks
	$(PYTHON) -m venv .venv
	./.venv/bin/pip install --upgrade pip
	./.venv/bin/pip install -e ".[ingestion,serving,orchestration,dev]"
	./.venv/bin/pre-commit install --install-hooks
	./.venv/bin/pre-commit install --hook-type commit-msg
	@echo "activate with: source .venv/bin/activate"

.env: ## Seed a local .env from the committed example
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

# ---------------------------------------------------------------------------
# Platform
# ---------------------------------------------------------------------------
.PHONY: up
up: .env ## Start the core tier (broker, storage, catalog)
	$(COMPOSE) $(CORE) up -d
	@$(MAKE) --no-print-directory wait-healthy

.PHONY: up-all
up-all: .env ## Start every profile
	$(COMPOSE) $(EVERYTHING) up -d --build
	@$(MAKE) --no-print-directory wait-healthy

.PHONY: down
down: ## Stop all services, keep the data volumes
	$(COMPOSE) $(EVERYTHING) down --remove-orphans

.PHONY: nuke
nuke: ## Stop everything and delete the volumes. Destroys the local lake.
	@read -p "This deletes all local data. Type 'yes' to continue: " ok && [ "$$ok" = yes ]
	$(COMPOSE) $(EVERYTHING) down -v --remove-orphans

.PHONY: wait-healthy
wait-healthy: ## Block until every started container reports healthy
	@echo "waiting for services to become healthy..."
	@for i in $$(seq 1 60); do \
		unhealthy=$$($(COMPOSE) ps --format '{{.Name}} {{.Health}}' 2>/dev/null | awk '$$2 != "healthy" && $$2 != "" {print $$1}'); \
		if [ -z "$$unhealthy" ]; then echo "all healthy"; exit 0; fi; \
		sleep 3; \
	done; \
	echo "still unhealthy: $$unhealthy"; $(COMPOSE) ps; exit 1

.PHONY: logs
logs: ## Tail logs for all running services
	$(COMPOSE) $(EVERYTHING) logs -f --tail=100

.PHONY: ps
ps: ## Show service status
	$(COMPOSE) $(EVERYTHING) ps

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
.PHONY: bootstrap
bootstrap: ## Provision topics, register schemas and create the Iceberg tables
	$(COMPOSE) run --rm producer marketpulse topics create
	$(COMPOSE) run --rm producer marketpulse schemas register
	$(COMPOSE) exec -T spark spark-sql -f /opt/marketpulse/src/marketpulse/streaming/ddl/bronze.sql

.PHONY: stream
stream: ## Run the live websocket producer in the foreground
	$(COMPOSE) --profile ingestion run --rm --service-ports producer marketpulse stream

.PHONY: bronze
bronze: ## Start the Kafka to Iceberg streaming jobs
	$(COMPOSE) exec -d spark spark-submit /opt/marketpulse/src/marketpulse/streaming/bronze_trades.py
	$(COMPOSE) exec -d spark spark-submit /opt/marketpulse/src/marketpulse/streaming/bronze_book_ticker.py

.PHONY: backfill
backfill: ## Backfill candles. SYMBOL=BTCUSDT START=2026-01-01 [END=...]
	$(COMPOSE) run --rm producer marketpulse backfill \
		--symbol $(SYMBOL) --start $(START) $(if $(END),--end $(END),)

.PHONY: dbt-deps
dbt-deps: ## Install dbt package dependencies
	cd $(DBT_DIR) && dbt deps

.PHONY: dbt-build
dbt-build: ## Run and test every dbt model
	cd $(DBT_DIR) && dbt build

.PHONY: dbt-docs
dbt-docs: ## Generate and serve the dbt documentation site
	cd $(DBT_DIR) && dbt docs generate && dbt docs serve

.PHONY: dagster
dagster: ## Run Dagster locally against the compose stack
	DAGSTER_HOME=$$PWD/.dagster_home dagster dev -w orchestration/workspace.yaml

# ---------------------------------------------------------------------------
# Quality gates -- the same commands CI runs
# ---------------------------------------------------------------------------
.PHONY: fmt
fmt: ## Format the codebase
	ruff format src tests orchestration
	ruff check --fix src tests orchestration

.PHONY: lint
lint: ## Lint without modifying anything
	ruff check src tests orchestration
	ruff format --check src tests orchestration

.PHONY: typecheck
typecheck: ## Static type analysis
	mypy

.PHONY: test
test: ## Run the hermetic unit tests
	pytest -m unit

.PHONY: test-integration
test-integration: ## Run the tests that need the compose stack
	pytest -m integration

.PHONY: coverage
coverage: ## Unit tests with a coverage report
	pytest -m unit --cov --cov-report=term-missing --cov-report=xml

.PHONY: sqlfmt
sqlfmt: ## Lint the dbt SQL
	sqlfluff lint $(DBT_DIR)/models

.PHONY: check
check: lint typecheck test ## Everything CI runs, in one command

# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------
.PHONY: maintenance
maintenance: ## Compact files and expire snapshots on every Iceberg table
	$(COMPOSE) exec -T spark python3 -m marketpulse.maintenance.iceberg_maintenance --all

.PHONY: dlq
dlq: ## Tail the dead-letter topic
	$(COMPOSE) exec -T redpanda rpk topic consume md.dead_letter.v1 --num 20

.PHONY: lag
lag: ## Show topic and consumer-group state
	$(COMPOSE) exec -T redpanda rpk group list
	$(COMPOSE) exec -T redpanda rpk topic list

.PHONY: sql
sql: ## Open a Trino shell against the lakehouse catalog
	$(COMPOSE) exec -it trino trino --catalog lakehouse --schema gold

.PHONY: urls
urls: ## Print the local service URLs
	@echo "Redpanda Console  http://localhost:8080"
	@echo "MinIO Console     http://localhost:9001  (minioadmin / minioadmin)"
	@echo "Iceberg REST      http://localhost:8181/v1/config"
	@echo "Trino             http://localhost:8090"
	@echo "Dagster           http://localhost:3000"
	@echo "Grafana           http://localhost:3001  (admin / admin)"
	@echo "Prometheus        http://localhost:9090"
	@echo "Serving API       http://localhost:8000/docs"
