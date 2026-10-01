SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

CARGO ?= cargo
COMPOSE ?= docker compose
PROFILES ?= --profile audit
SERVICE ?=

.PHONY: help build release test docker up down restart reset status logs db-drop db-shell clean

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / { printf "  \033[1m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1;33m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Stack Lifecycle
up: ## Start stack (postgres + grafana + audit)
	$(COMPOSE) $(PROFILES) up -d $(SERVICE)

down: ## Stop containers (keeps database volumes)
	$(COMPOSE) $(PROFILES) down --remove-orphans

restart: ## Restart containers (e.g. SERVICE=audit or all)
	$(COMPOSE) $(PROFILES) restart $(SERVICE)

reset: ## FULL RESET: wipe DB volume, rebuild docker image and start clean
	$(COMPOSE) $(PROFILES) down -v --remove-orphans
	$(MAKE) docker
	$(COMPOSE) $(PROFILES) up -d

status: ## Show status of running containers
	$(COMPOSE) $(PROFILES) ps

##@ Development & Build
build: ## Debug build of collector (cargo build)
	$(CARGO) build

release: ## Release build of collector (cargo build --release)
	$(CARGO) build --release

test: ## Run Rust test suite
	$(CARGO) test

docker: release ## Build release binary and package into Docker image
	$(COMPOSE) $(PROFILES) build audit

clean: ## Cargo clean
	$(CARGO) clean

##@ Database & Diagnostics
logs: ## Tail logs (e.g. SERVICE=audit or default all)
	$(COMPOSE) $(PROFILES) logs -f --tail=120 $(SERVICE)

db-drop: ## Drop provider_stats table to re-init schema without wiping full DB
	$(COMPOSE) exec postgres psql -U postgres -d shred_audit -c "DROP TABLE IF EXISTS provider_stats CASCADE;"

db-shell: ## Open interactive psql shell inside postgres container
	$(COMPOSE) exec postgres psql -U postgres -d shred_audit
