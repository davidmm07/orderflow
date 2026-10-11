.DEFAULT_GOAL := help
.PHONY: help env run test lint fmt bench ci up down consume topics postman-env flows flows-lint monitoring-check \
	run-pricefeed pricefeed-env test-integration load-test sdk sdk-types prices tf-validate

help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

env: ## Create .env with two traders and fresh API secrets
	./scripts/bootstrap-env.sh

run: ## Run the server locally with the log event sink
	cargo run -p orderflow-server

test: ## Run every test in the workspace
	cargo test --workspace

lint: ## Check formatting and run clippy with warnings as errors
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

fmt: ## Format the code
	cargo fmt --all

bench: ## Benchmark the matching engine
	cargo bench -p orderflow-domain

ci: lint test ## Everything CI runs, locally

up: ## Build and start Orderflow, the price feed, Redpanda, Postgres, Valkey, Prometheus and Grafana
	docker compose up --build -d
	@echo "API http://127.0.0.1:8080  Prices http://127.0.0.1:8090  Prometheus http://127.0.0.1:9090  Grafana http://127.0.0.1:3000"

down: ## Stop the stack and remove its volumes
	docker compose down -v

consume: ## Print events from the topic as they arrive
	docker compose exec redpanda rpk topic consume orderflow.events.v1 --format '%k %v\n'

topics: ## Show the event topic's partitions and how many records each holds
	docker compose exec redpanda rpk topic describe orderflow.events.v1 --print-partitions

postman-env: ## Write the "Orderflow local" Postman environment from .env (git-ignored, holds secrets)
	./scripts/postman-env.sh

POSTMAN_CLI := npx --yes --package postman-cli@1 postman
FLOWS := "postman/collections/Orderflow scenarios"
FLOWS_ENV ?= postman/environments/orderflow-local.environment.yaml

flows: ## Run the scenario flows (needs the exchange and the price feed running); FLOW="06 Stop orders" runs one
	@./scripts/flows-preflight.sh "$(FLOWS_ENV)" "$(FLOW)"
	$(POSTMAN_CLI) collection run $(FLOWS) -e "$(FLOWS_ENV)" $(if $(FLOW),-i "$(FLOW)")

flows-lint: ## Validate the scenario flows against the Postman collection schema
	$(POSTMAN_CLI) collection lint $(FLOWS)

PROMTOOL := docker run --rm -w /etc/prometheus -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" \
	--entrypoint promtool prom/prometheus:v3.15.0

monitoring-check: ## Validate the Prometheus config, unit test the alert rules, check the dashboards
	$(PROMTOOL) check config prometheus.yml
	$(PROMTOOL) test rules alerts.test.yml
	for dashboard in deploy/grafana/dashboards/*.json; do python3 -m json.tool "$$dashboard" > /dev/null; done

run-pricefeed: ## Run the price feed locally: live public sources, history in memory
	cargo run -p orderflow-pricefeed

pricefeed-env: ## Add the price feed database password to an existing .env
	./scripts/bootstrap-env.sh --add-pricefeed

test-integration: ## Run the Postgres and Redis adapter tests against throwaway containers
	./scripts/integration-test.sh

LOAD_URL ?= ws://127.0.0.1:8090/v1/ws
CONNECTIONS ?= 1000
LOAD_SECONDS ?= 30

load-test: ## WebSocket fanout load test; CONNECTIONS=10000 LOAD_SECONDS=60 LOAD_URL=ws://...
	cargo run --release -p orderflow-pricefeed --example ws_load -- \
		--url $(LOAD_URL) --connections $(CONNECTIONS) --seconds $(LOAD_SECONDS)

SDK := clients/typescript

sdk: ## Build and test the TypeScript client and CLI
	cd $(SDK) && npm ci --no-audit --no-fund && npm test

sdk-types: ## Regenerate the SDK's types after changing docs/pricefeed-openapi.yaml
	cd $(SDK) && npm run generate

PRICES_URL ?= http://127.0.0.1:8090

prices: ## Index prices from the TypeScript CLI; MARKETS="BTC-USD ETH-USD" streams them live
	@test -f $(SDK)/dist/cli.js || $(MAKE) --no-print-directory sdk
	@PRICEFEED_URL=$(PRICES_URL) node $(SDK)/dist/cli.js $(if $(MARKETS),watch $(MARKETS),prices)

TERRAFORM ?= terraform

tf-validate: ## Check formatting and validate the AWS Terraform (never applies anything)
	$(TERRAFORM) -chdir=deploy/terraform fmt -check -recursive
	$(TERRAFORM) -chdir=deploy/terraform init -backend=false -input=false > /dev/null
	$(TERRAFORM) -chdir=deploy/terraform validate
