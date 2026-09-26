.DEFAULT_GOAL := help
.PHONY: help env run test lint fmt bench ci up down consume postman-env flows

help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

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

up: ## Build and start Orderflow with Redpanda
	docker compose up --build -d

down: ## Stop the stack and remove its volumes
	docker compose down -v

consume: ## Print events from the topic as they arrive
	docker compose exec redpanda rpk topic consume orderflow.events.v1 --format '%k %v\n'

postman-env: ## Write the Postman environment from .env (git-ignored, holds secrets)
	./scripts/postman-env.sh

flows: ## Run the Postman scenario flows against a running server; FLOW="06 Stop orders" runs one
	@if command -v npx >/dev/null 2>&1; then \
		npx --yes newman@6 run postman/orderflow.postman_collection.json \
			-e postman/local.postman_environment.json $(if $(FLOW),--folder "$(FLOW)"); \
	else \
		docker run --rm --network host -v "$(CURDIR)/postman:/etc/newman" postman/newman:6-alpine \
			run orderflow.postman_collection.json -e local.postman_environment.json $(if $(FLOW),--folder "$(FLOW)"); \
	fi
