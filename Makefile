.DEFAULT_GOAL := help
.PHONY: help env run test lint fmt bench ci up down consume

help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

env: ## Create .env with a freshly generated API secret
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
