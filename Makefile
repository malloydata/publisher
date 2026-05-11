# Makefile for malloydata/publisher.
# Assumes `mise` (or `asdf`) has activated the versions pinned in .tool-versions.
# Run `make` or `make help` for the target list.

URL := http://localhost:4000

.DEFAULT_GOAL := help
.PHONY: help doctor \
        install reinstall clean \
        build start start-init stop \
        dev dev-init dev-server dev-react \
        status environments packages \
        open \
        test test-unit test-integration \
        lint format prettier-check typecheck \
        regen-api

# ── general ───────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z][a-zA-Z0-9_-]*:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

doctor: ## Print resolved tool versions
	@command -v mise >/dev/null && mise current || true
	@which bun node java python 2>/dev/null || true

# ── setup ─────────────────────────────────────────────────────────────
install: ## Install workspace deps (bun install)
	bun install

reinstall: ## Wipe node_modules + dist and reinstall
	bun run clean && bun install

clean: ## Remove node_modules and dist directories
	bun run clean

# ── build & run ───────────────────────────────────────────────────────
build: ## Production build: SDK → app → server bundle
	bun run build:server-deploy

start: ## Run the built server (production, foreground; Ctrl+C to stop)
	bun run start

start-init: ## Run the built server with INITIALIZE_STORAGE=true (clears persisted state)
	bun run start:init

stop: ## Kill any process listening on port 4000 or 4040
	@kill $$(lsof -ti:4000) 2>/dev/null || true
	@kill $$(lsof -ti:4040) 2>/dev/null || true
	@echo "stopped (if anything was running)"

# ── dev mode ──────────────────────────────────────────────────────────
# `dev` runs Express + Vite together with prefixed [server]/[react]
# logs in one terminal. Ctrl+C stops both. Use the -server / -react
# variants for separate terminals.
dev: ## Run Express (:4000) + Vite (:5173) together, combined logs
	bunx concurrently \
		--names "server,react" \
		--prefix-colors "cyan,magenta" \
		--kill-others-on-fail \
		"bun run start:dev" \
		"bun run start:dev:react"

dev-init: ## Same as `dev` but with INITIALIZE_STORAGE=true on the server
	bunx concurrently \
		--names "server,react" \
		--prefix-colors "cyan,magenta" \
		--kill-others-on-fail \
		"bun run start:dev:init" \
		"bun run start:dev:react"

dev-server: ## Express server alone (NODE_ENV=development, watch mode)
	bun run start:dev

dev-react: ## Vite dev server alone, at :5173
	bun run start:dev:react

# ── inspect ───────────────────────────────────────────────────────────
status: ## GET /api/v0/status
	@curl -sf $(URL)/api/v0/status | python3 -m json.tool

environments: ## List environments
	@curl -sf $(URL)/api/v0/environments | python3 -m json.tool

packages: ## List packages in the default `malloy-samples` env (override: ENV=foo make packages)
	@curl -sf $(URL)/api/v0/environments/$(or $(ENV),malloy-samples)/packages | python3 -m json.tool

open: ## Open the Publisher UI in the default browser (macOS)
	@open $(URL)

# ── tests / quality ───────────────────────────────────────────────────
test: ## Run all server tests (unit + integration)
	bun run test

test-unit: ## Run only unit tests
	cd packages/server && bun run test:unit

test-integration: ## Run only integration tests (max-workers=1)
	cd packages/server && bun run test:integration

lint: ## eslint across sdk/app/server
	bun run lint

format: ## prettier --write across sdk/app/server
	bun run format

prettier-check: ## CI's exact prettier check (fails on unformatted files)
	bun run prettier:check

typecheck: ## tsc --noEmit across sdk/app/server (chains codegen + SDK build)
	bun run typecheck

# ── codegen ───────────────────────────────────────────────────────────
regen-api: ## Regenerate server + SDK clients from api-doc.yaml (needs Java)
	bun run generate-api-types
