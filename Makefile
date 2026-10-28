# Relay Makefile — prefers Git Bash on Windows so recipes stay POSIX.
ifeq ($(OS),Windows_NT)
  SHELL := C:/Progra~1/Git/bin/bash.exe
  .SHELLFLAGS := -c
  EXE := .exe
else
  EXE :=
endif


GO ?= go
GOLANGCI_LINT ?= golangci-lint

# Docker build-arg for module downloads. Default goproxy.io for hosts where
# proxy.golang.org returns 403. CI / raw `docker compose` without this var keep
# the Dockerfile default (proxy.golang.org). Override:
#   make up COMPOSE_GOPROXY=https://proxy.golang.org,direct
COMPOSE_GOPROXY ?= https://goproxy.io,direct

.DEFAULT_GOAL := help

.PHONY: help tools generate lint test test-integration test-e2e cover build up down up-redis deps demo check-generated

help: ## list targets
	@echo "Relay Make targets:"
	@echo "  make help              - list targets (default)"
	@echo "  make tools             - download modules; verify golangci-lint"
	@echo "  make generate          - run sqlc + oapi-codegen (no-op until sources exist)"
	@echo "  make lint              - run golangci-lint"
	@echo "  make test              - unit tests with -race"
	@echo "  make test-integration  - integration tests (-tags=integration)"
	@echo "  make test-e2e          - end-to-end tests (-tags=e2e)"
	@echo "  make cover             - unit coverage report -> coverage.html"
	@echo "  make build             - build binaries into bin/"
	@echo "  make up / make down    - full compose stack up/down"
	@echo "  make up-redis          - compose with RELAY_STORAGE=redis"
	@echo "  make deps              - start postgres + redis only"
	@echo "  make demo              - up then run producer once"
	@echo "  make check-generated   - regenerate and fail on dirty git tree"

tools: ## go mod download + verify golangci-lint
	$(GO) mod download
	@if ! command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		echo "golangci-lint not found."; \
		echo "Install v2: https://golangci-lint.run/welcome/install/"; \
		echo "  winget install GolangCI.golangci-lint"; \
		echo "  scoop install golangci-lint"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; \
	fi
	$(GOLANGCI_LINT) version

generate: ## sqlc + oapi-codegen (skips when sources are missing)
	@if ls internal/adapter/storage/postgres/queries/*.sql >/dev/null 2>&1; then \
		$(GO) tool sqlc generate; \
	else \
		echo "generate: skipping sqlc (no query files yet)"; \
	fi
	@if [ -f api/openapi.yaml ]; then \
		$(GO) tool oapi-codegen -config oapi-codegen.yaml api/openapi.yaml; \
	else \
		echo "generate: skipping oapi-codegen (api/openapi.yaml missing)"; \
	fi

lint: ## golangci-lint run ./...
	$(GOLANGCI_LINT) run ./...

test: ## unit tests
	$(GO) test -race -count=1 ./...

test-integration: ## integration tests
	$(GO) test -race -count=1 -tags=integration ./...

test-e2e: ## e2e tests
	$(GO) test -race -count=1 -tags=e2e ./test/e2e/...

cover: ## unit coverage HTML
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

build: ## build server and examples
	$(GO) build -o bin/relay-server$(EXE) ./cmd/relay-server
	$(GO) build -o bin/example-worker$(EXE) ./examples/worker
	$(GO) build -o bin/example-producer$(EXE) ./examples/producer

up: ## docker compose up
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) docker compose up -d --build --scale example-worker=2

down: ## docker compose down
	docker compose down -v

up-redis: ## compose with redis storage
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) RELAY_STORAGE=redis docker compose up -d --build --scale example-worker=2

deps: ## start postgres + redis only
	docker compose up -d postgres redis

demo: up ## run demo producer
	# --no-deps: avoid compose run resetting example-worker scale to 1
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) docker compose --profile demo run --rm --no-deps example-producer

check-generated: generate ## fail if generated files drift
	@git diff --exit-code -- internal/adapter/storage/postgres/sqlcgen internal/adapter/http/oapi || ( \
		echo "generated files are dirty; run make generate and commit"; \
		exit 1; \
	)
