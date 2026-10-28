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

# Injected into relayctl / relay-server via -ldflags (Phase 4 GoReleaser will override).
VERSION ?= dev
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)
LDFLAGS ?= -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

# Docker build-arg for module downloads. Default goproxy.io for hosts where
# proxy.golang.org returns 403. CI / raw `docker compose` without this var keep
# the Dockerfile default (proxy.golang.org). Override:
#   make up COMPOSE_GOPROXY=https://goproxy.io,direct
COMPOSE_GOPROXY ?= https://goproxy.io,direct

.DEFAULT_GOAL := help

# Tailwind CSS standalone CLI (no Node). Pin version + per-platform SHA-256.
TAILWIND_VERSION ?= v4.1.18
TAILWIND_DIR := $(CURDIR)/.tools/tailwind/$(TAILWIND_VERSION)
ifeq ($(OS),Windows_NT)
  TAILWIND_ASSET := tailwindcss-windows-x64.exe
  TAILWIND_BIN := $(TAILWIND_DIR)/tailwindcss.exe
  TAILWIND_SHA256 := 55bc5a2e294520a74fe3523eaa11915ef50047e7228a545aa181ec413cf52612
else
  UNAME_S := $(shell uname -s)
  UNAME_M := $(shell uname -m)
  ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
      TAILWIND_ASSET := tailwindcss-macos-arm64
      TAILWIND_SHA256 := 7f27711dceac1a580b6ad58ddac46e59202c85a6c16f2f08f6fdcdee261008e1
    else
      TAILWIND_ASSET := tailwindcss-macos-x64
      TAILWIND_SHA256 := 1e8a77fd796a3a4aa3d8727887de926ef9d38477aba113fd7c32c0d31a32a3ab
    endif
  else
    ifeq ($(UNAME_M),aarch64)
      TAILWIND_ASSET := tailwindcss-linux-arm64
      TAILWIND_SHA256 := 7a7702db6c93718a9b6655d455304edda18600f5a4f195242342ed3b5b70ebe8
    else
      TAILWIND_ASSET := tailwindcss-linux-x64
      TAILWIND_SHA256 := 737becf8d4ad1115ea98df69fa94026d402ca8feb91306a035b5b004167da8dd
    endif
  endif
  TAILWIND_BIN := $(TAILWIND_DIR)/tailwindcss
endif
TAILWIND_URL := https://github.com/tailwindlabs/tailwindcss/releases/download/$(TAILWIND_VERSION)/$(TAILWIND_ASSET)

.PHONY: help tools generate css lint test test-integration test-e2e cover build release-snapshot up down up-redis up-obs open-obs check-alerts deps demo check-generated

help: ## list targets
	@echo "Relay Make targets:"
	@echo "  make help              - list targets (default)"
	@echo "  make tools             - download modules; verify golangci-lint"
	@echo "  make generate          - run sqlc + oapi-codegen + buf + templ"
	@echo "  make css               - build Tailwind CSS (standalone CLI, checksum-verified)"
	@echo "  make lint              - run golangci-lint"
	@echo "  make test              - unit tests with -race"
	@echo "  make test-integration  - integration tests (-tags=integration)"
	@echo "  make test-e2e          - end-to-end tests (-tags=e2e)"
	@echo "  make cover             - unit coverage report -> coverage.html"
	@echo "  make build             - build binaries into bin/ (add bin to PATH)"
	@echo "  make release-snapshot  - goreleaser snapshot (archives + local images; no publish)"
	@echo "  make up / make down    - full compose stack up/down"
	@echo "  make up-redis          - compose with RELAY_STORAGE=redis"
	@echo "  make up-obs            - stack + Prometheus/Grafana/Jaeger (profile observability)"
	@echo "  make open-obs          - print Grafana/Prometheus/Jaeger URLs"
	@echo "  make check-alerts      - promtool check rules (via Docker)"
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

generate: ## sqlc + oapi-codegen + buf + templ (skips when sources are missing)
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
	@if [ -f buf.yaml ] && [ -d proto/relay/v1 ]; then \
		$(GO) tool buf generate; \
	else \
		echo "generate: skipping buf (proto sources missing)"; \
	fi
	@if ls internal/adapter/web/templates/*.templ >/dev/null 2>&1; then \
		$(GO) tool templ generate ./internal/adapter/web/templates/; \
	else \
		echo "generate: skipping templ (no templates yet)"; \
	fi

$(TAILWIND_BIN):
	@mkdir -p "$(TAILWIND_DIR)"
	@echo "downloading Tailwind $(TAILWIND_VERSION) ($(TAILWIND_ASSET))..."
	@curl -fsSL "$(TAILWIND_URL)" -o "$(TAILWIND_BIN).tmp"
	@echo "$(TAILWIND_SHA256)  $(TAILWIND_BIN).tmp" | sha256sum -c -
	@mv "$(TAILWIND_BIN).tmp" "$(TAILWIND_BIN)"
	@chmod +x "$(TAILWIND_BIN)" || true

css: $(TAILWIND_BIN) ## compile Tailwind CSS into embedded static/app.css
	"$(TAILWIND_BIN)" -i internal/adapter/web/css/input.css -o internal/adapter/web/static/app.css --minify

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

build: ## build server, relayctl, and examples into bin/
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/relay-server$(EXE) ./cmd/relay-server
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/relayctl$(EXE) ./cmd/relayctl
	$(GO) build -o bin/example-worker$(EXE) ./examples/worker
	$(GO) build -o bin/example-producer$(EXE) ./examples/producer
	@echo "built bin/relay-server$(EXE) bin/relayctl$(EXE) (refresh PATH: export PATH=\"$$PWD/bin:$$PATH\")"

release-snapshot: ## goreleaser release --snapshot --clean (no publish)
	@if ! command -v goreleaser >/dev/null 2>&1; then \
		echo "goreleaser not found. Install: go install github.com/goreleaser/goreleaser/v2@latest"; \
		exit 1; \
	fi
	@if ! command -v syft >/dev/null 2>&1; then \
		echo "syft not found (needed for archive SBOMs)."; \
		echo "Install: https://github.com/anchore/syft#installation"; \
		echo "  scoop install syft"; \
		echo "  curl -sSfL https://raw.githubusercontent.com/anchore/syft/main/install.sh | sh -s -- -b \$$(go env GOPATH)/bin"; \
		exit 1; \
	fi
	goreleaser release --snapshot --clean

up: ## docker compose up
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) docker compose up -d --build --scale example-worker=2

down: ## docker compose down
	docker compose down -v

up-redis: ## compose with redis storage
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) RELAY_STORAGE=redis docker compose up -d --build --scale example-worker=2

up-obs: ## compose stack + observability profile (Prometheus/Grafana/Jaeger)
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) docker compose --profile observability up -d --build --scale example-worker=2
	@$(MAKE) open-obs

open-obs: ## print local observability URLs
	@echo "Grafana:    http://localhost:3300"
	@echo "Prometheus: http://localhost:9390"
	@echo "Jaeger:     http://localhost:16686"
	@echo "Server admin:   http://localhost:7429/metrics"
	@echo "Worker metrics: example-worker:9464/metrics (compose network; scraped by Prometheus)"

check-alerts: ## validate Prometheus alert rules with promtool
	# MSYS_NO_PATHCONV: Git Bash must not rewrite the container path.
	MSYS_NO_PATHCONV=1 docker run --rm --entrypoint promtool \
		-v "$(CURDIR)/deploy/observability:/etc/prometheus:ro" \
		prom/prometheus:v3.2.1 \
		check rules /etc/prometheus/alerts.yml

deps: ## start postgres + redis only
	docker compose up -d postgres redis

demo: up ## run demo producer
	# --build: pick up example changes; --no-deps: avoid resetting example-worker scale to 1
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) docker compose --profile demo build example-producer
	COMPOSE_GOPROXY=$(COMPOSE_GOPROXY) docker compose --profile demo run --rm --no-deps example-producer

check-generated: generate css ## fail if generated files drift
	@git diff --exit-code -- \
		internal/adapter/storage/postgres/sqlcgen \
		internal/adapter/http/oapi \
		gen/relay/v1 \
		internal/adapter/web/templates \
		internal/adapter/web/static/app.css \
		|| ( \
		echo "generated files are dirty; run make generate && make css and commit"; \
		exit 1; \
	)
