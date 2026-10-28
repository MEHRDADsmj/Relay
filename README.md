# Relay

[![CI](https://github.com/MEHRDADsmj/Relay/actions/workflows/ci.yml/badge.svg)](https://github.com/MEHRDADsmj/Relay/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MEHRDADsmj/relay)](https://goreportcard.com/report/github.com/MEHRDADsmj/relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)

**Durable distributed job queue in Go.** Producers enqueue work over HTTP or gRPC; workers lease jobs, heartbeat while running, and report success or failure. Storage is pluggable (**PostgreSQL** or **Redis**). Includes cron schedules, scoped API keys, a Go SDK, `relayctl`, and a server-rendered dashboard.

![Relay dashboard](docs/media/dashboard-overview-1440.png)

<p align="center"><sub>Operator UI at <code>/ui/</code> · <a href="docs/media/dashboard-overview-375.png">mobile screenshot</a></sub></p>

---

## Contents

- [Features](#features)
- [Quickstart](#quickstart)
- [How to use](#how-to-use)
  - [1. Enqueue a job](#1-enqueue-a-job)
  - [2. Process jobs with the Go SDK](#2-process-jobs-with-the-go-sdk)
  - [3. Use the CLI (`relayctl`)](#3-use-the-cli-relayctl)
  - [4. Open the dashboard](#4-open-the-dashboard)
  - [5. Schedule recurring work](#5-schedule-recurring-work)
  - [6. Authenticate with API keys](#6-authenticate-with-api-keys)
- [Ports](#ports)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Delivery guarantees](#delivery-guarantees)
- [Local development](#local-development)
- [Observability](#observability)
- [Kubernetes](#kubernetes)
- [Install & release](#install--release)
- [API reference](#api-reference)
- [Docs](#docs)
- [License](#license)

---

## Features

| Area | What you get |
|---|---|
| **Backends** | PostgreSQL 17 or Redis 7 — same job contract, swap with one env var |
| **APIs** | REST (OpenAPI) on `:7420`, gRPC on `:7421` |
| **SDK** | Typed Go client + worker (`pkg/sdk`) over HTTP or gRPC |
| **Reliability** | Leases, heartbeats, exponential backoff, reaper, cancel/release, dead-letter replay |
| **Scheduling** | Cron with leader election and fire idempotency |
| **Auth** | Scoped API keys (`producer` / `worker` / `admin` / `dashboard:read`) |
| **Ops** | Prometheus, OpenTelemetry, Helm chart, GoReleaser, runbook |
| **UI** | `/ui` dashboard (templ + HTMX) — no Node required |

---

## Quickstart

**Requirement:** Docker with Compose v2.

```bash
# Start Postgres, Redis, Relay, and two example workers
docker compose up -d --build --scale example-worker=2

# Enqueue a demo batch (optional)
docker compose --profile demo run --rm --no-deps example-producer

# Health check
curl -sS http://localhost:7420/readyz

# Dashboard
open http://localhost:7420/ui/   # or browse to that URL

# Tear down
docker compose down -v
```

**Redis instead of Postgres:**

```bash
RELAY_STORAGE=redis docker compose up -d --build --scale example-worker=2
```

**Make shortcuts** (optional): `make up` · `make demo` · `make up-redis` · `make down`

---

## How to use

### 1. Enqueue a job

With the stack running:

```bash
curl -sS -X POST http://localhost:7420/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "queue": "default",
    "type": "send_welcome_email",
    "payload": {"user_id": "u1", "email": "u1@example.com"},
    "idempotency_key": "welcome-u1"
  }'
```

- New job → **201**
- Same `(queue, idempotency_key)` again → **200** with header `Idempotent-Replayed: true`

Inspect:

```bash
JOB_ID=<id-from-response>
curl -sS "http://localhost:7420/v1/jobs/$JOB_ID"
curl -sS "http://localhost:7420/v1/queues/default"
```

More curl recipes: [examples/http-curl](examples/http-curl/README.md).

**gRPC** (reflection enabled in Compose):

```bash
grpcurl -plaintext \
  -d '{"queue":"default","type":"send_welcome_email","payload":"{\"user_id\":\"u1\"}"}' \
  localhost:7421 relay.v1.JobService/Enqueue
```

### 2. Process jobs with the Go SDK

```go
package main

import (
	"context"
	"log"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func main() {
	ctx := context.Background()

	// HTTP (default) or: sdk.NewClient("grpc://localhost:7421")
	client := sdk.NewClient("http://localhost:7420" /* , sdk.WithAPIKey("…") */)

	_, err := client.Enqueue(ctx, sdk.EnqueueRequest{
		Queue:   "default",
		Type:    "send_welcome_email",
		Payload: map[string]string{"user_id": "u1", "email": "u1@example.com"},
	})
	if err != nil {
		log.Fatal(err)
	}

	w, err := sdk.NewWorker(client, sdk.WorkerOptions{
		Queue:       "default",
		Concurrency: 10,
	})
	if err != nil {
		log.Fatal(err)
	}

	sdk.Handle(w, "send_welcome_email", func(ctx context.Context, job sdk.Job[map[string]string]) error {
		// Handlers must be idempotent — Relay is at-least-once.
		log.Printf("job %s attempt %d user=%s", job.ID, job.Attempt, job.Args["user_id"])
		return nil
	})

	log.Fatal(w.Run(ctx))
}
```

Runnable examples:

| Package | Role |
|---|---|
| [`examples/producer`](examples/producer) | Enqueues a mix of job types |
| [`examples/worker`](examples/worker) | Leases and processes with intentional retries |

Env for examples: `RELAY_URL`, `RELAY_API_KEY`, `RELAY_QUEUE`, `WORKER_CONCURRENCY`.

### 3. Use the CLI (`relayctl`)

```bash
make build

./bin/relayctl job enqueue \
  --queue default \
  --type send_welcome_email \
  --payload '{"user_id":"u1","email":"u1@example.com"}'

./bin/relayctl job get <id>
./bin/relayctl queue stats default
./bin/relayctl schedule list
./bin/relayctl apikey create --name worker --scopes worker
```

| Flag / env | Default |
|---|---|
| `--server` / `RELAY_URL` | `http://localhost:7420` |
| `--api-key` / `RELAY_API_KEY` | empty |
| `-o table\|json` | `table` |

Destructive commands need `--yes` when stdin is not a TTY.

### 4. Open the dashboard

1. Start Compose (`make up` or the Docker commands above).
2. Open **http://localhost:7420/ui/**
3. Sign in with an API key that has `admin` or `dashboard:read`.

Bootstrap a key without HTTP:

```bash
docker compose exec relay-server /relay-server apikey create --name ops --scopes admin
# or locally after make build:
./bin/relay-server apikey create --name ops --scopes admin
```

| Env | Purpose |
|---|---|
| `RELAY_UI_ENABLED` | Default `true` — set `false` to disable `/ui` |
| `RELAY_UI_SESSION_KEY` | Base64 32-byte AES key (**required in production**) |
| `RELAY_UI_SECURE_COOKIES` | `Secure` cookie flag |
| `RELAY_UI_TRACE_URL_TEMPLATE` | e.g. `http://localhost:16686/trace/{trace_id}` |

### 5. Schedule recurring work

```bash
curl -sS -X POST http://localhost:7420/v1/schedules \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $RELAY_API_KEY" \
  -d '{
    "name": "hourly-welcome",
    "cron": "@hourly",
    "timezone": "UTC",
    "queue": "default",
    "type": "send_welcome_email",
    "payload": {"user_id": "u1"}
  }'
```

```bash
./bin/relayctl schedule create \
  --name hourly-welcome \
  --cron "@hourly" \
  --queue default \
  --type send_welcome_email \
  --payload '{"user_id":"u1"}'

./bin/relayctl schedule trigger hourly-welcome
./bin/relayctl schedule disable hourly-welcome
```

Supports 5-field cron plus `@hourly`, `@daily`, `@every 5m`. Only the elected leader fires schedules; each fire uses an idempotency key so failover does not double-enqueue.

### 6. Authenticate with API keys

| Scope | Allowed |
|---|---|
| `producer` | Enqueue, get, list, attempts, cancel, retry |
| `worker` | Lease, heartbeat, complete, fail, release |
| `dashboard:read` | Dashboard (read) |
| `admin` | Everything (implies the others) |

```http
Authorization: Bearer <key>
```

- Keys look like `rly_<prefix>_<secret>` and are stored as SHA-256 hashes.
- Auth is **off** only when there are no stored keys **and** `RELAY_API_KEY` is empty (Relay logs a loud warning).
- `RELAY_API_KEY` is a bootstrap **admin** credential; prefer scoped keys for workers/producers.

`/healthz` and `/readyz` never require auth.

---

## Ports

| Port | Service |
|---:|---|
| **7420** | REST API + dashboard (`/ui`) |
| **7421** | gRPC |
| **7429** | Admin: `/metrics`, `/debug/pprof/*` |
| **54329** | Postgres (host) |
| **63799** | Redis (host) |
| **3300** / **9390** / **16686** | Grafana / Prometheus / Jaeger (`make up-obs`) |
| **9464** | Example worker metrics |

---

## Configuration

Copy [`.env.example`](.env.example) to `.env` and adjust.

| Variable | Default | Notes |
|---|---|---|
| `RELAY_STORAGE` | `postgres` | `postgres` or `redis` |
| `RELAY_POSTGRES_URL` | — | Required for Postgres |
| `RELAY_REDIS_URL` | — | Required for Redis |
| `RELAY_HTTP_ADDR` | `:7420` | |
| `RELAY_GRPC_ADDR` | `:7421` | |
| `RELAY_ADMIN_ADDR` | `:7429` | Metrics + pprof |
| `RELAY_API_KEY` | empty | Bootstrap admin Bearer token |
| `RELAY_MIGRATE_ON_START` | `false` | `true` in Compose |
| `RELAY_RETRY_POLICY` | `exponential` | or `fixed` |
| `RELAY_RETENTION` | `168h` | Purge terminal jobs; `0` = never |
| `RELAY_SCHEDULER_ENABLED` | `true` | Cron leader loop |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | Tracing off when empty |

Full list: [`.env.example`](.env.example).

Migrations (Postgres):

```bash
./bin/relay-server migrate up
./bin/relay-server migrate status
./bin/relay-server migrate down
```

---

## Architecture

```mermaid
flowchart LR
  Producer["Producer / SDK"] -->|HTTP / gRPC| API["relay-server"]
  Worker["Worker / SDK"] -->|lease / heartbeat / complete| API
  UI["Browser /ui"] --> API
  CLI["relayctl"] --> API
  API --> App["Application use cases"]
  App --> Port["Storage ports"]
  Port --> PG["Postgres"]
  Port --> RD["Redis"]
  Sched["Scheduler leader"] --> App
```

Dependencies point **inward** (Clean Architecture). Composition root: [`cmd/relay-server`](cmd/relay-server).

Deep dive: [docs/architecture.md](docs/architecture.md) · Decisions: [docs/adr](docs/adr/README.md)

---

## Delivery guarantees

Relay is **at-least-once**. Workers hold an exclusive lease, heartbeat, then complete or fail. Crashes and lease expiry can re-run a job — **handlers must be idempotent**. Producers can pass an `idempotency_key` on enqueue.

| Behavior | Effect |
|---|---|
| **Release** | Graceful SDK shutdown returns the lease without burning an attempt |
| **Cancel** | Pending → cancelled; running → `cancel_requested` (cooperative) |
| **Retry** | Exponential full jitter by default; `sdk.Permanent(err)` → dead immediately |
| **Long-poll** | Push wakeups are hints; workers always re-claim |

---

## Local development

**Requirements:** Go (see `go.mod`), Docker, Make, [`golangci-lint`](https://golangci-lint.run/) v2.

```bash
git clone https://github.com/MEHRDADsmj/Relay.git
cd Relay

make deps                    # Postgres + Redis only
cp .env.example .env

make generate                # sqlc, oapi-codegen, buf, templ
make lint && make test && make build

./bin/relay-server serve
```

| Target | Purpose |
|---|---|
| `make test` | Unit tests (`-race`) |
| `make test-integration` | Contract suite vs Docker Postgres/Redis |
| `make test-e2e` | Full stack e2e |
| `make demo` | Compose up + demo producer |
| `make loadtest` | k6 against a running stack |
| `make check-generated` | Fail if generated code drifts |
| `make check-links` | Markdown link check (lychee via Docker) |

Contributor guide: [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Observability

```bash
make up-obs && make demo && make open-obs
```

| UI | URL |
|---|---|
| Grafana | http://localhost:3300 |
| Prometheus | http://localhost:9390 |
| Jaeger | http://localhost:16686 |

Admin port (`:7429`): `/metrics`, `/debug/pprof/*`.  
Alerts & response: [docs/runbook.md](docs/runbook.md).

---

## Kubernetes

Chart: [`deploy/helm/relay`](deploy/helm/relay/) (does **not** bundle Postgres/Redis).

```bash
helm install relay ./deploy/helm/relay \
  --set config.storage=postgres \
  --set secret.postgresUrl='postgres://relay:relay@postgres:5432/relay?sslmode=disable' \
  --set secret.apiKey='change-me' \
  --set image.repository=ghcr.io/mehrdadsmj/relay-server \
  --set image.tag=0.1.0
```

---

## Install & release

```bash
go install github.com/MEHRDADsmj/relay/cmd/relay-server@latest
go install github.com/MEHRDADsmj/relay/cmd/relayctl@latest
```

GitHub Releases (`v*` tags): multi-arch binaries + SBOMs. Images on GHCR:

- `ghcr.io/mehrdadsmj/relay-server:<version>`
- `ghcr.io/mehrdadsmj/relayctl:<version>`

Local snapshot: `make release-snapshot`.

---

## API reference

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/jobs` | Enqueue |
| `GET` | `/v1/jobs/{id}` | Get job |
| `GET` | `/v1/jobs/{id}/attempts` | Attempt history |
| `POST` | `/v1/queues/{queue}/leases` | Lease (optional `wait_seconds`) |
| `POST` | `/v1/jobs/{id}/heartbeat` | Extend lease |
| `POST` | `/v1/jobs/{id}/complete` | Succeeded |
| `POST` | `/v1/jobs/{id}/fail` | Failure / permanent fail |
| `POST` | `/v1/jobs/{id}/release` | Return lease |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel |
| `POST` | `/v1/jobs/{id}/retry` | Re-queue dead/cancelled |
| `GET` | `/v1/queues` · `/v1/queues/{queue}` | List / stats |
| `POST` | `/v1/queues/{queue}/pause` · `/resume` | Pause claims |
| `GET` | `/v1/queues/{queue}/jobs` | Paginated jobs |
| `POST` | `/v1/queues/{queue}/dead/retry` | Bulk dead-letter retry |
| `*` | `/v1/schedules…` | Cron CRUD / enable / trigger |
| `*` | `/v1/api-keys…` | Key create / list / revoke |
| `GET` | `/healthz` · `/readyz` | Liveness / readiness |

- Contract: [`api/openapi.yaml`](api/openapi.yaml)
- Protos: [`proto/relay/v1`](proto/relay/v1/)
- Errors: RFC 9457 `application/problem+json`

Lease / heartbeat / complete / fail / release bodies require `worker_id` (see OpenAPI).

---

## Docs

| Doc | Description |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Layering and package map |
| [docs/adr](docs/adr/README.md) | Architecture Decision Records (0001–0022) |
| [docs/benchmarks.md](docs/benchmarks.md) | Throughput numbers & how to reproduce |
| [docs/runbook.md](docs/runbook.md) | Alert response |
| [docs/plan](docs/plan/) | Implementation phase plans |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Dev workflow |

---

## License

[MIT](LICENSE)
