# Relay

[![CI](https://github.com/MEHRDADsmj/relay/actions/workflows/ci.yml/badge.svg)](https://github.com/MEHRDADsmj/relay/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MEHRDADsmj/relay)](https://goreportcard.com/report/github.com/MEHRDADsmj/relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev/)

**Relay** is a durable, distributed job queue in Go. Producers enqueue jobs over HTTP;
workers built with the Relay SDK lease jobs, execute them, heartbeat while running, and
report success or failure. Storage is pluggable: **PostgreSQL** and **Redis** implement
the same port and pass the same contract tests.

## Architecture

```mermaid
flowchart LR
  Producer["Producer / SDK"] -->|HTTP| API["relay-server REST API"]
  Worker["Worker / SDK"] -->|lease / heartbeat / complete| API
  API --> App["Application use cases"]
  App --> Port["JobRepository port"]
  Port --> PG["Postgres adapter"]
  Port --> RD["Redis adapter"]
```

Dependencies point inward (Clean Architecture). The composition root in `cmd/relay-server`
wires config, storage, and HTTP. See [docs/adr](docs/adr/) for design decisions.

## Features and roadmap

| Phase | Status | Highlights |
|---|---|---|
| **1 — Core** | Done | Domain + app ports, Postgres & Redis adapters, OpenAPI REST API, `relay-server`, Go SDK, examples, Docker Compose, CI |
| [2 — Reliability](docs/plan/phase-2.md) | Planned | Exponential backoff, reaper, dead-letter replay, LISTEN/NOTIFY wakeups |
| [3 — Platform](docs/plan/phase-3.md) | Planned | Cron, gRPC, API keys, `relayctl` |
| [4 — Ops](docs/plan/phase-4.md) | Planned | Prometheus, OpenTelemetry, Helm, GoReleaser |
| [5 — Polish](docs/plan/phase-5.md) | Planned | Dashboard, load tests |

Full Phase 1 plan: [docs/plan/phase-1.md](docs/plan/phase-1.md).

## Quickstart

Requires only **Docker** (Compose v2).

```bash
# Start Postgres, Redis, relay-server, and two example workers
make up

# Enqueue a batch of demo jobs (producer profile)
make demo

# Or enqueue manually
curl -sS -X POST http://localhost:7420/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","type":"send_welcome_email","payload":{"user_id":"u1","email":"u1@example.com"}}'

curl -sS http://localhost:7420/readyz
curl -sS http://localhost:7420/v1/jobs/<job-id>

make down
```

Redis backend instead of Postgres:

```bash
make up-redis
```

Host ports avoid common clashes: API `7420`, Postgres `54329`, Redis `63799`.

## Local development

**Requirements:** Go (see `go.mod`), Docker, `golangci-lint` v2, Make (Git Bash on Windows).

```bash
make deps          # Postgres + Redis only
cp .env.example .env
make lint
make test
make build
./bin/relay-server serve   # or: go run ./cmd/relay-server serve
```

Useful targets: `make test-integration`, `make test-e2e`, `make generate`,
`make check-generated`, `make cover`.

In GoLand / VS Code, run `cmd/relay-server` with env from `.env.example` after `make deps`.
Set `RELAY_MIGRATE_ON_START=true` on first Postgres run (or `relay-server migrate`).

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/jobs` | Enqueue (`201` new; `200` + `Idempotent-Replayed: true` on replay) |
| `GET` | `/v1/jobs/{id}` | Get job |
| `POST` | `/v1/queues/{queue}/leases` | Lease up to `max_jobs` |
| `POST` | `/v1/jobs/{id}/heartbeat` | Extend lease |
| `POST` | `/v1/jobs/{id}/complete` | Mark succeeded |
| `POST` | `/v1/jobs/{id}/fail` | Report failure (retry or dead) |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness (storage ping) |

Full contract: [api/openapi.yaml](api/openapi.yaml). Errors use RFC 9457
`application/problem+json`. Optional auth: set `RELAY_API_KEY` and send
`Authorization: Bearer <key>` (health endpoints exempt).

## SDK example

```go
package main

import (
	"context"
	"log"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func main() {
	ctx := context.Background()
	client := sdk.NewClient("http://localhost:7420")

	job, err := client.Enqueue(ctx, sdk.EnqueueRequest{
		Queue:   "default",
		Type:    "send_welcome_email",
		Payload: map[string]string{"user_id": "u1", "email": "u1@example.com"},
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = job

	w, err := sdk.NewWorker(client, sdk.WorkerOptions{Queue: "default", Concurrency: 10})
	if err != nil {
		log.Fatal(err)
	}
	sdk.Handle(w, "send_welcome_email", func(ctx context.Context, job sdk.Job[map[string]string]) error {
		// process job.Args — must be idempotent
		return nil
	})
	log.Fatal(w.Run(ctx))
}
```

See `examples/producer` and `examples/worker` for a full demo.

## Delivery guarantees

Relay delivers jobs **at least once**. Workers hold an exclusive lease, heartbeat to keep
it, then complete or fail. Crashes and expired leases can cause **re-execution**. Handlers
must be idempotent. Producers can pass an idempotency key on enqueue to safely retry
creates. Exactly-once execution is out of scope (see [ADR 0003](docs/adr/0003-at-least-once-delivery-with-leases.md)).

## Design decisions

Architecture Decision Records live in [docs/adr/](docs/adr/):

1. [Clean Architecture](docs/adr/0001-clean-architecture.md)
2. [Pluggable Postgres and Redis storage](docs/adr/0002-pluggable-storage-postgres-and-redis.md)
3. [At-least-once delivery with leases](docs/adr/0003-at-least-once-delivery-with-leases.md)
4. [Spec-first OpenAPI](docs/adr/0004-spec-first-openapi.md)
5. [Time injected from the application layer](docs/adr/0005-time-injected-from-application-layer.md)
6. [Redis single-node and durability](docs/adr/0006-redis-single-node-and-durability.md)

## Testing

| Layer | How |
|---|---|
| Unit | `make test` (`-race`); domain/app/SDK/HTTP with fakes |
| Contract | Shared `storagetest` suite against memory, Postgres, Redis |
| Integration | `make test-integration` (`-tags=integration`, Testcontainers) |
| E2E | `make test-e2e` (`-tags=e2e`); full stack, both backends |
| Generated | `make check-generated` — sqlc + oapi-codegen must be clean |
| Lint / vulns | `make lint`; CI runs `govulncheck` |

## Configuration

Copy [`.env.example`](.env.example). Notable vars: `RELAY_STORAGE`, `RELAY_POSTGRES_URL`,
`RELAY_REDIS_URL`, `RELAY_HTTP_ADDR`, `RELAY_API_KEY`, `RELAY_MIGRATE_ON_START`.

## License

[MIT](LICENSE)
