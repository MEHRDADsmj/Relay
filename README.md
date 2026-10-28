# Relay

[![CI](https://github.com/MEHRDADsmj/relay/actions/workflows/ci.yml/badge.svg)](https://github.com/MEHRDADsmj/relay/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MEHRDADsmj/relay)](https://goreportcard.com/report/github.com/MEHRDADsmj/relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev/)

**Relay** is a durable, distributed job queue in Go. Producers enqueue jobs over HTTP or
gRPC; workers built with the Relay SDK lease jobs, execute them, heartbeat while running,
and report success or failure. Cron schedules enqueue on a leader-elected tick. Storage
is pluggable: **PostgreSQL** and **Redis** implement the same port and pass the same
contract tests.

## Architecture

```mermaid
flowchart LR
  Producer["Producer / SDK"] -->|HTTP / gRPC| API["relay-server"]
  Worker["Worker / SDK"] -->|lease / heartbeat / complete| API
  CLI["relayctl"] --> API
  API --> App["Application use cases"]
  App --> Port["Storage ports"]
  Port --> PG["Postgres adapter"]
  Port --> RD["Redis adapter"]
  Sched["Scheduler leader"] --> App
```

Dependencies point inward (Clean Architecture). The composition root in `cmd/relay-server`
wires config, storage, HTTP, gRPC, maintenance, and the scheduler. See [docs/adr](docs/adr/)
for design decisions.

## Features and roadmap

| Phase | Status | Highlights |
|---|---|---|
| **1 — Core** | Done | Domain + app ports, Postgres & Redis adapters, OpenAPI REST API, `relay-server`, Go SDK, examples, Docker Compose, CI |
| [2 — Reliability](docs/plan/phase-2.md) | Done | Exponential backoff, attempt history, reaper/retention, cancel/retry, queue pause, long-poll wakeups |
| [3 — Platform](docs/plan/phase-3.md) | Done | Cron schedules, leader election, gRPC, scoped API keys, `relayctl`, dual SDK transport |
| [4 — Ops](docs/plan/phase-4.md) | Planned | Prometheus, OpenTelemetry, Helm, GoReleaser |
| [5 — Polish](docs/plan/phase-5.md) | Planned | Dashboard, load tests |

Plans: [phase-1](docs/plan/phase-1.md), [phase-2](docs/plan/phase-2.md), [phase-3](docs/plan/phase-3.md).

## Quickstart

Requires only **Docker** (Compose v2). No Make needed for this path.

```bash
# Start Postgres, Redis, relay-server, and two example workers
docker compose up -d --build --scale example-worker=2

# Enqueue a batch of demo jobs (producer profile)
docker compose --profile demo run --rm --no-deps example-producer

# Or enqueue manually
curl -sS -X POST http://localhost:7420/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","type":"send_welcome_email","payload":{"user_id":"u1","email":"u1@example.com"}}'

curl -sS http://localhost:7420/readyz
curl -sS http://localhost:7420/v1/jobs/<job-id>

docker compose down -v
```

Redis backend instead of Postgres:

```bash
RELAY_STORAGE=redis docker compose up -d --build --scale example-worker=2
```

Optional Make shortcuts (same stack): `make up`, `make demo`, `make up-redis`, `make down`.

Host ports avoid common clashes: HTTP `7420`, gRPC `7421`, Postgres `54329`, Redis `63799`.

## Local development

**Requirements:** Go (see `go.mod`), Docker, `golangci-lint` v2, Make (Git Bash on Windows),
`buf` (via Go tools) for proto generation.

```bash
make deps          # Postgres + Redis only
cp .env.example .env
make lint
make test
make build
./bin/relay-server serve   # or: go run ./cmd/relay-server serve
./bin/relayctl version
```

Useful targets: `make test-integration`, `make test-e2e`, `make generate`,
`make check-generated`, `make cover`.

In GoLand / VS Code, run `cmd/relay-server` with env from `.env.example` after `make deps`.
Set `RELAY_MIGRATE_ON_START=true` on first Postgres run, or run migrations manually:
`relay-server migrate up` (also `migrate status`, `migrate down`). Bootstrap an admin key
without HTTP: `relay-server apikey create --name ops --scopes admin`.

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/jobs` | Enqueue (`201` new; `200` + `Idempotent-Replayed: true` on replay) |
| `GET` | `/v1/jobs/{id}` | Get job |
| `GET` | `/v1/jobs/{id}/attempts` | Attempt history |
| `POST` | `/v1/queues/{queue}/leases` | Lease up to `max_jobs` (optional `wait_seconds` long-poll) |
| `POST` | `/v1/jobs/{id}/heartbeat` | Extend lease (response may include `cancel_requested`) |
| `POST` | `/v1/jobs/{id}/complete` | Mark succeeded |
| `POST` | `/v1/jobs/{id}/fail` | Report failure (retry, dead, or `permanent: true`) |
| `POST` | `/v1/jobs/{id}/release` | Voluntary lease return |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel pending or request cancel while running |
| `POST` | `/v1/jobs/{id}/retry` | Re-queue dead or cancelled |
| `GET` | `/v1/queues` | List queues |
| `GET` | `/v1/queues/{queue}` | Queue stats |
| `POST` | `/v1/queues/{queue}/pause` | Pause claims |
| `POST` | `/v1/queues/{queue}/resume` | Resume claims |
| `GET` | `/v1/queues/{queue}/jobs` | Paginated list by status |
| `POST` | `/v1/queues/{queue}/dead/retry` | Bulk retry dead letters |
| `POST` | `/v1/schedules` | Create cron schedule |
| `GET` | `/v1/schedules` | List schedules |
| `GET` | `/v1/schedules/{id_or_name}` | Get schedule |
| `PATCH` | `/v1/schedules/{id}` | Update (`If-Match: <updated_at>` required) |
| `POST` | `/v1/schedules/{id}/enable` · `/disable` · `/trigger` | Enable, disable, or fire once |
| `DELETE` | `/v1/schedules/{id}` | Delete schedule |
| `POST` | `/v1/api-keys` | Create key (secret returned once) |
| `GET` | `/v1/api-keys` | List keys (prefix only) |
| `DELETE` | `/v1/api-keys/{id}` | Revoke key |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness (storage ping) |

Full contract: [api/openapi.yaml](api/openapi.yaml). Errors use RFC 9457
`application/problem+json`. Auth: `Authorization: Bearer <key>` (health endpoints exempt).
See [Auth and scopes](#auth-and-scopes).

## Schedules

Cron schedules enqueue jobs on the cluster leader ([ADR 0011](docs/adr/0011-cron-misfire-policy.md),
[ADR 0012](docs/adr/0012-leader-election-and-idempotent-fires.md)):

- Expression: standard 5-field cron plus descriptors (`@hourly`, `@daily`, `@every 5m`).
- Timezone: IANA name (default `UTC`). `NextRunAt` is always computed, including when disabled.
- **Misfire:** after downtime, at most one catch-up job for the most recent missed fire,
  then advance to the next future tick (no storm).
- Fire idempotency key: `schedule:{id}:{fire_unix}` — safe across leader failover.
- Optimistic edits: `PATCH` requires `If-Match: <updated_at RFC3339Nano>` (`428` if missing,
  `412` if stale).

```bash
curl -sS -X POST http://localhost:7420/v1/schedules \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $RELAY_API_KEY" \
  -d '{"name":"hourly-welcome","cron":"@hourly","queue":"default","type":"send_welcome_email","payload":{"user_id":"u1"}}'
```

Disable the in-process scheduler with `RELAY_SCHEDULER_ENABLED=false` when running
side-car workers only.

## gRPC

Same use cases as REST on port `7421` ([ADR 0014](docs/adr/0014-grpc-alongside-rest.md)).
Protos: [proto/relay/v1](proto/relay/v1/). Reflection is on in Compose
(`RELAY_GRPC_REFLECTION=true`); enable locally to use `grpcurl`:

```bash
# List services (reflection on)
grpcurl -plaintext localhost:7421 list

# Enqueue
grpcurl -plaintext -H "authorization: Bearer $RELAY_API_KEY" \
  -d '{"queue":"default","type":"send_welcome_email","payload":"{\"user_id\":\"u1\"}"}' \
  localhost:7421 relay.v1.JobService/Enqueue

# Health
grpcurl -plaintext localhost:7421 grpc.health.v1.Health/Check
```

SDK: use `grpc://localhost:7421` (or `grpcs://` for TLS), or
`sdk.WithTransport(sdk.TransportGRPC)`.

## Auth and scopes

| Scope | Can |
|---|---|
| `producer` | Enqueue, get job, list by status, attempts, cancel, retry |
| `worker` | Lease, heartbeat, complete, fail, release |
| `admin` | Everything (queues, schedules, API keys) — implies producer + worker |

Keys are `rly_<prefix>_<secret>`, stored as SHA-256
([ADR 0013](docs/adr/0013-api-key-format-and-hashing.md)). Create via API/`relayctl` or
bootstrap with `RELAY_API_KEY` / `relay-server apikey create`. Auth is off only when no
stored keys exist **and** `RELAY_API_KEY` is empty (loud startup warning). Verified-key
cache TTL: `RELAY_AUTH_CACHE_TTL` (default 30s; revocation within one TTL).

## relayctl

Operator CLI (`cmd/relayctl`). Global flags: `--server` / `RELAY_URL` (default
`http://localhost:7420`), `--api-key` / `RELAY_API_KEY`, `-o table|json`, `--timeout`.

```bash
make build
./bin/relayctl job enqueue --queue default --type send_welcome_email --payload '{"user_id":"u1"}'
./bin/relayctl schedule create --name hourly-welcome --cron '@hourly' --queue default --type send_welcome_email
./bin/relayctl schedule list
./bin/relayctl queue stats default
./bin/relayctl apikey create --name worker --scopes worker
./bin/relayctl apikey list
```

Destructive commands prompt unless `--yes` (non-TTY requires `--yes`).

<!-- asciinema / GIF placeholder: record a short relayctl demo when cutting a release -->

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
	// HTTP: "http://localhost:7420" — or gRPC: "grpc://localhost:7421"
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

Additional Phase 2 semantics:

- **Release** — on graceful shutdown the SDK returns in-flight leases to `pending` without
  burning an attempt, so another worker can pick them up immediately.
- **Cancel** — pending jobs become `cancelled` at once; running jobs get
  `cancel_requested` and resolve to `cancelled` when the worker next completes or fails
  (cooperative; see [ADR 0010](docs/adr/0010-cancellation-semantics.md)).
- **Permanent errors** — `sdk.Permanent(err)` / fail with `permanent: true` marks the job
  `dead` after that attempt (no retry schedule).
- **Retries** — default policy is exponential full jitter ([ADR 0007](docs/adr/0007-exponential-backoff-full-jitter.md));
  dead or cancelled jobs can be retried (attempts reset to 0).
- **Long-poll** — lease wait uses push wakeups as hints only; Claim remains the source of
  truth ([ADR 0008](docs/adr/0008-push-wakeups-are-hints.md)).

## Operating Relay

- **Retention** — the maintenance runner purges terminal jobs older than `RELAY_RETENTION`
  (default 7 days; `0` = never). Reaper reclaim of expired leases runs on every instance
  (`RELAY_REAP_*`). See [ADR 0009](docs/adr/0009-attempt-history-and-retention.md).
- **Pause / resume** — `POST /v1/queues/{queue}/pause` stops new claims (leases return
  empty); resume restores them. Useful for deploys or incident freezes.
- **Dead-letter replay** — inspect via `GET /v1/queues/{queue}/jobs?status=dead`, retry one
  job with `POST /v1/jobs/{id}/retry`, or bulk with `POST /v1/queues/{queue}/dead/retry`.
- **Attempts** — `GET /v1/jobs/{id}/attempts` shows per-claim outcomes for forensics.
- **Stats** — `GET /v1/queues/{queue}` returns counts including scheduled and oldest pending age.
- **Scheduler** — one leader per cluster ticks due schedules (`RELAY_SCHEDULER_*`). Compose
  runs two server instances so failover is visible.
- **API keys** — prefer scoped stored keys over a long-lived bootstrap `RELAY_API_KEY`.

## Design decisions

Architecture Decision Records live in [docs/adr/](docs/adr/):

1. [Clean Architecture](docs/adr/0001-clean-architecture.md)
2. [Pluggable Postgres and Redis storage](docs/adr/0002-pluggable-storage-postgres-and-redis.md)
3. [At-least-once delivery with leases](docs/adr/0003-at-least-once-delivery-with-leases.md)
4. [Spec-first OpenAPI](docs/adr/0004-spec-first-openapi.md)
5. [Time injected from the application layer](docs/adr/0005-time-injected-from-application-layer.md)
6. [Redis single-node and durability](docs/adr/0006-redis-single-node-and-durability.md)
7. [Exponential backoff with full jitter](docs/adr/0007-exponential-backoff-full-jitter.md)
8. [Push wakeups are hints](docs/adr/0008-push-wakeups-are-hints.md)
9. [Attempt history and retention](docs/adr/0009-attempt-history-and-retention.md)
10. [Cancellation semantics](docs/adr/0010-cancellation-semantics.md)
11. [Cron misfire policy](docs/adr/0011-cron-misfire-policy.md)
12. [Leader election and idempotent fires](docs/adr/0012-leader-election-and-idempotent-fires.md)
13. [API key format and hashing](docs/adr/0013-api-key-format-and-hashing.md)
14. [gRPC alongside REST](docs/adr/0014-grpc-alongside-rest.md)

## Testing

| Layer | How |
|---|---|
| Unit | `make test` (`-race`); domain/app/SDK/HTTP/gRPC/CLI with fakes |
| Contract | Shared `storagetest` suite against memory, Postgres, Redis |
| Integration | `make test-integration` (`-tags=integration`, Testcontainers) |
| E2E | `make test-e2e` (`-tags=e2e`); full stack, both backends (incl. Phase 3) |
| Generated | `make check-generated` — sqlc + oapi-codegen + buf must be clean |
| Lint / vulns | `make lint`; CI runs `govulncheck` |

## Configuration

Copy [`.env.example`](.env.example). Notable vars: `RELAY_STORAGE`, `RELAY_POSTGRES_URL`,
`RELAY_REDIS_URL`, `RELAY_HTTP_ADDR`, `RELAY_GRPC_ADDR`, `RELAY_GRPC_REFLECTION`,
`RELAY_API_KEY`, `RELAY_AUTH_CACHE_TTL`, `RELAY_SCHEDULER_ENABLED` / `RELAY_SCHEDULER_TICK`,
`RELAY_INSTANCE_ID`, `RELAY_MIGRATE_ON_START`, `RELAY_RETRY_POLICY` / `RELAY_RETRY_BASE` /
`RELAY_RETRY_MAX`, `RELAY_REAP_*`, `RELAY_PURGE_*`, `RELAY_RETENTION`, `RELAY_LEASE_MAX_WAIT`.

## License

[MIT](LICENSE)
