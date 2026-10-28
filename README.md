# Relay

[![CI](https://github.com/MEHRDADsmj/relay/actions/workflows/ci.yml/badge.svg)](https://github.com/MEHRDADsmj/relay/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MEHRDADsmj/relay)](https://goreportcard.com/report/github.com/MEHRDADsmj/relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev/)

**Durable distributed job queue in Go** — producers enqueue over HTTP or gRPC; workers
lease, heartbeat, and complete with the SDK; cron fires on a leader-elected tick; storage
is pluggable (**PostgreSQL** or **Redis**). Includes a server-rendered operator dashboard.

![Relay dashboard overview](docs/media/dashboard-overview-1440.png)

<p align="center"><sub>Dashboard at <code>/ui/</code> — also usable at phone width
(<a href="docs/media/dashboard-overview-375.png">375px screenshot</a>). Demo GIFs:
<a href="docs/media/PLACEHOLDERS.md">capture checklist</a>.</sub></p>

## Why Relay

- **Clean Architecture** — domain and use cases free of SQL/Redis/HTTP; adapters swap behind ports
- **Two backends, one contract** — Postgres and Redis pass the same `storagetest` suite
- **At-least-once with leases** — heartbeats, reaper, retries, dead letters; handlers stay idempotent
- **HTTP + gRPC + Go SDK** — OpenAPI-first REST, protos, typed worker runtime
- **Cron with safe failover** — leader election and fire idempotency keys
- **Ops-ready** — Prometheus metrics, OTel traces, Helm chart, GoReleaser, runbook
- **Dashboard without Node** — templ + HTMX + Tailwind in the same binary (`/ui/`)

## 60-second quickstart

Requires only **Docker** (Compose v2).

```bash
docker compose up -d --build --scale example-worker=2

docker compose --profile demo run --rm --no-deps example-producer

curl -sS -X POST http://localhost:7420/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","type":"send_welcome_email","payload":{"user_id":"u1","email":"u1@example.com"}}'

curl -sS http://localhost:7420/readyz
open http://localhost:7420/ui/   # or browse to /ui/

docker compose down -v
```

Redis backend: `RELAY_STORAGE=redis docker compose up -d --build --scale example-worker=2`.

Make shortcuts: `make up`, `make demo`, `make up-redis`, `make down`.

Host ports: HTTP `7420`, gRPC `7421`, admin/metrics `7429`, Postgres `54329`, Redis `63799`.

Curl-only walkthrough: [examples/http-curl](examples/http-curl/README.md).

## Feature matrix

| Phase | Status | Highlights |
|---|---|---|
| **1 — Core** | Done | Domain + app ports, Postgres & Redis, OpenAPI REST, `relay-server`, Go SDK, examples, Compose, CI |
| [2 — Reliability](docs/plan/phase-2.md) | Done | Backoff, attempts, reaper/retention, cancel/retry, pause, long-poll wakeups |
| [3 — Platform](docs/plan/phase-3.md) | Done | Cron, leader election, gRPC, scoped API keys, `relayctl`, dual SDK transport |
| [4 — Ops](docs/plan/phase-4.md) | Done | Prometheus, OpenTelemetry, Helm, GoReleaser, [runbook](docs/runbook.md) |
| [5 — Dashboard & polish](docs/plan/phase-5.md) | Done | `/ui` dashboard, `relay-bench`, k6, profiling optimization, docs |

Plans: [phase-1](docs/plan/phase-1.md) · [2](docs/plan/phase-2.md) · [3](docs/plan/phase-3.md) ·
[4](docs/plan/phase-4.md) · [5](docs/plan/phase-5.md).

## Architecture

```mermaid
flowchart LR
  Producer["Producer / SDK"] -->|HTTP / gRPC| API["relay-server"]
  Worker["Worker / SDK"] -->|lease / heartbeat / complete| API
  UI["Browser /ui"] --> API
  CLI["relayctl"] --> API
  API --> App["Application use cases"]
  App --> Port["Storage ports"]
  Port --> PG["Postgres adapter"]
  Port --> RD["Redis adapter"]
  Sched["Scheduler leader"] --> App
```

Dependencies point inward. Composition root: `cmd/relay-server`. Deep dive:
[docs/architecture.md](docs/architecture.md). Decisions: [docs/adr](docs/adr/README.md).

## Delivery guarantees

Relay delivers jobs **at least once**. Workers hold an exclusive lease, heartbeat, then
complete or fail. Crashes and expired leases can cause **re-execution** — handlers must be
idempotent. Producers may pass an idempotency key on enqueue. Exactly-once execution is
out of scope ([ADR 0003](docs/adr/0003-at-least-once-delivery-with-leases.md)).

- **Release** — graceful SDK shutdown returns leases without burning an attempt
- **Cancel** — pending cancels immediately; running jobs cooperate via `cancel_requested`
  ([ADR 0010](docs/adr/0010-cancellation-semantics.md))
- **Retries** — exponential full jitter by default ([ADR 0007](docs/adr/0007-exponential-backoff-full-jitter.md));
  `permanent: true` / `sdk.Permanent` → dead
- **Long-poll** — push wakeups are hints only ([ADR 0008](docs/adr/0008-push-wakeups-are-hints.md))

## Benchmarks (summary)

Indicative laptop numbers (`relay-bench`, 5000 jobs). Full tables, EXPLAIN, and reproduce
steps: **[docs/benchmarks.md](docs/benchmarks.md)**.

| Backend × transport | Enqueue jobs/s | Completions/s | e2e p50 |
|---|---:|---:|---:|
| postgres × http | 307 | 303 | 12.6 ms |
| postgres × grpc | 297 | 293 | 13.0 ms |
| redis × http | 586 | 573 | 5.7 ms |
| redis × grpc | 816 | 790 | 3.2 ms |

Phase 5 optimization: Postgres claim rewritten to use partial indexes
(~40% faster `BenchmarkPostgresClaim`, [ADR 0022](docs/adr/0022-postgres-claim-partial-indexes.md)).

## Dashboard

Browse **http://localhost:7420/ui/** after Compose is up. Sign in with an API key that has
`admin` or `dashboard:read`. Sessions use an encrypted cookie
([ADR 0021](docs/adr/0021-dashboard-session-security.md)); throughput charts do not need
Prometheus ([ADR 0020](docs/adr/0020-dashboard-stats-without-prometheus.md)).

| Env | Default | Notes |
|---|---|---|
| `RELAY_UI_ENABLED` | `true` | Disable the UI entirely |
| `RELAY_UI_SESSION_KEY` | empty | Base64 32-byte key; required in production |
| `RELAY_UI_SECURE_COOKIES` | prod `true` / dev `false` | Set `Secure` on the session cookie |
| `RELAY_UI_TRACE_URL_TEMPLATE` | empty | e.g. `http://localhost:16686/trace/{trace_id}` |

## Local development

**Requirements:** Go (see `go.mod`), Docker, `golangci-lint` v2, Make (Git Bash on Windows),
tools via `make generate` (`sqlc`, `oapi-codegen`, `buf`, `templ`).

```bash
make deps
cp .env.example .env
make lint && make test && make build
./bin/relay-server serve
./bin/relayctl version
```

Useful targets: `make test-integration`, `make test-e2e`, `make generate`,
`make check-generated`, `make check-links`, `make cover`, `make loadtest`.

Contributor guide: [CONTRIBUTING.md](CONTRIBUTING.md).

Bootstrap an admin key without HTTP: `relay-server apikey create --name ops --scopes admin`.
Set `RELAY_MIGRATE_ON_START=true` on first Postgres run, or `relay-server migrate up`.

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/jobs` | Enqueue (`201` new; `200` + `Idempotent-Replayed: true` on replay) |
| `GET` | `/v1/jobs/{id}` | Get job |
| `GET` | `/v1/jobs/{id}/attempts` | Attempt history |
| `POST` | `/v1/queues/{queue}/leases` | Lease up to `max_jobs` (optional `wait_seconds`) |
| `POST` | `/v1/jobs/{id}/heartbeat` | Extend lease |
| `POST` | `/v1/jobs/{id}/complete` | Mark succeeded |
| `POST` | `/v1/jobs/{id}/fail` | Report failure |
| `POST` | `/v1/jobs/{id}/release` | Voluntary lease return |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel pending / request cancel while running |
| `POST` | `/v1/jobs/{id}/retry` | Re-queue dead or cancelled |
| `GET` | `/v1/queues` · `/v1/queues/{queue}` | List / stats |
| `POST` | `/v1/queues/{queue}/pause` · `/resume` | Pause / resume claims |
| `GET` | `/v1/queues/{queue}/jobs` | Paginated list by status |
| `POST` | `/v1/queues/{queue}/dead/retry` | Bulk retry dead letters |
| `POST`/`GET`/`PATCH`/`DELETE` | `/v1/schedules…` | Cron schedules |
| `POST`/`GET`/`DELETE` | `/v1/api-keys…` | Scoped API keys |
| `GET` | `/healthz` · `/readyz` | Liveness / readiness |

Full contract: [api/openapi.yaml](api/openapi.yaml). Errors: RFC 9457
`application/problem+json`. Auth: `Authorization: Bearer <key>` (health exempt).

## Schedules

Cron on the cluster leader ([ADR 0011](docs/adr/0011-cron-misfire-policy.md),
[ADR 0012](docs/adr/0012-leader-election-and-idempotent-fires.md)):

- 5-field cron plus `@hourly`, `@daily`, `@every 5m`; IANA timezone (default `UTC`)
- Misfire: at most one catch-up for the most recent missed fire
- Fire idempotency key: `schedule:{id}:{fire_unix}`
- Optimistic edits: `PATCH` requires `If-Match: <updated_at>`

```bash
curl -sS -X POST http://localhost:7420/v1/schedules \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $RELAY_API_KEY" \
  -d '{"name":"hourly-welcome","cron":"@hourly","queue":"default","type":"send_welcome_email","payload":{"user_id":"u1"}}'
```

## gRPC

Same use cases on port `7421` ([ADR 0014](docs/adr/0014-grpc-alongside-rest.md)).
Protos: [proto/relay/v1](proto/relay/v1/). Reflection on in Compose.

```bash
grpcurl -plaintext -H "authorization: Bearer $RELAY_API_KEY" \
  -d '{"queue":"default","type":"send_welcome_email","payload":"{\"user_id\":\"u1\"}"}' \
  localhost:7421 relay.v1.JobService/Enqueue
```

SDK: `grpc://localhost:7421` or `sdk.WithTransport(sdk.TransportGRPC)`.

## Auth and scopes

| Scope | Can |
|---|---|
| `producer` | Enqueue, get, list, attempts, cancel, retry |
| `worker` | Lease, heartbeat, complete, fail, release |
| `dashboard:read` | Dashboard read-only (and matching read APIs) |
| `admin` | Everything (implies producer + worker) |

Keys are `rly_<prefix>_<secret>`, stored as SHA-256
([ADR 0013](docs/adr/0013-api-key-format-and-hashing.md)). Auth is off only when no stored
keys exist **and** `RELAY_API_KEY` is empty (loud startup warning).

## relayctl

```bash
make build
./bin/relayctl job enqueue --queue default --type send_welcome_email --payload '{"user_id":"u1"}'
./bin/relayctl schedule list
./bin/relayctl queue stats default
./bin/relayctl apikey create --name worker --scopes worker
```

Global flags: `--server` / `RELAY_URL`, `--api-key` / `RELAY_API_KEY`, `-o table|json`.
Terminal demo GIF: [docs/media/PLACEHOLDERS.md](docs/media/PLACEHOLDERS.md).

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
	client := sdk.NewClient("http://localhost:7420") // or grpc://localhost:7421

	_, err := client.Enqueue(ctx, sdk.EnqueueRequest{
		Queue:   "default",
		Type:    "send_welcome_email",
		Payload: map[string]string{"user_id": "u1", "email": "u1@example.com"},
	})
	if err != nil {
		log.Fatal(err)
	}

	w, err := sdk.NewWorker(client, sdk.WorkerOptions{Queue: "default", Concurrency: 10})
	if err != nil {
		log.Fatal(err)
	}
	sdk.Handle(w, "send_welcome_email", func(ctx context.Context, job sdk.Job[map[string]string]) error {
		return nil // must be idempotent
	})
	log.Fatal(w.Run(ctx))
}
```

See `examples/producer` and `examples/worker`.

## Operating Relay

- **Retention** — purge terminal jobs older than `RELAY_RETENTION` (default 7d)
- **Pause / resume** — freeze claims during deploys
- **Dead-letter replay** — API, `relayctl`, or dashboard
- **Incidents** — [docs/runbook.md](docs/runbook.md)

## Observability

Prometheus metrics + OpenTelemetry traces
([ADR 0015](docs/adr/0015-prometheus-metrics-otel-traces.md),
[ADR 0016](docs/adr/0016-trace-links-for-async-jobs.md),
[ADR 0017](docs/adr/0017-metric-cardinality-limits.md)).

```bash
make up-obs && make demo && make open-obs
```

| UI | URL |
|---|---|
| Grafana | http://localhost:3300 |
| Prometheus | http://localhost:9390 |
| Jaeger | http://localhost:16686 |

Admin port `RELAY_ADMIN_ADDR` (default `:7429`): `/metrics`, `/debug/pprof/*`.
Alert rules: [`deploy/observability/alerts.yml`](deploy/observability/alerts.yml).

Screenshot placeholders: [docs/media/PLACEHOLDERS.md](docs/media/PLACEHOLDERS.md).

## Kubernetes (Helm)

Chart: [`deploy/helm/relay`](deploy/helm/relay/) — probes, security context, optional HPA /
PDB / NetworkPolicy / ServiceMonitor, Postgres migration Job as a Helm hook
([ADR 0018](docs/adr/0018-helm-migrations-as-hooks.md)). Does **not** bundle Postgres/Redis.

```bash
helm install relay ./deploy/helm/relay \
  --set config.storage=postgres \
  --set secret.postgresUrl='postgres://relay:relay@postgres:5432/relay?sslmode=disable' \
  --set secret.apiKey='change-me' \
  --set image.repository=ghcr.io/mehrdadsmj/relay-server \
  --set image.tag=0.1.0
```

## Install and release

```bash
go install github.com/MEHRDADsmj/relay/cmd/relay-server@latest
go install github.com/MEHRDADsmj/relay/cmd/relayctl@latest
```

GitHub Releases (`v*` tags): multi-arch archives + SBOMs. Images:
`ghcr.io/mehrdadsmj/relay-server:<version>`, `ghcr.io/mehrdadsmj/relayctl:<version>`.

Local snapshot: `make release-snapshot`.

## Design decisions

Index: **[docs/adr/README.md](docs/adr/README.md)** (ADR 0001–0022).

## Testing

| Layer | How |
|---|---|
| Unit | `make test` (`-race`) |
| Contract | `storagetest` × memory / Postgres / Redis |
| Integration | `make test-integration` |
| E2E | `make test-e2e` |
| Generated | `make check-generated` |
| Links | `make check-links` (lychee via Docker) |
| Lint / vulns | `make lint`; CI `govulncheck` |

## Configuration

Copy [`.env.example`](.env.example). Notable vars: `RELAY_STORAGE`, `RELAY_POSTGRES_URL`,
`RELAY_REDIS_URL`, `RELAY_HTTP_ADDR`, `RELAY_GRPC_ADDR`, `RELAY_ADMIN_ADDR`,
`RELAY_API_KEY`, `RELAY_SCHEDULER_*`, `RELAY_STATS_RETENTION`, `RELAY_UI_*`,
`RELAY_METRICS_MAX_SERIES`, `RELAY_TRACE_SAMPLE_RATIO`, `OTEL_EXPORTER_OTLP_ENDPOINT`.

Repo profile tips for maintainers: [docs/github-profile.md](docs/github-profile.md).

## License

[MIT](LICENSE)
