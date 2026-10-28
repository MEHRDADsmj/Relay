# Relay — Phase 5 Implementation Plan: Dashboard, Performance, Polish

> Audience: autonomous coding agents. Prerequisite: Phases 1–4 complete and green.
> Phase 1 §1, §2, §14, §15 still apply. When something is ambiguous, **stop and ask**.

---

## 0. Goals

- **Web dashboard** (server-rendered, no Node toolchain required to build or run):
  queues, jobs, dead letters, schedules, live updates
- **Throughput stats** independent of Prometheus (dashboard works with zero extra infra)
- **Benchmark harness** (`relay-bench`) measuring throughput and end-to-end latency
  percentiles for both backends
- **k6 load tests** for the HTTP API
- **One profiling-driven optimization**, documented with before/after numbers
- **Portfolio polish:** README, demo GIFs, architecture docs, contributor docs

---

## 1. New dependencies

| Concern | Choice | Notes |
|---|---|---|
| Templates | `github.com/a-h/templ` | Type-safe Go templates; `templ` as Go tool; generated `*_templ.go` committed |
| Interactivity | HTMX 2.x + `htmx-ext-sse` | Vendored as static files (no CDN at runtime) |
| CSS | Tailwind CSS v4 **standalone CLI** (no Node) | Compiled `app.css` committed; `make css` rebuilds; CLI binary downloaded by Makefile, checksum-verified |
| Static assets | `embed.FS` | Single binary |
| Latency histograms | `github.com/HdrHistogram/hdrhistogram-go` | bench harness only |
| Load testing | k6 (run via `grafana/k6` Docker image) | no local install required |

---

## 2. Stats for the dashboard

Prometheus is optional, so the dashboard needs its own lightweight throughput data.

### 2.1 Domain / app

```go
type ThroughputPoint struct {
    Minute     time.Time // truncated to minute, UTC
    Enqueued   int64
    Succeeded  int64
    Failed     int64     // failed attempts (retries)
    Dead       int64
}

// Counters are written by the existing JobRepository/MaintenanceRepository write paths,
// inside the same atomic operation as the state change; this port only reads them.
type StatsRepository interface {
    Throughput(ctx context.Context, queue string, from, to time.Time) ([]domain.ThroughputPoint, error)
}
```

`QueueService.Throughput(ctx, queue, window)` with window ∈ {15m, 1h, 24h}; missing
minutes are zero-filled in the app layer.

### 2.2 Storage

- **Postgres:** migration `00005_queue_stats.sql` — table
  `queue_stats_minutely(queue text, minute timestamptz, enqueued bigint, succeeded bigint,
  failed bigint, dead bigint, PRIMARY KEY(queue, minute))`. Existing write paths
  (`Create`, `Complete`, `Fail`, Claim's dead-marking, reaper) upsert
  `... ON CONFLICT DO UPDATE SET x = x + 1` in the same transaction. Purge deletes rows
  older than `RELAY_STATS_RETENTION` (default 48h) in the maintenance runner.
  Benchmark the overhead in P5-T5 and record it; if > 10% throughput loss, switch to
  in-memory aggregation flushed every 5s per instance (document whichever is chosen in
  an ADR).
- **Redis:** hash `relay:stats:{queue}:{minute_unix}` with fields per counter, `HINCRBY`
  inside existing scripts, `EXPIRE` 48h.
- Contract tests: counters correct after scripted operations; zero-fill handled in app.

---

## 3. Dashboard (`internal/adapter/web`)

A third inbound adapter. It calls the same app services as REST/gRPC — **no direct
storage access, no business logic in handlers or templates**.

### 3.1 Pages

| Route | Content |
|---|---|
| `GET /ui/` | Overview: totals across queues, per-queue table (pending, scheduled, running, dead, oldest pending age, paused badge), throughput sparkline per queue (inline SVG rendered server-side from `ThroughputPoint`s — no chart library), leader instance |
| `GET /ui/queues/{queue}` | Queue detail: stats cards, throughput chart (15m/1h/24h tabs), pause/resume button, tabs for jobs by status (paginated with keyset cursor), "Retry all dead" with confirm dialog |
| `GET /ui/jobs/{id}` | Job detail: fields, pretty-printed payload (syntax highlighted via CSS only), attempts timeline with errors, actions (cancel, retry) conditional on status, link to Jaeger trace if `RELAY_UI_TRACE_URL_TEMPLATE` set (e.g. `http://localhost:16686/trace/{trace_id}`) |
| `GET /ui/schedules` | Schedules table: name, cron (with human-readable description), timezone, next/last run, enabled toggle, "Trigger now" |
| `GET /ui/schedules/{id}` | Schedule detail + recent jobs (by idempotency-key prefix — add `ListJobsBySchedule` app method backed by a `schedule_id` column/field set on fired jobs; include in migration 00005 and Redis index `relay:schedule:{id}:jobs` zset, capped at 100) |
| `GET /ui/login`, `POST /ui/login`, `POST /ui/logout` | Session auth |
| `GET /ui/events` | SSE stream for live updates |

All pages: responsive (usable at 375px width), dark/light via `prefers-color-scheme`,
accessible (semantic HTML, labels, focus states, `aria-live` for live regions, keyboard
navigable), empty states with helpful copy (e.g. "No dead jobs — nice." with a link to
docs), error page for 404/500.

### 3.2 Interactivity

- HTMX for actions (`hx-post` returning the updated fragment) and pagination
  (`hx-get` + `hx-push-url`).
- Live updates via SSE (`/ui/events?queue=...`): the server pushes rendered fragments
  (stats cards, queue table rows) every 2s **only while clients are connected**;
  implementation: one ticker per connection, fragments rendered from `QueueService.Stats`
  (already cached in Phase 4 collector — reuse the same cache via an app-level
  `CachedQueueStats` decorator, not via the metrics adapter). Respect client disconnect
  and server shutdown; `goleak` test.
- Every page fully works without JavaScript for read-only views (progressive
  enhancement); actions require JS (document).

### 3.3 Auth and security

- Login form takes an API key with `admin` scope (or a dedicated `dashboard` scope —
  add `ScopeDashboardRead` and use `admin` for actions; update the scope matrix).
- Session: signed + encrypted cookie (AES-GCM, key from `RELAY_UI_SESSION_KEY`,
  32 bytes base64; generated and logged as a warning if unset in development, required
  when `RELAY_ENVIRONMENT=production`). Cookie flags: `HttpOnly`, `SameSite=Lax`,
  `Secure` when served over HTTPS (`RELAY_UI_SECURE_COOKIES`, default true in production),
  `Path=/ui`, 12h expiry. Store only key ID + expiry — re-validate the key via
  `AuthService` on each request (cached), so revocation applies.
- CSRF: per-session token in a hidden field / `hx-headers` meta tag, verified on all
  non-GET requests (double-submit not used; synchronizer token from the session).
- Security headers on `/ui`: strict CSP (`default-src 'self'; script-src 'self';
  style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'`),
  `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`.
- Login rate limit: 5 attempts/min per IP (in-memory token bucket, `golang.org/x/time/rate`).
- Templ auto-escapes; payload display must never use raw HTML.
- Dashboard can be disabled: `RELAY_UI_ENABLED` (default `true`).

### 3.4 Tests

- Handler tests with `httptest` + memory store: each page renders (200) with seeded data
  and empty data; actions mutate state and return the right fragment; CSRF rejection;
  auth redirect; scope enforcement.
- Template snapshot tests for key components (golden HTML with stable IDs/timestamps
  via fake clock).
- A Playwright-free smoke test: `go test -tags=e2e` hits `/ui/` through the full stack
  and parses HTML with `golang.org/x/net/html` to assert queue rows exist.

---

## 4. Performance work

### 4.1 Benchmark harness (`cmd/relay-bench`)

```
relay-bench --server http://localhost:7420 --transport http|grpc \
            --producers 4 --workers 4 --concurrency 50 \
            --jobs 100000 --payload-bytes 512 --handler-sleep 0 \
            --queue bench-<random> --warmup 5s --output text|json
```

Measures and reports:

- Enqueue throughput (jobs/s) and enqueue latency p50/p90/p99/max
- Processing throughput (completions/s)
- End-to-end latency (enqueue accepted → handler start) p50/p90/p99/max, using
  timestamps embedded in the payload and HDR histograms
- Errors by type
- Environment block: Go version, GOMAXPROCS, CPU model, storage backend, server version

Uses the SDK only. Unique queue per run; optional `--cleanup` purges it afterwards via
admin API.

### 4.2 Go micro-benchmarks

`Benchmark*` functions for: domain validation, JSON mapping in HTTP adapter, Redis claim
script (integration tag), Postgres claim query (integration tag), SDK dispatch loop with
a fake transport. Run with `-benchmem`; results recorded with `benchstat`.

### 4.3 k6 load tests (`loadtest/k6/`)

- `enqueue.js`: ramp to N VUs posting jobs; thresholds `http_req_failed < 0.1%`,
  `p(95) < 50ms` at the documented rate.
- `mixed.js`: producers + simulated workers (lease/complete loop over HTTP).
- `make loadtest` runs k6 in Docker against the compose stack (`--network host` or the
  compose network) and writes a summary JSON to `loadtest/results/`.

### 4.4 Profiling-driven optimization (one, documented)

Process: run `relay-bench` against both backends → capture CPU and allocation profiles
from the admin port → identify the top hotspot → implement **one** targeted
optimization → re-run → record before/after in `docs/benchmarks.md` with the profiles'
top entries.

Candidate optimizations (pick based on data, don't pre-commit):

- **Batch acknowledgements:** `POST /v1/jobs/complete:batch` + SDK coalescing completes
  within a 5ms window (adds endpoint/RPC + contract method `CompleteMany`).
- Pre-allocated buffers / `sync.Pool` for JSON encoding in hot handlers.
- Postgres: prepared statement caching tuning, `claim` index adjustments verified with
  `EXPLAIN (ANALYZE, BUFFERS)` output included in the doc.
- Redis: pipelining heartbeats per worker.

Any new endpoint follows the full layer rules and gets contract + e2e tests.

### 4.5 Results document (`docs/benchmarks.md`)

Tables for both backends × both transports: enqueue throughput, processing throughput,
e2e latency percentiles, with hardware/software environment, exact commands, and a
"how to reproduce" section. Honest commentary on bottlenecks and trade-offs
(e.g. Postgres vs Redis durability/throughput).

---

## 5. Portfolio polish

- **README overhaul:** hero section (one-line pitch + badges + dashboard screenshot),
  "Why Relay" bullets, 60-second quickstart, feature matrix by phase, architecture
  diagram (Mermaid: layers + runtime topology), delivery guarantees, benchmarks summary
  table linking to `docs/benchmarks.md`, links to ADR index, roadmap, license.
- **Demo media** in `docs/media/` (committed, size-optimized): dashboard GIF (enqueue
  burst → live updates → dead job retry), `relayctl` terminal recording (asciinema →
  GIF via `agg`), Grafana and Jaeger screenshots.
- `docs/architecture.md`: layer diagram, package map, request lifecycle walkthroughs
  (enqueue, lease long-poll, failure → retry → dead, cron fire with failover), storage
  data models for both backends side by side.
- `docs/adr/README.md`: ADR index table.
- `CONTRIBUTING.md`: setup, make targets, commit conventions, how to add a storage
  backend (pointing at the contract suite), how to add an endpoint (spec → generate →
  handler → test).
- GitHub repo metadata recommendations for the owner (topics, description, social
  preview image) in `docs/github-profile.md` — agents do not change repo settings.
- `examples/`: add an `examples/http-curl/README.md` showing the API with curl only (for
  non-Go readers).

---

## 6. Config additions

| Env var | Default |
|---|---|
| `RELAY_UI_ENABLED` | `true` |
| `RELAY_UI_SESSION_KEY` | empty (dev: generated + warning; prod: required) |
| `RELAY_UI_SECURE_COOKIES` | `false` in development, `true` in production |
| `RELAY_UI_TRACE_URL_TEMPLATE` | empty |
| `RELAY_STATS_RETENTION` | `48h` |

---

## 7. Documentation (ADRs)

- ADR `0019-server-rendered-dashboard-templ-htmx.md`
- ADR `0020-dashboard-stats-without-prometheus.md`
- ADR `0021-dashboard-session-security.md`
- ADR `0022-<chosen-optimization>.md`

---

## 8. Tasks

Definition of done: as Phase 1 §13.

### P5-T1 — Throughput stats (domain/app/memory/contract + both stores)
Depends on: Phase 4. Deliverables: §2 and schedule-job linkage from §3.1.
Acceptance: contract suite passes for all stores; zero-fill tested.

### P5-T2 — Web adapter foundation
Depends on: P5-T1. Deliverables: templ/Tailwind/HTMX setup, `make css`, `make generate`
includes `templ generate`, layout, auth/session/CSRF/security headers, login/logout,
error pages. Acceptance: security tests (§3.3) pass; `make check-generated` covers templ
and CSS output.

### P5-T3 — Dashboard pages + SSE
Depends on: P5-T2. Deliverables: §3.1, §3.2, §3.4. Acceptance: all handler and snapshot
tests pass; manual check at 375px and 1440px recorded as screenshots in `docs/media/`.

### P5-T4 — Benchmark harness + micro-benchmarks + k6
Depends on: Phase 4 (parallel with P5-T1..T3). Deliverables: §4.1–§4.3.
Acceptance: `relay-bench` runs against compose for both backends/transports and prints
the full report; `make loadtest` produces a summary.

### P5-T5 — Profiling-driven optimization + benchmarks doc
Depends on: P5-T1, P5-T4. Deliverables: §4.4, §4.5, stats-overhead measurement from
§2.2. Acceptance: before/after numbers show a measurable improvement on the targeted
metric with no regression elsewhere beyond noise (benchstat p < 0.05).

### P5-T6 — Portfolio polish
Depends on: P5-T3, P5-T5. Deliverables: §5, §7. Acceptance: README quickstart verified
on a clean machine with only Docker; all doc links resolve (CI link check with
`lychee` via Docker).

```
P5-T1 → P5-T2 → P5-T3 ─────────┐
   └──────────┐                ├─ P5-T6
P5-T4 ────────┴─ P5-T5 ────────┘
```
