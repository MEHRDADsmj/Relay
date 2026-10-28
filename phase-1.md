# Relay — Phase 1 Implementation Plan

> Audience: autonomous coding agents. Follow this document literally. When something
> is ambiguous, **stop and ask** instead of guessing. Agents **may commit** in this
> repository following §15 (Git rules); they must **not push**, rebase, amend, or
> force-push unless the repository owner explicitly asks.

---

## 0. Context

**Relay** is a durable, distributed job queue written in Go. Producers enqueue jobs over
HTTP; workers (built with the Relay Go SDK) lease jobs, execute them, heartbeat while
running, and report success or failure. Storage is pluggable: **PostgreSQL** and
**Redis** backends implement the same port and pass the same contract test suite.

The repository is a portfolio piece. Code quality, structure, tests, and docs matter as
much as features. Every decision below is chosen to mirror what production Go teams
actually use.

### Phase 1 scope (this document)

In scope:

- Domain model and job state machine
- Application layer (use cases) with ports
- PostgreSQL storage adapter
- Redis storage adapter
- Shared storage contract test suite
- REST API (spec-first OpenAPI) for producers and workers
- `relay-server` binary with config, structured logging, health checks, graceful shutdown
- Go SDK: client + generic worker with concurrency, heartbeats, graceful shutdown
- Example producer and worker
- Dockerfile, docker-compose (full stack), Makefile, golangci-lint, GitHub Actions CI
- README and ADRs

Out of scope (later phases — do **not** implement, but do not design them out):

- Exponential backoff (phase 1 uses a fixed retry delay behind a `RetryPolicy` port)
- Separate reaper process, dead-letter replay, LISTEN/NOTIFY wakeups (phase 2)
- Cron scheduler, leader election, gRPC, `relayctl` CLI (phase 3)
- Prometheus metrics, OpenTelemetry, Helm (phase 4)
- Dashboard, load tests (phase 5)
- API key management (phase 1 has a single optional static key)
- Redis Cluster support (phase 1 targets a single Redis node)

---

## 1. Technology choices (fixed)

| Concern | Choice | Notes |
|---|---|---|
| Go | **1.25** (`go 1.25` + `toolchain go1.25.x` in `go.mod`) | Uses `tool` directive, `log/slog`, `net/http` patterns |
| HTTP router | `github.com/go-chi/chi/v5` | Most common router in Go shops |
| API contract | OpenAPI 3.0 + `github.com/oapi-codegen/oapi-codegen/v2` (chi strict server + models) | Spec-first; generated code is committed |
| Postgres driver | `github.com/jackc/pgx/v5` (`pgxpool`) | |
| SQL | `sqlc` (`github.com/sqlc-dev/sqlc`) | Generated code is committed |
| Migrations | `github.com/pressly/goose/v3` with `embed.FS` | Run via `relay-server migrate` or on start |
| Redis | `github.com/redis/go-redis/v9` | Atomic ops via Lua scripts (`redis.NewScript`) |
| IDs | `github.com/google/uuid` — **UUIDv7** (`uuid.NewV7`) | Time-ordered |
| Config | `github.com/caarlos0/env/v11` | Env vars only (12-factor) |
| Logging | `log/slog` (JSON handler) | No third-party logger |
| Concurrency helpers | `golang.org/x/sync/errgroup` | |
| Testing | stdlib `testing` + `github.com/stretchr/testify` (`require`, `assert`) | |
| Integration tests | `github.com/testcontainers/testcontainers-go` (+ `modules/postgres`, `modules/redis`) | Requires Docker |
| Leak detection | `go.uber.org/goleak` | In SDK and server tests |
| Lint | `golangci-lint` **v2** with `depguard` layer rules | |
| Container | Multi-stage Dockerfile → `gcr.io/distroless/static-debian12:nonroot` | |
| CI | GitHub Actions | |

Dev tools (`sqlc`, `oapi-codegen`, `goose`) are pinned with the Go `tool` directive
(`go get -tool <pkg>@<version>`) and invoked via `go tool <name>`. `golangci-lint` is
installed as a binary (the officially recommended way) — the Makefile target prints
install instructions if it is missing.

**Module path:** `github.com/OWNER/relay` — replace `OWNER` with the repository owner's
GitHub username before starting (ask if unknown).

---

## 2. Clean Architecture rules (enforced)

Dependencies point **inward only**:

```
cmd/*  ──►  adapters (http, postgres, redis)  ──►  app (use cases + ports)  ──►  domain
                                     composition root wires everything in cmd/
pkg/sdk  ──►  (HTTP contract only; never imports internal/)
```

| Layer | Package(s) | May import | Must NOT import |
|---|---|---|---|
| Domain | `internal/domain` | stdlib, `github.com/google/uuid` | anything else in module, any infra lib |
| Application | `internal/app` | `internal/domain`, stdlib | `internal/adapter/...`, `net/http`, pgx, redis, chi |
| Adapters | `internal/adapter/...` | `internal/app`, `internal/domain`, infra libs | other adapters (e.g. `http` must not import `postgres`) |
| Platform | `internal/platform/...` (config, logging) | stdlib, config/logging libs | domain/app/adapters |
| Composition root | `cmd/relay-server` | everything | — |
| SDK | `pkg/sdk` | stdlib only (+ `uuid` allowed) | `internal/...` |

Rules:

1. **Ports are defined by the consumer** — repository interfaces live in `internal/app`,
   not in adapters.
2. **No infra types cross a port.** No `pgx.Row`, `redis.Cmd`, `*http.Request` in `app`
   or `domain` signatures.
3. **Adapters map** between their own DTOs/rows and domain types. Generated code
   (sqlc, oapi-codegen) never leaks outside its adapter package.
4. **Time and IDs are injected** (`Clock`, `IDGenerator` ports). Storage never calls
   `now()` itself; the app layer passes `now` into every time-sensitive repo call.
   This keeps Postgres and Redis behavior identical and testable.
5. **Errors:** domain defines sentinel errors; adapters translate infra errors into them;
   the HTTP adapter maps them to status codes. Use `fmt.Errorf("...: %w", err)` for context.
6. Enforced by `depguard` in `.golangci.yml` (see Task T0). CI fails on violations.

---

## 3. Repository layout (target)

```
relay/
├── api/
│   └── openapi.yaml                    # source of truth for the HTTP API
├── cmd/
│   └── relay-server/
│       └── main.go                     # composition root; subcommands: serve (default), migrate
├── internal/
│   ├── domain/
│   │   ├── job.go                      # Job entity, Status, state transitions
│   │   ├── validation.go               # queue name, priority, limits
│   │   ├── errors.go                   # sentinel errors
│   │   └── *_test.go
│   ├── app/
│   │   ├── ports.go                    # JobRepository, Clock, IDGenerator, RetryPolicy
│   │   ├── service.go                  # JobService use cases
│   │   ├── commands.go                 # input structs (EnqueueCommand, LeaseCommand, ...)
│   │   ├── retry.go                    # FixedDelayRetryPolicy
│   │   ├── clock.go                    # SystemClock, UUIDv7Generator
│   │   └── *_test.go                   # uses in-memory fake repo
│   ├── adapter/
│   │   ├── storage/
│   │   │   ├── storagetest/            # shared contract test suite (exported RunContractTests)
│   │   │   ├── memory/                 # in-memory repo (used by app tests + contract suite)
│   │   │   ├── postgres/
│   │   │   │   ├── migrations/         # goose SQL files (embedded)
│   │   │   │   ├── queries/            # sqlc .sql query files
│   │   │   │   ├── sqlcgen/            # sqlc output (committed, do not edit)
│   │   │   │   ├── repository.go
│   │   │   │   ├── migrate.go
│   │   │   │   └── repository_test.go  # //go:build integration
│   │   │   └── redis/
│   │   │       ├── scripts/            # .lua files (embedded)
│   │   │       ├── repository.go
│   │   │       ├── keys.go
│   │   │       └── repository_test.go  # //go:build integration
│   │   └── http/
│   │       ├── oapi/                   # oapi-codegen output (committed, do not edit)
│   │       ├── handler.go              # implements oapi StrictServerInterface
│   │       ├── mapping.go              # domain <-> API DTOs
│   │       ├── errors.go               # domain error -> RFC 9457 problem+json
│   │       ├── middleware.go           # request ID, access log, auth, body limit, recover
│   │       ├── router.go
│   │       └── *_test.go               # httptest with memory repo
│   └── platform/
│       ├── config/config.go
│       └── logging/logging.go
├── pkg/
│   └── sdk/
│       ├── client.go                   # producer + low-level worker API calls
│       ├── worker.go                   # Worker, Handle[T], run loop
│       ├── types.go                    # public SDK types
│       ├── errors.go
│       └── *_test.go
├── examples/
│   ├── producer/main.go
│   └── worker/main.go
├── test/
│   └── e2e/e2e_test.go                 # //go:build e2e — full stack via testcontainers
├── docs/
│   ├── adr/0001-*.md ...
│   └── plan/phase-1.md                 # this file
├── deploy/
│   └── docker/Dockerfile
├── .github/workflows/ci.yml
├── docker-compose.yml
├── .golangci.yml
├── sqlc.yaml
├── oapi-codegen.yaml
├── Makefile
├── .env.example
├── .editorconfig
├── .gitignore
├── LICENSE                             # MIT
└── README.md
```

---

## 4. Domain specification

### 4.1 Job entity

```go
type JobID = uuid.UUID

type Status string
const (
    StatusPending   Status = "pending"
    StatusRunning   Status = "running"
    StatusSucceeded Status = "succeeded"
    StatusDead      Status = "dead"
)

type Job struct {
    ID             JobID
    Queue          string
    Type           string
    Payload        []byte      // raw JSON, opaque to Relay
    Status         Status
    Priority       int         // -100..100, higher runs first
    RunAt          time.Time   // earliest time it may be leased
    Attempts       int         // incremented on every lease
    MaxAttempts    int
    LeaseOwner     string      // worker ID; empty unless running
    LeaseExpiresAt *time.Time  // nil unless running
    IdempotencyKey string      // optional
    LastError      string
    CreatedAt      time.Time
    UpdatedAt      time.Time
    FinishedAt     *time.Time  // set on succeeded/dead
}
```

### 4.2 State machine

```
            enqueue
               │
               ▼
          ┌─────────┐   lease (attempts++)    ┌─────────┐
          │ pending │ ──────────────────────► │ running │
          └─────────┘                          └─────────┘
               ▲   fail, attempts < max            │   │  complete
               │   (run_at = now + retryDelay)     │   └──────────► succeeded
               └───────────────────────────────────┤
                  lease expired, attempts < max     │  fail, attempts >= max
                  (re-leasable directly)            │  lease expired, attempts >= max
                                                    └──────────────► dead
```

Domain methods (pure, no I/O) return a new state or a domain error. They are used by the
app layer to **decide**; repositories **apply** decisions atomically with conditional
writes.

- `NewJob(params NewJobParams, id JobID, now time.Time) (Job, error)` — validates and defaults.
- `(j Job) DecideFailure(now time.Time, retryDelay time.Duration) FailureDecision` —
  returns `{NextStatus: pending|dead, NextRunAt *time.Time}`.
- `(j Job) EnsureLeasedBy(owner string, now time.Time) error` — `ErrLeaseLost` if not
  running, owner mismatch, or lease expired.

### 4.3 Validation and defaults

| Field | Rule | Default |
|---|---|---|
| `Queue` | regex `^[a-z0-9][a-z0-9_-]{0,63}$` | required |
| `Type` | regex `^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$` | required |
| `Payload` | valid JSON, ≤ 256 KiB | `{}` |
| `Priority` | -100..100 | 0 |
| `RunAt` | any; past values allowed (treated as now) | now |
| `MaxAttempts` | 1..100 | 10 |
| `IdempotencyKey` | ≤ 255 chars, printable ASCII | empty |
| Lease duration | 5s..15m | 30s |
| Lease batch size (`max_jobs`) | 1..100 | 1 |
| Worker ID | 1..128 chars, printable ASCII | required |
| Error message on fail | truncated to 4096 bytes | — |

### 4.4 Sentinel errors (`internal/domain/errors.go`)

`ErrNotFound`, `ErrValidation` (wrap with field detail via a `ValidationError` type that
`Unwrap`s to `ErrValidation`), `ErrLeaseLost`, `ErrConflict` (reserved).

---

## 5. Application layer specification

### 5.1 Ports (`internal/app/ports.go`)

```go
type Clock interface { Now() time.Time }
type IDGenerator interface { NewID() domain.JobID }
type RetryPolicy interface { NextDelay(attempt int) time.Duration }

type JobRepository interface {
    // Create inserts the job. If IdempotencyKey is non-empty and a job with the same
    // (queue, key) exists, it returns the EXISTING job and created=false.
    Create(ctx context.Context, job domain.Job) (stored domain.Job, created bool, err error)

    Get(ctx context.Context, id domain.JobID) (domain.Job, error) // ErrNotFound

    // Claim atomically, for one queue:
    //  1. marks dead: running jobs with lease_expires_at <= now AND attempts >= max_attempts
    //     (last_error = "lease expired", finished_at = now)
    //  2. selects up to limit jobs that are either
    //       (status=pending AND run_at <= now) OR
    //       (status=running AND lease_expires_at <= now AND attempts < max_attempts)
    //     ordered by priority DESC, run_at ASC, id ASC
    //  3. sets status=running, lease_owner=owner, lease_expires_at=now+lease,
    //     attempts=attempts+1, updated_at=now
    // Concurrent callers must never receive the same job.
    Claim(ctx context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error)

    // ExtendLease sets lease_expires_at=now+lease iff status=running AND lease_owner=owner
    // AND lease_expires_at > now. Otherwise ErrLeaseLost (or ErrNotFound).
    ExtendLease(ctx context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error)

    // Complete sets status=succeeded, finished_at=now, clears lease, under the same
    // ownership condition as ExtendLease.
    Complete(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error)

    // Fail applies a FailureDecision under the same ownership condition:
    //   pending -> run_at = decision.NextRunAt, lease cleared
    //   dead    -> finished_at = now, lease cleared
    // last_error = errMsg in both cases.
    Fail(ctx context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error)

    // Ping checks connectivity (used by /readyz).
    Ping(ctx context.Context) error
}
```

### 5.2 `JobService` use cases

```go
func NewJobService(repo JobRepository, clock Clock, ids IDGenerator, retry RetryPolicy, log *slog.Logger) *JobService

func (s *JobService) Enqueue(ctx, EnqueueCommand) (job domain.Job, created bool, err error)
func (s *JobService) Get(ctx, id) (domain.Job, error)
func (s *JobService) Lease(ctx, LeaseCommand) ([]domain.Job, error)
func (s *JobService) Heartbeat(ctx, HeartbeatCommand) (domain.Job, error)
func (s *JobService) Complete(ctx, CompleteCommand) (domain.Job, error)
func (s *JobService) Fail(ctx, FailCommand) (domain.Job, error)
func (s *JobService) Ready(ctx) error
```

`Fail` flow: `repo.Get` → `job.EnsureLeasedBy(owner, now)` → `job.DecideFailure(now,
retry.NextDelay(job.Attempts))` → `repo.Fail(...)` (conditional write re-checks
ownership, so a lease lost between Get and Fail yields `ErrLeaseLost`, never a corrupt
state). Attempts cannot change while the caller still owns the lease, so the decision is
safe.

`FixedDelayRetryPolicy{Delay: cfg.RetryDelay}` is the only phase 1 implementation.

### 5.3 Tests

- Table-driven tests for every use case against `memory` repo + fake clock + fixed IDs.
- Coverage target for `internal/domain` and `internal/app`: **≥ 90%**.

---

## 6. Storage adapters

### 6.1 Shared contract suite (`internal/adapter/storage/storagetest`)

`func RunContractTests(t *testing.T, newRepo func(t *testing.T) app.JobRepository)`

Each subtest gets a fresh, empty repository. Required cases (minimum):

1. Create + Get round-trip preserves every field (times compared at millisecond precision, UTC).
2. Get unknown ID → `ErrNotFound`.
3. Idempotency: second Create with same (queue, key) returns the first job, `created=false`;
   same key on a different queue creates a new job.
4. Claim respects `run_at` (future jobs not returned).
5. Claim ordering: priority DESC, then run_at ASC, then id ASC.
6. Claim respects `limit` and queue isolation.
7. Claim increments attempts and sets lease fields.
8. Expired lease with attempts < max is re-claimable by another owner; attempts increments.
9. Expired lease with attempts >= max is marked dead on next Claim and not returned.
10. ExtendLease / Complete / Fail by wrong owner → `ErrLeaseLost`.
11. ExtendLease / Complete / Fail after lease expiry → `ErrLeaseLost`.
12. Complete sets succeeded + finished_at and clears lease.
13. Fail with pending decision → re-claimable only after NextRunAt.
14. Fail with dead decision → dead, finished_at set, never claimable.
15. **Concurrency:** 500 jobs, 20 goroutines claiming in batches of 5 with distinct owners
    → every job claimed exactly once (run with `-race`).

The `memory` repo must pass this suite too (run in normal unit tests). Postgres and Redis
run it under `//go:build integration` using testcontainers.

### 6.2 PostgreSQL adapter

Migration `00001_create_jobs.sql`:

```sql
-- +goose Up
CREATE TABLE jobs (
    id               uuid PRIMARY KEY,
    queue            text        NOT NULL,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL,
    status           text        NOT NULL CHECK (status IN ('pending','running','succeeded','dead')),
    priority         smallint    NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    run_at           timestamptz NOT NULL,
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    lease_owner      text,
    lease_expires_at timestamptz,
    idempotency_key  text,
    last_error       text,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    finished_at      timestamptz
);
CREATE UNIQUE INDEX jobs_idempotency_uq ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_claimable_pending ON jobs (queue, priority DESC, run_at, id)
    WHERE status = 'pending';
CREATE INDEX jobs_running_lease ON jobs (queue, lease_expires_at)
    WHERE status = 'running';
-- +goose Down
DROP TABLE jobs;
```

Implementation notes:

- `Create`: `INSERT ... ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING *`;
  if no row returned, `SELECT` the existing job → `created=false`.
- `Claim`: one transaction: (1) dead-marking `UPDATE`, (2) claim using
  `UPDATE jobs SET ... WHERE id IN (SELECT id FROM jobs WHERE <eligibility> ORDER BY priority DESC, run_at, id FOR UPDATE SKIP LOCKED LIMIT $n) RETURNING *`,
  then sort returned rows in Go by the same ordering (RETURNING order is unspecified).
- Conditional writes (`ExtendLease`, `Complete`, `Fail`): `UPDATE ... WHERE id=$1 AND status='running' AND lease_owner=$2 AND lease_expires_at > $now RETURNING *`;
  zero rows → `SELECT` to distinguish `ErrNotFound` vs `ErrLeaseLost`.
- All `now` values come from parameters, never SQL `now()`.
- `pgxpool` configured from `RELAY_POSTGRES_URL`; `Ping` uses `pool.Ping`.
- Migrations embedded with `//go:embed migrations/*.sql`, applied with goose's
  `goose.NewProvider(goose.DialectPostgres, db, fsys)` using `pgx/v5/stdlib`.

### 6.3 Redis adapter

Single-node Redis 7+. All state mutations happen inside Lua scripts for atomicity.

Keys (prefix configurable, default `relay`):

| Key | Type | Contents |
|---|---|---|
| `relay:job:{id}` | hash | all job fields (times as Unix ms strings, payload as JSON string) |
| `relay:q:{queue}:scheduled` | zset | pending jobs with `run_at > now`; score = run_at ms |
| `relay:q:{queue}:ready` | zset | pending jobs due; score = `(100 - priority) * 1e13 + run_at_ms` |
| `relay:q:{queue}:leased` | zset | running jobs; score = lease_expires_at ms |
| `relay:idem:{queue}:{key}` | string | job id (no TTL in phase 1) |

Score note: `run_at_ms < 1e13` until year 2286 and the maximum score is `2e15 < 2^53`,
so scores are exact in float64. Lower score = claimed first, giving priority DESC,
run_at ASC. Ties are broken by zset member (job id, UUIDv7 = time-ordered) — acceptable
and matches `id ASC`.

Scripts (`scripts/*.lua`, embedded, loaded with `redis.NewScript`):

- `enqueue.lua` — if idem key present and `SET NX` fails, return existing id; else
  `HSET` job, add to `scheduled` or `ready` depending on `run_at <= now`.
- `claim.lua` — args: now, owner, limit, lease_ms. Steps: (a) move due members from
  `scheduled` to `ready` (computing score from the job hash priority); (b) for members of
  `leased` with score `<= now`: if attempts >= max → mark dead, `ZREM`; else move back
  into `ready` with score based on original run_at/priority; (c) `ZRANGE ready 0 limit-1`,
  for each: `ZREM ready`, `ZADD leased`, `HINCRBY attempts`, `HSET` status/lease fields;
  return the hashes.
- `extend.lua`, `complete.lua`, `fail.lua` — check status/owner/expiry in the hash,
  return an error code `LEASE_LOST` / `NOT_FOUND` that Go maps to domain errors.

Documented limitation (ADR): scripts touch keys not declared in `KEYS[]`, which is fine on
a single node but not Redis Cluster–safe; cluster support is a non-goal for phase 1.
Durability: compose runs Redis with `--appendonly yes --appendfsync everysec`.

---

## 7. HTTP API

Spec-first: write `api/openapi.yaml`, generate with `oapi-codegen` (strict chi server +
models) into `internal/adapter/http/oapi`. Handlers implement the strict interface and map
to/from domain via `mapping.go`.

| Method | Path | Purpose | Success |
|---|---|---|---|
| POST | `/v1/jobs` | Enqueue | `201` new job; `200` + header `Idempotent-Replayed: true` on idempotent hit |
| GET | `/v1/jobs/{id}` | Get job | `200` |
| POST | `/v1/queues/{queue}/leases` | Lease up to `max_jobs` | `200 {"jobs":[...]}` (empty array if none) |
| POST | `/v1/jobs/{id}/heartbeat` | Extend lease | `200` job |
| POST | `/v1/jobs/{id}/complete` | Mark succeeded | `200` job |
| POST | `/v1/jobs/{id}/fail` | Report failure | `200` job (status pending or dead) |
| GET | `/healthz` | Liveness (process up) | `200 {"status":"ok"}` |
| GET | `/readyz` | Readiness (`repo.Ping`) | `200` or `503` |

Request bodies:

- Enqueue: `{queue, type, payload?, priority?, run_at?, max_attempts?, idempotency_key?}`
- Lease: `{worker_id, max_jobs?, lease_seconds?}`
- Heartbeat: `{worker_id, lease_seconds?}`
- Complete: `{worker_id}`
- Fail: `{worker_id, error}`

Job response: all entity fields in snake_case; `payload` as raw JSON; times RFC 3339 UTC.

Errors use **RFC 9457** `application/problem+json`:
`ErrValidation → 400` (include `errors: [{field, message}]`), `ErrNotFound → 404`,
`ErrLeaseLost → 409`, body too large → `413`, unauthenticated → `401`, other → `500`
(generic message; details only in logs).

Middleware (order): request ID (`X-Request-ID`, generate if absent) → recoverer →
slog access log (method, path, status, duration, request_id) → body limit (1 MiB) →
auth (if `RELAY_API_KEY` set: require `Authorization: Bearer <key>` with
`crypto/subtle.ConstantTimeCompare`; `/healthz` and `/readyz` are exempt).

Tests: `httptest` against the real router with the `memory` repo — cover every endpoint,
every error mapping, auth on/off, idempotent replay header.

---

## 8. `relay-server` (composition root)

`cmd/relay-server/main.go`:

- Subcommands via stdlib `flag`: `serve` (default) and `migrate up|down|status`
  (Postgres only; no-op with a log line for Redis).
- Load config → build logger → build repo by `RELAY_STORAGE` → build service → build
  router → run.
- `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)`; `errgroup` runs the HTTP
  server; on cancellation call `server.Shutdown` with `RELAY_SHUTDOWN_TIMEOUT`; then close
  the pool/client. Exit code non-zero on startup failure.
- `http.Server` with `ReadHeaderTimeout` 5s, `ReadTimeout` 15s, `WriteTimeout` 15s,
  `IdleTimeout` 60s.

Configuration (`internal/platform/config`):

| Env var | Default | Notes |
|---|---|---|
| `RELAY_HTTP_ADDR` | `:7420` | |
| `RELAY_STORAGE` | `postgres` | `postgres` or `redis` |
| `RELAY_POSTGRES_URL` | — | required when storage=postgres |
| `RELAY_REDIS_URL` | — | required when storage=redis (`redis://host:6379/0`) |
| `RELAY_REDIS_KEY_PREFIX` | `relay` | |
| `RELAY_MIGRATE_ON_START` | `false` | `true` in docker-compose |
| `RELAY_API_KEY` | empty | empty = auth disabled (log a warning) |
| `RELAY_RETRY_DELAY` | `5s` | |
| `RELAY_SHUTDOWN_TIMEOUT` | `15s` | |
| `RELAY_LOG_LEVEL` | `info` | debug/info/warn/error |
| `RELAY_LOG_FORMAT` | `json` | `json` or `text` |

Validation errors are reported all at once on startup.

---

## 9. Go SDK (`pkg/sdk`)

Public API (exact signatures):

```go
type Client struct { /* baseURL, apiKey, *http.Client */ }
func NewClient(baseURL string, opts ...ClientOption) *Client
func WithAPIKey(key string) ClientOption
func WithHTTPClient(c *http.Client) ClientOption

type EnqueueRequest struct {
    Queue, Type    string
    Payload        any           // marshaled to JSON
    Priority       int
    RunAt          time.Time     // zero = now
    MaxAttempts    int           // zero = server default
    IdempotencyKey string
}
func (c *Client) Enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error)
func (c *Client) GetJob(ctx context.Context, id string) (JobInfo, error)

type Job[T any] struct {
    ID       string
    Queue    string
    Type     string
    Attempt  int
    Args     T
}

type WorkerOptions struct {
    Queue           string        // required
    WorkerID        string        // default: hostname-pid-random
    Concurrency     int           // default 10
    LeaseDuration   time.Duration // default 30s
    PollMinInterval time.Duration // default 100ms
    PollMaxInterval time.Duration // default 2s
    ShutdownTimeout time.Duration // default 30s
    Logger          *slog.Logger  // default slog.Default()
}
func NewWorker(c *Client, opts WorkerOptions) (*Worker, error)
func Handle[T any](w *Worker, jobType string, h func(ctx context.Context, job Job[T]) error)
func (w *Worker) Run(ctx context.Context) error
```

Behavior:

1. `Run` loops while `ctx` is not done: compute free slots (semaphore of size
   Concurrency); if > 0, lease `min(free, 100)` jobs. Empty result → sleep with
   exponential backoff from PollMin to PollMax (+ jitter); any result resets backoff.
   Transport errors → log and back off (max 5s); never exit on transient errors.
2. Each job runs in its own goroutine with a context derived from a **worker-scoped**
   context (not `ctx` directly, so shutdown can drain first).
3. Heartbeat ticker every `LeaseDuration/3`; `409` from heartbeat → cancel that job's
   context and do not report complete/fail.
4. Handler result: `nil` → complete; error → fail with `err.Error()`; panic → recover,
   fail with panic value + truncated stack.
5. Unknown job type → fail with `no handler registered for type "<t>"`.
6. Payload unmarshal error into `T` → fail (no handler call).
7. Shutdown: when `ctx` is done, stop leasing, wait for in-flight jobs up to
   `ShutdownTimeout`, then cancel their contexts and wait for goroutines to exit; return
   `nil` on clean drain, `ErrShutdownTimeout` otherwise. No goroutine leaks (`goleak`).
8. Registering handlers after `Run` starts panics with a clear message.

Errors: `*APIError{StatusCode int, Title, Detail string}` parsed from problem+json;
`errors.Is(err, ErrLeaseLost)` works for 409; `ErrNotFound` for 404.

Tests: `httptest.Server` fake implementing the API; cover concurrency limit (never more
than N in flight), heartbeat cadence, lease-lost cancellation, panic recovery, unknown
type, graceful drain, shutdown timeout, backoff reset. Use `testing/synctest` where it
simplifies time-based tests.

---

## 10. Examples

- `examples/worker`: registers `send_welcome_email` (`{user_id, email}`; sleeps
  200–800ms; fails 20% of the time randomly) and `generate_report` (`{report_id}`; sleeps
  1–3s). Config via env: `RELAY_URL`, `RELAY_API_KEY`, `RELAY_QUEUE` (default `default`),
  `WORKER_CONCURRENCY`.
- `examples/producer`: flags `-n` (jobs, default 100), `-queue`, `-rate` (jobs/s,
  default 20); enqueues a mix of both types with idempotency keys; prints IDs.

---

## 11. Tooling, containers, CI

### Makefile targets

| Target | Action |
|---|---|
| `make help` | list targets (default) |
| `make tools` | `go mod download` + verify `golangci-lint` installed |
| `make generate` | `go tool sqlc generate` + `go tool oapi-codegen -config oapi-codegen.yaml api/openapi.yaml` |
| `make lint` | `golangci-lint run ./...` |
| `make test` | `go test -race -count=1 ./...` (unit only; no Docker needed) |
| `make test-integration` | `go test -race -count=1 -tags=integration ./...` |
| `make test-e2e` | `go test -race -count=1 -tags=e2e ./test/e2e/...` |
| `make cover` | unit coverage report to `coverage.html` |
| `make build` | build `bin/relay-server`, `bin/example-worker`, `bin/example-producer` |
| `make up` / `make down` | `docker compose up -d --build` / `docker compose down -v` |
| `make up-redis` | `RELAY_STORAGE=redis docker compose up -d --build` |
| `make deps` | start only postgres + redis (for running the server from GoLand) |
| `make demo` | `make up` then run the producer container once |
| `make check-generated` | regenerate and fail if `git diff` is non-empty (used in CI) |

### Dockerfile (`deploy/docker/Dockerfile`)

Multi-stage, `ARG CMD=relay-server` selects which binary (`./cmd/relay-server`,
`./examples/worker`, `./examples/producer`). Build with `CGO_ENABLED=0`,
`-trimpath -ldflags="-s -w"`, cache mounts for module and build caches. Final image
`gcr.io/distroless/static-debian12:nonroot`.

### docker-compose.yml

Services:

- `postgres` — `postgres:17-alpine`, host port `54329`, healthcheck `pg_isready`, named volume.
- `redis` — `redis:7-alpine`, host port `63799`, `--appendonly yes`, healthcheck `redis-cli ping`.
- `relay-server` — built from Dockerfile, host port `7420`, `RELAY_STORAGE=${RELAY_STORAGE:-postgres}`,
  `RELAY_MIGRATE_ON_START=true`, `depends_on` both with `condition: service_healthy`,
  healthcheck hitting `/readyz`.
- `example-worker` — 2 replicas, depends on `relay-server` healthy.
- `example-producer` — `profiles: ["demo"]` so it only runs on demand.

Host ports deliberately avoid 5432/6379/8080 to not clash with local services.

### `.golangci.yml` (v2)

Enable at least: `govet`, `staticcheck`, `errcheck`, `ineffassign`, `unused`, `revive`,
`gosec`, `bodyclose`, `noctx`, `errorlint`, `contextcheck`, `depguard`, `gocritic`,
`misspell`, `nolintlint`; formatters `gofumpt`, `goimports` (local prefix =
module path). Exclude generated dirs (`sqlcgen`, `oapi`). Implement the depguard rules
from §2 as separate rule sets keyed by file path globs.

### CI (`.github/workflows/ci.yml`)

On push and PR: jobs `lint` (golangci-lint action), `test` (unit, `-race`, upload
coverage artifact), `integration` (`-tags=integration`, runs on `ubuntu-latest` which has
Docker), `generated` (`make check-generated`), `docker` (build image, no push),
`govulncheck`. Cache Go modules.

---

## 12. Documentation

- `README.md`: one-paragraph pitch; architecture diagram (Mermaid); feature list with
  phase roadmap; **Quickstart** (`docker compose up` → `make demo` → curl examples);
  local development (requirements, `make deps`, GoLand run config hint); API table;
  SDK example; delivery guarantees (at-least-once, handlers must be idempotent);
  design-decisions section linking ADRs; testing strategy; badges (CI, Go Report Card,
  license, Go version).
- ADRs in `docs/adr/` (Michael Nygard format: Context / Decision / Consequences):
  - `0001-clean-architecture.md`
  - `0002-pluggable-storage-postgres-and-redis.md`
  - `0003-at-least-once-delivery-with-leases.md`
  - `0004-spec-first-openapi.md`
  - `0005-time-injected-from-application-layer.md`
  - `0006-redis-single-node-and-durability.md`
- `.env.example` listing every env var from §8.

---

## 13. Tasks for agents

Each task lists dependencies, deliverables, and acceptance criteria. A task is **done**
only when: `make lint` passes, `make test` passes, relevant integration tests pass,
no generated file is hand-edited, and the work is committed as clean, separate commits
per §15 (not pushed). Tasks with no shared dependency can run in parallel.

### T0 — Bootstrap repository
Depends on: —
Deliverables: `go.mod` (module path, Go 1.25, tool directives for sqlc/oapi-codegen/goose),
directory skeleton from §3 (with `doc.go` per package), `Makefile`, `.golangci.yml` with
depguard rules, `.editorconfig`, `.gitignore`, `.env.example`, `LICENSE`, minimal README
stub, `docker-compose.yml` with **only** postgres + redis (`make deps`).
Acceptance: `make lint` and `make test` pass on the empty skeleton; a deliberate
`internal/domain` import of `net/http` makes `make lint` fail (verify, then revert).

### T1 — Domain
Depends on: T0
Deliverables: §4 in full with tests.
Acceptance: ≥ 90% coverage on `internal/domain`; zero non-stdlib imports except `uuid`.

### T2 — Application layer + memory repo + contract suite
Depends on: T1
Deliverables: §5 in full; `internal/adapter/storage/memory`; `storagetest.RunContractTests`
with all cases from §6.1; memory repo passes the suite.
Acceptance: ≥ 90% coverage on `internal/app`; contract suite passes under `-race`.

### T3 — PostgreSQL adapter
Depends on: T2
Deliverables: §6.2; `sqlc.yaml`; migrations; generated `sqlcgen`; integration test that
runs the contract suite against a testcontainers Postgres 17.
Acceptance: `make test-integration` passes for this package; `make check-generated` clean.

### T4 — Redis adapter
Depends on: T2 (parallel with T3)
Deliverables: §6.3; Lua scripts; integration test running the contract suite against
testcontainers Redis 7.
Acceptance: `make test-integration` passes for this package.

### T5 — HTTP API
Depends on: T2 (parallel with T3/T4)
Deliverables: `api/openapi.yaml`, `oapi-codegen.yaml`, generated code, handlers,
mapping, problem+json errors, middleware, router, tests per §7.
Acceptance: every endpoint and error mapping tested; spec validates
(`go tool oapi-codegen` succeeds; optionally `redocly lint` in CI later).

### T6 — Server composition root
Depends on: T3, T4, T5
Deliverables: §8; `deploy/docker/Dockerfile`; `relay-server` service added to compose.
Acceptance: `make up` → `curl localhost:7420/readyz` returns 200 for both
`RELAY_STORAGE=postgres` and `redis`; SIGTERM shuts down cleanly within timeout
(verify via `docker compose stop` logs).

### T7 — SDK
Depends on: T5 (needs the API contract; can start from `openapi.yaml` once T5's spec is merged)
Deliverables: §9 with tests.
Acceptance: all behaviors in §9 tested; `goleak.VerifyTestMain` in the package passes.

### T8 — Examples + full compose stack
Depends on: T6, T7
Deliverables: §10; worker and producer services in compose; `make demo`.
Acceptance: `make demo` shows jobs being processed in worker logs; ~20% retried; all
eventually succeed or reach dead after max attempts.

### T9 — End-to-end test
Depends on: T8
Deliverables: `test/e2e` — spins up Postgres **and** Redis via testcontainers, runs
`relay-server` in-process for each backend (table-driven), uses the SDK to enqueue
2,000 jobs and process them with 3 workers × concurrency 20, where handlers fail on first
attempt for 10% of jobs.
Acceptance: for both backends, every job ends `succeeded`; per-job execution count is
≥ 1; failing jobs have attempts == 2; finishes under 60s on CI.

### T10 — CI + docs
Depends on: T9
Deliverables: §11 CI workflow, §12 README and ADRs.
Acceptance: CI green on a push; README quickstart works verbatim on a clean machine with
only Docker installed.

### Parallelization map

```
T0 → T1 → T2 ─┬─ T3 ─┐
              ├─ T4 ─┼─ T6 ─┐
              └─ T5 ─┴─ T7 ─┴─ T8 → T9 → T10
```

---

## 14. Global conventions for agents

- Idiomatic Go: accept interfaces, return structs; small interfaces; no global state
  (no `init()` side effects, no package-level mutable vars); constructors validate.
- `context.Context` is the first parameter of every function doing I/O.
- Wrap errors with context; never log and return the same error.
- No `panic` outside `main` and programmer-error guards (e.g. SDK handler registration).
- Exported identifiers have doc comments; comments explain *why*, not *what*.
- Tests: table-driven, `t.Parallel()` where safe, `require` for preconditions,
  no `time.Sleep` for synchronization (use channels, `synctest`, or eventually-style helpers).
- Generated code is never edited by hand; regenerate with `make generate`.
- Never commit secrets; config only via env vars; `.env` is git-ignored.
- Commit according to §15; never push unless the repository owner explicitly asks.

---

## 15. Git rules

The repository owner has granted a **project-specific exception**: agents may commit in
this repository only. This permission does not extend to any other repository.

Allowed:

- `git add` (specific paths only — never `git add -A` / `git add .` blindly) and
  `git commit` on the current branch.

Not allowed unless the owner explicitly asks:

- `git push` of any kind, `git rebase` (including `pull --rebase`), `git commit --amend`,
  force-push, `--no-verify`, creating/switching branches, merging.

Commit discipline:

1. **One logical change per commit.** A task usually produces several commits
   (e.g. T3: migrations → sqlc queries + generated code → repository → integration tests).
   Never mix unrelated changes (e.g. a lint config tweak inside a feature commit).
2. **Every commit builds and passes `make lint` and `make test`.** Do not commit
   broken intermediate states.
3. **Generated code is committed together with its source** (spec/query change +
   regenerated output in the same commit).
4. **Conventional Commits** format: `<type>(<scope>): <summary>`
   - types: `feat`, `fix`, `test`, `refactor`, `docs`, `build`, `ci`, `chore`
   - scopes: `domain`, `app`, `postgres`, `redis`, `storage`, `http`, `server`, `sdk`,
     `examples`, `e2e`, `deploy`, `ci`, `docs`
   - summary: imperative, lowercase, ≤ 72 chars, no trailing period.
   - body (optional, wrapped at 72): explain **why**, not what. Reference the task ID,
     e.g. `Task: T3`.
   - examples: `feat(postgres): claim jobs with FOR UPDATE SKIP LOCKED`,
     `test(storage): add concurrent claim contract test`,
     `build(deploy): add multi-stage distroless Dockerfile`.
5. **Never commit** secrets, `.env`, `bin/`, coverage output, IDE folders (`.idea/`), or
   local scratch files. Run `git status` before and after each commit to verify.
6. If a commit would require violating any rule above, stop and ask the owner.
