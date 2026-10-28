# Relay — Phase 3 Implementation Plan: Scheduling, gRPC, API Keys, CLI

> Audience: autonomous coding agents. Prerequisite: Phases 1–2 complete and green.
> Phase 1 §1, §2, §14, §15 still apply. When something is ambiguous, **stop and ask**.

---

## 0. Goals

- **Cron schedules** that enqueue jobs, with exactly-one-job-per-fire across a cluster
- **Leader election** (Postgres advisory lock / Redis lease) so only one scheduler runs
- **gRPC API** with parity to REST (same app services; second inbound adapter)
- **API keys** with scopes, hashed at rest, managed via API and CLI
- **`relayctl`** CLI for operators
- SDK transport abstraction (HTTP or gRPC)

Out of scope: metrics/tracing/Helm (Phase 4); dashboard/load tests (Phase 5).

---

## 1. New dependencies

| Concern | Choice |
|---|---|
| Cron parsing | `github.com/robfig/cron/v3` (parser only — **not** its runner) |
| gRPC | `google.golang.org/grpc`, `google.golang.org/protobuf` |
| Proto tooling | `buf` (`github.com/bufbuild/buf/cmd/buf` as Go tool), `protoc-gen-go`, `protoc-gen-go-grpc` as Go tools |
| gRPC health/reflection | `google.golang.org/grpc/health`, `google.golang.org/grpc/reflection` |
| CLI | `github.com/spf13/cobra` |
| Table output | `text/tabwriter` (stdlib) |

Generated protobuf code is committed (`gen/relay/v1/`), checked by `make check-generated`.

---

## 2. Cron schedules

### 2.1 Domain (`internal/domain/schedule.go`)

```go
type ScheduleID = uuid.UUID

type Schedule struct {
    ID          ScheduleID
    Name        string        // unique, regex ^[a-z0-9][a-z0-9_.-]{0,99}$
    Cron        string        // standard 5-field + descriptors (@hourly, @daily, @every 5m)
    Timezone    string        // IANA name, default "UTC"
    Queue       string
    JobType     string
    Payload     []byte
    Priority    int
    MaxAttempts int
    Enabled     bool
    NextRunAt   time.Time     // always computed, even when disabled
    LastRunAt   *time.Time
    LastJobID   *JobID
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

- `NewSchedule(params, id, now)` validates cron (via a `CronParser` port, see below),
  timezone (`time.LoadLocation`), and job fields (reuse Phase 1 job validation).
- **Misfire policy (fixed, documented):** if the scheduler was down and missed fires,
  enqueue **one** job for the most recent missed fire, then advance `NextRunAt` to the
  next future fire. No catch-up storms.
- Idempotency key for a fire: `schedule:{id}:{fire_unix}` — guarantees at most one job
  per fire even across leader failover or retries.

### 2.2 Ports (`internal/app`)

```go
type CronParser interface {
    // Next returns the first fire time strictly after `after`, in loc.
    Next(expr string, loc *time.Location, after time.Time) (time.Time, error)
    Validate(expr string) error
}

type ScheduleRepository interface {
    CreateSchedule(ctx, s domain.Schedule) (domain.Schedule, error)           // ErrConflict on name
    GetSchedule(ctx, id domain.ScheduleID) (domain.Schedule, error)
    GetScheduleByName(ctx, name string) (domain.Schedule, error)
    ListSchedules(ctx, page domain.PageRequest) (domain.Page[domain.Schedule], error) // by name
    UpdateSchedule(ctx, s domain.Schedule, expectedUpdatedAt time.Time) (domain.Schedule, error)
        // optimistic concurrency: ErrConflict if updated_at changed
    DeleteSchedule(ctx, id domain.ScheduleID) error
    // DueSchedules returns enabled schedules with next_run_at <= now, ordered by next_run_at, up to limit.
    DueSchedules(ctx, now time.Time, limit int) ([]domain.Schedule, error)
    // MarkFired sets last_run_at, last_job_id, next_run_at iff next_run_at still equals expectedNext.
    MarkFired(ctx, id domain.ScheduleID, expectedNext, firedAt, next time.Time, jobID domain.JobID) error
}

type LeaderElector interface {
    // Campaign blocks until leadership is acquired or ctx is done. The returned context
    // is cancelled when leadership is lost. resign releases leadership.
    Campaign(ctx context.Context, name string) (leaderCtx context.Context, resign func(), err error)
}
```

`CronParser` implementation lives in `internal/adapter/cron` (wraps robfig parser with
`cron.Minute|Hour|Dom|Month|Dow|Descriptor`). The app layer never imports robfig.

### 2.3 Scheduler (`internal/app/scheduler.go`)

```go
type SchedulerConfig struct {
    Tick      time.Duration // default 1s
    Batch     int           // default 100
}
func (s *Scheduler) Run(ctx context.Context) error
```

Loop:

1. `Campaign(ctx, "scheduler")`. Log on acquire/loss.
2. While `leaderCtx` is alive, every tick: `DueSchedules(now, batch)`; for each:
   compute `fire = s.NextRunAt`; compute `next = parser.Next(expr, loc, now)` (misfire
   policy: next is relative to **now**, not to `fire`); `JobService.Enqueue` with
   idempotency key `schedule:{id}:{fire.Unix()}`; `MarkFired(id, expectedNext=fire, now,
   next, jobID)`. A `MarkFired` conflict is logged at debug and skipped (another leader
   or an edit won).
3. On leadership loss → back to step 1. On `ctx` done → `resign()` and return.

Schedule CRUD lives in `ScheduleService` (create, get, list, update with optimistic
concurrency, enable/disable, delete, `TriggerNow` which enqueues immediately with key
`schedule:{id}:manual:{uuidv7}`).

### 2.4 Leader election adapters

- **Postgres** (`postgres.LeaderElector`): dedicated connection from the pool
  (`pool.Acquire`, held for the leadership term), `SELECT pg_try_advisory_lock(hashtext($name))`
  polled every 2s while not leader. While leader, ping the connection every 5s; on error,
  cancel `leaderCtx`, release the connection. `resign` → `pg_advisory_unlock` + release.
- **Redis** (`redis.LeaderElector`): `SET relay:leader:{name} <instanceID> NX PX 15000`;
  renew every 5s with a Lua compare-and-pexpire; release with Lua compare-and-del. Lost
  renewal → cancel `leaderCtx`. Document in ADR that this is a lease, not a fencing-token
  lock; correctness is preserved by idempotency keys + `MarkFired` conditional update.
- **Memory**: in-process mutex-based elector for tests.

### 2.5 Storage

Postgres migration `00003_schedules_and_api_keys.sql`:

```sql
CREATE TABLE schedules (
    id           uuid PRIMARY KEY,
    name         text UNIQUE NOT NULL,
    cron         text NOT NULL,
    timezone     text NOT NULL,
    queue        text NOT NULL,
    job_type     text NOT NULL,
    payload      jsonb NOT NULL,
    priority     smallint NOT NULL,
    max_attempts int NOT NULL,
    enabled      boolean NOT NULL,
    next_run_at  timestamptz NOT NULL,
    last_run_at  timestamptz,
    last_job_id  uuid,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);
CREATE INDEX schedules_due ON schedules (next_run_at) WHERE enabled;

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY,
    name         text NOT NULL,
    prefix       text UNIQUE NOT NULL,   -- first 8 chars after "rly_", shown in listings
    hash         bytea NOT NULL,         -- SHA-256 of the full secret
    scopes       text[] NOT NULL,
    created_at   timestamptz NOT NULL,
    last_used_at timestamptz,
    expires_at   timestamptz,
    revoked_at   timestamptz
);
```

Redis keys:

| Key | Type |
|---|---|
| `relay:schedule:{id}` | hash |
| `relay:schedules:by-name` | hash name → id |
| `relay:schedules:due` | zset id scored by next_run_at ms (enabled only) |
| `relay:apikey:{id}` | hash |
| `relay:apikeys:by-prefix` | hash prefix → id |

Optimistic concurrency and `MarkFired` are Lua compare-and-set scripts.

Contract suite additions: schedule CRUD, name conflict, optimistic-concurrency conflict,
`DueSchedules` ordering/limit/enabled filter, `MarkFired` conflict, leader election
(two electors → exactly one leader; leader resigns → other acquires within 5s; leader's
connection killed → other acquires), API key CRUD/lookup by prefix.

---

## 3. API keys and authorization

### 3.1 Domain

```go
type Scope string
const (
    ScopeProducer Scope = "producer" // enqueue, get job
    ScopeWorker   Scope = "worker"   // lease, heartbeat, complete, fail, release
    ScopeAdmin    Scope = "admin"    // everything incl. queues, schedules, keys
)
type APIKey struct {
    ID, Name, Prefix string; Hash []byte; Scopes []Scope
    CreatedAt time.Time; LastUsedAt, ExpiresAt, RevokedAt *time.Time
}
type Principal struct { KeyID string; Name string; Scopes []Scope }
func (p Principal) Can(s Scope) bool // admin implies all
```

### 3.2 Format and verification

- Secret format: `rly_<8-char prefix>_<32 random bytes base62>`; generated with
  `crypto/rand`. Shown **once** at creation.
- Stored: SHA-256(secret) (high-entropy secrets do not need bcrypt/argon2; document in
  ADR). Lookup by prefix, compare with `subtle.ConstantTimeCompare`.
- `last_used_at` updated at most once per minute per key (in-memory throttle) to avoid a
  write per request.
- Verified-key cache: in-memory LRU, TTL 30s (revocation takes effect within 30s; ADR).
- `RELAY_API_KEY` (Phase 1) remains a **bootstrap admin** credential; log a warning if
  it is set alongside stored keys.
- Auth disabled entirely only if no stored keys exist **and** `RELAY_API_KEY` is empty —
  log a loud warning at startup.

### 3.3 App layer

`AuthService.Authenticate(ctx, secret) (Principal, error)` and
`APIKeyService` (create, list, revoke). Authorization is enforced in the app layer via a
`RequireScope(ctx, scope)` helper reading the `Principal` from context
(`app.WithPrincipal` / `app.PrincipalFrom`), so REST and gRPC share the same rules.
Every service method declares its required scope; table-test the full matrix.

### 3.4 Endpoints

REST: `POST /v1/api-keys` (returns secret once), `GET /v1/api-keys`,
`DELETE /v1/api-keys/{id}` (revoke). Unauthorized → 401, forbidden scope → 403
(problem+json).

Server subcommand: `relay-server apikey create --name <n> --scopes admin` prints the
secret — for bootstrapping without HTTP.

---

## 4. Schedule REST endpoints

| Method | Path | Scope |
|---|---|---|
| POST | `/v1/schedules` | admin |
| GET | `/v1/schedules?limit=&cursor=` | admin |
| GET | `/v1/schedules/{id_or_name}` | admin |
| PATCH | `/v1/schedules/{id}` (JSON merge patch; `If-Match: <updated_at RFC3339Nano>` required) | admin |
| POST | `/v1/schedules/{id}/enable` · `/disable` · `/trigger` | admin |
| DELETE | `/v1/schedules/{id}` | admin |

Missing `If-Match` → 428; stale → 412.

---

## 5. gRPC API

### 5.1 Proto (`proto/relay/v1/*.proto`, package `relay.v1`)

Services mirror REST one-to-one: `JobService` (Enqueue, GetJob, Lease, Heartbeat,
Complete, Fail, Release, Cancel, Retry, ListAttempts), `QueueService`, `ScheduleService`,
`APIKeyService`. Conventions:

- Follow Google AIP style where reasonable (`GetJobRequest{ string id = 1; }`).
- `google.protobuf.Timestamp`, `google.protobuf.Duration`, payload as `bytes` containing
  JSON (document; not `google.protobuf.Struct`, to keep payload opaque and lossless).
- `buf.yaml` with `lint: DEFAULT` and `breaking: FILE` against the main branch in CI.

### 5.2 Adapter (`internal/adapter/grpc`)

- Implements generated servers; maps to/from app commands in `mapping.go`.
- Error mapping: `ErrValidation→InvalidArgument` (with `errdetails.BadRequest`),
  `ErrNotFound→NotFound`, `ErrLeaseLost→FailedPrecondition` (reason `LEASE_LOST`),
  `ErrInvalidTransition→FailedPrecondition` (reason `INVALID_TRANSITION`),
  `ErrConflict→Aborted`, unauthenticated→`Unauthenticated`, forbidden→`PermissionDenied`.
- Interceptors (unary + stream): recovery, request ID (metadata `x-request-id`), slog
  access log, auth (metadata `authorization: Bearer ...`).
- Long-poll `Lease` honors `wait` and the RPC deadline (whichever is shorter).
- Health service (reports NOT_SERVING when storage ping fails) and reflection (enabled
  by `RELAY_GRPC_REFLECTION`, default `true` in compose, `false` otherwise).
- Listens on `RELAY_GRPC_ADDR` (default `:7421`). Separate port from HTTP (simpler and
  common); document alternative (cmux / single port h2c) in ADR as rejected.
- Graceful shutdown: `GracefulStop` with timeout, then `Stop`.

Adapter tests use `bufconn` with the memory store.

---

## 6. SDK transport abstraction

- Internal interface `transport` in `pkg/sdk` with `httpTransport` and `grpcTransport`.
  Public API unchanged.
- `sdk.NewClient(target string, opts...)`: `http://`/`https://` → HTTP; `grpc://host:port`
  / `grpcs://` → gRPC. Option `WithTransport(sdk.TransportHTTP|TransportGRPC)` overrides.
- The SDK imports the generated proto package from `gen/relay/v1` (public, not internal).
- Run the full SDK test suite against both transports (table-driven transport factory).
- New client methods: schedules CRUD, API keys CRUD.

---

## 7. `relayctl` (`cmd/relayctl`)

Built with cobra; uses the SDK client exclusively (no internal imports — enforce via
depguard).

Global flags / env: `--server` (`RELAY_URL`, default `http://localhost:7420`),
`--api-key` (`RELAY_API_KEY`), `--output/-o table|json` (default `table`),
`--timeout` (default 10s). Exit codes: 0 ok, 1 usage error, 2 API error, 3 not found.

Commands:

```
relayctl job enqueue --queue q --type t [--payload '{}' | --payload-file f] [--priority n] [--run-at RFC3339|+5m] [--max-attempts n] [--idempotency-key k]
relayctl job get <id>
relayctl job attempts <id>
relayctl job cancel <id>
relayctl job retry <id>
relayctl job list --queue q --status s [--limit n] [--all]
relayctl queue list
relayctl queue stats <queue> [--watch 2s]
relayctl queue pause <queue> | resume <queue>
relayctl queue retry-dead <queue> [--limit n]
relayctl schedule create --name n --cron "*/5 * * * *" [--timezone Europe/Berlin] --queue q --type t [--payload ...]
relayctl schedule list | get <id|name> | enable <id> | disable <id> | trigger <id> | delete <id> [--yes]
relayctl schedule edit <id> [--cron ...] [--payload ...] ...   # uses If-Match automatically
relayctl apikey create --name n --scopes producer,worker | list | revoke <id> [--yes]
relayctl version
relayctl completion bash|zsh|fish|powershell
```

- Destructive commands prompt unless `--yes`; non-TTY without `--yes` → error.
- `--watch` redraws until Ctrl-C (`signal.NotifyContext`).
- Golden-file tests for table and JSON output (`testdata/*.golden`, `-update` flag);
  command tests run against an `httptest` server backed by the real router + memory store.
- `version` prints version/commit/date injected via `-ldflags` (Makefile + GoReleaser in Phase 4).

---

## 8. Server wiring and config

`errgroup` now runs: HTTP server, gRPC server, maintenance, scheduler. Shutdown order:
stop scheduler (resign leadership) → stop accepting HTTP/gRPC → drain → maintenance →
notifier → storage.

| Env var | Default |
|---|---|
| `RELAY_GRPC_ADDR` | `:7421` |
| `RELAY_GRPC_REFLECTION` | `false` |
| `RELAY_SCHEDULER_ENABLED` | `true` |
| `RELAY_SCHEDULER_TICK` | `1s` |
| `RELAY_INSTANCE_ID` | hostname-pid |
| `RELAY_AUTH_CACHE_TTL` | `30s` |

docker-compose: expose 7421; run **two** `relay-server` replicas (`deploy.replicas: 2`
behind no LB — producers/workers use `relay-server:7420` DNS round-robin) to demonstrate
leader election; add an example schedule created on startup by a one-shot
`relayctl schedule create ... || true` service.

---

## 9. Documentation

- ADR `0011-cron-misfire-policy.md`
- ADR `0012-leader-election-and-idempotent-fires.md`
- ADR `0013-api-key-format-and-hashing.md`
- ADR `0014-grpc-alongside-rest.md`
- README: schedules section, gRPC quickstart with `grpcurl`, `relayctl` usage with an
  asciinema/GIF placeholder, auth/scopes table.

---

## 10. Tasks

Definition of done: as Phase 1 §13.

### P3-T1 — Domain: schedules, API keys, scopes
Depends on: Phase 2. Deliverables: §2.1, §3.1. Acceptance: ≥ 90% coverage.

### P3-T2 — App: ports, ScheduleService, Scheduler, AuthService, APIKeyService, scope matrix, memory adapter, cron adapter, contract suite
Depends on: P3-T1. Deliverables: §2.2, §2.3, §3.2, §3.3, memory/cron adapters, §2.5
contract additions. Acceptance: scheduler tested with fake clock for normal fires,
misfire (scheduler down 3h on an hourly schedule → exactly one job), leadership loss
mid-tick, edit during fire (conflict skipped); scope matrix covers every service method.

### P3-T3 — Postgres adapter (schedules, keys, leader election)
Depends on: P3-T2. Acceptance: contract suite passes; leader-election kill test passes.

### P3-T4 — Redis adapter (schedules, keys, leader election)
Depends on: P3-T2 (parallel with P3-T3). Acceptance: same as P3-T3.

### P3-T5 — REST: schedules, API keys, authz
Depends on: P3-T2 (parallel). Acceptance: 401/403/412/428 paths tested.

### P3-T6 — Proto + gRPC adapter
Depends on: P3-T2 (parallel). Deliverables: §5. Acceptance: `buf lint` clean; bufconn
tests cover every RPC and error mapping; health reflects storage status.

### P3-T7 — SDK transport abstraction
Depends on: P3-T5, P3-T6. Acceptance: SDK suite passes against both transports;
`goleak` clean.

### P3-T8 — relayctl
Depends on: P3-T7. Acceptance: golden tests for every command; `--yes`/TTY behavior
tested.

### P3-T9 — Server wiring, compose, e2e
Depends on: P3-T3, P3-T4, P3-T7. Deliverables: §8; e2e additions: (a) two server
instances, `@every 2s` schedule, run 30s → 15±1 jobs, no duplicate fire keys; kill the
leader instance mid-run → still no duplicates, gap ≤ 1 fire; (b) worker on gRPC
transport processes 1,000 jobs; (c) producer key cannot lease (403).
Acceptance: green for both backends.

### P3-T10 — Docs
Depends on: P3-T9. Deliverables: §9.

```
P3-T1 → P3-T2 ─┬─ P3-T3 ──────────┐
               ├─ P3-T4 ──────────┼─ P3-T9 → P3-T10
               ├─ P3-T5 ─┐        │
               └─ P3-T6 ─┴─ P3-T7 ┴─ P3-T8
```
