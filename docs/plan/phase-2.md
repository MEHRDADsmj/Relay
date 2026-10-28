# Relay — Phase 2 Implementation Plan: Reliability

> Audience: autonomous coding agents. Prerequisite: Phase 1 is complete and green.
> Everything in Phase 1 §1 (technology), §2 (Clean Architecture rules), §14 (conventions)
> and §15 (Git rules) still applies and is not repeated here. When something is
> ambiguous, **stop and ask**.

---

## 0. Goals

Turn the Phase 1 queue into something you could trust in production:

- Exponential backoff with full jitter
- Attempt history per job
- Background **reaper** (lease expiry across all queues) and **retention** (purge old jobs)
- Dead-letter inspection and replay
- Job cancellation (pending and running)
- Voluntary lease release on worker shutdown
- Queue registry: list, stats, pause/resume
- Long-polling leases with push wakeups (Postgres `LISTEN/NOTIFY`, Redis Pub/Sub)
- Keyset pagination for list endpoints

Out of scope: cron, gRPC, CLI, API key management (Phase 3); metrics/tracing (Phase 4);
dashboard (Phase 5).

---

## 1. New dependencies

None required. Postgres `LISTEN` uses `pgx` directly (`pgxpool.Conn.Hijack` / dedicated
`pgx.Conn` + `WaitForNotification`). Redis Pub/Sub uses `go-redis`.

---

## 2. Domain changes (`internal/domain`)

### 2.1 New status

Add `StatusCancelled Status = "cancelled"` (terminal).

Updated state machine (additions in **bold**):

```
pending ──lease──► running ──complete──► succeeded
   ▲                  │ fail (attempts<max) → pending (run_at = now + backoff)
   │                  │ fail (attempts>=max) → dead
   │                  │ lease expired (attempts<max) → pending   [reaper or Claim]
   │                  │ lease expired (attempts>=max) → dead     [reaper or Claim]
   │                  │ **release → pending (attempts--, run_at = now)**
   │                  │ **cancel requested → running w/ cancel_requested=true;
   │                  │   worker reports fail/complete → cancelled**
   │ **cancel** ──────┴──► **cancelled** (from pending directly)
   │
 **retry (from dead or cancelled) → pending, attempts = 0, run_at = now**
```

### 2.2 New fields on `Job`

| Field | Type | Meaning |
|---|---|---|
| `CancelRequested` | `bool` | Set when cancel is requested while running |
| `CancelledAt` | `*time.Time` | Set on transition to cancelled |

`FinishedAt` is also set for `cancelled`.

### 2.3 Attempt entity

```go
type Attempt struct {
    JobID      JobID
    Number     int           // 1-based, equals Job.Attempts at claim time
    WorkerID   string
    StartedAt  time.Time
    FinishedAt *time.Time
    Outcome    AttemptOutcome // running|succeeded|failed|expired|released|cancelled
    Error      string
}
```

### 2.4 Queue entity

```go
type Queue struct {
    Name      string
    Paused    bool
    CreatedAt time.Time   // first time a job was enqueued to it
    UpdatedAt time.Time
}

type QueueStats struct {
    Queue      string
    Paused     bool
    Pending    int64   // includes scheduled (run_at in future)
    Scheduled  int64   // subset of pending with run_at > now
    Running    int64
    Succeeded  int64
    Dead       int64
    Cancelled  int64
    OldestPendingAge time.Duration // 0 if none
}
```

### 2.5 New domain methods

- `(j Job) CanCancel() error` — error unless pending or running.
- `(j Job) CanRetry() error` — error unless dead or cancelled.
- `(j Job) EnsureLeasedBy` unchanged.
- `DecideFailure` unchanged in signature; the app layer now supplies the backoff delay.
- If a running job has `CancelRequested=true`, any worker outcome (complete or fail)
  transitions to `cancelled` — implement as `(j Job) ResolveOutcome(requested Status) Status`.

### 2.6 Pagination types

```go
type PageRequest struct { Limit int; Cursor string } // Limit 1..200, default 50
type Page[T any]  struct { Items []T; NextCursor string } // empty = last page
```

Cursors are opaque base64url-encoded JSON (`{"t":<unix_ms>,"id":"<uuid>"}`) — encoding
lives in `internal/app/cursor.go`, not in adapters.

---

## 3. Application layer changes (`internal/app`)

### 3.1 Split ports (Interface Segregation)

Replace the single growing interface with focused ports. Each storage adapter implements
all of them; the composition root passes the same instance where needed.

```go
type JobRepository interface {
    // Phase 1 methods, with these signature updates:
    Create(ctx, job domain.Job) (domain.Job, bool, error)       // also upserts queue registry
    Get(ctx, id) (domain.Job, error)
    Claim(ctx, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error)
        // returns empty slice without error if queue is paused
        // inserts an Attempt{Outcome: running} per claimed job
        // expired leases it reclaims get their running Attempt closed as `expired`
    ExtendLease(ctx, id, owner string, lease time.Duration, now time.Time) (domain.Job, error)
    Complete(ctx, id, owner string, now time.Time) (domain.Job, error)
        // closes the Attempt as succeeded (or cancelled if CancelRequested)
    Fail(ctx, id, owner, errMsg string, d domain.FailureDecision, now time.Time) (domain.Job, error)
        // closes the Attempt as failed (or cancelled if CancelRequested)
    Ping(ctx) error

    // New:
    Release(ctx, id, owner string, now time.Time) (domain.Job, error)
        // ownership-checked; status=pending, attempts=attempts-1, run_at=now,
        // Attempt closed as released
    Cancel(ctx, id domain.JobID, now time.Time) (domain.Job, error)
        // pending → cancelled; running → CancelRequested=true; else ErrInvalidTransition
    Retry(ctx, id domain.JobID, now time.Time) (domain.Job, error)
        // dead|cancelled → pending, attempts=0, run_at=now, last_error kept,
        // finished_at/cancelled_at cleared, CancelRequested=false
    ListAttempts(ctx, id domain.JobID) ([]domain.Attempt, error)
    ListByStatus(ctx, queue string, status domain.Status, page domain.PageRequest) (domain.Page[domain.Job], error)
        // ordered by updated_at DESC, id DESC (keyset)
}

type QueueRepository interface {
    ListQueues(ctx) ([]domain.Queue, error)
    Stats(ctx, queue string, now time.Time) (domain.QueueStats, error)   // ErrNotFound for unknown queue
    SetPaused(ctx, queue string, paused bool, now time.Time) (domain.Queue, error)
}

type MaintenanceRepository interface {
    // ReapExpired handles expired leases across ALL queues, up to limit jobs per call.
    // Same semantics as Claim's step 1 plus returning attempts<max jobs to pending.
    ReapExpired(ctx, now time.Time, limit int) (ReapResult, error)
    // PurgeFinished deletes succeeded|dead|cancelled jobs (and their attempts and
    // idempotency keys) with finished_at < before, up to limit per call.
    PurgeFinished(ctx, before time.Time, limit int) (int, error)
}

type ReapResult struct { Requeued, Dead int }

// JobNotifier lets the app wait for "a job may be ready on queue X".
// Notifications are hints: receivers must always re-Claim; spurious wakeups are fine.
type JobNotifier interface {
    Subscribe(ctx context.Context, queue string) (ch <-chan struct{}, unsubscribe func())
}

type RetryPolicy interface { NextDelay(attempt int) time.Duration } // unchanged
```

Add `domain.ErrInvalidTransition` (→ HTTP 409).

### 3.2 Retry policy

`ExponentialJitterPolicy{Base, Max time.Duration, Rand func() float64}` implementing
"full jitter": `delay = rand[0,1) * min(Max, Base * 2^(attempt-1))`. Overflow-safe
(cap the exponent). `Rand` injected for deterministic tests. Remove nothing: keep
`FixedDelayRetryPolicy`; config selects which (`RELAY_RETRY_POLICY=exponential|fixed`,
default `exponential`).

### 3.3 Services

Keep `JobService` for job use cases (add `Release`, `Cancel`, `Retry`, `ListAttempts`,
`ListByStatus`, `LeaseWait`). Add `QueueService` (list, stats, pause, resume, bulk-retry
dead). Add `Maintenance` runner (§3.5).

`LeaseWait(ctx, LeaseCommand{..., Wait time.Duration})` (Wait 0..30s):

1. `Claim`. If non-empty or `Wait == 0` → return.
2. `Subscribe(queue)`, then `Claim` again (closes the race between step 1 and subscribe).
3. Loop until deadline: wait on notification **or** a 1s re-check ticker (covers
   scheduled jobs becoming due and missed notifications) → `Claim` → return if non-empty.
4. On deadline return empty slice. Always `unsubscribe`. Respect `ctx` cancellation
   (client disconnect) and return promptly without leaking goroutines.

`QueueService.RetryAllDead(ctx, queue, limit)` — pages through dead jobs and calls
`Retry` for each; returns count. Limit 1..10,000, default 1,000.

### 3.4 Heartbeat response

`Heartbeat` returns the job; the HTTP layer exposes `cancel_requested` so workers can
stop. Heartbeat on a job with `CancelRequested=true` still extends the lease (so the
worker can finish cleanly).

### 3.5 Maintenance runner (`internal/app/maintenance.go`)

```go
type MaintenanceConfig struct {
    ReapInterval      time.Duration // default 5s
    ReapBatch         int           // default 500
    PurgeInterval     time.Duration // default 1m
    PurgeBatch        int           // default 1000
    Retention         time.Duration // default 168h (7d); 0 = never purge
}
func (m *Maintenance) Run(ctx context.Context) error
```

- Two tickers in one goroutine group (`errgroup`). Loop each batch until a call returns
  fewer than the batch size, then wait for the next tick.
- Errors are logged and retried next tick; `Run` only returns when `ctx` is done.
- Safe to run on **every** server instance concurrently (all operations are atomic and
  idempotent). Leader election is not needed until Phase 3's scheduler.
- Log a summary line when anything was reaped/purged.

---

## 4. Storage adapter changes

### 4.1 Contract suite additions (`storagetest`)

Add to `RunContractTests` (split into `RunJobContract`, `RunQueueContract`,
`RunMaintenanceContract`, `RunNotifierContract` called from the umbrella function):

1. Claim on paused queue returns empty; resume → claimable.
2. Attempts: claim → complete produces one attempt `succeeded`; claim → fail → claim →
   complete produces `failed`, `succeeded` in order with correct numbers/workers.
3. Expired lease reclaimed by Claim records `expired` on the old attempt.
4. Release: attempts decremented, immediately re-claimable, attempt `released`;
   wrong owner → `ErrLeaseLost`.
5. Cancel pending → cancelled, not claimable. Cancel running → `CancelRequested=true`;
   subsequent Complete or Fail → `cancelled`. Cancel terminal → `ErrInvalidTransition`.
6. Retry dead → pending, attempts 0, claimable; retry pending → `ErrInvalidTransition`.
7. ListByStatus pagination: 120 dead jobs, limit 50 → pages of 50/50/20, no duplicates
   or gaps, stable under concurrent inserts of *pending* jobs.
8. Stats counts correct after a scripted mix of operations; `Scheduled` and
   `OldestPendingAge` correct.
9. ReapExpired across 3 queues requeues/deads correctly and respects `limit`.
10. PurgeFinished removes only terminal jobs older than cutoff, including attempts and
    idempotency key (re-enqueue with the same key afterwards creates a new job).
11. Notifier: subscriber on queue A receives a signal within 1s of Create on A (with
    `run_at <= now`), receives nothing for queue B, and unsubscribe closes cleanly.
12. Concurrency: maintenance `ReapExpired` running concurrently with 10 claimers never
    double-assigns a job (`-race`).

The `memory` adapter implements everything (its notifier is an in-process fan-out).

### 4.2 PostgreSQL

Migration `00002_reliability.sql`:

```sql
-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('pending','running','succeeded','dead','cancelled'));
ALTER TABLE jobs ADD COLUMN cancel_requested boolean NOT NULL DEFAULT false;
ALTER TABLE jobs ADD COLUMN cancelled_at timestamptz;

CREATE TABLE job_attempts (
    job_id      uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    number      int         NOT NULL,
    worker_id   text        NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    outcome     text        NOT NULL CHECK (outcome IN
                  ('running','succeeded','failed','expired','released','cancelled')),
    error       text,
    PRIMARY KEY (job_id, number, started_at)
);

CREATE TABLE queues (
    name       text PRIMARY KEY,
    paused     boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
INSERT INTO queues (name, created_at, updated_at)
    SELECT DISTINCT queue, now(), now() FROM jobs ON CONFLICT DO NOTHING;

CREATE INDEX jobs_status_updated ON jobs (queue, status, updated_at DESC, id DESC);
CREATE INDEX jobs_finished ON jobs (finished_at)
    WHERE status IN ('succeeded','dead','cancelled');
CREATE INDEX jobs_running_lease_all ON jobs (lease_expires_at) WHERE status = 'running';
-- +goose Down
-- reverse in opposite order
```

Notes:

- `PRIMARY KEY (job_id, number, started_at)`: `number` can repeat after `Release`
  (attempts decremented) or `Retry` (attempts reset), so `started_at` disambiguates.
- The phase 1 constraint name must be verified (`\d jobs`); if it was auto-named
  differently, use the actual name. Do not guess.
- `Create` upserts into `queues` (`ON CONFLICT DO NOTHING`) in the same transaction and
  calls `pg_notify('relay_jobs', queue)` when `run_at <= now`.
- `Claim` joins `queues` and returns nothing when paused (check first, cheap PK lookup).
- `Stats`: one `SELECT status, count(*) ... GROUP BY status` plus scheduled count and
  `min(run_at)` for pending. Acceptable for portfolio scale; document in ADR that large
  deployments would maintain counters.
- `ReapExpired`/`PurgeFinished`: CTE with `SELECT ... FOR UPDATE SKIP LOCKED LIMIT $n`,
  so concurrent server instances split the work.
- `ListByStatus`: keyset `WHERE (updated_at, id) < ($t, $id)`.
- **Notifier:** `postgres.Notifier` owns one dedicated connection running
  `LISTEN relay_jobs`; a goroutine loops on `WaitForNotification` and fans out to
  subscribers by queue (non-blocking sends on buffered-1 channels). On connection loss,
  reconnect with backoff and broadcast a wakeup to all subscribers (they re-Claim).
  `Close()` stops the goroutine; leak-checked in tests.

### 4.3 Redis

New keys:

| Key | Type | Contents |
|---|---|---|
| `relay:queues` | set | all queue names |
| `relay:queue:{queue}` | hash | `paused`, `created_at`, `updated_at` |
| `relay:q:{queue}:counts` | hash | per-status counters, maintained atomically by every script |
| `relay:q:{queue}:status:{status}` | zset | job ids by `updated_at` ms (for terminal statuses: succeeded, dead, cancelled) |
| `relay:job:{id}:attempts` | list | JSON-encoded attempts, appended in order |
| `relay:leased:all` | zset | `{queue}|{id}` scored by lease expiry (for cross-queue reaping) |
| `relay:finished` | zset | job ids scored by `finished_at` ms (for purge) |
| channel `relay:notify:{queue}` | pub/sub | published by enqueue/release/retry scripts |

Notes:

- Every script that changes status must update `counts`, the per-status zsets,
  `relay:leased:all`, and `relay:finished` in the same script. Add a Go helper test that
  runs a random sequence of operations and asserts `counts` equals a recount
  (property-style test, seeded RNG, under `integration` tag).
- Pending/running counts come from `ZCARD` of `scheduled`+`ready` and `leased`;
  terminal counts from `counts` hash. Pick one approach per status and document it.
- `ListByStatus` for `pending`/`running` reads `ready`/`scheduled`/`leased`; for terminal
  statuses reads the status zset. Cursor uses score+id.
- `PUBLISH` inside Lua is allowed; publish only when the job is immediately due.
- **Notifier:** one `PSUBSCRIBE relay:notify:*` connection fanning out by queue; on
  reconnect broadcast a wakeup.
- Purge removes job hash, attempts list, idempotency key (store the key name in the job
  hash so purge can find it), and all index entries.

---

## 5. HTTP API changes (`api/openapi.yaml`)

| Method | Path | Purpose | Success | Errors |
|---|---|---|---|---|
| POST | `/v1/queues/{queue}/leases` | add optional `wait_seconds` (0..30) | `200` | — |
| POST | `/v1/jobs/{id}/heartbeat` | response job now includes `cancel_requested` | `200` | 409 |
| POST | `/v1/jobs/{id}/release` | `{worker_id}` | `200` | 404, 409 |
| POST | `/v1/jobs/{id}/cancel` | — | `200` | 404, 409 |
| POST | `/v1/jobs/{id}/retry` | — | `200` | 404, 409 |
| GET | `/v1/jobs/{id}/attempts` | attempt history | `200 {"attempts":[...]}` | 404 |
| GET | `/v1/queues` | list queues | `200 {"queues":[...]}` | — |
| GET | `/v1/queues/{queue}` | stats | `200` | 404 |
| POST | `/v1/queues/{queue}/pause` | pause | `200` | 404 |
| POST | `/v1/queues/{queue}/resume` | resume | `200` | 404 |
| GET | `/v1/queues/{queue}/jobs?status=&limit=&cursor=` | paginated list | `200 {"jobs":[...],"next_cursor":""}` | 400 |
| POST | `/v1/queues/{queue}/dead/retry` | `{limit?}` bulk retry | `200 {"retried":n}` | 404 |

- Long-poll: the server's `WriteTimeout` must exceed max wait — set `WriteTimeout` to
  45s and document why.
- Invalid cursor → 400 with field `cursor`.
- `ErrInvalidTransition → 409` with problem `type` distinct from lease-lost
  (`https://relay.dev/problems/invalid-transition` vs `.../lease-lost`). Define both
  `type` URIs in the spec.

---

## 6. SDK changes (`pkg/sdk`)

- Leases use `wait_seconds` (option `LeaseWait`, default 20s) instead of client-side
  polling when supported; keep client-side backoff only for transport errors.
  `PollMinInterval`/`PollMaxInterval` become fallbacks used when `LeaseWait == 0`.
- Heartbeat response `cancel_requested=true` → cancel that job's context; handler result
  is still reported (server resolves it to `cancelled`).
- On shutdown timeout: for every job still in flight, call `Release` before returning
  (best-effort, bounded by a 5s context), so another worker can pick them up immediately
  without consuming an attempt.
- New error helper: `sdk.Permanent(err) error` — handler returns it to signal "do not
  retry". The SDK sends `fail` with `{"permanent": true}`; extend the fail endpoint and
  `FailCommand` so the app layer forces a `dead` decision. (Add to §5 table, contract
  test, and domain `DecideFailure(now, delay, permanent bool)`.)
- Client methods: `CancelJob`, `RetryJob`, `ListAttempts`, `ListQueues`, `QueueStats`,
  `PauseQueue`, `ResumeQueue`, `ListJobs` (returns an iterator: `func(yield func(JobInfo, error) bool)`
  using Go range-over-func), `RetryDead`.

---

## 7. Server changes

- `cmd/relay-server` starts `Maintenance.Run` and the notifier alongside the HTTP server
  in the same `errgroup`; shutdown order: stop accepting HTTP → wait for in-flight
  long-polls to return (they observe ctx) → stop maintenance → close notifier → close
  storage.
- New config:

| Env var | Default |
|---|---|
| `RELAY_RETRY_POLICY` | `exponential` |
| `RELAY_RETRY_BASE` | `1s` |
| `RELAY_RETRY_MAX` | `1h` |
| `RELAY_REAP_INTERVAL` | `5s` |
| `RELAY_REAP_BATCH` | `500` |
| `RELAY_PURGE_INTERVAL` | `1m` |
| `RELAY_PURGE_BATCH` | `1000` |
| `RELAY_RETENTION` | `168h` |
| `RELAY_LEASE_MAX_WAIT` | `30s` |

---

## 8. Documentation

- ADR `0007-exponential-backoff-full-jitter.md`
- ADR `0008-push-wakeups-are-hints.md` (why notifications never carry jobs; re-Claim always)
- ADR `0009-attempt-history-and-retention.md`
- ADR `0010-cancellation-semantics.md` (cooperative cancellation of running jobs)
- README: delivery guarantees section updated (release, cancel, permanent errors);
  "Operating Relay" section (retention, pausing, dead-letter replay).

---

## 9. Tasks

Definition of done: same as Phase 1 §13 (lint, tests, integration tests, clean commits
per Phase 1 §15, not pushed).

### P2-T1 — Domain additions
Depends on: Phase 1. Deliverables: §2. Acceptance: ≥ 90% coverage on domain; state
machine table-tested for every (status, action) pair including invalid ones.

### P2-T2 — Ports, services, retry policy, maintenance runner, memory adapter, contract suite
Depends on: P2-T1. Deliverables: §3, §4.1, memory adapter updates.
Acceptance: memory passes full contract suite under `-race`; `LeaseWait` tested with a
fake notifier for: immediate hit, wakeup hit, timeout, ctx cancel, spurious wakeup;
`goleak` clean.

### P2-T3 — Postgres adapter
Depends on: P2-T2. Deliverables: §4.2. Acceptance: full contract suite passes
(integration); migration up/down/up round-trips on a DB containing Phase 1 data
(seed fixture test).

### P2-T4 — Redis adapter
Depends on: P2-T2 (parallel with P2-T3). Deliverables: §4.3. Acceptance: full contract
suite + counter property test pass (integration).

### P2-T5 — HTTP API
Depends on: P2-T2 (parallel with P2-T3/T4). Deliverables: §5 including permanent fail.
Acceptance: every new endpoint and error type tested; long-poll test returns within
`wait + 1s` and promptly on client disconnect.

### P2-T6 — SDK
Depends on: P2-T5. Deliverables: §6. Acceptance: tests for long-poll path, cancel
propagation, release-on-timeout, permanent errors, iterator pagination; `goleak` clean.

### P2-T7 — Server wiring + e2e
Depends on: P2-T3, P2-T4, P2-T5, P2-T6. Deliverables: §7; extend `test/e2e` with:
(a) kill a worker mid-job (cancel its ctx without release) → job completes on another
worker after lease expiry via the reaper; (b) cancel 100 running jobs → all `cancelled`;
(c) 50 permanently failing jobs → `dead` after 1 attempt, then bulk retry → processed;
(d) end-to-end latency from enqueue to start < 200ms p95 with long-poll (both backends).
Acceptance: e2e green for both backends.

### P2-T8 — Docs
Depends on: P2-T7. Deliverables: §8.

```
P2-T1 → P2-T2 ─┬─ P2-T3 ─┐
               ├─ P2-T4 ─┼─ P2-T7 → P2-T8
               └─ P2-T5 → P2-T6 ─┘
```
