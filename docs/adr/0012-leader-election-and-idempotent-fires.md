# 0012. Leader election and idempotent fires

## Status

Accepted

## Context

Only one scheduler should tick due schedules in a multi-instance deployment. Leadership
can flip during a tick (kill, network partition, lease expiry). Correctness must not
depend on a perfect exclusive lock with fencing tokens.

## Decision

**Leader election** selects which instance runs the scheduler loop; **idempotency** makes
fires safe under overlap:

### Election

- **Postgres:** dedicated pooled connection holds
  `pg_try_advisory_lock(hashtext('scheduler'))`. Non-leaders poll every 2s; leaders ping
  every 5s and resign on connection loss.
- **Redis:** lease `SET relay:leader:scheduler <instanceID> NX PX 15000`, renewed every 5s
  with compare-and-pexpire. Lost renewal cancels `leaderCtx`. This is a **lease**, not a
  fencing-token lock: two instances may briefly both believe they are leader.
- **Memory:** in-process mutex for tests.

`Campaign` returns a context cancelled on leadership loss; shutdown calls `resign` before
draining servers.

### Idempotent fire

Each automatic fire enqueues with key `schedule:{id}:{fire_unix}` and
`MarkFired(..., expectedNext=fire, ...)`. Conditional `MarkFired` fails (conflict) if
another instance already advanced `next_run_at`; that path is logged and skipped. Enqueue
idempotency returns the same job on replay.

## Consequences

- Exactly-one-job-per-fire holds across failover without distributed transactions.
- Redis election may allow brief dual leadership; schedule correctness still holds via
  keys + conditional `MarkFired`.
- Operators should run ≥2 server replicas so a follower becomes leader within a few
  seconds after kill (compose ships a second instance for this).
