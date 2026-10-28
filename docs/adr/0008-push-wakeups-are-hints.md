# 0008. Push wakeups are hints

## Status

Accepted

## Context

Workers need low enqueue-to-start latency without busy-polling. Postgres `LISTEN/NOTIFY`
and Redis Pub/Sub can wake waiters, but notifications can be lost on reconnect, delivered
out of order, or fire when the job is not yet claimable (paused queue, future `run_at`,
another worker already leased). Carrying job payloads on the wire would also couple the
notifier to serialization and create a second, untrusted claim path.

## Decision

`JobNotifier` delivers **empty wakeups only** — “a job may be ready on queue X”:

- Subscribers always **re-Claim** after a notification; spurious and missed wakeups are OK.
- Long-poll (`LeaseWait`) still uses a 1s re-check ticker for scheduled jobs becoming due
  and for missed notifications.
- On notifier reconnect, broadcast a wakeup to all subscribers so they re-Claim.
- Create/release/retry publish only when the job is immediately due (`run_at <= now`).

Postgres owns one dedicated `LISTEN relay_jobs` connection; Redis uses
`PSUBSCRIBE relay:notify:*`. Neither path returns jobs through the notification channel.

## Consequences

- Correctness stays in Claim (locking / Lua); the notifier cannot double-assign work.
- Latency is good under normal conditions, with a bounded fallback when pushes fail.
- Implementations stay simple: buffered-1 fan-out channels, non-blocking sends.
- Callers must not treat “no notification” as “no work” without Claim or the ticker.
