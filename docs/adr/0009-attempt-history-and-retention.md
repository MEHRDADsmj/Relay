# 0009. Attempt history and retention

## Status

Accepted

## Context

Operators debugging dead letters and flaky workers need per-attempt provenance (who,
when, outcome, error). Keeping every terminal job forever grows storage without bound.
Reaping expired leases must also work across all queues without a separate cron product
in Phase 2.

## Decision

Record an **Attempt** on every claim (outcome `running`), and close it on complete, fail,
expire, release, or cancel. Attempts are queryable via `GET /v1/jobs/{id}/attempts`.

Background **Maintenance** runs on every server instance (no leader election):

- **Reaper** (`RELAY_REAP_*`): reclaim expired leases across all queues (requeue or dead),
  closing the open attempt as `expired`.
- **Retention purge** (`RELAY_PURGE_*`, `RELAY_RETENTION`, default 7d): delete
  `succeeded`/`dead`/`cancelled` jobs older than the cutoff, including attempts and
  idempotency keys. `RELAY_RETENTION=0` disables purge.

Purge and reap use batched, lock-friendly operations (`SKIP LOCKED` / atomic scripts) so
concurrent instances share work safely.

## Consequences

- Failure forensics no longer depend solely on `last_error` on the job row.
- Disk/memory growth is bounded when retention is enabled; operators who need longer
  history raise retention or export before purge.
- After purge, the same idempotency key can create a **new** job (by design).
- Release decrements `attempts` and can reuse attempt numbers; attempt identity includes
  `started_at` so history remains unambiguous.
