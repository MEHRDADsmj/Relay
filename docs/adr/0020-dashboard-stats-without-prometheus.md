# 0020. Dashboard stats without Prometheus

## Status

Accepted

## Context

The dashboard needs throughput charts and queue cards. Prometheus is optional ops
infrastructure; requiring it for the UI would break the “Compose up and browse” demo and
complicate deployments that only scrape `/metrics` elsewhere.

## Decision

- Persist **minutely counters** in storage, updated in the same atomic write path as job
  state changes:
  - **Postgres:** `queue_stats_minutely` (migration `00005`), upserted
    `ON CONFLICT DO UPDATE` inside existing transactions.
  - **Redis:** `HINCRBY` on `relay:stats:{queue}:{minute_unix}` inside existing Lua
    scripts, with TTL ≈ retention.
- Expose reads via `StatsRepository.Throughput` and `QueueService.Throughput` with
  windows `{15m, 1h, 24h}`; the app layer zero-fills missing minutes.
- Purge stats older than `RELAY_STATS_RETENTION` (default `48h`) in the maintenance runner.
- SSE / overview cards reuse `CachedQueueStats` (short TTL) so live updates do not hammer
  storage and do not depend on the Prometheus collector adapter.
- Measure upsert overhead in benchmarks. If e2e throughput loss exceeded ~10%, switch to
  in-memory aggregation flushed every 5s and document that in a follow-up ADR.

## Consequences

- Dashboard works with zero Prometheus/Grafana.
- Each enqueue/complete/fail/dead path pays a small counter bump; measured overhead stayed
  under the 10% threshold, so transactional upserts are retained (see
  [docs/benchmarks.md](../benchmarks.md) and [ADR 0022](0022-postgres-claim-partial-indexes.md)
  for related performance work).
- Contract tests cover counter correctness; zero-fill is tested in the app layer.
