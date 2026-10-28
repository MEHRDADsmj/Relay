# 0022. Postgres claim via partial indexes

## Status

Accepted

## Context

Phase 5 profiling (`relay-bench` + CPU pprof on Postgres) showed `LeaseJobs` →
`Repository.Claim` as the dominant hotspot (~25% of samples). `EXPLAIN (ANALYZE, BUFFERS)`
with a deep pending set showed the claimable subquery scanning all pending rows via
`jobs_status_updated`, sorting, then `LIMIT`, because a single OR predicate mixed
claimable-pending with expired-running reclaim and prevented use of
`jobs_claimable_pending`.

## Decision

- Rewrite Postgres `ClaimJobs` as **two CTEs** (pending claimable + expired-running
  reclaim), each `ORDER BY … FOR UPDATE SKIP LOCKED LIMIT $n`, then merge top-K.
- Keep using partial indexes `jobs_claimable_pending` and `jobs_running_lease`.
- Skip `CloseRunningAttempt` on first claim (`attempts == 1`) to avoid a no-op UPDATE per
  lease.
- Document before/after EXPLAIN, benchstat, and e2e numbers in
  [docs/benchmarks.md](../benchmarks.md).
- Do **not** pursue batch-complete or JSON buffer pools as the Phase 5 optimization;
  Claim dominated the profile.

## Consequences

- `BenchmarkPostgresClaim` improved ~40% sec/op (benchstat p < 0.05) with fewer allocs.
- Shallow-queue end-to-end `relay-bench` rates stay within laptop noise; the win appears
  when claim would otherwise scan a deep pending set.
- Redis claim path is unchanged; Postgres operators benefit most under backlog pressure.
