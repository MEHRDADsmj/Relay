# Relay benchmarks

Measured on a development laptop against the local Docker Compose stack (Postgres 17 /
Redis 7, two `relay-server` replicas, observability profile enabled). Numbers are
**indicative**, not SLOs — reproduce with the commands below before comparing machines.

## Environment

| Field | Value |
|---|---|
| Client Go | go1.26.4 windows/amd64 |
| Server Go (image) | go1.26.10 |
| GOMAXPROCS | 16 |
| CPU | AMD Ryzen 9 5900HX |
| Storage | Compose `postgres` / `redis` (switched via `RELAY_STORAGE`) |
| Server | `relay-server` compose service, admin `:7429` |
| Harness | `bin/relay-bench` (SDK-only) |
| Workload | 5000 jobs, 4 producers, 4 workers, concurrency 50, 512-byte payload, 3s warmup, handler sleep 0 |

## How to reproduce

```bash
# Stack (postgres default)
make up

# Build harness
make build

# Single run (HTTP → :7420, gRPC rewritten to :7421)
./bin/relay-bench --server http://localhost:7420 --transport http \
  --producers 4 --workers 4 --concurrency 50 --jobs 5000 \
  --payload-bytes 512 --warmup 3s --cleanup --output text

./bin/relay-bench --server http://localhost:7420 --transport grpc \
  --producers 4 --workers 4 --concurrency 50 --jobs 5000 \
  --payload-bytes 512 --warmup 3s --cleanup --output text

# Redis backend
RELAY_STORAGE=redis docker compose up -d --build relay-server relay-server-2
# …re-run relay-bench…
RELAY_STORAGE=postgres docker compose up -d --build relay-server relay-server-2

# Micro-benchmarks + benchstat
go test -bench=BenchmarkPostgresClaim -benchmem -count=6 -benchtime=300ms \
  -tags=integration ./internal/adapter/storage/postgres/ | tee claim.txt
# (compare two claim.txt files with benchstat)

# CPU profile while a run is in flight
go tool pprof http://localhost:7429/debug/pprof/profile?seconds=30
```

k6 HTTP load scripts live under `loadtest/k6/` (`make loadtest`).

## End-to-end results (`relay-bench`)

| Backend × transport | Enqueue jobs/s | Completions/s | Enqueue p50 / p90 / p99 | e2e p50 / p90 / p99 |
|---|---:|---:|---|---|
| postgres × http | 307 | 303 | 8.9 / 27.3 / 53.6 ms | 12.6 / 30.8 / 57.6 ms |
| postgres × grpc | 297 | 293 | 9.2 / 28.0 / 58.1 ms | 13.0 / 31.7 / 65.1 ms |
| redis × http | 586 | 573 | 4.3 / 15.4 / 27.3 ms | 5.7 / 16.6 / 29.0 ms |
| redis × grpc | 816 | 790 | 2.7 / 13.0 / 43.5 ms | 3.2 / 14.4 / 44.1 ms |

### Commentary

- **Redis is faster** on this workload: single-node Lua claim/complete vs Postgres
  transactions, attempt rows, and otelpgx span cost per query. Prefer Postgres when you
  need durable SQL semantics and multi-instance safety; Redis when latency/throughput
  matter more than that durability model (see ADR 0002 / 0006).
- **gRPC vs HTTP** is mixed: Redis benefits from gRPC; Postgres was within noise on this
  run. Payload mapping and storage dominate over transport for Postgres.
- Compose runs with **OpenTelemetry export enabled**. Alloc profiles show OTel / otelpgx
  as large attribution; production runs without OTLP will look better.
- Absolute rates vary run-to-run on a busy laptop (±10–20% observed). Treat tables as a
  baseline, not a ranking contest.

## Profiling-driven optimization: Postgres claim index use

### Hotspot

CPU profile during `relay-bench` (postgres × http, 30s) attributed ~25% of samples under
`LeaseJobs` → `Repository.Claim` → pgx. Heap profiles similarly concentrated on Claim /
Complete under the HTTP+OTel middleware stack.

### Finding

`EXPLAIN (ANALYZE, BUFFERS)` on `ClaimJobs` with **5000 pending** rows showed the
claimable subquery using `jobs_status_updated`, scanning **all 5000 rows**, sorting, then
`LIMIT 10` (~3.7 ms, ~5200 buffer hits). The partial index `jobs_claimable_pending`
`(queue, priority DESC, run_at, id) WHERE status = 'pending'` was unused because the
predicate OR’d pending with expired-running reclaim.

### Change

Rewrite `ClaimJobs` as two CTEs (pending + expired-running), each `ORDER BY … FOR UPDATE
SKIP LOCKED LIMIT $n`, then merge top-K. Also skip `CloseRunningAttempt` on first claim
(`attempts == 1`) to avoid a no-op UPDATE per lease.

After the rewrite, the same EXPLAIN uses `jobs_claimable_pending` / `jobs_running_lease`,
scans 10 rows, ~0.6 ms, ~180 buffer hits.

### benchstat (targeted metric)

`BenchmarkPostgresClaim` (`-tags=integration`, `-count=6`, `-benchtime=300ms`):

| Metric | Before | After | Δ | p |
|---|---:|---:|---:|---:|
| sec/op | 1.949 ms ± 5% | 1.167 ms ± 2% | **−40.14%** | **0.002** |
| B/op | 5.43 KiB ± 3% | 4.50 KiB ± 2% | −17.08% | 0.002 |
| allocs/op | 110.0 ± 3% | 79.5 ± 2% | −27.73% | 0.002 |

`p < 0.05` on the primary metric (sec/op). End-to-end `relay-bench` rates on a shallow
queue (workers keep depth low) stay within run-to-run noise; the win shows up when claim
would otherwise scan a deep pending set — the profiled pathology.

## Stats counter overhead (§2.2)

Dashboard minutely counters upsert inside the same transaction / Lua script as state
changes (`queue_stats_minutely` / Redis `HINCRBY`).

**Postgres (SQL micro-loop, 2000 inserts):** bare `INSERT` into `jobs` ~53 ms; same inserts
plus `INSERT … ON CONFLICT DO UPDATE` into `queue_stats_minutely` ~156 ms → **~3×** on that
narrow loop, **~50 µs absolute** per enqueue-side bump.

Relative to observed enqueue latency (~3–9 ms p50 on HTTP), that absolute cost is on the
order of **1–2%** of end-to-end enqueue time — **under the 10% throughput-loss threshold**
in the phase plan. **Keep transactional upserts** (no switch to in-memory 5s flush). Redis
stats ride existing scripts (no extra RTT); overhead is similarly subdominant to network
and handler work.

## Micro-benchmarks (unit)

Sample (`go test -bench=. -benchmem` without integration tags):

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkValidateQueue` | ~200 | 0 | 0 |
| `BenchmarkNewJob` | ~290 | 8 | 1 |
| `BenchmarkJobToAPI` | ~640 | 656 | 5 |
| `BenchmarkWorkerDispatchLoop` | ~1.1e5–2e5 | ~31 KiB | ~330 |

Integration claim benches: see above (`-tags=integration`).
