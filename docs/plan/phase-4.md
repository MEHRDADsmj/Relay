# Relay — Phase 4 Implementation Plan: Observability, Operations, Release

> Audience: autonomous coding agents. Prerequisite: Phases 1–3 complete and green.
> Phase 1 §1, §2, §14, §15 still apply. When something is ambiguous, **stop and ask**.

---

## 0. Goals

- **Metrics:** Prometheus metrics for server (HTTP, gRPC, jobs, queues, storage) and SDK
- **Tracing:** OpenTelemetry traces end-to-end — producer → server → storage → worker —
  with trace context **persisted on the job** so the worker span links to the enqueue span
- **Logs ↔ traces:** `trace_id`/`span_id` on every log line
- **Profiling:** `pprof` on a private admin port
- **Local observability stack:** Prometheus, Grafana (provisioned dashboards), Jaeger
- **Kubernetes:** Helm chart with probes, HPA, PDB, security context, ServiceMonitor
- **Release:** GoReleaser multi-arch binaries + container images to GHCR, SBOM, changelog

Out of scope: dashboard UI and load tests (Phase 5).

---

## 1. New dependencies

| Concern | Choice |
|---|---|
| Metrics | `github.com/prometheus/client_golang` |
| Tracing API/SDK | `go.opentelemetry.io/otel`, `.../sdk`, `.../exporters/otlp/otlptrace/otlptracegrpc` |
| HTTP instrumentation | `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` |
| gRPC instrumentation | `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` (stats handler) |
| pgx instrumentation | `github.com/exaring/otelpgx` |
| Redis instrumentation | `github.com/redis/go-redis/extra/redisotel/v9` |
| Release | GoReleaser (binary, run in CI only) |
| Helm testing | `helm`, `kind`, `chart-testing (ct)` in CI |

Metrics are Prometheus-native (not OTel metrics) because that is still the dominant
setup in Go shops; document the choice in an ADR.

---

## 2. Architecture rule for observability

Observability must not leak into `domain` or `app` as vendor types.

- `internal/app` defines a small port for business metrics:

```go
type Metrics interface {
    JobEnqueued(queue, jobType string, created bool)
    JobLeased(queue string, n int, waited time.Duration)
    JobFinished(queue, jobType string, outcome domain.AttemptOutcome, runDuration time.Duration)
    JobDead(queue, jobType string)
    LeaseLost(queue string)
    SchedulerFired(schedule string, ok bool)
    LeaderChanged(name string, isLeader bool)
    MaintenanceRun(kind string, affected int, d time.Duration, err error)
}
```

  with a `NoopMetrics` default. The Prometheus implementation lives in
  `internal/adapter/metrics/prom`.
- Tracing in `app`: use only the OTel **API** (`go.opentelemetry.io/otel/trace`) via a
  package-level `otel.Tracer("relay/app")` obtained from an injected
  `trace.TracerProvider` (constructor parameter, default noop). The OTel API is a
  vendor-neutral interface package — allowed in `app`, **forbidden in `domain`**. Update
  depguard: `app` may import `go.opentelemetry.io/otel/trace` and
  `go.opentelemetry.io/otel/attribute` only.
- SDK/exporter setup lives in `internal/platform/telemetry`.

---

## 3. Metrics specification

Namespace `relay`. Labels kept low-cardinality: **never** label by job ID, worker ID,
API key, or payload. `job_type` is allowed but capped: the adapter tracks distinct
`(queue, job_type)` pairs and maps anything beyond `RELAY_METRICS_MAX_SERIES`
(default 1000) to `job_type="__other__"`.

### 3.1 Server

| Metric | Type | Labels |
|---|---|---|
| `relay_http_requests_total` | counter | `route` (chi route pattern), `method`, `code` |
| `relay_http_request_duration_seconds` | histogram | `route`, `method` |
| `grpc_server_*` | standard set from `github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus` | `grpc_service`, `grpc_method`, `grpc_code` |
| `relay_jobs_enqueued_total` | counter | `queue`, `job_type`, `result` (`created`/`replayed`) |
| `relay_jobs_leased_total` | counter | `queue` |
| `relay_lease_wait_seconds` | histogram | `queue` |
| `relay_jobs_finished_total` | counter | `queue`, `job_type`, `outcome` |
| `relay_job_run_duration_seconds` | histogram | `queue`, `job_type` (from attempt started→finished) |
| `relay_jobs_dead_total` | counter | `queue`, `job_type` |
| `relay_lease_lost_total` | counter | `queue` |
| `relay_queue_jobs` | gauge | `queue`, `status` |
| `relay_queue_oldest_pending_age_seconds` | gauge | `queue` |
| `relay_queue_paused` | gauge | `queue` |
| `relay_scheduler_fires_total` | counter | `schedule`, `result` |
| `relay_leader` | gauge | `name` (1 if this instance is leader) |
| `relay_maintenance_runs_total` / `_affected_total` / `_duration_seconds` | counter/counter/histogram | `kind` (`reap`/`purge`) |
| `relay_storage_pool_*` | gauges | pgxpool stats (acquired, idle, total, wait count) / go-redis pool stats |
| `relay_build_info` | gauge (=1) | `version`, `commit`, `go_version`, `storage` |
| Go runtime + process | default collectors | — |

Add the `grpc-ecosystem/go-grpc-middleware/providers/prometheus` dependency for gRPC
server metrics (standard `grpc_server_*` names).

**Queue gauges** come from a `QueueStatsCollector` (custom `prometheus.Collector`) that
calls `QueueService.Stats` for all queues with a 10s cache and a 2s timeout, so scrapes
never hammer storage and a slow storage never blocks a scrape beyond the timeout (on
timeout, emit last cached values and increment `relay_stats_collect_errors_total`).

Histogram buckets: HTTP `prometheus.DefBuckets`; job run duration
`[.01,.05,.1,.25,.5,1,2.5,5,10,30,60,300,900]`; lease wait `[.001,.005,.01,.05,.1,.5,1,5,10,30]`.

### 3.2 SDK

New package `pkg/sdk/sdkprom` (separate package so the core SDK stays dependency-free):

```go
func NewHooks(reg prometheus.Registerer, opts ...Option) sdk.Hooks
```

`sdk.Hooks` (added to core SDK, plain Go interface, no deps):

```go
type Hooks interface {
    OnLease(queue string, n int, d time.Duration, err error)
    OnJobStart(queue, jobType string)
    OnJobEnd(queue, jobType string, d time.Duration, outcome string) // succeeded|failed|permanent|panic|cancelled|lease_lost
    OnHeartbeat(queue string, err error)
}
```

Worker metrics: `relay_worker_jobs_in_flight`, `relay_worker_job_duration_seconds`,
`relay_worker_jobs_total{outcome}`, `relay_worker_lease_errors_total`.

---

## 4. Tracing specification

### 4.1 Setup (`internal/platform/telemetry`)

- `Setup(ctx, cfg) (shutdown func(context.Context) error, err error)` builds a
  `TracerProvider` with resource attributes (`service.name=relay-server`,
  `service.version`, `service.instance.id`, `deployment.environment`), OTLP gRPC
  exporter, `ParentBased(TraceIDRatioBased(cfg.SampleRatio))` sampler, batch span
  processor. Sets global propagator `TraceContext + Baggage`.
- Disabled (noop provider) when `OTEL_EXPORTER_OTLP_ENDPOINT` is empty. Standard `OTEL_*`
  env vars are honored by the exporter; Relay adds only `RELAY_TRACE_SAMPLE_RATIO`
  (default `1.0` in compose, `0.1` default otherwise).
- Shutdown flushes spans within the server's shutdown timeout.

### 4.2 Instrumentation points

- Inbound: `otelhttp.NewHandler` around the chi router (span name = route pattern via
  `otelhttp.WithSpanNameFormatter`); `otelgrpc.NewServerHandler()` stats handler.
- App: spans for `JobService.Enqueue`, `Lease`, `Complete`, `Fail`, `Scheduler.fire`,
  `Maintenance.reap/purge`; attributes `relay.queue`, `relay.job_type`, `relay.job_id`
  (job ID **is** fine as a span attribute — not as a metric label), `relay.attempt`.
  Record errors with `span.RecordError` + `codes.Error` only for unexpected errors (not
  for `ErrNotFound`/validation).
- Storage: `otelpgx` tracer on the pgx config; `redisotel.InstrumentTracing(client)`.
- Outbound SDK HTTP client: `otelhttp.NewTransport` (only if the user passes
  `sdk.WithTracerProvider`; core SDK depends on the OTel API only — acceptable and
  vendor-neutral; document).

### 4.3 Trace context persisted on jobs

- Domain: add `TraceContext map[string]string` to `Job` (W3C `traceparent`, `tracestate`;
  max 2 entries, max 512 bytes each — validated).
- Enqueue: app layer injects the current span context with the global propagator into
  `TraceContext` (so it works for REST, gRPC, and scheduler fires — scheduler fires
  create their own root span `scheduler.fire`).
- Storage: Postgres migration `00004_trace_context.sql` adds `trace_context jsonb`;
  Redis stores it as a JSON hash field. Contract test: round-trip.
- API: job responses include `trace_context` (REST/gRPC).
- SDK worker: for each job, start span `relay.process {job_type}` with kind CONSUMER,
  **linked** (`trace.WithLinks`) to the extracted producer span context — not a child,
  because the job may run hours later and retries produce multiple consumer spans
  (document in ADR, following OTel messaging semantic conventions).
  Use messaging semconv attributes: `messaging.system=relay`,
  `messaging.destination.name=<queue>`, `messaging.message.id=<job id>`,
  `messaging.operation.type=process`.

### 4.4 Logs

`internal/platform/logging`: wrap the slog handler with a handler that adds `trace_id`
and `span_id` from `ctx` when present. All app/adapter logging uses `logger.InfoContext`
(lint: add `sloglint` with `context: scope` to golangci config).

---

## 5. Admin port and profiling

- New listener `RELAY_ADMIN_ADDR` (default `:7429`) serving `/metrics`,
  `/debug/pprof/*` (explicit `net/http/pprof` handlers registered on a **private** mux —
  never `http.DefaultServeMux`), `/healthz`, `/readyz`.
- `/healthz` and `/readyz` stay on the public port too (load balancers).
- Admin port is not exposed by the Helm Service by default; separate `metrics` Service
  port for Prometheus scraping only `/metrics`.
- Document in README how to capture a CPU profile:
  `go tool pprof http://localhost:7429/debug/pprof/profile?seconds=30`.

---

## 6. Local observability stack (docker-compose)

Add services under compose profile `observability`:

- `jaeger` — `jaegertracing/jaeger:2.x` (all-in-one, OTLP gRPC 4317, UI 16686).
- `prometheus` — `prom/prometheus`, config `deploy/observability/prometheus.yml`
  scraping `relay-server:7429` (all replicas via DNS SD `dns_sd_configs`) and
  `example-worker` metrics (`:9464`).
- `grafana` — `grafana/grafana`, anonymous viewer enabled, provisioned datasources
  (Prometheus, Jaeger) and dashboards from `deploy/observability/grafana/`.

Dashboards (JSON, committed):

1. **Relay Overview:** enqueue/complete/fail rate by queue, queue depth, oldest pending
   age, dead rate, lease wait p50/p95/p99, job run duration p95 by type, leader instance,
   HTTP/gRPC error rate and latency.
2. **Relay Storage & Runtime:** pool stats, maintenance throughput, goroutines, GC,
   heap, CPU.

Alert rules (`deploy/observability/alerts.yml`, loaded by Prometheus — demonstrative):
`RelayQueueBacklogGrowing`, `RelayOldestPendingTooOld` (> 5m), `RelayDeadJobsSpike`,
`RelayNoLeader` (sum(relay_leader)=0 for 1m), `RelayHighErrorRate`.

Make targets: `make up-obs` (stack + observability profile), `make open-obs` prints
URLs: Grafana `http://localhost:3300`, Prometheus `http://localhost:9390`,
Jaeger `http://localhost:16686` (host ports chosen to avoid common defaults).

Example worker: exposes `/metrics` on `:9464` using `sdkprom`, configures OTLP tracing
from env.

---

## 7. Helm chart (`deploy/helm/relay`)

Chart `relay`, appVersion = Relay version. Templates:

- `deployment.yaml` — relay-server; ports http 7420, grpc 7421, admin 7429; env from
  ConfigMap + Secret; `livenessProbe` `/healthz`, `readinessProbe` `/readyz`,
  `startupProbe`; resources (requests/limits configurable); `securityContext`
  (`runAsNonRoot`, `readOnlyRootFilesystem`, drop ALL caps, `seccompProfile:
  RuntimeDefault`); `terminationGracePeriodSeconds` > `RELAY_SHUTDOWN_TIMEOUT` + 5s;
  topology spread across nodes; config checksum annotation for rollouts.
- `service.yaml` — http + grpc (ClusterIP); `service-metrics.yaml` — admin port, metrics only.
- `configmap.yaml`, `secret.yaml` (or `existingSecret` value) for `RELAY_POSTGRES_URL` /
  `RELAY_REDIS_URL` / `RELAY_API_KEY`.
- `hpa.yaml` (CPU + optional custom metric note), `pdb.yaml` (`minAvailable: 1`),
  `servicemonitor.yaml` (if `metrics.serviceMonitor.enabled`), `serviceaccount.yaml`,
  `networkpolicy.yaml` (optional).
- `migration-job.yaml` — Helm `pre-install,pre-upgrade` hook running
  `relay-server migrate up` (Postgres only); server runs with `RELAY_MIGRATE_ON_START=false`.
- `NOTES.txt`, `values.schema.json` (validates values), README for the chart.

The chart does **not** bundle Postgres/Redis (document using Bitnami charts or managed
services); the CI smoke test installs them separately in kind.

CI job `helm`: `helm lint`, `ct lint`, then kind cluster → install postgres + redis
(minimal manifests in `deploy/helm/ci/`) → install chart for each storage → wait for
ready → port-forward → enqueue + lease a job with `relayctl` → uninstall.

---

## 8. Release engineering

- `.goreleaser.yaml`: builds `relay-server` and `relayctl` for linux/darwin/windows ×
  amd64/arm64 with `-trimpath` and ldflags `-X main.version -X main.commit -X main.date`;
  archives + checksums; multi-arch Docker images via `dockers_v2`/manifests to
  `ghcr.io/OWNER/relay-server` and `ghcr.io/OWNER/relayctl`; SBOMs (syft); changelog from
  Conventional Commits grouped by type.
- `.github/workflows/release.yml`: on tag `v*` → run full CI → GoReleaser with
  `GITHUB_TOKEN` (packages: write). Agents **must not** create tags or push; the
  repository owner does releases.
- `make release-snapshot` → `goreleaser release --snapshot --clean` locally (no publish).
- Version info exposed: `relay-server --version`, `relayctl version`, `relay_build_info`.
- Optional (behind a TODO in docs, not implemented): cosign keyless signing.

---

## 9. Config additions

| Env var | Default |
|---|---|
| `RELAY_ADMIN_ADDR` | `:7429` |
| `RELAY_METRICS_MAX_SERIES` | `1000` |
| `RELAY_TRACE_SAMPLE_RATIO` | `0.1` |
| `RELAY_ENVIRONMENT` | `development` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty (tracing off) |
| `OTEL_*` | standard OTel env vars |

---

## 10. Documentation

- ADR `0015-prometheus-metrics-otel-traces.md`
- ADR `0016-trace-links-for-async-jobs.md`
- ADR `0017-metric-cardinality-limits.md`
- ADR `0018-helm-migrations-as-hooks.md`
- README: Observability section with Grafana and Jaeger screenshots (placeholders until
  captured), metrics reference table, Kubernetes install instructions, release/install
  instructions (`go install`, GitHub Releases, GHCR images).
- `docs/runbook.md`: one entry per alert — meaning, likely causes, diagnosis queries,
  remediation (`relayctl` commands).

---

## 11. Tasks

Definition of done: as Phase 1 §13.

### P4-T1 — Metrics port + Prometheus adapter + admin port
Depends on: Phase 3. Deliverables: §2 (metrics port), §3.1, §5. Acceptance: unit tests
use `prometheus/testutil` to assert counters after service calls; cardinality cap
tested; stats collector timeout/caching tested; pprof not reachable on public port
(test).

### P4-T2 — Tracing setup + instrumentation + log correlation
Depends on: Phase 3 (parallel with P4-T1). Deliverables: §4.1, §4.2, §4.4. Acceptance:
tests using `sdk/trace/tracetest.SpanRecorder` assert span names, attributes, and
parent/child structure for an enqueue → lease → complete flow over HTTP and gRPC; log
lines contain `trace_id` when a span is active.

### P4-T3 — Trace context on jobs + SDK consumer spans + SDK hooks/sdkprom
Depends on: P4-T2. Deliverables: §4.3, §3.2. Acceptance: contract round-trip for both
stores; SDK test asserts the consumer span links to the producer span context; scheduler
fire jobs carry the fire span context; `sdkprom` metrics asserted with testutil.

### P4-T4 — Local observability stack
Depends on: P4-T1, P4-T3. Deliverables: §6. Acceptance: `make up-obs` + `make demo` →
Grafana dashboards show data within 30s; Jaeger shows a producer trace with linked
consumer spans; Prometheus loads alert rules without errors (`promtool check rules` in CI).

### P4-T5 — Helm chart + CI smoke test
Depends on: Phase 3 (parallel). Deliverables: §7. Acceptance: `helm lint`, `ct lint`,
`values.schema.json` validation, kind smoke test green for both storages in CI.

### P4-T6 — Release engineering
Depends on: P4-T1 (for build info). Deliverables: §8. Acceptance: `make release-snapshot`
produces all archives and images locally; `goreleaser check` passes in CI.

### P4-T7 — Docs + runbook
Depends on: P4-T4, P4-T5, P4-T6. Deliverables: §10.

```
P4-T1 ─────────────┬─ P4-T4 ─┐
P4-T2 → P4-T3 ─────┘         ├─ P4-T7
P4-T5 ───────────────────────┤
P4-T1 → P4-T6 ───────────────┘
```
