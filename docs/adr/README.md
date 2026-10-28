# Architecture Decision Records

ADRs capture significant design choices for Relay. Status is **Accepted** unless noted.

| ADR | Title |
|---|---|
| [0001](0001-clean-architecture.md) | Clean Architecture layering |
| [0002](0002-pluggable-storage-postgres-and-redis.md) | Pluggable Postgres and Redis storage |
| [0003](0003-at-least-once-delivery-with-leases.md) | At-least-once delivery with leases |
| [0004](0004-spec-first-openapi.md) | Spec-first OpenAPI |
| [0005](0005-time-injected-from-application-layer.md) | Time injected from the application layer |
| [0006](0006-redis-single-node-and-durability.md) | Redis single-node and durability |
| [0007](0007-exponential-backoff-full-jitter.md) | Exponential backoff with full jitter |
| [0008](0008-push-wakeups-are-hints.md) | Push wakeups are hints |
| [0009](0009-attempt-history-and-retention.md) | Attempt history and retention |
| [0010](0010-cancellation-semantics.md) | Cancellation semantics |
| [0011](0011-cron-misfire-policy.md) | Cron misfire policy |
| [0012](0012-leader-election-and-idempotent-fires.md) | Leader election and idempotent fires |
| [0013](0013-api-key-format-and-hashing.md) | API key format and hashing |
| [0014](0014-grpc-alongside-rest.md) | gRPC alongside REST |
| [0015](0015-prometheus-metrics-otel-traces.md) | Prometheus metrics and OpenTelemetry traces |
| [0016](0016-trace-links-for-async-jobs.md) | Trace links for async jobs |
| [0017](0017-metric-cardinality-limits.md) | Metric cardinality limits |
| [0018](0018-helm-migrations-as-hooks.md) | Helm migrations as hooks |
| [0019](0019-server-rendered-dashboard-templ-htmx.md) | Server-rendered dashboard (templ + HTMX) |
| [0020](0020-dashboard-stats-without-prometheus.md) | Dashboard stats without Prometheus |
| [0021](0021-dashboard-session-security.md) | Dashboard session security |
| [0022](0022-postgres-claim-partial-indexes.md) | Postgres claim via partial indexes |

Narrative overview: [architecture.md](../architecture.md). Phase plans live under
[docs/plan/](../plan/).
