# 0015. Prometheus metrics and OpenTelemetry traces

## Status

Accepted

## Context

Phase 4 needs production-grade observability: scrapeable metrics, distributed traces
across producer → server → worker, and log correlation. Two ecosystems dominate Go
shops today: Prometheus for metrics and OpenTelemetry for traces. Mixing OTel Metrics
with Prometheus scrapes (or vice versa) adds exporter complexity without clear gain for
Relay’s first ops phase.

Clean Architecture still applies: vendor SDKs must not leak into `domain`, and business
metrics in `app` should stay behind a small port.

## Decision

### Metrics: Prometheus-native

- Server metrics use `prometheus/client_golang` in `internal/adapter/metrics/prom`.
- `internal/app` defines a vendor-free `Metrics` port (`NoopMetrics` default); adapters
  call it from use cases. HTTP/gRPC/runtime collectors live only in adapters.
- Workers use optional `pkg/sdk/sdkprom` hooks so the core SDK stays dependency-light.
- Namespace `relay_*` (plus standard `grpc_server_*` and Go/process collectors).
- Cardinality policy: [ADR 0017](0017-metric-cardinality-limits.md).

### Traces: OpenTelemetry

- Setup/exporter in `internal/platform/telemetry` (OTLP gRPC). Disabled (noop) when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is empty.
- `app` may import only the OTel **API** (`otel/trace`, `otel/attribute`) via an injected
  `TracerProvider`; domain stays free of OTel.
- Instrumentation: `otelhttp`, `otelgrpc`, `otelpgx`, `redisotel`; SDK outbound transport
  when the user passes `sdk.WithTracerProvider`.
- Async job linking: [ADR 0016](0016-trace-links-for-async-jobs.md).
- Logs: slog handler injects `trace_id` / `span_id` from context when a span is active.

### Rejected (for now)

- OTel Metrics as the primary server metrics path (Prometheus remains the scrape contract).
- Vendor types (Prometheus collectors, OTLP exporters) inside `domain` or as required
  dependencies of the core SDK.

## Consequences

- Operators scrape `/metrics` on the admin port and ship traces to any OTLP backend
  (Jaeger in the local stack).
- Dual stack is intentional and documented; switching metrics to OTel later would be an
  adapter swap behind `app.Metrics`, not a domain rewrite.
- Depguard allows the narrow OTel API imports in `app` only.
