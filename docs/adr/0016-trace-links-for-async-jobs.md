# 0016. Trace links for async jobs

## Status

Accepted

## Context

Jobs may run long after enqueue (delayed `run_at`, retries, worker downtime). Treating the
worker span as a child of the producer span would create multi-hour parent/child trees and
duplicate children on each retry. OpenTelemetry messaging semantic conventions recommend
**links** for asynchronous consumers.

## Decision

- Persist W3C `traceparent`/`tracestate` on the job at enqueue (from the active span via
  the global propagator).
- SDK workers start a CONSUMER span `relay.process {job_type}` **linked** to the extracted
  producer span context (`trace.WithLinks`), not as a child.
- Each attempt gets its own consumer span; all link to the same producer context.

## Consequences

- Jaeger/Tempo show producer → linked consumer(s); retries appear as sibling linked spans.
- Empty/missing `trace_context` still creates a consumer root span (no link).
