# Relay — Implementation Plans

Execute phases in order; each phase assumes the previous one is complete and green.

| Phase | File | Theme |
|---|---|---|
| 1 | [phase-1.md](phase-1.md) | Core: domain, Postgres + Redis storage, REST API, server, SDK, examples, CI |
| 2 | [phase-2.md](phase-2.md) | Reliability: backoff, reaper, retention, dead letters, cancel, release, queues, long-poll |
| 3 | [phase-3.md](phase-3.md) | Cron scheduler + leader election, gRPC, API keys & scopes, `relayctl` |
| 4 | [phase-4.md](phase-4.md) | Prometheus, OpenTelemetry, Grafana/Jaeger, pprof, Helm, GoReleaser |
| 5 | [phase-5.md](phase-5.md) | Dashboard (templ + HTMX), benchmarks, load tests, optimization, polish |

Rules that apply to **every** phase live in Phase 1: technology choices (§1),
Clean Architecture boundaries (§2), coding conventions (§14), and Git rules (§15).

## Migration and ADR numbering

| Phase | Postgres migrations | ADRs |
|---|---|---|
| 1 | 00001 | 0001–0006 |
| 2 | 00002 | 0007–0010 |
| 3 | 00003 | 0011–0014 |
| 4 | 00004 | 0015–0018 |
| 5 | 00005 | 0019–0022 |

## Ports used by the stack

| Port | Purpose | Phase |
|---|---|---|
| 7420 | REST API (+ dashboard at `/ui` from phase 5) | 1 |
| 7421 | gRPC | 3 |
| 7429 | Admin: metrics, pprof, health | 4 |
| 54329 | Postgres (host) | 1 |
| 63799 | Redis (host) | 1 |
| 9464 | Example worker metrics | 4 |
| 3300 / 9390 / 16686 | Grafana / Prometheus / Jaeger | 4 |
