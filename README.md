# Relay

Durable job queue with pluggable Postgres / Redis storage, a REST API, and a Go SDK.

**Status:** Phase 1 implementation in progress. Full README arrives in Task T10.

## Plans

| Phase | File | Theme |
|---|---|---|
| 1 | [phase-1.md](phase-1.md) | Core: domain, storage, REST API, server, SDK, examples, CI |
| 2 | [phase-2.md](phase-2.md) | Reliability |
| 3 | [phase-3.md](phase-3.md) | Cron, gRPC, API keys, `relayctl` |
| 4 | [phase-4.md](phase-4.md) | Observability, Helm, GoReleaser |
| 5 | [phase-5.md](phase-5.md) | Dashboard, benchmarks, polish |

Rules that apply to every phase live in Phase 1: technology choices (§1), Clean
Architecture (§2), conventions (§14), and Git rules (§15).

## Quick local deps

```bash
make deps    # postgres :54329, redis :63799
make lint
make test
```

See `.env.example` for configuration.
