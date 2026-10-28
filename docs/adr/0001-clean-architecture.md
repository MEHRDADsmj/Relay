# 0001. Clean Architecture layering

## Status

Accepted

## Context

Relay is a portfolio-quality Go service with domain logic, multiple storage backends, an
HTTP API, and a public SDK. Without explicit dependency rules, infrastructure concerns
(SQL, Redis, HTTP) tend to leak into business logic, making adapters hard to swap and
tests slow or flaky.

## Decision

Organize the codebase as Clean Architecture with dependencies pointing inward only:

- `internal/domain` — entities and pure rules (stdlib + `uuid` only)
- `internal/app` — use cases and ports (no HTTP, pgx, or Redis)
- `internal/adapter/...` — HTTP, Postgres, Redis (adapters must not import each other)
- `internal/platform/...` — config and logging
- `cmd/relay-server` — composition root that wires everything
- `pkg/sdk` — public client; may talk to the HTTP contract only, never `internal/`

Enforce the rules with `depguard` in `golangci-lint` so violations fail CI.

## Consequences

- Storage and transport can be replaced behind ports without rewriting use cases.
- Unit tests run against in-memory fakes; contract tests prove adapters share behavior.
- Composition lives only in `cmd/`; packages stay focused and reviewable.
- Contributors must learn the layer map; crossing a boundary requires a deliberate port.
