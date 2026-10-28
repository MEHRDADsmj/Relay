# 0002. Pluggable storage: PostgreSQL and Redis

## Status

Accepted

## Context

Different deployments prefer different durability and operational trade-offs. Some teams
already run Postgres as the system of record; others want Redis for low-latency leasing.
A single hard-coded store would force an awkward fork or dual products.

## Decision

Define a `JobRepository` port in the application layer and ship two first-class adapters:

- **PostgreSQL** — SQL migrations (goose), typed queries (sqlc), `FOR UPDATE SKIP LOCKED`
  for claims
- **Redis** — key layout + Lua scripts for atomic claim/complete/fail

Both adapters must pass the same `storagetest` contract suite. Selection is runtime via
`RELAY_STORAGE=postgres|redis`. An in-memory adapter exists for app tests only.

## Consequences

- Operators can choose the backend that matches their ops model without API changes.
- Contract tests catch behavioral drift between adapters early.
- Features that are natural in one store must still be expressible in the other (or be
  deferred), which constrains design.
- Dual maintenance cost is accepted for Phase 1 portfolio depth.
