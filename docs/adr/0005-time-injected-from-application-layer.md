# 0005. Time injected from the application layer

## Status

Accepted

## Context

Job scheduling (`run_at`), lease expiry, retries, and state transitions are time-sensitive.
Calling `time.Now()` deep in adapters or domain makes tests flake and hides race windows
(lease expiry, reclaim).

## Decision

Inject a `Clock` port from the application layer. Use cases pass an explicit `now` into
repository methods. Production uses a system clock; tests use a controllable fake.
ID generation similarly goes through an `IDGenerator` port (UUIDv7 in production).

Adapters must not invent "now" for business decisions; they apply the timestamps the
application supplies (except driver-level bookkeeping that does not affect the state
machine).

## Consequences

- Lease expiry, retries, and reclaim behavior are deterministic in tests.
- Contract tests can advance time without sleeping.
- Call sites are slightly more verbose (`now` parameters), which is intentional.
- Misbehaving adapters that ignore injected time are caught by contract coverage.
