# 0003. At-least-once delivery with leases

## Status

Accepted

## Context

Workers can crash, lose network, or hang while executing a job. Exactly-once execution
across process boundaries is expensive and fragile. Producers and handlers still need a
clear reliability contract.

## Decision

Relay provides **at-least-once** delivery via exclusive leases:

- Workers lease jobs (`running` + owner + expiry), heartbeat to extend, then complete or fail.
- Expired leases are reclaimable (attempts incremented) or marked dead when max attempts
  are exhausted.
- Handlers must be **idempotent**; retries and reclaim after crash may re-run work.
- Enqueue supports an optional idempotency key so producers can safely retry create.

Phase 1 uses a fixed retry delay behind a `RetryPolicy` port (exponential backoff later).

## Consequences

- Crash recovery is correct without distributed transactions.
- Duplicate side effects are possible; application code owns idempotency.
- Lease duration and heartbeat cadence become operational knobs.
- Exactly-once is explicitly out of scope.
