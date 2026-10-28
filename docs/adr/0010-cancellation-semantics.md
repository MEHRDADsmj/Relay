# 0010. Cancellation semantics

## Status

Accepted

## Context

Operators need to stop work that should not finish (bad deploy, wrong payload, drained
queue). Forcibly killing a worker mid-handler is unsafe: side effects may already have
started, and the lease must still resolve to a terminal status. Pending jobs can stop
immediately; running jobs need cooperation from the worker.

## Decision

Cancellation is **cooperative for running jobs**:

- **Pending** → `cancelled` immediately (not claimable).
- **Running** → set `cancel_requested=true`; the job stays `running` with its lease.
  Heartbeats still extend the lease so the worker can finish cleanly. Heartbeat responses
  expose `cancel_requested` so the SDK cancels the handler context.
- When the worker later **complete**s or **fail**s, `ResolveOutcome` forces status
  `cancelled` (handler result is reported but does not become `succeeded`/`dead`/`pending`).
- Terminal jobs cannot be cancelled (`ErrInvalidTransition` → HTTP 409).
- **Retry** from `dead` or `cancelled` resets to `pending` with `attempts=0`.
- **Release** (voluntary lease return on shutdown) returns the job to `pending` with
  `attempts` decremented and attempt outcome `released` — distinct from cancel.

## Consequences

- No orphaned `running` rows from admin cancel; workers own the final transition.
- Cancel latency for running work is bounded by heartbeat/handler responsiveness, not by
  lease expiry alone (reaper still covers crashed workers).
- SDK/handlers should treat context cancellation as “stop soon and report”; permanent
  vs transient fail still applies only when cancel was not requested.
- Bulk cancel of many running jobs is safe: each resolves independently when workers report.
