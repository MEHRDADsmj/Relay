# 0007. Exponential backoff with full jitter

## Status

Accepted

## Context

Phase 1 used a fixed retry delay. Under correlated failures (shared dependency outage,
thundering herd after a deploy), every worker retries on the same schedule and amplifies
load. Operators still need a predictable upper bound so jobs do not sit forever between
attempts.

## Decision

Default retry policy is **exponential backoff with full jitter**:

```
delay = rand[0,1) * min(Max, Base * 2^(attempt-1))
```

- Config: `RELAY_RETRY_POLICY=exponential` (default), `RELAY_RETRY_BASE` (default `1s`),
  `RELAY_RETRY_MAX` (default `1h`). Exponent is capped to avoid `int64` overflow.
- `Rand` is injected so unit tests are deterministic.
- `FixedDelayRetryPolicy` remains available via `RELAY_RETRY_POLICY=fixed` (delay =
  `RELAY_RETRY_BASE`; legacy `RELAY_RETRY_DELAY` still sets the base when
  `RELAY_RETRY_BASE` is unset).
- Permanent failures (`sdk.Permanent` / fail with `permanent: true`) skip backoff and go
  straight to `dead` without consuming the retry schedule.

## Consequences

- Retries desynchronize under load; average delay is half the exponential ceiling.
- Operators tune Base/Max as operational knobs; wrong Max either retries too aggressively
  or delays recovery after transient outages.
- Fixed policy stays useful for demos and tightly controlled environments.
