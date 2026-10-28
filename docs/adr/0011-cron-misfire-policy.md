# 0011. Cron misfire policy

## Status

Accepted

## Context

The scheduler may be down (deploy, crash, leadership gap) while cron fires pass. Catching
up every missed fire would enqueue storms after outages. Skipping straight to the next
future tick without acknowledging the gap would under-deliver for low-frequency schedules
operators still expect to run “once since downtime.”

## Decision

On each due schedule, fire **exactly one** job for the most recent missed tick, then
advance `NextRunAt` to the **next future** fire relative to **now** (not relative to the
missed `fire` time):

1. `fire = schedule.NextRunAt` (the due tick that made the row eligible).
2. Enqueue with idempotency key `schedule:{id}:{fire.Unix()}`.
3. `next = CronParser.Next(expr, loc, now)` — first fire strictly after wall-clock `now`.
4. `MarkFired` sets `last_run_at`, `last_job_id`, and `next_run_at = next`.

Example: an `@hourly` schedule was due at 09:00; the scheduler returns at 12:10 → one job
for the 09:00 fire key, then `NextRunAt` becomes 13:00 (not 10:00/11:00/12:00 catch-up).

Manual `TriggerNow` uses a distinct key `schedule:{id}:manual:{uuidv7}` and does not
consume a cron fire.

## Consequences

- No catch-up storms after long outages; at most one job per schedule wake.
- Operators may miss intermediate ticks by design; document SLA as “at most one fire per
  tick, and one catch-up job after downtime,” not “every historical tick.”
- Idempotency keys keep dual leaders or retries from double-enqueueing the same fire
  (see [ADR 0012](0012-leader-election-and-idempotent-fires.md)).
