# 0017. Metric cardinality limits

## Status

Accepted

## Context

Prometheus series cardinality grows with label combinations. Job IDs, worker IDs, API
keys, and unbounded `job_type` values would explode memory and scrape cost. Relay still
needs `job_type` on several counters/histograms for useful dashboards.

## Decision

- **Never** use job ID, worker ID, API key, or payload-derived values as metric labels.
  Job ID is allowed as a **span attribute** only ([ADR 0016](0016-trace-links-for-async-jobs.md)).
- Allow `queue` and `job_type` where specified in the Phase 4 metrics table.
- Cap distinct `(queue, job_type)` pairs with `RELAY_METRICS_MAX_SERIES` (default
  `1000`). Once the budget is exhausted, further pairs map to `job_type="__other__"`.
- Already-seen pairs keep their real `job_type` for the process lifetime; the cap applies
  to *new* pairs after the budget is full.
- Queue gauges (`relay_queue_*`) label by `queue` / `status` only — no `job_type`.
- Stats collection for gauges uses a short timeout and cache so scrapes never block on
  storage; failures increment `relay_stats_collect_errors_total`.

## Consequences

- Dashboards remain usable under high type churn; overflow aggregates into `__other__`.
- Operators who need finer type breakdown raise `RELAY_METRICS_MAX_SERIES` deliberately
  and watch Prometheus memory.
- Misconfigured high-cardinality labels are a review gate for new metrics.
