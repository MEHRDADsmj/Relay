# Relay operations runbook

Alert rules live in [`deploy/observability/alerts.yml`](../deploy/observability/alerts.yml)
(loaded by the local Prometheus stack; suitable as a starting point for production).

Set `RELAY_URL` / `RELAY_API_KEY` (or `--server` / `--api-key`) for `relayctl` examples.

---

## RelayQueueBacklogGrowing

**Meaning:** Pending jobs on a queue grew by more than 50 over a 5-minute window and
stayed elevated for 2 minutes. Workers are not draining as fast as producers enqueue.

**Likely causes**

- Worker replicas down, misconfigured, or auth/scope failures
- Queue paused
- Handlers too slow / concurrency too low
- Storage latency or connection pool exhaustion

**Diagnosis**

```promql
sum by (queue) (relay_queue_jobs{status="pending"})
sum by (queue) (rate(relay_jobs_leased_total[5m]))
sum by (queue) (rate(relay_jobs_enqueued_total[5m]))
relay_queue_paused
histogram_quantile(0.95, sum by (le, queue) (rate(relay_lease_wait_seconds_bucket[5m])))
```

```bash
relayctl queue stats <queue>
relayctl queue list
relayctl job list --queue <queue> --status pending
```

**Remediation**

1. If paused: `relayctl queue resume <queue>`
2. Scale workers or raise concurrency; fix handler errors (check dead spike).
3. Inspect storage pool gauges on the *Relay Storage & Runtime* dashboard.
4. Temporarily slow producers if the backlog is intentional load.

---

## RelayOldestPendingTooOld

**Meaning:** Oldest pending job age exceeded 5 minutes (`relay_queue_oldest_pending_age_seconds > 300`).

**Likely causes**

- Queue paused or no workers leasing
- Poison / low-priority jobs stuck behind capacity
- `run_at` far in the future counted incorrectly (verify status is truly pending)
- Leader/scheduler issues only if the backlog is schedule-driven

**Diagnosis**

```promql
relay_queue_oldest_pending_age_seconds
relay_queue_jobs{status="pending"}
relay_queue_paused
```

```bash
relayctl queue stats <queue>
relayctl job list --queue <queue> --status pending
relayctl job get <id>
relayctl job attempts <id>
```

**Remediation**

1. Resume if paused: `relayctl queue resume <queue>`
2. Ensure workers are healthy and leasing the queue.
3. Cancel or retry stuck jobs: `relayctl job cancel <id>` / `relayctl job retry <id>`.
4. If a single type poisons the queue, pause, fix handlers, then resume.

---

## RelayDeadJobsSpike

**Meaning:** More than 10 jobs moved to dead in 5 minutes on a queue.

**Likely causes**

- Handler bugs / permanent failures
- Downstream dependency outage
- Retry policy exhausted (`max_attempts` too low for transient errors)
- Bad payload from a producer deploy

**Diagnosis**

```promql
sum by (queue, job_type) (increase(relay_jobs_dead_total[5m]))
sum by (queue, job_type, outcome) (rate(relay_jobs_finished_total[5m]))
```

```bash
relayctl job list --queue <queue> --status dead
relayctl job get <id>
relayctl job attempts <id>
```

In Jaeger, open a dead job’s `trace_context.traceparent` (from job get) and inspect
linked consumer spans for errors.

**Remediation**

1. Fix the handler / dependency; redeploy workers.
2. Replay after fix: `relayctl job retry <id>` or
   `relayctl queue retry-dead <queue>`
3. For intentional poison messages, leave dead and fix producers.
4. Tune retry policy env (`RELAY_RETRY_*`) only after confirming failures are transient.

---

## RelayNoLeader

**Meaning:** `sum(relay_leader) == 0` for 1 minute — no instance holds the scheduler lease.
Cron schedules will not fire.

**Likely causes**

- All `relay-server` replicas down or not ready
- `RELAY_SCHEDULER_ENABLED=false` on every instance
- Storage unreachable (leader election backend)
- Split network / clock issues delaying lease renewal

**Diagnosis**

```promql
relay_leader
sum(relay_leader)
relay_scheduler_fires_total
```

```bash
# Pod / process health
curl -sS http://<pod>:7420/readyz
relayctl schedule list
```

Check server logs for leader election errors; confirm storage connectivity.

**Remediation**

1. Restore at least one healthy server with scheduler enabled.
2. Verify `RELAY_SCHEDULER_ENABLED` and storage URLs in the Deployment/ConfigMap.
3. After a leader appears (`relay_leader == 1`), confirm fires:
   `rate(relay_scheduler_fires_total[5m])` or `relayctl schedule trigger <id>` for a test.
4. Review misfire behavior ([ADR 0011](adr/0011-cron-misfire-policy.md)) — at most one
   catch-up after downtime.

---

## RelayHighErrorRate

**Meaning:** HTTP 5xx rate above 5% of all requests for 2 minutes.

**Likely causes**

- Storage outage or pool exhaustion
- Panic / bug in a hot path
- Auth/config misdeploy (less often 5xx; still check logs)
- Resource limits (CPU/memory throttling)

**Diagnosis**

```promql
sum(rate(relay_http_requests_total{code=~"5.."}[5m]))
  / clamp_min(sum(rate(relay_http_requests_total[5m])), 1e-9)
sum by (route, code) (rate(relay_http_requests_total{code=~"5.."}[5m]))
histogram_quantile(0.95, sum by (le, route) (rate(relay_http_request_duration_seconds_bucket[5m])))
relay_storage_pool_acquired
relay_stats_collect_errors_total
```

Correlate with `trace_id` in logs and Jaeger. Capture a profile if CPU-bound:

```bash
go tool pprof http://localhost:7429/debug/pprof/profile?seconds=30
```

(Admin port only — not exposed on the public Service by default.)

**Remediation**

1. Check storage health and connection strings; scale or raise pool limits if saturated.
2. Roll back a bad deploy; inspect recent changes to HTTP/gRPC adapters.
3. If a single route dominates 5xx, feature-flag or shed load on that path.
4. Ensure readiness probes fail when storage is down so traffic shifts off bad pods.

---

## Cross-cutting checks

| Check | Command / signal |
|---|---|
| Queue depth & pause | `relayctl queue stats <q>` · `relay_queue_*` |
| Build / version | `relay-server --version` · `relay_build_info` |
| Local obs stack | `make up-obs` · `make open-obs` |
| Helm migrate hook | [ADR 0018](adr/0018-helm-migrations-as-hooks.md) |
| Design notes | [docs/adr](adr/) · [phase-4 plan](plan/phase-4.md) |
