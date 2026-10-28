# Relay HTTP API with curl

No Go required. Start the stack, then copy-paste the commands below.

## Start Relay

```bash
# From the repo root — Postgres backend, two example workers
docker compose up -d --build --scale example-worker=2

# Optional: enqueue the demo batch
docker compose --profile demo run --rm --no-deps example-producer
```

Base URL: `http://localhost:7420`. When `RELAY_API_KEY` is set (or stored keys exist),
add `-H "Authorization: Bearer $RELAY_API_KEY"`.

## Enqueue a job

```bash
curl -sS -X POST http://localhost:7420/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "queue": "default",
    "type": "send_welcome_email",
    "payload": {"user_id": "u1", "email": "u1@example.com"},
    "idempotency_key": "welcome-u1"
  }'
```

Save the returned `id` as `JOB_ID`.

## Get job and attempts

```bash
curl -sS "http://localhost:7420/v1/jobs/$JOB_ID"
curl -sS "http://localhost:7420/v1/jobs/$JOB_ID/attempts"
```

## Queue stats and job list

```bash
curl -sS http://localhost:7420/v1/queues
curl -sS http://localhost:7420/v1/queues/default
curl -sS "http://localhost:7420/v1/queues/default/jobs?status=pending"
```

## Lease loop (worker-shaped)

```bash
# Claim up to 5 jobs; wait up to 20s if the queue is empty
curl -sS -X POST http://localhost:7420/v1/queues/default/leases \
  -H 'Content-Type: application/json' \
  -d '{"max_jobs": 5, "wait_seconds": 20}'
```

For each leased job (use `id` + `lease_token` from the response):

```bash
curl -sS -X POST "http://localhost:7420/v1/jobs/$JOB_ID/heartbeat" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$LEASE_TOKEN\"}"

# Success
curl -sS -X POST "http://localhost:7420/v1/jobs/$JOB_ID/complete" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$LEASE_TOKEN\"}"

# Or failure (retry / dead per policy)
curl -sS -X POST "http://localhost:7420/v1/jobs/$JOB_ID/fail" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$LEASE_TOKEN\", \"error\": \"smtp timeout\"}"
```

## Pause, resume, dead-letter retry

```bash
curl -sS -X POST http://localhost:7420/v1/queues/default/pause
curl -sS -X POST http://localhost:7420/v1/queues/default/resume
curl -sS -X POST http://localhost:7420/v1/queues/default/dead/retry
```

## Schedules

```bash
curl -sS -X POST http://localhost:7420/v1/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "hourly-welcome",
    "cron": "@hourly",
    "queue": "default",
    "type": "send_welcome_email",
    "payload": {"user_id": "u1"}
  }'

curl -sS http://localhost:7420/v1/schedules
```

## Health

```bash
curl -sS http://localhost:7420/healthz
curl -sS http://localhost:7420/readyz
```

## Dashboard

Open http://localhost:7420/ui/ in a browser. Full OpenAPI contract:
[`api/openapi.yaml`](../../api/openapi.yaml).

```bash
docker compose down -v
```
