# Relay Helm Chart

Deploys [Relay](https://github.com/MEHRDADsmj/relay) (`relay-server`) with probes, optional HPA/PDB, security context, and Prometheus `ServiceMonitor`.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.14+
- **Postgres and/or Redis** provided separately (this chart does not bundle them)

Recommended companions:

- [Bitnami PostgreSQL](https://github.com/bitnami/charts/tree/main/bitnami/postgresql)
- [Bitnami Redis](https://github.com/bitnami/charts/tree/main/bitnami/redis)
- Managed cloud databases

## Install

```bash
# Example: Postgres backend
helm install relay ./deploy/helm/relay \
  --set config.storage=postgres \
  --set secret.postgresUrl='postgres://relay:relay@postgres:5432/relay?sslmode=disable' \
  --set secret.apiKey='change-me' \
  --set image.repository=ghcr.io/mehrdadsmj/relay-server \
  --set image.tag=0.1.0
```

```bash
# Redis backend (no migration Job)
helm install relay ./deploy/helm/relay \
  --set config.storage=redis \
  --set migration.enabled=false \
  --set secret.redisUrl='redis://redis:6379/0' \
  --set secret.apiKey='change-me'
```

Prefer an existing Secret in production:

```bash
kubectl create secret generic relay-creds \
  --from-literal=RELAY_POSTGRES_URL='...' \
  --from-literal=RELAY_API_KEY='...'

helm install relay ./deploy/helm/relay \
  --set secret.create=false \
  --set secret.existingSecret=relay-creds \
  --set config.storage=postgres
```

## Migrations

When `config.storage=postgres` and `migration.enabled=true` (default), a Helm
`pre-install` / `pre-upgrade` Job runs `relay-server migrate up`. The Deployment
sets `RELAY_MIGRATE_ON_START=false`.

Redis storage skips the Job (migrations are a no-op).

## Metrics

Admin port `7429` is exposed only via the `*-metrics` ClusterIP Service (not the
main Service). Enable Prometheus Operator scraping:

```yaml
metrics:
  serviceMonitor:
    enabled: true
```

## Values

See [`values.yaml`](values.yaml). Schema validation lives in
[`values.schema.json`](values.schema.json) (`helm lint` / `helm install` enforce it).

| Key | Default | Notes |
|---|---|---|
| `replicaCount` | `1` | Ignored when HPA enabled |
| `config.storage` | `postgres` | `postgres` or `redis` |
| `terminationGracePeriodSeconds` | `60` | Must be > `config.shutdownTimeout` + 5s |
| `autoscaling.enabled` | `false` | CPU HPA; add custom metrics via `additionalMetrics` |
| `podDisruptionBudget.enabled` | `false` | Set `minAvailable: 1` when running ≥2 replicas |
| `networkPolicy.enabled` | `false` | Optional ingress lockdown |

## CI smoke

Kind smoke tests live under `deploy/helm/ci/` and run in GitHub Actions (`helm` job):
`helm lint`, `ct lint`, then kind + Postgres/Redis manifests + chart install for both
`postgres` and `redis` storages (enqueue via `relayctl`, lease via HTTP API).

Locally (optional): Docker, Helm 3+, kind, kubectl. Chart-testing (`ct`) is installed
in CI via `helm/chart-testing-action`; install separately if you want `ct lint` locally.

```bash
docker build -t relay-server:ci -f deploy/docker/Dockerfile --build-arg CMD=relay-server .
kind create cluster --name relay-helm
kind load docker-image relay-server:ci --name relay-helm
kubectl apply -f deploy/helm/ci/postgres.yaml -f deploy/helm/ci/redis.yaml
go build -o bin/relayctl ./cmd/relayctl
bash deploy/helm/ci/smoke.sh postgres
bash deploy/helm/ci/smoke.sh redis
```
