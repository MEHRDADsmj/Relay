# 0018. Helm migrations as hooks

## Status

Accepted

## Context

Postgres schema changes ship as goose migrations (`relay-server migrate up`). Running
migrations from every replica on start races and complicates rollouts. In-process
`RELAY_MIGRATE_ON_START=true` is fine for Compose/dev, not for Kubernetes multi-replica
deploys.

## Decision

- Helm chart `deploy/helm/relay` runs a **Job** with hooks `pre-install,pre-upgrade`
  (`helm.sh/hook-weight: "-5"`) that executes `relay-server migrate up` when
  `config.storage=postgres` and `migration.enabled=true`.
- The Deployment sets `RELAY_MIGRATE_ON_START=false` whenever the migration hook is
  enabled so pods never migrate concurrently with the Job.
- Redis storage omits the Job (migrations are a no-op).
- The Job needs DB credentials before the chart Secret exists on first install: either
  inline `secret.postgresUrl` or `secret.existingSecret`. Without credentials the Job
  template is omitted so `helm lint` on defaults stays clean.
- Hook delete policy: `before-hook-creation,hook-succeeded` so upgrades recreate a fresh
  Job and succeeded Jobs are cleaned up.

## Consequences

- Schema advances once per install/upgrade before new pods start.
- Failed migrations fail the Helm release before traffic moves — operators fix schema
  issues without mixed-version pods.
- Chart does not bundle Postgres/Redis; operators supply connection URLs via Secret.
- Local Compose may still use `RELAY_MIGRATE_ON_START=true`; that path is independent of
  the Helm hook.
