# 0006. Redis single-node target and durability settings

## Status

Accepted

## Context

Redis Cluster, Sentinel, and multi-key transaction semantics complicate Lua scripts and
key hashing. Phase 1 needs a correct Redis adapter without pretending to solve every HA
topology. Durability still matters for a job queue.

## Decision

Phase 1 targets a **single Redis node**. Atomic operations use embedded Lua scripts over
keys that share a deliberate prefix layout (`RELAY_REDIS_KEY_PREFIX`). Docker Compose runs
Redis with AOF enabled (`--appendonly yes`; fsync policy `everysec` as documented in the
plan). Redis Cluster support is explicitly deferred.

## Consequences

- Lua scripts stay understandable and testable against one node.
- Operators must not point Phase 1 Relay at a Cluster without a future adapter redesign.
- AOF reduces (but does not eliminate) data-loss windows on crash; Postgres remains the
  stronger durability default.
- Failover/HA is an ops concern outside the Phase 1 adapter.
