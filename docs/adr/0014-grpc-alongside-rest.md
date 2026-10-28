# 0014. gRPC alongside REST

## Status

Accepted

## Context

Workers and operators benefit from a typed, multiplexed RPC surface, but existing HTTP
clients, OpenAPI, and curl-based ops must keep working. Sharing ports via cmux or h2c
adds TLS/proxy complexity for little gain in Phase 3.

## Decision

Expose **gRPC as a second inbound adapter** on a **separate port**, calling the same
application services as REST:

- Listen on `RELAY_GRPC_ADDR` (default `:7421`); HTTP stays on `RELAY_HTTP_ADDR`
  (`:7420`).
- Protos in `proto/relay/v1/` (`JobService`, `QueueService`, `ScheduleService`,
  `APIKeyService`); generated code committed under `gen/relay/v1/`.
- Payload fields are `bytes` holding JSON (opaque, lossless) — not
  `google.protobuf.Struct`.
- Auth via metadata `authorization: Bearer …`; scopes enforced in app (same as REST).
- Health (`grpc.health.v1`) reflects storage readiness; reflection gated by
  `RELAY_GRPC_REFLECTION` (on in compose, off by default locally).
- SDK: `http(s)://` → HTTP transport; `grpc://` / `grpcs://` → gRPC; `WithTransport`
  overrides.

**Rejected for Phase 3:** single-port cmux / h2c multiplexing (harder local debugging,
proxy quirks, weaker operational clarity). May revisit in a later ops phase.

## Consequences

- REST and gRPC stay feature-parity through shared use cases; drift is a wiring bug.
- Operators open two ports (and two Compose mappings); load balancers treat them as
  distinct backends.
- Proto breaking changes are gated by `buf breaking` against main in CI.
