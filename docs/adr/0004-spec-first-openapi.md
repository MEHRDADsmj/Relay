# 0004. Spec-first OpenAPI HTTP contract

## Status

Accepted

## Context

The server HTTP API and the Go SDK must stay aligned. Hand-written handlers and ad-hoc
client DTOs drift. External consumers also need a stable, documented contract.

## Decision

Treat `api/openapi.yaml` as the source of truth. Generate Chi strict-server interfaces and
models with `oapi-codegen`; commit the output under `internal/adapter/http/oapi`. Handlers
implement the generated interface and map to/from domain types. CI runs
`make check-generated` so drift fails the build.

The SDK speaks the same JSON shapes over HTTP and does not import `internal/`.

## Consequences

- API changes start in the spec; codegen and reviews stay mechanical.
- Problem+json error mapping and request validation stay consistent with the document.
- Generated files must never be hand-edited.
- Spec quality (naming, examples, constraints) directly affects ergonomics of both server
  and clients.
