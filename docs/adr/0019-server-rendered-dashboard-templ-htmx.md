# 0019. Server-rendered dashboard (templ + HTMX)

## Status

Accepted

## Context

Phase 5 needs an operator dashboard for queues, jobs, dead letters, schedules, and live
updates. A separate SPA (React/Vue) would add a Node toolchain, a second deployable, and
duplicate auth/API surface. Relay already ships as a single Go binary with embedded
assets; the dashboard should match that operational model.

## Decision

- Implement the dashboard as a third inbound adapter in `internal/adapter/web`.
- Render HTML with [`a-h/templ`](https://templ.guide/) (type-safe Go templates);
  generated `*_templ.go` is committed.
- Progressive enhancement with **HTMX 2.x** + `htmx-ext-sse` for actions, pagination, and
  live fragments. Static JS/CSS are vendored and embedded (`embed.FS`) — no CDN at runtime.
- Style with **Tailwind CSS v4 standalone CLI** (no Node). Compiled `app.css` is committed;
  `make css` rebuilds with a checksum-verified CLI binary.
- Handlers call the same `app` services as REST/gRPC. **No storage access and no business
  logic** in templates or web handlers.
- Live updates: SSE at `/ui/events` pushes rendered fragments on a per-connection ticker
  (~2s) while clients are connected; reads go through `CachedQueueStats`.
- Read-only views work without JavaScript; mutating actions require JS (documented).

## Consequences

- Operators get `/ui/` from the same `relay-server` binary and Compose ports.
- Contributors need `templ` + Tailwind CLI via Make; `make check-generated` covers drift.
- CSP can stay strict (`script-src 'self'`; `style-src 'self'`) because assets are local.
- UX is deliberately simple (server HTML + HTMX), not a full SPA design system.
