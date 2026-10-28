# Contributing to Relay

Thanks for taking an interest in Relay. This guide covers local setup, Make targets,
commit style, and the two most common extension paths: a new storage backend and a new
HTTP/gRPC endpoint.

## Setup

**Requirements:** Go (see `go.mod`), Docker Compose v2, `golangci-lint` v2, Make
(Git Bash on Windows), and Go tools pulled by `make generate` (`sqlc`, `oapi-codegen`,
`buf`, `templ`).

```bash
git clone https://github.com/MEHRDADsmj/relay.git
cd relay
make tools
cp .env.example .env
make deps          # Postgres + Redis only
make lint
make test
make build
./bin/relay-server serve
```

Full stack (servers + example workers): `make up`. Demo producer: `make demo`.
Dashboard: http://localhost:7420/ui/ (sign in with an `admin` or `dashboard:read` API key
when auth is enabled).

## Make targets

| Target | Purpose |
|---|---|
| `make lint` | `golangci-lint` (includes `depguard` layer rules) |
| `make test` | Unit tests with `-race` |
| `make test-integration` | Contract/integration (`-tags=integration`, Testcontainers) |
| `make test-e2e` | End-to-end (`-tags=e2e`) |
| `make generate` | sqlc + oapi-codegen + buf + templ |
| `make css` | Rebuild Tailwind `app.css` (standalone CLI) |
| `make check-generated` | Regenerate and fail on dirty tree |
| `make check-links` | Markdown link check via lychee (Docker) |
| `make build` | `relay-server`, `relayctl`, `relay-bench`, examples → `bin/` |
| `make up` / `down` / `demo` | Compose stack helpers |
| `make up-obs` | Stack + Prometheus / Grafana / Jaeger |
| `make loadtest` | k6 in Docker against Compose |

## Commit conventions

Prefer short, imperative subjects with a type prefix when it clarifies intent:

- `feat(web): …`, `fix(sdk): …`, `perf(postgres): …`, `docs: …`, `ci: …`, `chore: …`

One logical change per commit. Do not commit secrets (`.env`, keys, tokens). Do not push
tags or force-push `main`/`master` unless a maintainer explicitly asks.

## Architecture rules

Read [docs/architecture.md](docs/architecture.md) and the [ADR index](docs/adr/README.md).

- `domain` → no infrastructure imports
- `app` → ports only; no HTTP/pgx/Redis client types
- adapters must not import other adapters
- wire everything in `cmd/relay-server`

Crossing a boundary requires a deliberate port and usually an ADR.

## How to add a storage backend

1. Implement the storage ports used by `internal/app` (jobs, queues, schedules, stats,
   leadership, etc.) under `internal/adapter/storage/<name>/`.
2. Register the backend in the composition root (`cmd/relay-server`) behind
   `RELAY_STORAGE`.
3. Pass the shared contract suite in `internal/adapter/storage/storagetest` — every
   existing test must go green for the new adapter (memory/Postgres/Redis are the
   reference).
4. Add integration coverage (`-tags=integration`) and an e2e path if the backend is
   first-class.
5. Document durability/topology assumptions in a new ADR (see ADR 0002 / 0006).

## How to add an endpoint

1. **Spec first:** edit `api/openapi.yaml` (and `proto/relay/v1` if gRPC should expose the
   same use case).
2. **Generate:** `make generate` (oapi-codegen / buf).
3. **App:** add or extend a use case in `internal/app` with clear scope checks.
4. **Handler:** implement the HTTP (and optionally gRPC) adapter; keep mapping thin.
5. **Test:** unit tests with fakes; contract/e2e when storage or full-stack behavior
   matters.
6. **Docs:** update README API table / OpenAPI descriptions; ADR if the decision is
   non-obvious.

Dashboard-only pages go in `internal/adapter/web` and must call app services only
([ADR 0019](docs/adr/0019-server-rendered-dashboard-templ-htmx.md)).

## Docs and links

Markdown links are checked in CI with lychee (`make check-links`). Prefer relative links
inside the repo. Localhost URLs in docs are fine (excluded from the checker).

## License

By contributing, you agree that your contributions are licensed under the MIT License.
