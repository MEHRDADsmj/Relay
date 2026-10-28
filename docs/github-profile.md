# GitHub repository profile (owner checklist)

Agents **do not** change GitHub repository settings. Maintainers can apply the following
when publishing or refreshing the public repo page.

## Suggested metadata

| Field | Recommendation |
|---|---|
| **Description** | Durable distributed job queue in Go — Postgres or Redis, HTTP/gRPC, cron, SDK, and a server-rendered dashboard. |
| **Website** | Leave empty, or link a demo / docs site if you host one. |
| **Topics** | `go`, `golang`, `job-queue`, `background-jobs`, `postgres`, `redis`, `grpc`, `openapi`, `htmx`, `clean-architecture`, `observability` |
| **License** | MIT (already in-repo) |

## Social preview

| Asset | Notes |
|---|---|
| Open Graph / social image | Prefer a 1280×640 (or 2:1) crop of the dashboard overview, or a simple brand card with the architecture Mermaid export. Source stills: [`docs/media/dashboard-overview-1440.png`](media/dashboard-overview-1440.png). |
| README hero | Same screenshot; keep file size modest (<500 KB). |

## Pins and About

- Pin this repo if it is the primary portfolio piece.
- About blurb can reuse the one-line pitch from the root [README](../README.md).

## Releases

GitHub Releases are produced by GoReleaser on `v*` tags (binaries + GHCR images + SBOM).
Do not push tags from automation agents unless a human explicitly requests it.
