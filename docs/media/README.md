# Demo media

Committed screenshots and (optional) GIFs for the README and docs. Keep files size-optimized
(PNG/GIF under ~1–2 MB where possible).

## Present

| File | Description |
|---|---|
| `dashboard-overview-1440.png` | Dashboard overview at 1440×900 |
| `dashboard-overview-375.png` | Dashboard overview at 375×812 (mobile) |

## Placeholders (capture before a public release)

| Target file | How to capture |
|---|---|
| `dashboard-demo.gif` | Compose up → open `/ui/` → run `make demo` → pause/resume a queue → retry a dead job. Record with a screen tool; optimize (e.g. `gifsicle -O3`). |
| `relayctl-demo.gif` | Record a short `relayctl` session with [asciinema](https://asciinema.org/), convert with [`agg`](https://github.com/asciinema/agg): `agg demo.cast docs/media/relayctl-demo.gif`. |
| `grafana-overview.png` | `make up-obs && make demo`, open Grafana Relay Overview, capture the panel set. |
| `jaeger-linked-spans.png` | Same stack; find a producer span with a linked consumer in Jaeger UI. |

Until those files exist, the README links the overview PNGs and points here for the GIF /
ops screenshots.
