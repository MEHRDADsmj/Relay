# k6 load tests

Scripts exercise the Relay HTTP API. Run via Docker (no local k6 install required):

```bash
make loadtest                 # enqueue.js → loadtest/results/
make loadtest SCRIPT=mixed.js
```

Environment (passed into the container):

| Variable | Default | Notes |
|---|---|---|
| `RELAY_URL` | `http://host.docker.internal:7420` | On Linux compose network, prefer `http://relay-server:7420` with `--network relay_default` |
| `RELAY_API_KEY` | empty | Bearer token when auth is enabled |
| `RELAY_QUEUE` | `loadtest` | Target queue |

Thresholds (documented rates in `enqueue.js`):

- `http_req_failed < 0.1%`
- enqueue `p(95) < 50ms`
