import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

/**
 * Ramp producers posting jobs to Relay HTTP API.
 *
 * Env:
 *   RELAY_URL      default http://host.docker.internal:7420
 *   RELAY_API_KEY  optional Bearer token
 *   RELAY_QUEUE    default loadtest
 */
const baseURL = __ENV.RELAY_URL || 'http://host.docker.internal:7420';
const apiKey = __ENV.RELAY_API_KEY || '';
const queue = __ENV.RELAY_QUEUE || 'loadtest';

const enqueueLatency = new Trend('relay_enqueue_latency', true);

export const options = {
  scenarios: {
    enqueue: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 20 },
        { duration: '30s', target: 50 },
        { duration: '15s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'], // < 0.1%
    http_req_duration: ['p(95)<50'], // p95 < 50ms at this rate
    relay_enqueue_latency: ['p(95)<50'],
  },
};

function headers() {
  const h = { 'Content-Type': 'application/json' };
  if (apiKey) {
    h.Authorization = `Bearer ${apiKey}`;
  }
  return h;
}

export default function () {
  const payload = JSON.stringify({
    queue: queue,
    type: 'loadtest',
    payload: { n: __ITER, vu: __VU, ts: Date.now() },
    idempotency_key: uuidv4(),
  });
  const res = http.post(`${baseURL}/v1/jobs`, payload, { headers: headers() });
  enqueueLatency.add(res.timings.duration);
  check(res, {
    'enqueue 2xx': (r) => r.status === 200 || r.status === 201,
  });
  sleep(0.05);
}
