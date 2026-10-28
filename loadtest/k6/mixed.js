import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

/**
 * Mixed load: producers enqueue while simulated workers lease/complete.
 *
 * Env: RELAY_URL, RELAY_API_KEY, RELAY_QUEUE (same as enqueue.js)
 */
const baseURL = __ENV.RELAY_URL || 'http://host.docker.internal:7420';
const apiKey = __ENV.RELAY_API_KEY || '';
const queue = __ENV.RELAY_QUEUE || 'loadtest';

export const options = {
  scenarios: {
    producers: {
      executor: 'constant-vus',
      vus: 10,
      duration: '45s',
      exec: 'produce',
    },
    workers: {
      executor: 'constant-vus',
      vus: 10,
      duration: '45s',
      exec: 'work',
      startTime: '2s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
  },
};

function headers() {
  const h = { 'Content-Type': 'application/json' };
  if (apiKey) {
    h.Authorization = `Bearer ${apiKey}`;
  }
  return h;
}

export function produce() {
  const payload = JSON.stringify({
    queue: queue,
    type: 'loadtest',
    payload: { n: __ITER, vu: __VU, ts: Date.now() },
    idempotency_key: uuidv4(),
  });
  const res = http.post(`${baseURL}/v1/jobs`, payload, { headers: headers() });
  check(res, {
    'enqueue 2xx': (r) => r.status === 200 || r.status === 201,
  });
  sleep(0.1);
}

export function work() {
  const workerId = `k6-w-${__VU}`;
  const leaseBody = JSON.stringify({
    worker_id: workerId,
    max_jobs: 5,
    lease_seconds: 30,
    wait_seconds: 2,
  });
  const lease = http.post(`${baseURL}/v1/queues/${queue}/leases`, leaseBody, {
    headers: headers(),
  });
  check(lease, { 'lease 200': (r) => r.status === 200 });
  if (lease.status !== 200) {
    sleep(0.2);
    return;
  }
  let body;
  try {
    body = lease.json();
  } catch (e) {
    return;
  }
  const jobs = body.jobs || [];
  for (const job of jobs) {
    const complete = http.post(
      `${baseURL}/v1/jobs/${job.id}/complete`,
      JSON.stringify({ worker_id: workerId }),
      { headers: headers() },
    );
    check(complete, { 'complete 2xx': (r) => r.status === 200 || r.status === 204 });
  }
  if (jobs.length === 0) {
    sleep(0.1);
  }
}
