#!/usr/bin/env bash
# Kind smoke: install chart for one storage backend, enqueue + lease a job, uninstall.
# Usage: smoke.sh <postgres|redis>
set -euo pipefail

STORAGE="${1:?storage required (postgres|redis)}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NS="${RELAY_SMOKE_NS:-relay-smoke}"
RELEASE="relay-${STORAGE}"
VALUES="${ROOT}/deploy/helm/ci/smoke-values-${STORAGE}.yaml"
HTTP_PORT=17420
API_KEY="smoke-ci-key"

echo "==> smoke storage=${STORAGE} release=${RELEASE} ns=${NS}"

kubectl get ns "${NS}" >/dev/null 2>&1 || kubectl create namespace "${NS}"

helm upgrade --install "${RELEASE}" "${ROOT}/deploy/helm/relay" \
  --namespace "${NS}" \
  --create-namespace \
  --values "${VALUES}" \
  --wait --timeout 3m

kubectl -n "${NS}" rollout status "deploy/${RELEASE}" --timeout=180s

# Port-forward HTTP
kubectl -n "${NS}" port-forward "svc/${RELEASE}" "${HTTP_PORT}:7420" >/tmp/relay-pf.log 2>&1 &
PF_PID=$!
cleanup() {
  kill "${PF_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Wait for port-forward
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${HTTP_PORT}/readyz" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "http://127.0.0.1:${HTTP_PORT}/readyz" >/dev/null

RELAYCTL="${ROOT}/bin/relayctl"
if [[ ! -x "${RELAYCTL}" ]]; then
  RELAYCTL="$(command -v relayctl)"
fi

"${RELAYCTL}" --server "http://127.0.0.1:${HTTP_PORT}" --api-key "${API_KEY}" \
  job enqueue --queue default --type helm-smoke --payload '{"from":"helm-ci"}'

# relayctl has no lease subcommand — hit the lease API directly.
LEASE_RESP="$(curl -sf -X POST "http://127.0.0.1:${HTTP_PORT}/v1/queues/default/leases" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"worker_id":"helm-smoke","max_jobs":1,"lease_seconds":30,"wait_seconds":5}')"

echo "lease response: ${LEASE_RESP}"
# Expect a non-empty jobs array (relayctl has no lease subcommand).
if command -v jq >/dev/null 2>&1; then
  n="$(echo "${LEASE_RESP}" | jq '.jobs | length')"
  [[ "${n}" -ge 1 ]]
else
  echo "${LEASE_RESP}" | grep -Eq '"jobs"[[:space:]]*:[[:space:]]*\[[[:space:]]*\{'
fi

helm uninstall "${RELEASE}" --namespace "${NS}" --wait --timeout 2m || true
kubectl delete namespace "${NS}" --wait=false >/dev/null 2>&1 || true

echo "==> smoke OK (${STORAGE})"
