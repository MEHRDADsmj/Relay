package sdkprom_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/pkg/sdk/sdkprom"
)

func TestHooks_Metrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	h := sdkprom.NewHooks(reg)

	h.OnLease("default", 0, time.Millisecond, assertErr{})
	h.OnJobStart("default", "email.send")
	h.OnJobEnd("default", "email.send", 250*time.Millisecond, "succeeded")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP relay_worker_jobs_in_flight Jobs currently being processed by this worker.
# TYPE relay_worker_jobs_in_flight gauge
relay_worker_jobs_in_flight{queue="default"} 0
# HELP relay_worker_jobs_total Jobs finished by this worker.
# TYPE relay_worker_jobs_total counter
relay_worker_jobs_total{job_type="email.send",outcome="succeeded",queue="default"} 1
# HELP relay_worker_lease_errors_total Lease RPC errors observed by this worker.
# TYPE relay_worker_lease_errors_total counter
relay_worker_lease_errors_total{queue="default"} 1
`),
		"relay_worker_jobs_in_flight",
		"relay_worker_jobs_total",
		"relay_worker_lease_errors_total",
	))

	count, err := testutil.GatherAndCount(reg, "relay_worker_job_duration_seconds")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

type assertErr struct{}

func (assertErr) Error() string { return "lease failed" }
