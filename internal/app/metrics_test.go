package app_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/metrics/prom"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestJobService_MetricsInstrumentation(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(prom.Options{Registerer: reg, MaxSeries: 100})
	require.NoError(t, err)

	repo := newMemRepo()
	clock := &fixedClock{now: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	svc := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Minute}, slog.Default(), nil).
		WithMetrics(m)

	ctx := app.WithPrincipal(context.Background(), app.BootstrapAdmin)
	job, created, err := svc.Enqueue(ctx, app.EnqueueCommand{
		Queue:   "default",
		Type:    "email.send",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, created)

	leased, err := svc.Lease(ctx, app.LeaseCommand{
		Queue:         "default",
		WorkerID:      "w1",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, leased, 1)

	_, err = svc.Complete(ctx, app.CompleteCommand{JobID: job.ID, WorkerID: "w1"})
	require.NoError(t, err)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP relay_jobs_enqueued_total Jobs enqueued (created or idempotent replay).
# TYPE relay_jobs_enqueued_total counter
relay_jobs_enqueued_total{job_type="email.send",queue="default",result="created"} 1
# HELP relay_jobs_leased_total Jobs leased to workers.
# TYPE relay_jobs_leased_total counter
relay_jobs_leased_total{queue="default"} 1
# HELP relay_jobs_finished_total Jobs finished by workers (complete/fail outcomes).
# TYPE relay_jobs_finished_total counter
relay_jobs_finished_total{job_type="email.send",outcome="succeeded",queue="default"} 1
`),
		"relay_jobs_enqueued_total",
		"relay_jobs_leased_total",
		"relay_jobs_finished_total",
	))
}

func TestJobService_MetricsJobDead(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(prom.Options{Registerer: reg, MaxSeries: 100})
	require.NoError(t, err)

	repo := newMemRepo()
	clock := &fixedClock{now: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	svc := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Minute}, nil, nil).
		WithMetrics(m)

	ctx := app.WithPrincipal(context.Background(), app.BootstrapAdmin)
	max := 1
	job, _, err := svc.Enqueue(ctx, app.EnqueueCommand{
		Queue:       "default",
		Type:        "fail.me",
		Payload:     []byte(`{}`),
		MaxAttempts: &max,
	})
	require.NoError(t, err)

	_, err = svc.Lease(ctx, app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	require.NoError(t, err)

	_, err = svc.Fail(ctx, app.FailCommand{
		JobID: job.ID, WorkerID: "w1", Error: "boom", Permanent: true,
	})
	require.NoError(t, err)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP relay_jobs_dead_total Jobs marked dead after exhausting retries.
# TYPE relay_jobs_dead_total counter
relay_jobs_dead_total{job_type="fail.me",queue="default"} 1
`), "relay_jobs_dead_total"))
}

func TestNoopMetrics_Implements(t *testing.T) {
	var _ app.Metrics = app.NoopMetrics{}
	app.NoopMetrics{}.JobEnqueued("q", "t", true)
	app.NoopMetrics{}.JobLeased("q", 1, time.Second)
	app.NoopMetrics{}.JobFinished("q", "t", domain.AttemptSucceeded, time.Second)
	app.NoopMetrics{}.JobDead("q", "t")
	app.NoopMetrics{}.LeaseLost("q")
	app.NoopMetrics{}.SchedulerFired("s", true)
	app.NoopMetrics{}.LeaderChanged("n", true)
	app.NoopMetrics{}.MaintenanceRun("reap", 1, time.Millisecond, nil)
}
