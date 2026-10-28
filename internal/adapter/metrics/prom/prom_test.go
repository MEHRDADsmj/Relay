package prom_test

import (
	"context"
	"errors"
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

func TestMetrics_JobLifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(prom.Options{Registerer: reg, MaxSeries: 100})
	require.NoError(t, err)

	m.JobEnqueued("default", "email", true)
	m.JobEnqueued("default", "email", false)
	m.JobLeased("default", 2, 15*time.Millisecond)
	m.JobFinished("default", "email", domain.AttemptSucceeded, 100*time.Millisecond)
	m.JobDead("default", "email")
	m.LeaseLost("default")
	m.SchedulerFired("hourly", true)
	m.LeaderChanged("scheduler", true)
	m.MaintenanceRun("reap", 3, 5*time.Millisecond, nil)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP relay_jobs_enqueued_total Jobs enqueued (created or idempotent replay).
# TYPE relay_jobs_enqueued_total counter
relay_jobs_enqueued_total{job_type="email",queue="default",result="created"} 1
relay_jobs_enqueued_total{job_type="email",queue="default",result="replayed"} 1
# HELP relay_jobs_leased_total Jobs leased to workers.
# TYPE relay_jobs_leased_total counter
relay_jobs_leased_total{queue="default"} 2
# HELP relay_jobs_finished_total Jobs finished by workers (complete/fail outcomes).
# TYPE relay_jobs_finished_total counter
relay_jobs_finished_total{job_type="email",outcome="succeeded",queue="default"} 1
# HELP relay_jobs_dead_total Jobs marked dead after exhausting retries.
# TYPE relay_jobs_dead_total counter
relay_jobs_dead_total{job_type="email",queue="default"} 1
# HELP relay_lease_lost_total Lease ownership lost during worker operations.
# TYPE relay_lease_lost_total counter
relay_lease_lost_total{queue="default"} 1
# HELP relay_scheduler_fires_total Scheduler fire attempts.
# TYPE relay_scheduler_fires_total counter
relay_scheduler_fires_total{result="ok",schedule="hourly"} 1
# HELP relay_leader 1 if this instance holds leadership for name.
# TYPE relay_leader gauge
relay_leader{name="scheduler"} 1
# HELP relay_maintenance_runs_total Maintenance loop iterations.
# TYPE relay_maintenance_runs_total counter
relay_maintenance_runs_total{kind="reap"} 1
# HELP relay_maintenance_affected_total Jobs affected by maintenance.
# TYPE relay_maintenance_affected_total counter
relay_maintenance_affected_total{kind="reap"} 3
`),
		"relay_jobs_enqueued_total",
		"relay_jobs_leased_total",
		"relay_jobs_finished_total",
		"relay_jobs_dead_total",
		"relay_lease_lost_total",
		"relay_scheduler_fires_total",
		"relay_leader",
		"relay_maintenance_runs_total",
		"relay_maintenance_affected_total",
	))
}

func TestMetrics_CardinalityCap(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(prom.Options{Registerer: reg, MaxSeries: 2})
	require.NoError(t, err)

	m.JobEnqueued("q", "a", true)
	m.JobEnqueued("q", "b", true)
	m.JobEnqueued("q", "c", true) // beyond cap → __other__
	m.JobEnqueued("q", "c", true) // stays __other__

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP relay_jobs_enqueued_total Jobs enqueued (created or idempotent replay).
# TYPE relay_jobs_enqueued_total counter
relay_jobs_enqueued_total{job_type="__other__",queue="q",result="created"} 2
relay_jobs_enqueued_total{job_type="a",queue="q",result="created"} 1
relay_jobs_enqueued_total{job_type="b",queue="q",result="created"} 1
`), "relay_jobs_enqueued_total"))
}

type fakeQueueStats struct {
	queues    []domain.Queue
	stats     map[string]domain.QueueStats
	listErr   error
	statsErr  error
	listCalls int
	block     chan struct{}
}

func (f *fakeQueueStats) ListQueues(ctx context.Context) ([]domain.Queue, error) {
	f.listCalls++
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.queues, nil
}

func (f *fakeQueueStats) Stats(ctx context.Context, queue string) (domain.QueueStats, error) {
	if f.statsErr != nil {
		return domain.QueueStats{}, f.statsErr
	}
	st, ok := f.stats[queue]
	if !ok {
		return domain.QueueStats{}, domain.ErrNotFound
	}
	return st, nil
}

func TestQueueStatsCollector_CacheAndTimeout(t *testing.T) {
	src := &fakeQueueStats{
		queues: []domain.Queue{{Name: "default"}},
		stats: map[string]domain.QueueStats{
			"default": {Queue: "default", Pending: 5, Running: 1, OldestPendingAge: 2 * time.Second},
		},
	}
	now := time.Unix(1000, 0).UTC()
	c := prom.NewQueueStatsCollector(src, prom.QueueStatsOptions{
		CacheTTL: 10 * time.Second,
		Timeout:  50 * time.Millisecond,
		Clock:    func() time.Time { return now },
	})
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(c))
	require.NoError(t, reg.Register(c.CollectErrors))

	// First scrape populates cache.
	_ = testutil.CollectAndCount(c)
	require.Equal(t, 1, src.listCalls)

	// Second scrape within TTL uses cache.
	_ = testutil.CollectAndCount(c)
	require.Equal(t, 1, src.listCalls)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP relay_queue_jobs Jobs in a queue by status.
# TYPE relay_queue_jobs gauge
relay_queue_jobs{queue="default",status="cancelled"} 0
relay_queue_jobs{queue="default",status="dead"} 0
relay_queue_jobs{queue="default",status="pending"} 5
relay_queue_jobs{queue="default",status="running"} 1
relay_queue_jobs{queue="default",status="scheduled"} 0
relay_queue_jobs{queue="default",status="succeeded"} 0
# HELP relay_queue_oldest_pending_age_seconds Age of the oldest pending job in the queue.
# TYPE relay_queue_oldest_pending_age_seconds gauge
relay_queue_oldest_pending_age_seconds{queue="default"} 2
# HELP relay_queue_paused 1 if the queue is paused.
# TYPE relay_queue_paused gauge
relay_queue_paused{queue="default"} 0
`), "relay_queue_jobs", "relay_queue_oldest_pending_age_seconds", "relay_queue_paused"))

	// Expire cache; source blocks past timeout → emit last cache + error.
	now = now.Add(11 * time.Second)
	src.block = make(chan struct{})
	_ = testutil.CollectAndCount(c)
	require.GreaterOrEqual(t, testutil.ToFloat64(c.CollectErrors), 1.0)

	// Unblock for cleanliness.
	close(src.block)
}

func TestQueueStatsCollector_ErrorIncrements(t *testing.T) {
	src := &fakeQueueStats{listErr: errors.New("boom")}
	c := prom.NewQueueStatsCollector(src, prom.QueueStatsOptions{
		CacheTTL: time.Millisecond,
		Timeout:  time.Second,
	})
	_ = testutil.CollectAndCount(c)
	require.Equal(t, float64(1), testutil.ToFloat64(c.CollectErrors))
}

var _ app.Metrics = (*prom.Metrics)(nil)
