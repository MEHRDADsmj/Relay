package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// queueMem is a minimal QueueRepository + JobRepository for QueueService tests.
type queueMem struct {
	*memRepo
	queues map[string]domain.Queue
}

func newQueueMem() *queueMem {
	return &queueMem{memRepo: newMemRepo(), queues: make(map[string]domain.Queue)}
}

func (r *queueMem) Create(ctx context.Context, job domain.Job) (domain.Job, bool, error) {
	stored, created, err := r.memRepo.Create(ctx, job)
	if err != nil {
		return stored, created, err
	}
	if created {
		if _, ok := r.queues[job.Queue]; !ok {
			r.queues[job.Queue] = domain.Queue{Name: job.Queue, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt}
		}
	}
	return stored, created, nil
}

func (r *queueMem) ListQueues(context.Context) ([]domain.Queue, error) {
	out := make([]domain.Queue, 0, len(r.queues))
	for _, q := range r.queues {
		out = append(out, q)
	}
	return out, nil
}

func (r *queueMem) Stats(_ context.Context, queue string, _ time.Time) (domain.QueueStats, error) {
	q, ok := r.queues[queue]
	if !ok {
		return domain.QueueStats{}, domain.ErrNotFound
	}
	var pending int64
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, j := range r.jobs {
		if j.Queue == queue && j.Status == domain.StatusPending {
			pending++
		}
	}
	return domain.QueueStats{Queue: queue, Paused: q.Paused, Pending: pending}, nil
}

func (r *queueMem) SetPaused(_ context.Context, queue string, paused bool, now time.Time) (domain.Queue, error) {
	q, ok := r.queues[queue]
	if !ok {
		return domain.Queue{}, domain.ErrNotFound
	}
	q.Paused = paused
	q.UpdatedAt = now
	r.queues[queue] = q
	return q, nil
}

func (r *queueMem) Claim(ctx context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error) {
	if q, ok := r.queues[queue]; ok && q.Paused {
		return nil, nil
	}
	return r.memRepo.Claim(ctx, queue, owner, limit, lease, now)
}

func TestQueueService_PauseResumeStatsRetryAll(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	repo := newQueueMem()
	clock := &fixedClock{now: now}
	qs := app.NewQueueService(repo, repo, clock)
	js := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, nil, nil)

	_, _, err := js.Enqueue(context.Background(), app.EnqueueCommand{Queue: "q1", Type: "t"})
	require.NoError(t, err)

	paused, err := qs.Pause(context.Background(), "q1")
	require.NoError(t, err)
	assert.True(t, paused.Paused)

	jobs, err := js.Lease(context.Background(), app.LeaseCommand{
		Queue: "q1", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	assert.Empty(t, jobs)

	_, err = qs.Resume(context.Background(), "q1")
	require.NoError(t, err)

	stats, err := qs.Stats(context.Background(), "q1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Pending)

	leased, err := js.Lease(context.Background(), app.LeaseCommand{
		Queue: "q1", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, leased, 1)
	_, err = js.Fail(context.Background(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: "x", Permanent: true,
	})
	require.NoError(t, err)

	n, err := qs.RetryAllDead(context.Background(), app.RetryAllDeadCommand{Queue: "q1", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	lists, err := qs.ListQueues(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, lists)
}

func TestQueueService_RetryAllDeadValidation(t *testing.T) {
	t.Parallel()
	repo := newQueueMem()
	qs := app.NewQueueService(repo, repo, &fixedClock{now: time.Now().UTC()})
	_, err := qs.RetryAllDead(context.Background(), app.RetryAllDeadCommand{Queue: "BAD"})
	require.ErrorIs(t, err, domain.ErrValidation)
}
