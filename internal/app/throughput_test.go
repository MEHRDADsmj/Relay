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

type fakeStatsRepo struct {
	points []domain.ThroughputPoint
	from   time.Time
	to     time.Time
	queue  string
}

func (f *fakeStatsRepo) Throughput(_ context.Context, queue string, from, to time.Time) ([]domain.ThroughputPoint, error) {
	f.queue = queue
	f.from = from
	f.to = to
	return f.points, nil
}

func TestQueueService_ThroughputZeroFill(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 7, 30, 0, time.UTC)
	to := time.Date(2026, 10, 28, 12, 7, 0, 0, time.UTC)
	from := to.Add(-15 * time.Minute)

	stats := &fakeStatsRepo{
		points: []domain.ThroughputPoint{
			{Minute: to.Add(-10 * time.Minute), Enqueued: 3, Succeeded: 1},
			{Minute: to, Failed: 2, Dead: 1},
		},
	}
	repo := newQueueMem()
	qs := app.NewQueueService(repo, repo, stats, &fixedClock{now: now})

	points, err := qs.Throughput(adminCtx(), "q1", 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "q1", stats.queue)
	assert.Equal(t, from, stats.from)
	assert.Equal(t, to, stats.to)

	require.Len(t, points, 16) // inclusive [from, to] at 1m steps for 15m window
	assert.Equal(t, from, points[0].Minute)
	assert.Equal(t, to, points[len(points)-1].Minute)

	var foundSparse int
	for _, p := range points {
		switch {
		case p.Minute.Equal(to.Add(-10 * time.Minute)):
			assert.Equal(t, int64(3), p.Enqueued)
			assert.Equal(t, int64(1), p.Succeeded)
			assert.Zero(t, p.Failed)
			assert.Zero(t, p.Dead)
			foundSparse++
		case p.Minute.Equal(to):
			assert.Zero(t, p.Enqueued)
			assert.Zero(t, p.Succeeded)
			assert.Equal(t, int64(2), p.Failed)
			assert.Equal(t, int64(1), p.Dead)
			foundSparse++
		default:
			assert.Zero(t, p.Enqueued)
			assert.Zero(t, p.Succeeded)
			assert.Zero(t, p.Failed)
			assert.Zero(t, p.Dead)
		}
	}
	assert.Equal(t, 2, foundSparse)
}

func TestQueueService_ThroughputWindowValidation(t *testing.T) {
	t.Parallel()
	repo := newQueueMem()
	qs := app.NewQueueService(repo, repo, &fakeStatsRepo{}, &fixedClock{now: time.Now().UTC()})

	_, err := qs.Throughput(adminCtx(), "q1", 5*time.Minute)
	require.ErrorIs(t, err, domain.ErrValidation)

	for _, w := range []time.Duration{15 * time.Minute, time.Hour, 24 * time.Hour} {
		_, err := qs.Throughput(adminCtx(), "q1", w)
		require.NoError(t, err, "window %s", w)
	}
}
