package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// RunQueueContract exercises queue registry and stats.
func RunQueueContract(t *testing.T, newRepo func(t *testing.T) Backend) {
	t.Helper()

	t.Run("p2_queue_stats", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)

		// due pending
		due := sampleJob(mustParse("018f3333-0000-7000-8000-000000000001"), "stats", "t", now)
		due.RunAt = now.Add(-10 * time.Minute)
		_, _, err := repo.Create(ctx, due)
		require.NoError(t, err)

		// scheduled pending
		sched := sampleJob(mustParse("018f3333-0000-7000-8000-000000000002"), "stats", "t", now)
		sched.RunAt = now.Add(time.Hour)
		_, _, err = repo.Create(ctx, sched)
		require.NoError(t, err)

		// succeeded on stats
		succ := sampleJob(mustParse("018f3333-0000-7000-8000-000000000004"), "stats", "t", now)
		succ.Priority = 100 // claim before due (priority 0)
		_, _, err = repo.Create(ctx, succ)
		require.NoError(t, err)
		cSucc, err := repo.Claim(ctx, "stats", "w2", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Equal(t, succ.ID, cSucc[0].ID)
		_, err = repo.Complete(ctx, cSucc[0].ID, "w2", now)
		require.NoError(t, err)

		// dead
		dead := sampleJob(mustParse("018f3333-0000-7000-8000-000000000005"), "stats", "t", now)
		dead.Priority = 100
		_, _, err = repo.Create(ctx, dead)
		require.NoError(t, err)
		cDead, err := repo.Claim(ctx, "stats", "w2", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Equal(t, dead.ID, cDead[0].ID)
		_, err = repo.Fail(ctx, cDead[0].ID, "w2", "x", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
		require.NoError(t, err)

		// cancelled pending
		canc := sampleJob(mustParse("018f3333-0000-7000-8000-000000000006"), "stats", "t", now)
		_, _, err = repo.Create(ctx, canc)
		require.NoError(t, err)
		_, err = repo.Cancel(ctx, canc.ID, now)
		require.NoError(t, err)

		// running on stats: high-prio claim, leave leased
		runOnStats := sampleJob(mustParse("018f3333-0000-7000-8000-000000000007"), "stats", "t", now)
		runOnStats.Priority = 100
		_, _, err = repo.Create(ctx, runOnStats)
		require.NoError(t, err)
		cRun, err := repo.Claim(ctx, "stats", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Equal(t, runOnStats.ID, cRun[0].ID)

		stats, err := repo.Stats(ctx, "stats", now)
		require.NoError(t, err)
		assert.Equal(t, "stats", stats.Queue)
		assert.False(t, stats.Paused)
		assert.Equal(t, int64(2), stats.Pending) // due + scheduled
		assert.Equal(t, int64(1), stats.Scheduled)
		assert.Equal(t, int64(1), stats.Running)
		assert.Equal(t, int64(1), stats.Succeeded)
		assert.Equal(t, int64(1), stats.Dead)
		assert.Equal(t, int64(1), stats.Cancelled)
		assert.Equal(t, 10*time.Minute, stats.OldestPendingAge)

		stats2, err := repo.Stats(ctx, "stats", now.Add(2*time.Hour))
		require.NoError(t, err)
		assert.Equal(t, int64(2), stats2.Pending)
		assert.Equal(t, int64(0), stats2.Scheduled)
		assert.Equal(t, 2*time.Hour+10*time.Minute, stats2.OldestPendingAge)

		_, err = repo.Stats(ctx, "missing", now)
		require.ErrorIs(t, err, domain.ErrNotFound)

		queues, err := repo.ListQueues(ctx)
		require.NoError(t, err)
		found := false
		for _, q := range queues {
			if q.Name == "stats" {
				found = true
			}
		}
		assert.True(t, found)
	})
}
