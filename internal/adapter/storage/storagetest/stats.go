package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// RunStatsContract exercises minutely throughput counters written by job paths.
func RunStatsContract(t *testing.T, newRepo func(t *testing.T) Backend) {
	t.Helper()

	t.Run("p5_throughput_counters", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 10, 28, 12, 5, 30, 0, time.UTC)
		minute := now.Truncate(time.Minute)

		j1 := sampleJob(mustParse("018f5555-0000-7000-8000-000000000001"), "stats", "t", now)
		j1.IdempotencyKey = "stats-idem"
		j1.MaxAttempts = 3
		_, created, err := repo.Create(ctx, j1)
		require.NoError(t, err)
		require.True(t, created)

		dup := sampleJob(mustParse("018f5555-0000-7000-8000-000000000099"), "stats", "t", now)
		dup.IdempotencyKey = "stats-idem"
		_, created, err = repo.Create(ctx, dup)
		require.NoError(t, err)
		require.False(t, created)

		j2 := sampleJob(mustParse("018f5555-0000-7000-8000-000000000002"), "stats", "t", now)
		j2.MaxAttempts = 3
		_, _, err = repo.Create(ctx, j2)
		require.NoError(t, err)

		j3 := sampleJob(mustParse("018f5555-0000-7000-8000-000000000003"), "stats", "t", now)
		j3.MaxAttempts = 1
		_, _, err = repo.Create(ctx, j3)
		require.NoError(t, err)

		claimed, err := repo.Claim(ctx, "stats", "w1", 10, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, claimed, 3)

		byID := make(map[domain.JobID]domain.Job, len(claimed))
		for _, j := range claimed {
			byID[j.ID] = j
		}

		_, err = repo.Complete(ctx, j1.ID, "w1", now)
		require.NoError(t, err)

		c2 := byID[j2.ID]
		_, err = repo.Fail(ctx, c2.ID, "w1", "boom", c2.DecideFailure(now, time.Second, false), now)
		require.NoError(t, err)

		c3 := byID[j3.ID]
		_, err = repo.Fail(ctx, c3.ID, "w1", "gone", c3.DecideFailure(now, time.Second, true), now)
		require.NoError(t, err)

		points, err := repo.Throughput(ctx, "stats", minute, minute)
		require.NoError(t, err)
		require.Len(t, points, 1)
		p := points[0]
		assert.True(t, p.Minute.Equal(minute))
		assert.Equal(t, int64(3), p.Enqueued, "enqueued")
		assert.Equal(t, int64(1), p.Succeeded, "succeeded")
		assert.Equal(t, int64(1), p.Failed, "failed retries")
		assert.Equal(t, int64(1), p.Dead, "dead")
	})

	t.Run("p5_claim_dead_marks_stats", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 10, 28, 13, 0, 0, 0, time.UTC)

		j := sampleJob(mustParse("018f5555-0000-7000-8000-000000000010"), "claimdead", "t", now)
		j.MaxAttempts = 1
		_, _, err := repo.Create(ctx, j)
		require.NoError(t, err)
		_, err = repo.Claim(ctx, "claimdead", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)

		later := now.Add(31 * time.Second)
		_, err = repo.Claim(ctx, "claimdead", "w2", 1, 30*time.Second, later)
		require.NoError(t, err)

		minute := later.Truncate(time.Minute)
		points, err := repo.Throughput(ctx, "claimdead", minute, minute)
		require.NoError(t, err)
		require.NotEmpty(t, points)
		assert.Equal(t, int64(1), points[0].Dead)
	})

	t.Run("p5_reap_dead_marks_stats", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 10, 28, 14, 0, 0, 0, time.UTC)

		j := sampleJob(mustParse("018f5555-0000-7000-8000-000000000020"), "reapdead", "t", now)
		j.MaxAttempts = 1
		_, _, err := repo.Create(ctx, j)
		require.NoError(t, err)
		_, err = repo.Claim(ctx, "reapdead", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)

		later := now.Add(31 * time.Second)
		res, err := repo.ReapExpired(ctx, later, 10)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Dead)

		minute := later.Truncate(time.Minute)
		points, err := repo.Throughput(ctx, "reapdead", minute, minute)
		require.NoError(t, err)
		require.NotEmpty(t, points)
		assert.Equal(t, int64(1), points[0].Dead)
	})

	t.Run("p5_purge_stats", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		old := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
		fresh := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

		jOld := sampleJob(mustParse("018f5555-0000-7000-8000-000000000030"), "purgeStats", "t", old)
		_, _, err := repo.Create(ctx, jOld)
		require.NoError(t, err)
		jFresh := sampleJob(mustParse("018f5555-0000-7000-8000-000000000031"), "purgeStats", "t", fresh)
		_, _, err = repo.Create(ctx, jFresh)
		require.NoError(t, err)

		n, err := repo.PurgeStats(ctx, fresh)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, n, 1)

		oldPts, err := repo.Throughput(ctx, "purgeStats", old.Truncate(time.Minute), old.Truncate(time.Minute))
		require.NoError(t, err)
		assert.Empty(t, oldPts)

		freshPts, err := repo.Throughput(ctx, "purgeStats", fresh.Truncate(time.Minute), fresh.Truncate(time.Minute))
		require.NoError(t, err)
		require.Len(t, freshPts, 1)
		assert.Equal(t, int64(1), freshPts[0].Enqueued)
	})
}
