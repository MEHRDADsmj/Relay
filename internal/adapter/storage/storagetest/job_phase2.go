package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func runJobContractPhase2(t *testing.T, newRepo func(t *testing.T) Backend) {
	t.Helper()

	t.Run("p2_claim_paused_queue", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f2222-0000-7000-8000-000000000001"), "pausedq", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		_, err = repo.SetPaused(ctx, "pausedq", true, now)
		require.NoError(t, err)

		claimed, err := repo.Claim(ctx, "pausedq", "w1", 10, 30*time.Second, now)
		require.NoError(t, err)
		assert.Empty(t, claimed)

		_, err = repo.SetPaused(ctx, "pausedq", false, now)
		require.NoError(t, err)

		claimed, err = repo.Claim(ctx, "pausedq", "w1", 10, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
	})

	t.Run("p2_attempts_claim_complete", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f2222-0000-7000-8000-000000000010"), "att", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		claimed, err := repo.Claim(ctx, "att", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		_, err = repo.Complete(ctx, claimed[0].ID, "w1", now)
		require.NoError(t, err)

		atts, err := repo.ListAttempts(ctx, claimed[0].ID)
		require.NoError(t, err)
		require.Len(t, atts, 1)
		assert.Equal(t, 1, atts[0].Number)
		assert.Equal(t, "w1", atts[0].WorkerID)
		assert.Equal(t, domain.AttemptSucceeded, atts[0].Outcome)
		require.NotNil(t, atts[0].FinishedAt)
	})

	t.Run("p2_attempts_fail_then_complete", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f2222-0000-7000-8000-000000000011"), "att2", "t", now)
		job.MaxAttempts = 5
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		c1, err := repo.Claim(ctx, "att2", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		next := now.Add(time.Minute)
		_, err = repo.Fail(ctx, c1[0].ID, "w1", "boom", domain.FailureDecision{
			NextStatus: domain.StatusPending, NextRunAt: &next,
		}, now)
		require.NoError(t, err)

		c2, err := repo.Claim(ctx, "att2", "w2", 1, 30*time.Second, next)
		require.NoError(t, err)
		_, err = repo.Complete(ctx, c2[0].ID, "w2", next)
		require.NoError(t, err)

		atts, err := repo.ListAttempts(ctx, c1[0].ID)
		require.NoError(t, err)
		require.Len(t, atts, 2)
		assert.Equal(t, domain.AttemptFailed, atts[0].Outcome)
		assert.Equal(t, 1, atts[0].Number)
		assert.Equal(t, "w1", atts[0].WorkerID)
		assert.Equal(t, domain.AttemptSucceeded, atts[1].Outcome)
		assert.Equal(t, 2, atts[1].Number)
		assert.Equal(t, "w2", atts[1].WorkerID)
	})

	t.Run("p2_expired_lease_attempt_expired", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f2222-0000-7000-8000-000000000012"), "exp", "t", now)
		job.MaxAttempts = 5
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		c1, err := repo.Claim(ctx, "exp", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		later := now.Add(31 * time.Second)
		c2, err := repo.Claim(ctx, "exp", "w2", 1, 30*time.Second, later)
		require.NoError(t, err)
		require.Len(t, c2, 1)

		atts, err := repo.ListAttempts(ctx, c1[0].ID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(atts), 2)
		assert.Equal(t, domain.AttemptExpired, atts[0].Outcome)
		assert.Equal(t, domain.AttemptRunning, atts[1].Outcome)
	})

	t.Run("p2_release", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f2222-0000-7000-8000-000000000020"), "rel", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "rel", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		assert.Equal(t, 1, claimed[0].Attempts)

		released, err := repo.Release(ctx, claimed[0].ID, "w1", now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusPending, released.Status)
		assert.Equal(t, 0, released.Attempts)
		assertEqualTime(t, now, released.RunAt)

		_, err = repo.Release(ctx, claimed[0].ID, "other", now)
		require.ErrorIs(t, err, domain.ErrLeaseLost)

		again, err := repo.Claim(ctx, "rel", "w2", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, again, 1)

		atts, err := repo.ListAttempts(ctx, claimed[0].ID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(atts), 1)
		assert.Equal(t, domain.AttemptReleased, atts[0].Outcome)
	})

	t.Run("p2_cancel", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

		pending := sampleJob(mustParse("018f2222-0000-7000-8000-000000000030"), "can", "t", now)
		_, _, err := repo.Create(ctx, pending)
		require.NoError(t, err)
		got, err := repo.Cancel(ctx, pending.ID, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusCancelled, got.Status)
		require.NotNil(t, got.CancelledAt)
		empty, err := repo.Claim(ctx, "can", "w1", 10, 30*time.Second, now)
		require.NoError(t, err)
		assert.Empty(t, empty)

		run := sampleJob(mustParse("018f2222-0000-7000-8000-000000000031"), "can2", "t", now)
		_, _, err = repo.Create(ctx, run)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "can2", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		flagged, err := repo.Cancel(ctx, claimed[0].ID, now)
		require.NoError(t, err)
		assert.True(t, flagged.CancelRequested)
		assert.Equal(t, domain.StatusRunning, flagged.Status)

		done, err := repo.Complete(ctx, claimed[0].ID, "w1", now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusCancelled, done.Status)

		run2 := sampleJob(mustParse("018f2222-0000-7000-8000-000000000032"), "can3", "t", now)
		run2.MaxAttempts = 5
		_, _, err = repo.Create(ctx, run2)
		require.NoError(t, err)
		c2, err := repo.Claim(ctx, "can3", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		_, err = repo.Cancel(ctx, c2[0].ID, now)
		require.NoError(t, err)
		failed, err := repo.Fail(ctx, c2[0].ID, "w1", "x", domain.FailureDecision{NextStatus: domain.StatusPending, NextRunAt: &now}, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusCancelled, failed.Status)

		_, err = repo.Cancel(ctx, pending.ID, now)
		require.ErrorIs(t, err, domain.ErrInvalidTransition)
	})

	t.Run("p2_retry", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f2222-0000-7000-8000-000000000040"), "rty", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "rty", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		dead, err := repo.Fail(ctx, claimed[0].ID, "w1", "gone", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusDead, dead.Status)

		retried, err := repo.Retry(ctx, dead.ID, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusPending, retried.Status)
		assert.Equal(t, 0, retried.Attempts)
		assert.Equal(t, "gone", retried.LastError)
		assert.Nil(t, retried.FinishedAt)

		again, err := repo.Claim(ctx, "rty", "w2", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, again, 1)

		_, err = repo.Retry(ctx, again[0].ID, now)
		require.ErrorIs(t, err, domain.ErrInvalidTransition)
	})

	t.Run("p2_list_by_status_pagination", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		base := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

		var deadIDs []domain.JobID
		for i := 0; i < 120; i++ {
			now := base.Add(time.Duration(i) * time.Millisecond)
			j := sampleJob(uuid.Must(uuid.NewV7()), "page", "t", now)
			_, _, err := repo.Create(ctx, j)
			require.NoError(t, err)
			claimed, err := repo.Claim(ctx, "page", "w1", 1, 30*time.Second, now)
			require.NoError(t, err)
			require.Len(t, claimed, 1)
			dead, err := repo.Fail(ctx, claimed[0].ID, "w1", "x", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
			require.NoError(t, err)
			deadIDs = append(deadIDs, dead.ID)
		}

		var (
			seen   = make(map[domain.JobID]struct{})
			cursor string
			pages  []int
		)
		for {
			page, err := repo.ListByStatus(ctx, "page", domain.StatusDead, domain.PageRequest{Limit: 50, Cursor: cursor})
			require.NoError(t, err)
			pages = append(pages, len(page.Items))
			for _, j := range page.Items {
				_, dup := seen[j.ID]
				assert.False(t, dup, "duplicate %s", j.ID)
				seen[j.ID] = struct{}{}
			}
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor

			// Concurrent inserts of pending jobs must not disturb dead pagination.
			pj := sampleJob(uuid.Must(uuid.NewV7()), "page", "t", base.Add(time.Hour))
			_, _, err = repo.Create(ctx, pj)
			require.NoError(t, err)
		}
		assert.Equal(t, []int{50, 50, 20}, pages)
		assert.Len(t, seen, 120)
		for _, id := range deadIDs {
			_, ok := seen[id]
			assert.True(t, ok, "missing %s", id)
		}
	})
}
