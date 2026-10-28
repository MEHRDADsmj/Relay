package storagetest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// RunMaintenanceContract exercises ReapExpired and PurgeFinished.
func RunMaintenanceContract(t *testing.T, newRepo func(t *testing.T) Backend) {
	t.Helper()

	t.Run("p2_reap_expired", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)

		// 3 queues: each gets one expired reclaimable and one that should go dead.
		for _, q := range []string{"r1", "r2", "r3"} {
			okJob := sampleJob(uuid.Must(uuid.NewV7()), q, "t", now)
			okJob.MaxAttempts = 5
			_, _, err := repo.Create(ctx, okJob)
			require.NoError(t, err)
			_, err = repo.Claim(ctx, q, "w1", 1, 30*time.Second, now)
			require.NoError(t, err)

			deadJob := sampleJob(uuid.Must(uuid.NewV7()), q, "t", now)
			deadJob.MaxAttempts = 1
			_, _, err = repo.Create(ctx, deadJob)
			require.NoError(t, err)
			_, err = repo.Claim(ctx, q, "w1", 1, 30*time.Second, now)
			require.NoError(t, err)
		}

		later := now.Add(31 * time.Second)
		res, err := repo.ReapExpired(ctx, later, 4)
		require.NoError(t, err)
		assert.Equal(t, 4, res.Requeued+res.Dead)
		assert.True(t, res.Requeued >= 1)
		assert.True(t, res.Dead >= 1)

		res2, err := repo.ReapExpired(ctx, later, 10)
		require.NoError(t, err)
		assert.Equal(t, 2, res2.Requeued+res2.Dead)

		// Reclaimable jobs should be claimable again.
		total := 0
		for _, q := range []string{"r1", "r2", "r3"} {
			claimed, err := repo.Claim(ctx, q, "w2", 10, 30*time.Second, later)
			require.NoError(t, err)
			total += len(claimed)
		}
		assert.Equal(t, 3, total)
	})

	t.Run("p2_purge_finished", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)

		old := sampleJob(mustParse("018f4444-0000-7000-8000-000000000001"), "purge", "t", now)
		old.IdempotencyKey = "purge-key"
		_, _, err := repo.Create(ctx, old)
		require.NoError(t, err)
		c, err := repo.Claim(ctx, "purge", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		done, err := repo.Complete(ctx, c[0].ID, "w1", now)
		require.NoError(t, err)

		freshAt := now.Add(time.Minute)
		fresh := sampleJob(mustParse("018f4444-0000-7000-8000-000000000002"), "purge", "t", freshAt)
		_, _, err = repo.Create(ctx, fresh)
		require.NoError(t, err)
		c2, err := repo.Claim(ctx, "purge", "w1", 1, 30*time.Second, freshAt)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, c2[0].ID)
		kept, err := repo.Complete(ctx, c2[0].ID, "w1", freshAt)
		require.NoError(t, err)

		// Cutoff between old and fresh finished_at.
		n, err := repo.PurgeFinished(ctx, now.Add(30*time.Second), 10)
		require.NoError(t, err)
		assert.Equal(t, 1, n)

		_, err = repo.Get(ctx, done.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, err = repo.ListAttempts(ctx, done.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)

		still, err := repo.Get(ctx, kept.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusSucceeded, still.Status)

		// Idempotency key freed: re-enqueue creates a new job.
		again := sampleJob(mustParse("018f4444-0000-7000-8000-000000000003"), "purge", "t", freshAt)
		again.IdempotencyKey = "purge-key"
		stored, created, err := repo.Create(ctx, again)
		require.NoError(t, err)
		assert.True(t, created)
		assert.Equal(t, again.ID, stored.ID)
	})

	t.Run("p2_reap_concurrent_with_claimers", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 3, 14, 0, 0, 0, time.UTC)
		const nJobs = 100

		for i := 0; i < nJobs; i++ {
			j := sampleJob(uuid.Must(uuid.NewV7()), "race", "t", now)
			j.MaxAttempts = 5
			_, _, err := repo.Create(ctx, j)
			require.NoError(t, err)
		}
		// Claim all so they become running, then expire.
		for {
			batch, err := repo.Claim(ctx, "race", "owner", 20, 30*time.Second, now)
			require.NoError(t, err)
			if len(batch) == 0 {
				break
			}
		}
		later := now.Add(31 * time.Second)

		var (
			mu      sync.Mutex
			claimed = make(map[domain.JobID]string)
			errCh   = make(chan error, 16)
			wg      sync.WaitGroup
		)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := repo.ReapExpired(ctx, later, 10); err != nil {
					errCh <- err
					return
				}
			}
		}()

		for w := 0; w < 10; w++ {
			wg.Add(1)
			owner := fmt.Sprintf("w-%d", w)
			go func(owner string) {
				defer wg.Done()
				for {
					jobs, err := repo.Claim(ctx, "race", owner, 5, 30*time.Second, later)
					if err != nil {
						errCh <- err
						return
					}
					if len(jobs) == 0 {
						return
					}
					mu.Lock()
					for _, j := range jobs {
						if prev, ok := claimed[j.ID]; ok {
							errCh <- fmt.Errorf("double assign %s by %s and %s", j.ID, prev, owner)
							mu.Unlock()
							return
						}
						claimed[j.ID] = owner
					}
					mu.Unlock()
				}
			}(owner)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}
	})
}
