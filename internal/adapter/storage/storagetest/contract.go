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

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// RunContractTests exercises the JobRepository contract (phase-1 §6.1).
// Each subtest receives a fresh empty repository from newRepo.
func RunContractTests(t *testing.T, newRepo func(t *testing.T) app.JobRepository) {
	t.Helper()

	t.Run("1_create_get_round_trip", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 123456789, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000001"), "default", "email.send", now)
		job.Priority = 5
		job.IdempotencyKey = "key-1"
		job.Payload = []byte(`{"n":1}`)

		stored, created, err := repo.Create(ctx, job)
		require.NoError(t, err)
		require.True(t, created)

		got, err := repo.Get(ctx, stored.ID)
		require.NoError(t, err)
		assertJobsEqual(t, stored, got)
	})

	t.Run("2_get_unknown_not_found", func(t *testing.T) {
		repo := newRepo(t)
		_, err := repo.Get(context.Background(), mustParse("018f1111-0000-7000-8000-000000000099"))
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("3_idempotency", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

		first := sampleJob(mustParse("018f1111-0000-7000-8000-000000000010"), "q1", "t", now)
		first.IdempotencyKey = "same"
		a, created, err := repo.Create(ctx, first)
		require.NoError(t, err)
		require.True(t, created)

		dup := sampleJob(mustParse("018f1111-0000-7000-8000-000000000011"), "q1", "t", now)
		dup.IdempotencyKey = "same"
		b, created, err := repo.Create(ctx, dup)
		require.NoError(t, err)
		assert.False(t, created)
		assert.Equal(t, a.ID, b.ID)

		otherQ := sampleJob(mustParse("018f1111-0000-7000-8000-000000000012"), "q2", "t", now)
		otherQ.IdempotencyKey = "same"
		c, created, err := repo.Create(ctx, otherQ)
		require.NoError(t, err)
		assert.True(t, created)
		assert.NotEqual(t, a.ID, c.ID)
	})

	t.Run("4_claim_respects_run_at", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		future := now.Add(time.Hour)

		ready := sampleJob(mustParse("018f1111-0000-7000-8000-000000000020"), "default", "t", now)
		ready.RunAt = now
		_, _, err := repo.Create(ctx, ready)
		require.NoError(t, err)

		later := sampleJob(mustParse("018f1111-0000-7000-8000-000000000021"), "default", "t", now)
		later.RunAt = future
		later.CreatedAt = now
		later.UpdatedAt = now
		_, _, err = repo.Create(ctx, later)
		require.NoError(t, err)

		claimed, err := repo.Claim(ctx, "default", "w1", 10, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		assert.Equal(t, ready.ID, claimed[0].ID)
	})

	t.Run("5_claim_ordering", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		t0 := now
		t1 := now.Add(time.Minute)

		// Expected order: high prio first; same prio → earlier run_at; same run_at → lower id
		jobs := []domain.Job{
			withPriRun(sampleJob(mustParse("018f1111-0000-7000-8000-000000000033"), "ord", "t", now), 1, t1),
			withPriRun(sampleJob(mustParse("018f1111-0000-7000-8000-000000000031"), "ord", "t", now), 10, t0),
			withPriRun(sampleJob(mustParse("018f1111-0000-7000-8000-000000000032"), "ord", "t", now), 1, t0),
			withPriRun(sampleJob(mustParse("018f1111-0000-7000-8000-000000000030"), "ord", "t", now), 1, t0),
		}
		for _, j := range jobs {
			_, _, err := repo.Create(ctx, j)
			require.NoError(t, err)
		}

		claimed, err := repo.Claim(ctx, "ord", "w1", 10, 30*time.Second, now.Add(2*time.Minute))
		require.NoError(t, err)
		require.Len(t, claimed, 4)
		assert.Equal(t, jobs[1].ID, claimed[0].ID) // prio 10
		assert.Equal(t, jobs[3].ID, claimed[1].ID) // prio 1, t0, lower id
		assert.Equal(t, jobs[2].ID, claimed[2].ID) // prio 1, t0, higher id
		assert.Equal(t, jobs[0].ID, claimed[3].ID) // prio 1, t1
	})

	t.Run("6_claim_limit_and_queue_isolation", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		for i := 0; i < 5; i++ {
			j := sampleJob(uuid.Must(uuid.NewV7()), "qa", "t", now)
			_, _, err := repo.Create(ctx, j)
			require.NoError(t, err)
		}
		other := sampleJob(uuid.Must(uuid.NewV7()), "qb", "t", now)
		_, _, err := repo.Create(ctx, other)
		require.NoError(t, err)

		claimed, err := repo.Claim(ctx, "qa", "w1", 2, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, claimed, 2)
		for _, j := range claimed {
			assert.Equal(t, "qa", j.Queue)
		}
		qb, err := repo.Claim(ctx, "qb", "w1", 10, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, qb, 1)
		assert.Equal(t, other.ID, qb[0].ID)
	})

	t.Run("7_claim_increments_attempts_sets_lease", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000040"), "default", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		claimed, err := repo.Claim(ctx, "default", "worker-a", 1, 45*time.Second, now)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		assert.Equal(t, domain.StatusRunning, claimed[0].Status)
		assert.Equal(t, 1, claimed[0].Attempts)
		assert.Equal(t, "worker-a", claimed[0].LeaseOwner)
		require.NotNil(t, claimed[0].LeaseExpiresAt)
		assertEqualTime(t, now.Add(45*time.Second), *claimed[0].LeaseExpiresAt)
		assertEqualTime(t, now, claimed[0].UpdatedAt)
	})

	t.Run("8_expired_lease_reclaimable", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		max := 5
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000050"), "default", "t", now)
		job.MaxAttempts = max
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		first, err := repo.Claim(ctx, "default", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, first, 1)
		assert.Equal(t, 1, first[0].Attempts)

		later := now.Add(31 * time.Second)
		second, err := repo.Claim(ctx, "default", "w2", 1, 30*time.Second, later)
		require.NoError(t, err)
		require.Len(t, second, 1)
		assert.Equal(t, first[0].ID, second[0].ID)
		assert.Equal(t, "w2", second[0].LeaseOwner)
		assert.Equal(t, 2, second[0].Attempts)
	})

	t.Run("9_expired_lease_dead_when_attempts_exhausted", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		max := 1
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000060"), "default", "t", now)
		job.MaxAttempts = max
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)

		first, err := repo.Claim(ctx, "default", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)
		require.Len(t, first, 1)
		assert.Equal(t, 1, first[0].Attempts)

		later := now.Add(31 * time.Second)
		second, err := repo.Claim(ctx, "default", "w2", 1, 30*time.Second, later)
		require.NoError(t, err)
		assert.Empty(t, second)

		got, err := repo.Get(ctx, first[0].ID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusDead, got.Status)
		assert.Equal(t, "lease expired", got.LastError)
		require.NotNil(t, got.FinishedAt)
		assertEqualTime(t, later, *got.FinishedAt)
		assert.Empty(t, got.LeaseOwner)
		assert.Nil(t, got.LeaseExpiresAt)
	})

	t.Run("10_wrong_owner_lease_lost", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000070"), "default", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "default", "owner", 1, 30*time.Second, now)
		require.NoError(t, err)
		id := claimed[0].ID

		_, err = repo.ExtendLease(ctx, id, "other", 30*time.Second, now)
		require.ErrorIs(t, err, domain.ErrLeaseLost)
		_, err = repo.Complete(ctx, id, "other", now)
		require.ErrorIs(t, err, domain.ErrLeaseLost)
		_, err = repo.Fail(ctx, id, "other", "x", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
		require.ErrorIs(t, err, domain.ErrLeaseLost)
	})

	t.Run("11_expired_lease_ops_lease_lost", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000080"), "default", "t", now)
		job.MaxAttempts = 10
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "default", "owner", 1, 30*time.Second, now)
		require.NoError(t, err)
		id := claimed[0].ID
		expired := now.Add(31 * time.Second)

		_, err = repo.ExtendLease(ctx, id, "owner", 30*time.Second, expired)
		require.ErrorIs(t, err, domain.ErrLeaseLost)
		_, err = repo.Complete(ctx, id, "owner", expired)
		require.ErrorIs(t, err, domain.ErrLeaseLost)
		_, err = repo.Fail(ctx, id, "owner", "x", domain.FailureDecision{NextStatus: domain.StatusPending, NextRunAt: &expired}, expired)
		require.ErrorIs(t, err, domain.ErrLeaseLost)
	})

	t.Run("12_complete_sets_succeeded", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-000000000090"), "default", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "default", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)

		done, err := repo.Complete(ctx, claimed[0].ID, "w1", now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusSucceeded, done.Status)
		require.NotNil(t, done.FinishedAt)
		assertEqualTime(t, now, *done.FinishedAt)
		assert.Empty(t, done.LeaseOwner)
		assert.Nil(t, done.LeaseExpiresAt)
	})

	t.Run("13_fail_pending_respects_next_run_at", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-0000000000a0"), "default", "t", now)
		job.MaxAttempts = 5
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "default", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)

		next := now.Add(2 * time.Minute)
		failed, err := repo.Fail(ctx, claimed[0].ID, "w1", "retry me", domain.FailureDecision{
			NextStatus: domain.StatusPending,
			NextRunAt:  &next,
		}, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusPending, failed.Status)
		assertEqualTime(t, next, failed.RunAt)
		assert.Equal(t, "retry me", failed.LastError)
		assert.Nil(t, failed.LeaseExpiresAt)

		tooSoon, err := repo.Claim(ctx, "default", "w2", 1, 30*time.Second, now.Add(time.Minute))
		require.NoError(t, err)
		assert.Empty(t, tooSoon)

		ready, err := repo.Claim(ctx, "default", "w2", 1, 30*time.Second, next)
		require.NoError(t, err)
		require.Len(t, ready, 1)
		assert.Equal(t, claimed[0].ID, ready[0].ID)
	})

	t.Run("14_fail_dead_never_claimable", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		job := sampleJob(mustParse("018f1111-0000-7000-8000-0000000000b0"), "default", "t", now)
		_, _, err := repo.Create(ctx, job)
		require.NoError(t, err)
		claimed, err := repo.Claim(ctx, "default", "w1", 1, 30*time.Second, now)
		require.NoError(t, err)

		dead, err := repo.Fail(ctx, claimed[0].ID, "w1", "gone", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusDead, dead.Status)
		require.NotNil(t, dead.FinishedAt)
		assertEqualTime(t, now, *dead.FinishedAt)

		again, err := repo.Claim(ctx, "default", "w2", 1, 30*time.Second, now.Add(time.Hour))
		require.NoError(t, err)
		assert.Empty(t, again)
	})

	t.Run("15_concurrent_claim_exactly_once", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		const nJobs = 500
		const nWorkers = 20
		const batch = 5

		for i := 0; i < nJobs; i++ {
			j := sampleJob(uuid.Must(uuid.NewV7()), "conc", "t", now)
			_, _, err := repo.Create(ctx, j)
			require.NoError(t, err)
		}

		var (
			mu       sync.Mutex
			claimed  = make(map[domain.JobID]string)
			wg       sync.WaitGroup
			claimErr error
		)
		wg.Add(nWorkers)
		for w := 0; w < nWorkers; w++ {
			owner := fmt.Sprintf("worker-%02d", w)
			go func(owner string) {
				defer wg.Done()
				for {
					jobs, err := repo.Claim(ctx, "conc", owner, batch, 30*time.Second, now)
					if err != nil {
						mu.Lock()
						claimErr = err
						mu.Unlock()
						return
					}
					if len(jobs) == 0 {
						return
					}
					mu.Lock()
					for _, j := range jobs {
						if prev, ok := claimed[j.ID]; ok {
							claimErr = fmt.Errorf("job %s claimed by %s and %s", j.ID, prev, owner)
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
		require.NoError(t, claimErr)
		assert.Len(t, claimed, nJobs)
	})

	t.Run("extend_complete_fail_not_found", func(t *testing.T) {
		// Distinguishes ErrNotFound from ErrLeaseLost for unknown IDs.
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Now().UTC()
		missing := mustParse("018f1111-0000-7000-8000-00000000ffff")
		_, err := repo.ExtendLease(ctx, missing, "w", 30*time.Second, now)
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, err = repo.Complete(ctx, missing, "w", now)
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, err = repo.Fail(ctx, missing, "w", "x", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("ping", func(t *testing.T) {
		repo := newRepo(t)
		require.NoError(t, repo.Ping(context.Background()))
	})
}

func sampleJob(id domain.JobID, queue, typ string, now time.Time) domain.Job {
	return domain.Job{
		ID:          id,
		Queue:       queue,
		Type:        typ,
		Payload:     []byte("{}"),
		Status:      domain.StatusPending,
		Priority:    0,
		RunAt:       now,
		Attempts:    0,
		MaxAttempts: 10,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func withPriRun(j domain.Job, pri int, runAt time.Time) domain.Job {
	j.Priority = pri
	j.RunAt = runAt
	return j
}

func mustParse(s string) domain.JobID {
	id, err := uuid.Parse(s)
	if err != nil {
		panic(err)
	}
	return id
}

func assertEqualTime(t *testing.T, want, got time.Time) {
	t.Helper()
	assert.True(t, want.UTC().Truncate(time.Millisecond).Equal(got.UTC().Truncate(time.Millisecond)),
		"times: want %v got %v", want, got)
}

func assertJobsEqual(t *testing.T, want, got domain.Job) {
	t.Helper()
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.Queue, got.Queue)
	assert.Equal(t, want.Type, got.Type)
	assert.Equal(t, want.Payload, got.Payload)
	assert.Equal(t, want.Status, got.Status)
	assert.Equal(t, want.Priority, got.Priority)
	assertEqualTime(t, want.RunAt, got.RunAt)
	assert.Equal(t, want.Attempts, got.Attempts)
	assert.Equal(t, want.MaxAttempts, got.MaxAttempts)
	assert.Equal(t, want.LeaseOwner, got.LeaseOwner)
	assert.Equal(t, want.IdempotencyKey, got.IdempotencyKey)
	assert.Equal(t, want.LastError, got.LastError)
	assertEqualTime(t, want.CreatedAt, got.CreatedAt)
	assertEqualTime(t, want.UpdatedAt, got.UpdatedAt)
	if want.LeaseExpiresAt == nil {
		assert.Nil(t, got.LeaseExpiresAt)
	} else {
		require.NotNil(t, got.LeaseExpiresAt)
		assertEqualTime(t, *want.LeaseExpiresAt, *got.LeaseExpiresAt)
	}
	if want.FinishedAt == nil {
		assert.Nil(t, got.FinishedAt)
	} else {
		require.NotNil(t, got.FinishedAt)
		assertEqualTime(t, *want.FinishedAt, *got.FinishedAt)
	}
}
