package app_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func newService(t *testing.T, now time.Time, ids ...domain.JobID) (*app.JobService, *memRepo, *fixedClock) {
	t.Helper()
	repo := newMemRepo()
	clock := &fixedClock{now: now}
	var gen app.IDGenerator
	if len(ids) > 0 {
		gen = &seqIDs{ids: ids}
	} else {
		gen = app.UUIDv7Generator{}
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := app.NewJobService(repo, clock, gen, app.FixedDelayRetryPolicy{Delay: time.Minute}, log)
	return svc, repo, clock
}

func TestEnqueue_CreatesPendingJob(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000001")
	svc, _, _ := newService(t, now, id)

	job, created, err := svc.Enqueue(context.Background(), app.EnqueueCommand{
		Queue:   "default",
		Type:    "email.send",
		Payload: []byte(`{"to":"a@b.c"}`),
	})
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, id, job.ID)
	assert.Equal(t, domain.StatusPending, job.Status)
	assert.Equal(t, "default", job.Queue)
	assert.Equal(t, "email.send", job.Type)
	assert.Equal(t, now, job.CreatedAt)
	assert.Equal(t, now, job.RunAt)
	assert.Equal(t, 10, job.MaxAttempts)
}

func TestEnqueue_ValidationError(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC(), mustID("018f0000-0000-7000-8000-000000000002"))
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "BAD", Type: "x"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrValidation))
}

func TestEnqueue_IdempotentReplay(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id1 := mustID("018f0000-0000-7000-8000-000000000010")
	id2 := mustID("018f0000-0000-7000-8000-000000000011")
	svc, _, _ := newService(t, now, id1, id2)

	first, created, err := svc.Enqueue(context.Background(), app.EnqueueCommand{
		Queue: "default", Type: "t", IdempotencyKey: "k1",
	})
	require.NoError(t, err)
	require.True(t, created)

	second, created, err := svc.Enqueue(context.Background(), app.EnqueueCommand{
		Queue: "default", Type: "t", IdempotencyKey: "k1",
	})
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, id1, second.ID)
}

func TestGet_AndNotFound(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000020")
	svc, _, _ := newService(t, now, id)

	_, err := svc.Get(context.Background(), id)
	require.ErrorIs(t, err, domain.ErrNotFound)

	created, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "q", Type: "t"})
	require.NoError(t, err)
	got, err := svc.Get(context.Background(), created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
}

func TestLease_ClaimsAndIncrementsAttempts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000030")
	svc, _, _ := newService(t, now, id)

	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)

	jobs, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, domain.StatusRunning, jobs[0].Status)
	assert.Equal(t, "w1", jobs[0].LeaseOwner)
	assert.Equal(t, 1, jobs[0].Attempts)
	require.NotNil(t, jobs[0].LeaseExpiresAt)
	assert.Equal(t, now.Add(30*time.Second), *jobs[0].LeaseExpiresAt)
}

func TestLease_Validation(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Lease(context.Background(), app.LeaseCommand{Queue: "default", WorkerID: ""})
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestLease_Defaults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000031")
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)

	jobs, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", // MaxJobs 0, LeaseDuration 0 → defaults
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, now.Add(domain.DefaultLeaseDuration), *jobs[0].LeaseExpiresAt)
}

func TestHeartbeat_ExtendsLease(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000040")
	svc, _, clock := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	later := now.Add(10 * time.Second)
	clock.Set(later)
	job, err := svc.Heartbeat(context.Background(), app.HeartbeatCommand{
		JobID: leased[0].ID, WorkerID: "w1", LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	require.NotNil(t, job.LeaseExpiresAt)
	assert.Equal(t, later.Add(30*time.Second), *job.LeaseExpiresAt)
}

func TestHeartbeat_WrongOwner(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000041")
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	_, err = svc.Heartbeat(context.Background(), app.HeartbeatCommand{
		JobID: leased[0].ID, WorkerID: "other", LeaseDuration: 30 * time.Second,
	})
	require.ErrorIs(t, err, domain.ErrLeaseLost)
}

func TestComplete_Succeeds(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000050")
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	job, err := svc.Complete(context.Background(), app.CompleteCommand{JobID: leased[0].ID, WorkerID: "w1"})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusSucceeded, job.Status)
	require.NotNil(t, job.FinishedAt)
	assert.Equal(t, now, *job.FinishedAt)
	assert.Empty(t, job.LeaseOwner)
	assert.Nil(t, job.LeaseExpiresAt)
}

func TestFail_RetriesWhenAttemptsRemain(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000060")
	max := 3
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{
		Queue: "default", Type: "t", MaxAttempts: &max,
	})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 1, leased[0].Attempts)

	job, err := svc.Fail(context.Background(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: "boom",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, job.Status)
	assert.Equal(t, "boom", job.LastError)
	assert.Equal(t, now.Add(time.Minute), job.RunAt)
	assert.Nil(t, job.LeaseExpiresAt)
}

func TestFail_MarksDeadWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000061")
	max := 1
	svc, _, clock := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{
		Queue: "default", Type: "t", MaxAttempts: &max,
	})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 1, leased[0].Attempts)

	job, err := svc.Fail(context.Background(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: "fatal",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusDead, job.Status)
	require.NotNil(t, job.FinishedAt)
	assert.Equal(t, clock.Now(), *job.FinishedAt)
}

func TestFail_LeaseLostBeforeWrite(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000062")
	svc, _, clock := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	clock.Set(now.Add(31 * time.Second))
	_, err = svc.Fail(context.Background(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: "late",
	})
	require.ErrorIs(t, err, domain.ErrLeaseLost)
}

func TestFail_TruncatesErrorMessage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000063")
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	long := strings.Repeat("x", domain.MaxErrorMessageBytes+50)
	job, err := svc.Fail(context.Background(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: long,
	})
	require.NoError(t, err)
	assert.Len(t, job.LastError, domain.MaxErrorMessageBytes)
}

func TestReady(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	require.NoError(t, svc.Ready(context.Background()))
}

func TestFixedDelayRetryPolicy(t *testing.T) {
	t.Parallel()
	p := app.FixedDelayRetryPolicy{Delay: 5 * time.Second}
	assert.Equal(t, 5*time.Second, p.NextDelay(1))
	assert.Equal(t, 5*time.Second, p.NextDelay(99))
}

func TestSystemClockAndUUIDv7(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC().Add(-time.Second)
	now := app.SystemClock{}.Now()
	after := time.Now().UTC().Add(time.Second)
	assert.True(t, !now.Before(before) && !now.After(after))
	assert.Equal(t, time.UTC, now.Location())

	id := app.UUIDv7Generator{}.NewID()
	assert.Equal(t, uuidVersion(id), byte(7))
}

func uuidVersion(id domain.JobID) byte {
	return id[6] >> 4
}

func TestNewJobService_NilLogger(t *testing.T) {
	t.Parallel()
	svc := app.NewJobService(newMemRepo(), &fixedClock{now: time.Now().UTC()}, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, nil)
	require.NotNil(t, svc)
	require.NoError(t, svc.Ready(context.Background()))
}

func TestComplete_Validation(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Complete(context.Background(), app.CompleteCommand{WorkerID: ""})
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestHeartbeat_Validation(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Heartbeat(context.Background(), app.HeartbeatCommand{WorkerID: "w", LeaseDuration: time.Second})
	require.ErrorIs(t, err, domain.ErrValidation) // lease too short
}

func TestFail_NotFound(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Fail(context.Background(), app.FailCommand{
		JobID: mustID("018f0000-0000-7000-8000-000000000099"), WorkerID: "w1", Error: "x",
	})
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestLease_InvalidMaxJobs(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 101,
	})
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestLease_InvalidQueue(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "BAD", WorkerID: "w1",
	})
	require.ErrorIs(t, err, domain.ErrValidation)
}
