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

func TestFail_PermanentForcesDead(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000110")
	max := 10
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{
		Queue: "default", Type: "t", MaxAttempts: &max,
	})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	job, err := svc.Fail(context.Background(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: "nope", Permanent: true,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusDead, job.Status)
}

func TestRelease_ReturnsToPending(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000111")
	svc, _, _ := newService(t, now, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	leased, err := svc.Lease(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second,
	})
	require.NoError(t, err)

	job, err := svc.Release(context.Background(), app.ReleaseCommand{JobID: leased[0].ID, WorkerID: "w1"})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, job.Status)
	assert.Equal(t, 0, job.Attempts)
}

func TestCancel_AndRetry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000112")
	svc, _, _ := newService(t, now, id)
	created, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)

	cancelled, err := svc.Cancel(context.Background(), created.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, cancelled.Status)

	retried, err := svc.Retry(context.Background(), created.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, retried.Status)
	assert.Equal(t, 0, retried.Attempts)
}

func TestListByStatus_Validates(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.ListByStatus(context.Background(), app.ListByStatusCommand{
		Queue: "BAD", Status: domain.StatusDead,
	})
	require.ErrorIs(t, err, domain.ErrValidation)
}
