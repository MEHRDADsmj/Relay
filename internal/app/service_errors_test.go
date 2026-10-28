package app_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

var errRepoBoom = errors.New("repo boom")

// errRepo fails every mutating call; Get returns a leased job for Fail tests.
type errRepo struct {
	job domain.Job
}

func (r *errRepo) Create(context.Context, domain.Job) (domain.Job, bool, error) {
	return domain.Job{}, false, errRepoBoom
}

func (r *errRepo) Get(context.Context, domain.JobID) (domain.Job, error) {
	if r.job.ID == (domain.JobID{}) {
		return domain.Job{}, domain.ErrNotFound
	}
	return r.job, nil
}

func (r *errRepo) Claim(context.Context, string, string, int, time.Duration, time.Time) ([]domain.Job, error) {
	return nil, errRepoBoom
}

func (r *errRepo) ExtendLease(context.Context, domain.JobID, string, time.Duration, time.Time) (domain.Job, error) {
	return domain.Job{}, errRepoBoom
}

func (r *errRepo) Complete(context.Context, domain.JobID, string, time.Time) (domain.Job, error) {
	return domain.Job{}, errRepoBoom
}

func (r *errRepo) Fail(context.Context, domain.JobID, string, string, domain.FailureDecision, time.Time) (domain.Job, error) {
	return domain.Job{}, errRepoBoom
}

func (r *errRepo) Ping(context.Context) error { return errRepoBoom }

func TestService_RepoErrorsWrapped(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	exp := now.Add(30 * time.Second)
	id := mustID("018f0000-0000-7000-8000-000000000070")
	repo := &errRepo{job: domain.Job{
		ID: id, Status: domain.StatusRunning, LeaseOwner: "w1",
		LeaseExpiresAt: &exp, Attempts: 1, MaxAttempts: 10,
	}}
	clock := &fixedClock{now: now}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := app.NewJobService(repo, clock, &seqIDs{ids: []domain.JobID{id}}, app.FixedDelayRetryPolicy{Delay: time.Minute}, log)

	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errRepoBoom)

	_, err = svc.Lease(context.Background(), app.LeaseCommand{Queue: "default", WorkerID: "w1"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errRepoBoom)

	_, err = svc.Heartbeat(context.Background(), app.HeartbeatCommand{JobID: id, WorkerID: "w1"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errRepoBoom)

	_, err = svc.Complete(context.Background(), app.CompleteCommand{JobID: id, WorkerID: "w1"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errRepoBoom)

	_, err = svc.Fail(context.Background(), app.FailCommand{JobID: id, WorkerID: "w1", Error: "x"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errRepoBoom)

	require.ErrorIs(t, svc.Ready(context.Background()), errRepoBoom)
}

func TestFail_InvalidWorkerID(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Fail(context.Background(), app.FailCommand{WorkerID: "", Error: "x"})
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestHeartbeat_InvalidWorkerID(t *testing.T) {
	t.Parallel()
	svc, _, _ := newService(t, time.Now().UTC())
	_, err := svc.Heartbeat(context.Background(), app.HeartbeatCommand{WorkerID: ""})
	require.ErrorIs(t, err, domain.ErrValidation)
}
