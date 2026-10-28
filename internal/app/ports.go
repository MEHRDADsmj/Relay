package app

import (
	"context"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Clock provides the current time for use cases (injected for tests).
type Clock interface {
	Now() time.Time
}

// IDGenerator creates new job identifiers.
type IDGenerator interface {
	NewID() domain.JobID
}

// RetryPolicy computes the delay before the next retry attempt.
type RetryPolicy interface {
	NextDelay(attempt int) time.Duration
}

// JobRepository is the persistence port for jobs.
type JobRepository interface {
	// Create inserts the job. If IdempotencyKey is non-empty and a job with the same
	// (queue, key) exists, it returns the EXISTING job and created=false.
	Create(ctx context.Context, job domain.Job) (stored domain.Job, created bool, err error)

	// Get returns a job by ID or domain.ErrNotFound.
	Get(ctx context.Context, id domain.JobID) (domain.Job, error)

	// Claim atomically, for one queue:
	//  1. marks dead: running jobs with lease_expires_at <= now AND attempts >= max_attempts
	//     (last_error = "lease expired", finished_at = now)
	//  2. selects up to limit jobs that are either
	//       (status=pending AND run_at <= now) OR
	//       (status=running AND lease_expires_at <= now AND attempts < max_attempts)
	//     ordered by priority DESC, run_at ASC, id ASC
	//  3. sets status=running, lease_owner=owner, lease_expires_at=now+lease,
	//     attempts=attempts+1, updated_at=now
	// Concurrent callers must never receive the same job.
	Claim(ctx context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error)

	// ExtendLease sets lease_expires_at=now+lease iff status=running AND lease_owner=owner
	// AND lease_expires_at > now. Otherwise ErrLeaseLost (or ErrNotFound).
	ExtendLease(ctx context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error)

	// Complete sets status=succeeded, finished_at=now, clears lease, under the same
	// ownership condition as ExtendLease.
	Complete(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error)

	// Fail applies a FailureDecision under the same ownership condition:
	//   pending -> run_at = decision.NextRunAt, lease cleared
	//   dead    -> finished_at = now, lease cleared
	// last_error = errMsg in both cases.
	Fail(ctx context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error)

	// Ping checks connectivity (used by /readyz).
	Ping(ctx context.Context) error
}
