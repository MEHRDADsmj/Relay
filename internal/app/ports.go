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

// ReapResult counts jobs handled by MaintenanceRepository.ReapExpired.
type ReapResult struct {
	Requeued int
	Dead     int
}

// JobRepository is the persistence port for jobs.
type JobRepository interface {
	// Create inserts the job. If IdempotencyKey is non-empty and a job with the same
	// (queue, key) exists, it returns the EXISTING job and created=false.
	// Also upserts the queue registry entry.
	Create(ctx context.Context, job domain.Job) (stored domain.Job, created bool, err error)

	// Get returns a job by ID or domain.ErrNotFound.
	Get(ctx context.Context, id domain.JobID) (domain.Job, error)

	// Claim atomically, for one queue:
	//  1. marks dead: running jobs with lease_expires_at <= now AND attempts >= max_attempts
	//     (last_error = "lease expired", finished_at = now); closes running Attempt as expired
	//  2. returns empty without error if the queue is paused
	//  3. selects up to limit jobs that are either
	//       (status=pending AND run_at <= now) OR
	//       (status=running AND lease_expires_at <= now AND attempts < max_attempts)
	//     ordered by priority DESC, run_at ASC, id ASC
	//  4. for reclaimed expired leases, closes the previous Attempt as expired
	//  5. sets status=running, lease_owner=owner, lease_expires_at=now+lease,
	//     attempts=attempts+1, updated_at=now; inserts Attempt{Outcome: running}
	// Concurrent callers must never receive the same job.
	Claim(ctx context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error)

	// ExtendLease sets lease_expires_at=now+lease iff status=running AND lease_owner=owner
	// AND lease_expires_at > now. Otherwise ErrLeaseLost (or ErrNotFound).
	ExtendLease(ctx context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error)

	// Complete sets status=succeeded (or cancelled if CancelRequested), finished_at=now,
	// clears lease, under the same ownership condition as ExtendLease.
	// Closes the Attempt as succeeded or cancelled.
	Complete(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error)

	// Fail applies a FailureDecision under the same ownership condition, or cancelled
	// if CancelRequested. Closes the Attempt as failed or cancelled.
	//   pending -> run_at = decision.NextRunAt, lease cleared
	//   dead    -> finished_at = now, lease cleared
	//   cancelled -> finished_at/cancelled_at = now, lease cleared
	// last_error = errMsg in all cases.
	Fail(ctx context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error)

	// Release returns a running job to pending: ownership-checked; attempts--; run_at=now;
	// Attempt closed as released.
	Release(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error)

	// Cancel: pending → cancelled; running → CancelRequested=true; else ErrInvalidTransition.
	Cancel(ctx context.Context, id domain.JobID, now time.Time) (domain.Job, error)

	// Retry: dead|cancelled → pending, attempts=0, run_at=now; else ErrInvalidTransition.
	Retry(ctx context.Context, id domain.JobID, now time.Time) (domain.Job, error)

	// ListAttempts returns attempt history for a job (ErrNotFound if job missing).
	ListAttempts(ctx context.Context, id domain.JobID) ([]domain.Attempt, error)

	// ListByStatus returns jobs in queue with status, ordered by updated_at DESC, id DESC (keyset).
	ListByStatus(ctx context.Context, queue string, status domain.Status, page domain.PageRequest) (domain.Page[domain.Job], error)

	// Ping checks connectivity (used by /readyz).
	Ping(ctx context.Context) error
}

// QueueRepository is the persistence port for queue registry and stats.
type QueueRepository interface {
	ListQueues(ctx context.Context) ([]domain.Queue, error)
	Stats(ctx context.Context, queue string, now time.Time) (domain.QueueStats, error) // ErrNotFound for unknown
	SetPaused(ctx context.Context, queue string, paused bool, now time.Time) (domain.Queue, error)
}

// MaintenanceRepository supports background reaping and retention purge.
type MaintenanceRepository interface {
	// ReapExpired handles expired leases across ALL queues, up to limit jobs per call.
	ReapExpired(ctx context.Context, now time.Time, limit int) (ReapResult, error)
	// PurgeFinished deletes succeeded|dead|cancelled jobs (and attempts + idempotency keys)
	// with finished_at < before, up to limit per call.
	PurgeFinished(ctx context.Context, before time.Time, limit int) (int, error)
}

// JobNotifier lets the app wait for "a job may be ready on queue X".
// Notifications are hints: receivers must always re-Claim; spurious wakeups are fine.
type JobNotifier interface {
	Subscribe(ctx context.Context, queue string) (ch <-chan struct{}, unsubscribe func())
}
