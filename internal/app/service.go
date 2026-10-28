package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// JobService orchestrates job use cases against injected ports.
type JobService struct {
	repo     JobRepository
	notifier JobNotifier
	clock    Clock
	ids      IDGenerator
	retry    RetryPolicy
	log      *slog.Logger
}

// NewJobService constructs a JobService. log may be nil (defaults to slog.Default).
// notifier may be nil (defaults to NopNotifier).
func NewJobService(repo JobRepository, clock Clock, ids IDGenerator, retry RetryPolicy, log *slog.Logger, notifier JobNotifier) *JobService {
	if log == nil {
		log = slog.Default()
	}
	if notifier == nil {
		notifier = NopNotifier{}
	}
	return &JobService{
		repo:     repo,
		notifier: notifier,
		clock:    clock,
		ids:      ids,
		retry:    retry,
		log:      log,
	}
}

// Enqueue validates and creates a pending job (idempotent on queue+key).
func (s *JobService) Enqueue(ctx context.Context, cmd EnqueueCommand) (domain.Job, bool, error) {
	now := s.clock.Now()
	job, err := domain.NewJob(domain.NewJobParams{
		Queue:          cmd.Queue,
		Type:           cmd.Type,
		Payload:        cmd.Payload,
		Priority:       cmd.Priority,
		RunAt:          cmd.RunAt,
		MaxAttempts:    cmd.MaxAttempts,
		IdempotencyKey: cmd.IdempotencyKey,
	}, s.ids.NewID(), now)
	if err != nil {
		return domain.Job{}, false, err
	}
	stored, created, err := s.repo.Create(ctx, job)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("create job: %w", err)
	}
	return stored, created, nil
}

// Get returns a job by ID.
func (s *JobService) Get(ctx context.Context, id domain.JobID) (domain.Job, error) {
	job, err := s.repo.Get(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Lease claims up to MaxJobs from a queue for a worker (no long-poll).
func (s *JobService) Lease(ctx context.Context, cmd LeaseCommand) ([]domain.Job, error) {
	cmd.Wait = 0
	return s.LeaseWait(ctx, cmd)
}

// LeaseWait claims jobs, optionally waiting up to Wait for a push wakeup or ticker.
func (s *JobService) LeaseWait(ctx context.Context, cmd LeaseCommand) ([]domain.Job, error) {
	if err := domain.ValidateQueue(cmd.Queue); err != nil {
		return nil, err
	}
	if err := domain.ValidateWorkerID(cmd.WorkerID); err != nil {
		return nil, err
	}
	maxJobs, err := domain.NormalizeMaxJobs(cmd.MaxJobs)
	if err != nil {
		return nil, err
	}
	lease, err := domain.NormalizeLeaseDuration(cmd.LeaseDuration)
	if err != nil {
		return nil, err
	}
	wait, err := domain.NormalizeLeaseWait(cmd.Wait)
	if err != nil {
		return nil, err
	}

	now := s.clock.Now()
	jobs, err := s.repo.Claim(ctx, cmd.Queue, cmd.WorkerID, maxJobs, lease, now)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	if len(jobs) > 0 || wait == 0 {
		return jobs, nil
	}

	notifyCh, unsubscribe := s.notifier.Subscribe(ctx, cmd.Queue)
	defer unsubscribe()

	now = s.clock.Now()
	jobs, err = s.repo.Claim(ctx, cmd.Queue, cmd.WorkerID, maxJobs, lease, now)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	if len(jobs) > 0 {
		return jobs, nil
	}

	// Wait timing uses wall clock; Claim timestamps use the injected clock.
	deadline := time.Now().Add(wait)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(remain)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-notifyCh:
			timer.Stop()
		case <-ticker.C:
			timer.Stop()
		case <-timer.C:
			return nil, nil
		}
		now = s.clock.Now()
		jobs, err = s.repo.Claim(ctx, cmd.Queue, cmd.WorkerID, maxJobs, lease, now)
		if err != nil {
			return nil, fmt.Errorf("claim jobs: %w", err)
		}
		if len(jobs) > 0 {
			return jobs, nil
		}
		if !time.Now().Before(deadline) {
			return nil, nil
		}
	}
}

// Heartbeat extends a job lease owned by the worker.
func (s *JobService) Heartbeat(ctx context.Context, cmd HeartbeatCommand) (domain.Job, error) {
	if err := domain.ValidateWorkerID(cmd.WorkerID); err != nil {
		return domain.Job{}, err
	}
	lease, err := domain.NormalizeLeaseDuration(cmd.LeaseDuration)
	if err != nil {
		return domain.Job{}, err
	}
	job, err := s.repo.ExtendLease(ctx, cmd.JobID, cmd.WorkerID, lease, s.clock.Now())
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Complete marks a leased job as succeeded (or cancelled if requested).
func (s *JobService) Complete(ctx context.Context, cmd CompleteCommand) (domain.Job, error) {
	if err := domain.ValidateWorkerID(cmd.WorkerID); err != nil {
		return domain.Job{}, err
	}
	job, err := s.repo.Complete(ctx, cmd.JobID, cmd.WorkerID, s.clock.Now())
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Fail records a failure and either schedules a retry or marks the job dead.
// Permanent forces a dead decision. CancelRequested jobs become cancelled in the repo.
func (s *JobService) Fail(ctx context.Context, cmd FailCommand) (domain.Job, error) {
	if err := domain.ValidateWorkerID(cmd.WorkerID); err != nil {
		return domain.Job{}, err
	}
	now := s.clock.Now()
	job, err := s.repo.Get(ctx, cmd.JobID)
	if err != nil {
		return domain.Job{}, err
	}
	if err := job.EnsureLeasedBy(cmd.WorkerID, now); err != nil {
		return domain.Job{}, err
	}
	decision := job.DecideFailure(now, s.retry.NextDelay(job.Attempts), cmd.Permanent)
	errMsg := domain.TruncateErrorMessage(cmd.Error)
	updated, err := s.repo.Fail(ctx, cmd.JobID, cmd.WorkerID, errMsg, decision, now)
	if err != nil {
		return domain.Job{}, err
	}
	return updated, nil
}

// Release voluntarily returns a leased job to pending without consuming the attempt.
func (s *JobService) Release(ctx context.Context, cmd ReleaseCommand) (domain.Job, error) {
	if err := domain.ValidateWorkerID(cmd.WorkerID); err != nil {
		return domain.Job{}, err
	}
	job, err := s.repo.Release(ctx, cmd.JobID, cmd.WorkerID, s.clock.Now())
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Cancel cancels a pending job or requests cancel on a running job.
func (s *JobService) Cancel(ctx context.Context, id domain.JobID) (domain.Job, error) {
	job, err := s.repo.Cancel(ctx, id, s.clock.Now())
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Retry requeues a dead or cancelled job.
func (s *JobService) Retry(ctx context.Context, id domain.JobID) (domain.Job, error) {
	job, err := s.repo.Retry(ctx, id, s.clock.Now())
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// ListAttempts returns attempt history for a job.
func (s *JobService) ListAttempts(ctx context.Context, id domain.JobID) ([]domain.Attempt, error) {
	return s.repo.ListAttempts(ctx, id)
}

// ListByStatus returns a page of jobs filtered by queue and status.
func (s *JobService) ListByStatus(ctx context.Context, cmd ListByStatusCommand) (domain.Page[domain.Job], error) {
	if err := domain.ValidateQueue(cmd.Queue); err != nil {
		return domain.Page[domain.Job]{}, err
	}
	limit, err := domain.NormalizePageLimit(cmd.Page.Limit)
	if err != nil {
		return domain.Page[domain.Job]{}, err
	}
	cmd.Page.Limit = limit
	if cmd.Page.Cursor != "" {
		if _, _, err := DecodeCursor(cmd.Page.Cursor); err != nil {
			return domain.Page[domain.Job]{}, err
		}
	}
	return s.repo.ListByStatus(ctx, cmd.Queue, cmd.Status, cmd.Page)
}

// Ready checks repository connectivity.
func (s *JobService) Ready(ctx context.Context) error {
	return s.repo.Ping(ctx)
}
