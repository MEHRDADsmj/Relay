package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// JobService orchestrates job use cases against injected ports.
type JobService struct {
	repo  JobRepository
	clock Clock
	ids   IDGenerator
	retry RetryPolicy
	log   *slog.Logger
}

// NewJobService constructs a JobService. log may be nil (defaults to slog.Default).
func NewJobService(repo JobRepository, clock Clock, ids IDGenerator, retry RetryPolicy, log *slog.Logger) *JobService {
	if log == nil {
		log = slog.Default()
	}
	return &JobService{
		repo:  repo,
		clock: clock,
		ids:   ids,
		retry: retry,
		log:   log,
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

// Lease claims up to MaxJobs from a queue for a worker.
func (s *JobService) Lease(ctx context.Context, cmd LeaseCommand) ([]domain.Job, error) {
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
	jobs, err := s.repo.Claim(ctx, cmd.Queue, cmd.WorkerID, maxJobs, lease, s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	return jobs, nil
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

// Complete marks a leased job as succeeded.
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
	decision := job.DecideFailure(now, s.retry.NextDelay(job.Attempts), false)
	errMsg := domain.TruncateErrorMessage(cmd.Error)
	updated, err := s.repo.Fail(ctx, cmd.JobID, cmd.WorkerID, errMsg, decision, now)
	if err != nil {
		return domain.Job{}, err
	}
	return updated, nil
}

// Ready checks repository connectivity.
func (s *JobService) Ready(ctx context.Context) error {
	return s.repo.Ping(ctx)
}
