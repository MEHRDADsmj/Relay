package domain

import (
	"time"

	"github.com/google/uuid"
)

// JobID uniquely identifies a job.
type JobID = uuid.UUID

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead"
)

// Job is the core Relay work unit.
type Job struct {
	ID             JobID
	Queue          string
	Type           string
	Payload        []byte // raw JSON, opaque to Relay
	Status         Status
	Priority       int // -100..100, higher runs first
	RunAt          time.Time
	Attempts       int
	MaxAttempts    int
	LeaseOwner     string
	LeaseExpiresAt *time.Time
	IdempotencyKey string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

// NewJobParams holds caller-supplied fields for NewJob.
// Nil optional pointers select domain defaults.
type NewJobParams struct {
	Queue          string
	Type           string
	Payload        []byte
	Priority       *int
	RunAt          *time.Time
	MaxAttempts    *int
	IdempotencyKey string
}

// FailureDecision is the pure outcome of DecideFailure for the app/repo to apply.
type FailureDecision struct {
	NextStatus Status
	NextRunAt  *time.Time
}

// NewJob validates params, applies defaults, and returns a pending Job.
func NewJob(params NewJobParams, id JobID, now time.Time) (Job, error) {
	if err := ValidateQueue(params.Queue); err != nil {
		return Job{}, err
	}
	if err := ValidateType(params.Type); err != nil {
		return Job{}, err
	}
	payload, err := NormalizePayload(params.Payload)
	if err != nil {
		return Job{}, err
	}
	priority, err := NormalizePriority(params.Priority)
	if err != nil {
		return Job{}, err
	}
	maxAttempts, err := NormalizeMaxAttempts(params.MaxAttempts)
	if err != nil {
		return Job{}, err
	}
	if err := ValidateIdempotencyKey(params.IdempotencyKey); err != nil {
		return Job{}, err
	}

	runAt := NormalizeRunAt(params.RunAt, now)

	return Job{
		ID:             id,
		Queue:          params.Queue,
		Type:           params.Type,
		Payload:        payload,
		Status:         StatusPending,
		Priority:       priority,
		RunAt:          runAt,
		Attempts:       0,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: params.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// DecideFailure chooses pending (retry) or dead based on Attempts vs MaxAttempts.
// Attempts is assumed already incremented by the lease that is being failed.
func (j Job) DecideFailure(now time.Time, retryDelay time.Duration) FailureDecision {
	if j.Attempts < j.MaxAttempts {
		runAt := now.Add(retryDelay)
		return FailureDecision{
			NextStatus: StatusPending,
			NextRunAt:  &runAt,
		}
	}
	return FailureDecision{NextStatus: StatusDead}
}

// EnsureLeasedBy returns ErrLeaseLost unless the job is running under owner with an unexpired lease.
func (j Job) EnsureLeasedBy(owner string, now time.Time) error {
	if j.Status != StatusRunning {
		return ErrLeaseLost
	}
	if j.LeaseOwner != owner {
		return ErrLeaseLost
	}
	if j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.After(now) {
		return ErrLeaseLost
	}
	return nil
}
