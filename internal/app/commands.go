package app

import (
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// EnqueueCommand is the input for JobService.Enqueue.
type EnqueueCommand struct {
	Queue          string
	Type           string
	Payload        []byte
	Priority       *int
	RunAt          *time.Time
	MaxAttempts    *int
	IdempotencyKey string
}

// LeaseCommand is the input for JobService.Lease and LeaseWait.
type LeaseCommand struct {
	Queue         string
	WorkerID      string
	MaxJobs       int
	LeaseDuration time.Duration
	Wait          time.Duration // 0..30s; used by LeaseWait only
}

// HeartbeatCommand is the input for JobService.Heartbeat.
type HeartbeatCommand struct {
	JobID         domain.JobID
	WorkerID      string
	LeaseDuration time.Duration
}

// CompleteCommand is the input for JobService.Complete.
type CompleteCommand struct {
	JobID    domain.JobID
	WorkerID string
}

// FailCommand is the input for JobService.Fail.
type FailCommand struct {
	JobID     domain.JobID
	WorkerID  string
	Error     string
	Permanent bool // force dead without retry
}

// ReleaseCommand is the input for JobService.Release.
type ReleaseCommand struct {
	JobID    domain.JobID
	WorkerID string
}

// ListByStatusCommand is the input for JobService.ListByStatus.
type ListByStatusCommand struct {
	Queue  string
	Status domain.Status
	Page   domain.PageRequest
}

// RetryAllDeadCommand is the input for QueueService.RetryAllDead.
type RetryAllDeadCommand struct {
	Queue string
	Limit int // 1..10000, default 1000
}
