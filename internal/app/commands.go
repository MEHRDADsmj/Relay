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

// LeaseCommand is the input for JobService.Lease.
type LeaseCommand struct {
	Queue         string
	WorkerID      string
	MaxJobs       int
	LeaseDuration time.Duration
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
	JobID    domain.JobID
	WorkerID string
	Error    string
}
