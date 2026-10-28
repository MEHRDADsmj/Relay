package sdk

import (
	"log/slog"
	"time"
)

// JobInfo is a job as returned by the Relay HTTP API.
type JobInfo struct {
	ID              string
	Queue           string
	Type            string
	Payload         []byte
	Status          string
	Priority        int
	RunAt           time.Time
	Attempts        int
	MaxAttempts     int
	LeaseOwner      string
	LeaseExpiresAt  *time.Time
	IdempotencyKey  string
	LastError       string
	CancelRequested bool
	CancelledAt     *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	FinishedAt      *time.Time
}

// EnqueueRequest is the producer-side enqueue payload.
type EnqueueRequest struct {
	Queue, Type    string
	Payload        any // marshaled to JSON
	Priority       int
	RunAt          time.Time // zero = now
	MaxAttempts    int       // zero = server default
	IdempotencyKey string
}

// Job is a leased job delivered to a typed handler.
type Job[T any] struct {
	ID      string
	Queue   string
	Type    string
	Attempt int
	Args    T
}

// AttemptInfo is one attempt history row from the API.
type AttemptInfo struct {
	JobID      string
	Number     int
	WorkerID   string
	StartedAt  time.Time
	FinishedAt *time.Time
	Outcome    string
	Error      string
}

// QueueInfo is a registered queue.
type QueueInfo struct {
	Name      string
	Paused    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// QueueStatsInfo is queue depth / outcome counters.
type QueueStatsInfo struct {
	Queue            string
	Paused           bool
	Pending          int64
	Scheduled        int64
	Running          int64
	Succeeded        int64
	Dead             int64
	Cancelled        int64
	OldestPendingAge time.Duration
}

// ListJobsOptions configures ListJobs pagination.
type ListJobsOptions struct {
	Limit int // 1..200; zero selects the server default
}

// WorkerOptions configures a Worker. Zero values select defaults from §9 / phase-2 §6.
type WorkerOptions struct {
	Queue           string        // required
	WorkerID        string        // default: hostname-pid-random
	Concurrency     int           // default 10
	LeaseDuration   time.Duration // default 30s
	LeaseWait       time.Duration // long-poll wait; zero → 20s; negative → disabled (client poll)
	PollMinInterval time.Duration // default 100ms; used when LeaseWait is disabled or on transport errors
	PollMaxInterval time.Duration // default 2s
	ShutdownTimeout time.Duration // default 30s
	Logger          *slog.Logger  // default slog.Default()
}
