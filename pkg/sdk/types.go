package sdk

import (
	"log/slog"
	"time"
)

// JobInfo is a job as returned by the Relay HTTP API.
type JobInfo struct {
	ID             string
	Queue          string
	Type           string
	Payload        []byte
	Status         string
	Priority       int
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

// WorkerOptions configures a Worker. Zero values select defaults from §9.
type WorkerOptions struct {
	Queue           string        // required
	WorkerID        string        // default: hostname-pid-random
	Concurrency     int           // default 10
	LeaseDuration   time.Duration // default 30s
	PollMinInterval time.Duration // default 100ms
	PollMaxInterval time.Duration // default 2s
	ShutdownTimeout time.Duration // default 30s
	Logger          *slog.Logger  // default slog.Default()
}
