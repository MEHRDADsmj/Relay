package domain

import "time"

// AttemptOutcome is the recorded result of a single lease attempt.
type AttemptOutcome string

const (
	AttemptRunning   AttemptOutcome = "running"
	AttemptSucceeded AttemptOutcome = "succeeded"
	AttemptFailed    AttemptOutcome = "failed"
	AttemptExpired   AttemptOutcome = "expired"
	AttemptReleased  AttemptOutcome = "released"
	AttemptCancelled AttemptOutcome = "cancelled"
)

// Attempt is one claim/lease cycle for a job.
type Attempt struct {
	JobID      JobID
	Number     int // 1-based, equals Job.Attempts at claim time
	WorkerID   string
	StartedAt  time.Time
	FinishedAt *time.Time
	Outcome    AttemptOutcome
	Error      string
}
