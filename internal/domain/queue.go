package domain

import "time"

// Queue is a named work stream that can be paused independently.
type Queue struct {
	Name      string
	Paused    bool
	CreatedAt time.Time // first time a job was enqueued to it
	UpdatedAt time.Time
}

// QueueStats holds aggregate counts for a single queue.
type QueueStats struct {
	Queue            string
	Paused           bool
	Pending          int64 // includes scheduled (run_at in future)
	Scheduled        int64 // subset of pending with run_at > now
	Running          int64
	Succeeded        int64
	Dead             int64
	Cancelled        int64
	OldestPendingAge time.Duration // 0 if none
}
