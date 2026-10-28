package app

import (
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// SystemClock returns wall-clock UTC time.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}

// UUIDv7Generator creates time-ordered UUIDv7 job IDs.
type UUIDv7Generator struct{}

// NewID implements IDGenerator.
func (UUIDv7Generator) NewID() domain.JobID {
	for {
		id, err := uuid.NewV7()
		if err == nil {
			return id
		}
	}
}
