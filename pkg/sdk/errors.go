package sdk

import (
	"errors"
	"fmt"
)

// Sentinel errors for common API outcomes and worker shutdown.
var (
	// ErrLeaseLost indicates the worker no longer owns the job lease (HTTP 409).
	ErrLeaseLost = errors.New("relay: lease lost")
	// ErrNotFound indicates the requested job does not exist (HTTP 404).
	ErrNotFound = errors.New("relay: not found")
	// ErrShutdownTimeout is returned by Worker.Run when in-flight jobs did not
	// finish within ShutdownTimeout after the run context was cancelled.
	ErrShutdownTimeout = errors.New("relay: shutdown timeout")
)

// APIError is an RFC 9457 problem+json response from the Relay API.
type APIError struct {
	StatusCode int
	Title      string
	Detail     string
}

func (e *APIError) Error() string {
	if e == nil {
		return "relay: api error"
	}
	if e.Detail != "" {
		return fmt.Sprintf("relay: %s (%d): %s", e.Title, e.StatusCode, e.Detail)
	}
	return fmt.Sprintf("relay: %s (%d)", e.Title, e.StatusCode)
}

// Is reports whether target matches this APIError via status-code sentinels.
func (e *APIError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrLeaseLost:
		return e.StatusCode == 409
	case ErrNotFound:
		return e.StatusCode == 404
	default:
		return false
	}
}
