package sdk

import (
	"errors"
	"fmt"
)

const (
	problemTypeLeaseLost         = "https://relay.dev/problems/lease-lost"
	problemTypeInvalidTransition = "https://relay.dev/problems/invalid-transition"
)

// Sentinel errors for common API outcomes and worker shutdown.
var (
	// ErrLeaseLost indicates the worker no longer owns the job lease (HTTP 409 lease-lost).
	ErrLeaseLost = errors.New("relay: lease lost")
	// ErrInvalidTransition indicates the job cannot perform the requested state change
	// (HTTP 409 invalid-transition).
	ErrInvalidTransition = errors.New("relay: invalid transition")
	// ErrNotFound indicates the requested job does not exist (HTTP 404).
	ErrNotFound = errors.New("relay: not found")
	// ErrShutdownTimeout is returned by Worker.Run when in-flight jobs did not
	// finish within ShutdownTimeout after the run context was cancelled.
	ErrShutdownTimeout = errors.New("relay: shutdown timeout")
)

// APIError is an RFC 9457 problem+json response from the Relay API.
type APIError struct {
	StatusCode int
	Type       string
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

// Is reports whether target matches this APIError via status-code / problem-type sentinels.
func (e *APIError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrLeaseLost:
		return e.StatusCode == 409 && e.Type != problemTypeInvalidTransition
	case ErrInvalidTransition:
		return e.StatusCode == 409 && e.Type == problemTypeInvalidTransition
	case ErrNotFound:
		return e.StatusCode == 404
	default:
		return false
	}
}

type permanentError struct {
	err error
}

// Permanent wraps err so a worker handler signals "do not retry".
// The SDK reports fail with permanent=true; the server marks the job dead.
func Permanent(err error) error {
	if err == nil {
		return &permanentError{err: errors.New("permanent failure")}
	}
	return &permanentError{err: err}
}

func (e *permanentError) Error() string {
	if e == nil || e.err == nil {
		return "relay: permanent failure"
	}
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func isPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
