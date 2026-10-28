package domain

import "errors"

// Sentinel errors returned by domain and application layers.
var (
	ErrNotFound          = errors.New("not found")
	ErrValidation        = errors.New("validation error")
	ErrLeaseLost         = errors.New("lease lost")
	ErrConflict          = errors.New("conflict")
	ErrInvalidTransition = errors.New("invalid transition")
)

// ValidationError carries field-level detail and unwraps to ErrValidation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Field == "" {
		return e.Message
	}
	return e.Field + ": " + e.Message
}

func (e *ValidationError) Unwrap() error {
	return ErrValidation
}

func validationErrorf(field, message string) error {
	return &ValidationError{Field: field, Message: message}
}
