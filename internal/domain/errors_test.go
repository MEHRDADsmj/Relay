package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"not found", ErrNotFound},
		{"validation", ErrValidation},
		{"lease lost", ErrLeaseLost},
		{"conflict", ErrConflict},
		{"invalid transition", ErrInvalidTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.err == nil {
				t.Fatal("sentinel must be non-nil")
			}
			if tt.err.Error() == "" {
				t.Fatal("sentinel must have a message")
			}
		})
	}
}

func TestValidationError_UnwrapsToErrValidation(t *testing.T) {
	t.Parallel()

	err := &ValidationError{Field: "queue", Message: "required"}
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("errors.Is(ValidationError, ErrValidation) = false")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("ValidationError must not match ErrNotFound")
	}
	got := err.Error()
	if !strings.Contains(got, "queue") || !strings.Contains(got, "required") {
		t.Fatalf("Error() = %q, want field and message", got)
	}
}

func TestValidationError_ErrorFormatting(t *testing.T) {
	t.Parallel()

	var nilErr *ValidationError
	if got := nilErr.Error(); got != "" {
		t.Fatalf("nil Error() = %q", got)
	}
	msgOnly := &ValidationError{Message: "oops"}
	if got := msgOnly.Error(); got != "oops" {
		t.Fatalf("message-only Error() = %q", got)
	}
}
