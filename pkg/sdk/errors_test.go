package sdk

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAPIError_ConflictWithoutType_NotLeaseLost(t *testing.T) {
	t.Parallel()
	// gRPC Aborted / bare conflict → HTTP 409 with empty problem type must not
	// match ErrLeaseLost (or ErrInvalidTransition).
	err := &APIError{
		StatusCode: http.StatusConflict,
		Type:       "",
		Title:      "Aborted",
		Detail:     "schedule name already exists",
	}
	require.False(t, errors.Is(err, ErrLeaseLost), "empty-type 409 must not match ErrLeaseLost")
	require.False(t, errors.Is(err, ErrInvalidTransition))
}

func TestAPIError_PositiveTypeMatch(t *testing.T) {
	t.Parallel()
	lease := &APIError{StatusCode: http.StatusConflict, Type: problemTypeLeaseLost}
	require.True(t, errors.Is(lease, ErrLeaseLost))
	require.False(t, errors.Is(lease, ErrInvalidTransition))

	inv := &APIError{StatusCode: http.StatusConflict, Type: problemTypeInvalidTransition}
	require.True(t, errors.Is(inv, ErrInvalidTransition))
	require.False(t, errors.Is(inv, ErrLeaseLost))

	// Positive type match does not depend on status code.
	require.True(t, errors.Is(&APIError{StatusCode: 500, Type: problemTypeLeaseLost}, ErrLeaseLost))
}

func TestMapGRPCError_BareFailedPrecondition_NotLeaseLost(t *testing.T) {
	t.Parallel()
	mapped := mapGRPCError(status.Error(codes.FailedPrecondition, "precondition failed"))
	require.False(t, errors.Is(mapped, ErrLeaseLost),
		"bare FailedPrecondition must not be coerced to LEASE_LOST")
	var ae *APIError
	require.True(t, errors.As(mapped, &ae))
	require.Equal(t, http.StatusConflict, ae.StatusCode)
	require.Empty(t, ae.Type)
}
