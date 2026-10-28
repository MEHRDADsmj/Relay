package grpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestMapError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"nil", nil, codes.OK},
		{"validation sentinel", domain.ErrValidation, codes.InvalidArgument},
		{"validation field", &domain.ValidationError{Field: "queue", Message: "required"}, codes.InvalidArgument},
		{"unauthenticated", domain.ErrUnauthenticated, codes.Unauthenticated},
		{"forbidden", domain.ErrForbidden, codes.PermissionDenied},
		{"not found", domain.ErrNotFound, codes.NotFound},
		{"lease lost", domain.ErrLeaseLost, codes.FailedPrecondition},
		{"invalid transition", domain.ErrInvalidTransition, codes.FailedPrecondition},
		{"conflict", domain.ErrConflict, codes.Aborted},
		{"unknown", errors.New("boom"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mapError(tc.err)
			if tc.err == nil {
				require.NoError(t, got)
				return
			}
			require.Equal(t, tc.code, status.Code(got))
		})
	}

	err := mapError(&domain.ValidationError{Field: "queue", Message: "required"})
	st, ok := status.FromError(err)
	require.True(t, ok)
	var bad *errdetails.BadRequest
	for _, d := range st.Details() {
		if br, ok := d.(*errdetails.BadRequest); ok {
			bad = br
		}
	}
	require.NotNil(t, bad)
	require.Equal(t, "queue", bad.FieldViolations[0].Field)

	err = mapError(domain.ErrLeaseLost)
	st, ok = status.FromError(err)
	require.True(t, ok)
	var info *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			info = ei
		}
	}
	require.NotNil(t, info)
	require.Equal(t, "LEASE_LOST", info.Reason)
}
