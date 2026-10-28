package grpc

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

const errorDomain = "relay.dev"

// mapError converts domain/app errors to gRPC status errors.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var ve *domain.ValidationError
	if errors.As(err, &ve) {
		st := status.New(codes.InvalidArgument, "validation failed")
		st, detailErr := st.WithDetails(&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{{
				Field:       ve.Field,
				Description: ve.Message,
			}},
		})
		if detailErr != nil {
			return status.Error(codes.InvalidArgument, "validation failed")
		}
		return st.Err()
	}
	switch {
	case errors.Is(err, domain.ErrValidation):
		return status.Error(codes.InvalidArgument, "validation failed")
	case errors.Is(err, domain.ErrUnauthenticated):
		return status.Error(codes.Unauthenticated, "missing or invalid API key")
	case errors.Is(err, domain.ErrForbidden):
		return status.Error(codes.PermissionDenied, "insufficient scope")
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, domain.ErrLeaseLost):
		return failedPrecondition("lease lost", "LEASE_LOST")
	case errors.Is(err, domain.ErrInvalidTransition):
		return failedPrecondition("invalid transition", "INVALID_TRANSITION")
	case errors.Is(err, domain.ErrConflict):
		return status.Error(codes.Aborted, "conflict")
	default:
		return status.Error(codes.Internal, "an unexpected error occurred")
	}
}

func failedPrecondition(msg, reason string) error {
	st := status.New(codes.FailedPrecondition, msg)
	st, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: errorDomain,
	})
	if err != nil {
		return status.Error(codes.FailedPrecondition, msg)
	}
	return st.Err()
}
