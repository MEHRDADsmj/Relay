package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/MEHRDADsmj/relay/internal/adapter/http/oapi"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

const (
	problemTypeBlank             = "about:blank"
	problemTypeLeaseLost         = "https://relay.dev/problems/lease-lost"
	problemTypeInvalidTransition = "https://relay.dev/problems/invalid-transition"

	titleBadRequest         = "Bad Request"
	titleUnauthorized       = "Unauthorized"
	titleNotFound           = "Not Found"
	titleConflict           = "Conflict"
	titlePayloadTooLarge    = "Payload Too Large"
	titleInternalError      = "Internal Server Error"
	titleServiceUnavailable = "Service Unavailable"
)

// problem is the RFC 9457 payload written to clients.
type problem struct {
	Type     string            `json:"type"`
	Title    string            `json:"title"`
	Status   int               `json:"status"`
	Detail   string            `json:"detail,omitempty"`
	Instance string            `json:"instance,omitempty"`
	Errors   []oapi.FieldError `json:"errors,omitempty"`
}

func writeProblem(w http.ResponseWriter, status int, problemType, title, detail string, fieldErrors []oapi.FieldError) {
	if problemType == "" {
		problemType = problemTypeBlank
	}
	p := problem{
		Type:   problemType,
		Title:  title,
		Status: status,
		Detail: detail,
		Errors: fieldErrors,
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeUnauthorized(w http.ResponseWriter) {
	writeProblem(w, http.StatusUnauthorized, problemTypeBlank, titleUnauthorized, "missing or invalid API key", nil)
}

func writePayloadTooLarge(w http.ResponseWriter) {
	writeProblem(w, http.StatusRequestEntityTooLarge, problemTypeBlank, titlePayloadTooLarge, "request body exceeds 1 MiB", nil)
}

func writeInternal(w http.ResponseWriter) {
	writeProblem(w, http.StatusInternalServerError, problemTypeBlank, titleInternalError, "an unexpected error occurred", nil)
}

func mapDomainError(err error) (status int, problemType, title, detail string, fieldErrors []oapi.FieldError) {
	var ve *domain.ValidationError
	if errors.As(err, &ve) {
		field := ve.Field
		switch field {
		case "lease_duration":
			field = "lease_seconds"
		case "wait":
			field = "wait_seconds"
		}
		return http.StatusBadRequest, problemTypeBlank, titleBadRequest, "validation failed", []oapi.FieldError{
			{Field: field, Message: ve.Message},
		}
	}
	switch {
	case errors.Is(err, domain.ErrValidation):
		return http.StatusBadRequest, problemTypeBlank, titleBadRequest, "validation failed", nil
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound, problemTypeBlank, titleNotFound, "resource not found", nil
	case errors.Is(err, domain.ErrLeaseLost):
		return http.StatusConflict, problemTypeLeaseLost, titleConflict, "lease lost", nil
	case errors.Is(err, domain.ErrInvalidTransition):
		return http.StatusConflict, problemTypeInvalidTransition, titleConflict, "invalid transition", nil
	case errors.Is(err, domain.ErrConflict):
		return http.StatusConflict, problemTypeBlank, titleConflict, "conflict", nil
	default:
		return http.StatusInternalServerError, problemTypeBlank, titleInternalError, "an unexpected error occurred", nil
	}
}

func requestErrorHandler(w http.ResponseWriter, _ *http.Request, err error) {
	if isMaxBytesError(err) {
		writePayloadTooLarge(w)
		return
	}
	writeProblem(w, http.StatusBadRequest, problemTypeBlank, titleBadRequest, err.Error(), nil)
}

func responseErrorHandler(log *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		status, problemType, title, detail, fields := mapDomainError(err)
		if status >= http.StatusInternalServerError {
			log.ErrorContext(r.Context(), "request failed", "err", err, "request_id", RequestIDFromContext(r.Context()), "path", r.URL.Path)
		}
		writeProblem(w, status, problemType, title, detail, fields)
	}
}

func chiErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	// Path/param binding failures are client errors.
	writeProblem(w, http.StatusBadRequest, problemTypeBlank, titleBadRequest, err.Error(), nil)
}

func readinessProblem() oapi.Problem {
	return oapi.Problem{
		Type:   problemTypeBlank,
		Title:  titleServiceUnavailable,
		Status: http.StatusServiceUnavailable,
		Detail: ptr("storage unavailable"),
	}
}

func ptr[T any](v T) *T { return &v }
