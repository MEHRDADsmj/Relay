package http

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type contextKey int

const requestIDKey contextKey = 1

const (
	headerRequestID     = "X-Request-ID"
	headerAuthorization = "Authorization"
	bearerPrefix        = "Bearer "

	defaultMaxBodyBytes = 1 << 20 // 1 MiB
)

// RequestIDFromContext returns the request ID set by RequestID middleware, or "".
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// RequestID ensures every request has an X-Request-ID (generated if absent).
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get(headerRequestID))
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set(headerRequestID, id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}

func logPanic(ctx context.Context, log *slog.Logger, path string, rec any) {
	log.ErrorContext(ctx, "panic recovered", "panic", rec, "request_id", RequestIDFromContext(ctx), "path", path)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Recoverer converts panics into RFC 9457 500 responses.
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			path := r.URL.Path
			defer func() {
				if rec := recover(); rec != nil {
					logPanic(ctx, log, path, rec)
					writeInternal(w)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// AccessLog logs method, path, status, duration, and request_id after the handler returns.
func AccessLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			log.InfoContext(r.Context(), "access",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration", time.Since(start),
				"request_id", RequestIDFromContext(r.Context()),
			)
		})
	}
}

// BodyLimit rejects bodies larger than maxBytes with 413 problem+json.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	if maxBytes <= 0 {
		maxBytes = defaultMaxBodyBytes
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
			// MaxBytesReader returns *http.MaxBytesError on read overflow; decode
			// errors are handled by oapi request error handler. Detect via
			// ResponseWriter if already written; otherwise probe is in handler path.
		})
	}
}

// MaxBytesError reports whether err (or a wrapped cause) is a body-too-large error.
func isMaxBytesError(err error) bool {
	var mb *http.MaxBytesError
	return errors.As(err, &mb)
}

// Auth requires Authorization: Bearer <apiKey> when apiKey is non-empty.
// /healthz and /readyz are always exempt.
func Auth(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if apiKey == "" {
			return next
		}
		expected := []byte(apiKey)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			if path == "/healthz" || path == "/readyz" {
				next.ServeHTTP(w, r)
				return
			}
			auth := r.Header.Get(headerAuthorization)
			if !strings.HasPrefix(auth, bearerPrefix) {
				writeUnauthorized(w)
				return
			}
			got := []byte(strings.TrimPrefix(auth, bearerPrefix))
			if subtle.ConstantTimeCompare(got, expected) != 1 {
				writeUnauthorized(w)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
