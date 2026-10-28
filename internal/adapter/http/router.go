package http

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/MEHRDADsmj/relay/internal/adapter/http/oapi"
	"github.com/MEHRDADsmj/relay/internal/app"
)

// Options configures the HTTP adapter.
type Options struct {
	// APIKey, when non-empty, enables Bearer auth (health endpoints exempt).
	APIKey string
	// Logger is used for access logs and error details (defaults to slog.Default).
	Logger *slog.Logger
	// MaxBodyBytes limits request bodies (defaults to 1 MiB).
	MaxBodyBytes int64
}

// NewRouter builds the chi router with middleware and OpenAPI routes.
func NewRouter(svc *app.JobService, opts Options) http.Handler {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	maxBody := opts.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}

	r := chi.NewRouter()
	// Middleware order per phase-1 §7:
	// request ID → recoverer → access log → body limit → auth
	r.Use(RequestID)
	r.Use(Recoverer(log))
	r.Use(AccessLog(log))
	r.Use(BodyLimit(maxBody))
	r.Use(Auth(opts.APIKey))

	h := NewHandler(svc, log)
	strict := oapi.NewStrictHandlerWithOptions(h, nil, oapi.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  requestErrorHandler,
		ResponseErrorHandlerFunc: responseErrorHandler(log),
	})
	return oapi.HandlerWithOptions(strict, oapi.ChiServerOptions{
		BaseRouter:       r,
		ErrorHandlerFunc: chiErrorHandler,
	})
}
