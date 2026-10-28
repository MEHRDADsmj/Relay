package logging

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// TraceHandler wraps a slog.Handler and adds trace_id/span_id from context when a span is active.
type TraceHandler struct {
	inner slog.Handler
}

// NewTraceHandler returns a handler that correlates logs with the active span.
func NewTraceHandler(inner slog.Handler) *TraceHandler {
	return &TraceHandler{inner: inner}
}

// Enabled implements slog.Handler.
func (h *TraceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle implements slog.Handler.
func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanFromContext(ctx)
	if sc := span.SpanContext(); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{inner: h.inner.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{inner: h.inner.WithGroup(name)}
}
