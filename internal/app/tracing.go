package app

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

const appTracerName = "relay/app"

func tracerOrNoop(t trace.Tracer) trace.Tracer {
	if t == nil {
		return noop.NewTracerProvider().Tracer(appTracerName)
	}
	return t
}

func startSpan(ctx context.Context, tr trace.Tracer, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return tr.Start(ctx, name, trace.WithAttributes(attrs...))
}

func endSpan(span trace.Span, err error) {
	if err != nil && !isExpectedDomainError(err) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func isExpectedDomainError(err error) bool {
	return errors.Is(err, domain.ErrNotFound) ||
		errors.Is(err, domain.ErrValidation) ||
		errors.Is(err, domain.ErrLeaseLost) ||
		errors.Is(err, domain.ErrConflict) ||
		errors.Is(err, domain.ErrInvalidTransition) ||
		errors.Is(err, domain.ErrUnauthenticated) ||
		errors.Is(err, domain.ErrForbidden)
}

func attrQueue(q string) attribute.KeyValue {
	return attribute.String("relay.queue", q)
}

func attrJobType(t string) attribute.KeyValue {
	return attribute.String("relay.job_type", t)
}

func attrJobID(id domain.JobID) attribute.KeyValue {
	return attribute.String("relay.job_id", id.String())
}

func attrAttempt(n int) attribute.KeyValue {
	return attribute.Int("relay.attempt", n)
}
