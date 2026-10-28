package sdk

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func producerSpanContext(tc map[string]string) trace.SpanContext {
	if len(tc) == 0 {
		return trace.SpanContext{}
	}
	carrier := propagation.MapCarrier{}
	for k, v := range tc {
		carrier[k] = v
	}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	return trace.SpanContextFromContext(ctx)
}

func (w *Worker) startConsumerSpan(ctx context.Context, job apiJob) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "relay"),
			attribute.String("messaging.destination.name", job.Queue),
			attribute.String("messaging.message.id", job.ID),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("relay.job_type", job.Type),
		),
	}
	if sc := producerSpanContext(job.TraceContext); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	return w.client.tracer().Start(ctx, "relay.process "+job.Type, opts...)
}

func endConsumerSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
