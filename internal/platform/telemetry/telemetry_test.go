package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/MEHRDADsmj/relay/internal/platform/telemetry"
)

func TestSetup_EmptyEndpoint_NoopProvider(t *testing.T) {
	t.Cleanup(func() {
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
	})

	shutdown, err := telemetry.Setup(context.Background(), telemetry.Config{
		Endpoint:    "",
		SampleRatio: 1,
	})
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))

	tr := otel.Tracer("test")
	_, span := tr.Start(context.Background(), "noop-span")
	require.False(t, span.IsRecording())
	span.End()
}

func TestSetup_WithRecorder_SamplesSpans(t *testing.T) {
	// Unit path: inject sdk TracerProvider with SpanRecorder instead of real OTLP.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(nil)
	})

	tr := otel.Tracer("relay/test")
	ctx, span := tr.Start(context.Background(), "unit")
	span.End()
	_ = ctx

	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "unit", spans[0].Name())
	require.True(t, spans[0].SpanContext().IsValid())
}

func TestPropagator_IsTraceContextAndBaggage(t *testing.T) {
	t.Cleanup(func() {
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
	})
	_, err := telemetry.Setup(context.Background(), telemetry.Config{})
	require.NoError(t, err)

	prop := otel.GetTextMapPropagator()
	require.NotNil(t, prop)

	carrier := propagationCarrier{}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	prop.Inject(ctx, carrier)
	require.Contains(t, carrier["traceparent"], "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01")
}

type propagationCarrier map[string]string

func (c propagationCarrier) Get(key string) string { return c[key] }
func (c propagationCarrier) Set(key, value string) { c[key] = value }
func (c propagationCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
