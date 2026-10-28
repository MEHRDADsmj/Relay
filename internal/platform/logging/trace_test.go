package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTraceHandler_AddsTraceAndSpanIDs(t *testing.T) {
	var buf bytes.Buffer
	log, err := NewWithWriter("info", "json", &buf)
	require.NoError(t, err)

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("test").Start(context.Background(), "log-corr")
	log.InfoContext(ctx, "correlated")
	span.End()

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "correlated", entry["msg"])
	require.NotEmpty(t, entry["trace_id"])
	require.NotEmpty(t, entry["span_id"])
	require.Equal(t, span.SpanContext().TraceID().String(), entry["trace_id"])
	require.Equal(t, span.SpanContext().SpanID().String(), entry["span_id"])
}

func TestTraceHandler_OmitsIDsWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	log, err := NewWithWriter("info", "json", &buf)
	require.NoError(t, err)

	log.InfoContext(context.Background(), "plain")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "plain", entry["msg"])
	_, hasTrace := entry["trace_id"]
	_, hasSpan := entry["span_id"]
	require.False(t, hasTrace)
	require.False(t, hasSpan)
}
