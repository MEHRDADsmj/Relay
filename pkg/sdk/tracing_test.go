package sdk_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func TestWorker_ConsumerSpanLinksProducer(t *testing.T) {
	// Not parallel: mutates global text-map propagator.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	pctx, psp := tp.Tracer("test").Start(context.Background(), "producer.enqueue")
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(pctx, carrier)
	psp.End()
	producerSC := psp.SpanContext()

	var leased bool
	var once sync.Once
	done := make(chan struct{})

	api := &fakeAPI{}
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		if leased {
			return http.StatusOK, nil
		}
		leased = true
		job := jobWire("018f1111-0000-7000-8000-000000000001", "default", "email.send", map[string]any{}, 1)
		job["trace_context"] = map[string]string(carrier)
		return http.StatusOK, []map[string]any{job}
	}
	api.completeFn = func(string, map[string]any) int { return http.StatusOK }
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("018f1111-0000-7000-8000-000000000001", nil)
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL, sdk.WithTracerProvider(tp))
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		Concurrency:     1,
		LeaseWait:       -1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "email.send", func(ctx context.Context, job sdk.Job[map[string]any]) error {
		once.Do(func() { close(done) })
		return nil
	})

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(runCtx) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked")
	}
	cancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit")
	}

	var consumer sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "relay.process email.send" {
			consumer = s
			break
		}
	}
	require.NotNil(t, consumer, "consumer span missing")
	assert.Equal(t, trace.SpanKindConsumer, consumer.SpanKind())
	require.Len(t, consumer.Links(), 1)
	assert.Equal(t, producerSC.TraceID(), consumer.Links()[0].SpanContext.TraceID())
	assert.Equal(t, producerSC.SpanID(), consumer.Links()[0].SpanContext.SpanID())
	assert.False(t, consumer.Parent().IsValid() && consumer.Parent().SpanID() == producerSC.SpanID(),
		"consumer must link producer, not be its child")
}
