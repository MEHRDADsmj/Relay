package http_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	relayhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
	"github.com/MEHRDADsmj/relay/internal/platform/logging"
)

func TestTracing_EnqueueLeaseComplete_HTTP(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
	})

	var logBuf bytes.Buffer
	log, err := logging.NewWithWriter("info", "json", &logBuf)
	require.NoError(t, err)

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	id := uuid.Must(uuid.Parse("01900000-0000-7000-8000-0000000000aa"))
	repo := memory.New()
	clock := fixedClock{t: now}
	svc := app.NewJobService(
		repo, clock, &seqIDs{ids: []domain.JobID{id}},
		app.FixedDelayRetryPolicy{Delay: time.Second}, log, repo,
	).WithTracerProvider(tp)
	queues := app.NewQueueService(repo, repo, repo, clock)
	schedules := app.NewScheduleService(repo, svc, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	h := mountRouter(repo, clock, svc, queues, schedules, apiKeys, relayhttp.Options{
		Logger:  log,
		Tracing: true,
	})

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue":   "default",
		"type":    "email.send",
		"payload": map[string]any{"n": 1},
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
		"max_jobs":  1,
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id.String()+"/complete", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	spans := rec.Ended()
	byName := indexSpansByName(spans)
	require.Contains(t, byName, "JobService.Enqueue")
	require.Contains(t, byName, "JobService.Lease")
	require.Contains(t, byName, "JobService.Complete")

	enq := byName["JobService.Enqueue"]
	require.Equal(t, "default", attrString(enq, "relay.queue"))
	require.Equal(t, "email.send", attrString(enq, "relay.job_type"))
	require.Equal(t, id.String(), attrString(enq, "relay.job_id"))
	require.True(t, enq.Parent().HasSpanID(), "enqueue should be child of HTTP span")

	httpParents := serverSpans(spans)
	require.NotEmpty(t, httpParents)
	require.Equal(t, enq.SpanContext().TraceID(), httpParents[0].SpanContext().TraceID())

	complete := byName["JobService.Complete"]
	require.Equal(t, id.String(), attrString(complete, "relay.job_id"))

	require.Contains(t, logBuf.String(), `"trace_id"`)
	require.Contains(t, logBuf.String(), enq.SpanContext().TraceID().String())
}

func indexSpansByName(spans []sdktrace.ReadOnlySpan) map[string]sdktrace.ReadOnlySpan {
	out := make(map[string]sdktrace.ReadOnlySpan, len(spans))
	for _, s := range spans {
		out[s.Name()] = s
	}
	return out
}

func attrString(s sdktrace.ReadOnlySpan, key string) string {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

func serverSpans(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.SpanKind() == trace.SpanKindServer {
			out = append(out, s)
		}
	}
	return out
}
