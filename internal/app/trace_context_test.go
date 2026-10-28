package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestEnqueue_PersistsInjectedTraceContext(t *testing.T) {
	// Not parallel: mutates global text-map propagator.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	svc, _, _ := newService(t, now)
	svc.WithTracerProvider(tp)

	ctx, span := tp.Tracer("test").Start(adminCtx(), "client.request")
	job, created, err := svc.Enqueue(ctx, app.EnqueueCommand{Queue: "default", Type: "email.send"})
	span.End()
	require.NoError(t, err)
	assert.True(t, created)
	require.NotEmpty(t, job.TraceContext)
	assert.Contains(t, job.TraceContext, domain.TraceContextKeyParent)
	assert.Contains(t, job.TraceContext[domain.TraceContextKeyParent], span.SpanContext().TraceID().String())
}

func TestScheduler_Fire_CarriesFireSpanTraceContext(t *testing.T) {
	// Not parallel: mutates global text-map propagator.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := &fixedClock{now: now}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	js := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Minute}, log, nil).
		WithTracerProvider(tp)
	parser := adaptercron.NewParser()
	ss := app.NewScheduleService(repo, js, parser, clock, app.UUIDv7Generator{})
	elector := memory.NewLeaderElector()
	sched := app.NewScheduler(repo, js, elector, parser, clock, app.SchedulerConfig{Tick: 10 * time.Millisecond, Batch: 100}, log).
		WithTracerProvider(tp)

	created, err := ss.Create(adminCtx(), app.CreateScheduleCommand{
		Name: "hourly-trace", Cron: "@hourly", Queue: "default", JobType: "t",
	})
	require.NoError(t, err)
	clock.Set(created.NextRunAt)
	require.NoError(t, sched.Tick(context.Background(), clock.Now()))

	page, err := js.ListByStatus(adminCtx(), app.ListByStatusCommand{
		Queue: "default", Status: domain.StatusPending, Page: domain.PageRequest{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.NotEmpty(t, page.Items[0].TraceContext)
	parent := page.Items[0].TraceContext[domain.TraceContextKeyParent]
	require.NotEmpty(t, parent)

	var fireTraceID string
	for _, s := range rec.Ended() {
		if s.Name() == "Scheduler.fire" {
			fireTraceID = s.SpanContext().TraceID().String()
			break
		}
	}
	require.NotEmpty(t, fireTraceID)
	// Injected context is the Enqueue child span; TraceID matches the fire root.
	assert.Contains(t, parent, fireTraceID)
}
