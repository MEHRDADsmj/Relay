package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	adaptergrpc "github.com/MEHRDADsmj/relay/internal/adapter/grpc"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestTracing_EnqueueLeaseComplete_GRPC(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
	})

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	id := uuid.Must(uuid.Parse("01900000-0000-7000-8000-0000000000bb"))
	repo := memory.New()
	clock := fixedClock{t: now}
	jobs := app.NewJobService(
		repo, clock, &seqIDs{ids: []domain.JobID{id}},
		app.FixedDelayRetryPolicy{Delay: time.Second}, discardLog(), repo,
	).WithTracerProvider(tp)
	queues := app.NewQueueService(repo, repo, clock)
	schedules := app.NewScheduleService(repo, jobs, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: "k"}, discardLog())

	lis := bufconn.Listen(bufSize)
	srv := adaptergrpc.NewServer(adaptergrpc.Services{
		Jobs: jobs, Queues: queues, Schedules: schedules, APIKeys: apiKeys,
	}, adaptergrpc.Options{
		Auth:    auth,
		Logger:  discardLog(),
		Tracing: true,
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		adaptergrpc.GracefulStop(srv, time.Second)
		_ = lis.Close()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx := ctxWithKey(context.Background(), "k")
	client := relayv1.NewJobServiceClient(conn)

	enq, err := client.Enqueue(ctx, &relayv1.EnqueueRequest{
		Queue:   "default",
		Type:    "email.send",
		Payload: []byte(`{"n":1}`),
	})
	require.NoError(t, err)
	require.True(t, enq.Created)

	lease, err := client.Lease(ctx, &relayv1.LeaseRequest{
		Queue:        "default",
		WorkerId:     "w1",
		MaxJobs:      ptrInt32(1),
		LeaseSeconds: ptrInt32(30),
	})
	require.NoError(t, err)
	require.Len(t, lease.Jobs, 1)

	_, err = client.Complete(ctx, &relayv1.CompleteRequest{
		Id:       id.String(),
		WorkerId: "w1",
	})
	require.NoError(t, err)

	spans := rec.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}
	require.Contains(t, byName, "JobService.Enqueue")
	require.Contains(t, byName, "JobService.Lease")
	require.Contains(t, byName, "JobService.Complete")

	appEnq := byName["JobService.Enqueue"]
	require.Equal(t, "default", spanAttr(appEnq, "relay.queue"))
	require.Equal(t, "email.send", spanAttr(appEnq, "relay.job_type"))
	require.Equal(t, id.String(), spanAttr(appEnq, "relay.job_id"))
	require.True(t, appEnq.Parent().HasSpanID(), "enqueue should be child of gRPC server span")

	var serverKind bool
	for _, s := range spans {
		if s.SpanKind() == trace.SpanKindServer {
			serverKind = true
			require.Equal(t, appEnq.SpanContext().TraceID(), s.SpanContext().TraceID())
			break
		}
	}
	require.True(t, serverKind, "expected gRPC server span")
}

func spanAttr(s sdktrace.ReadOnlySpan, key string) string {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}
