package grpc_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	adaptergrpc "github.com/MEHRDADsmj/relay/internal/adapter/grpc"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

const bufSize = 1 << 20

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type seqIDs struct {
	ids []domain.JobID
	i   int
}

func (s *seqIDs) NewID() domain.JobID {
	if s.i >= len(s.ids) {
		return uuid.Must(uuid.NewV7())
	}
	id := s.ids[s.i]
	s.i++
	return id
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testEnv struct {
	jobs      *app.JobService
	queues    *app.QueueService
	schedules *app.ScheduleService
	apiKeys   *app.APIKeyService
	auth      *app.AuthService
	repo      *memory.Repository
	clock     fixedClock
	conn      *grpc.ClientConn
	readyMu   sync.Mutex
	readyErr  error
}

func (e *testEnv) setReadyErr(err error) {
	e.readyMu.Lock()
	e.readyErr = err
	e.readyMu.Unlock()
}

func (e *testEnv) ready(ctx context.Context) error {
	e.readyMu.Lock()
	defer e.readyMu.Unlock()
	if e.readyErr != nil {
		return e.readyErr
	}
	return e.jobs.Ready(ctx)
}

func startTestServer(t *testing.T, bootstrap string) *testEnv {
	t.Helper()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := fixedClock{t: now}
	ids := &seqIDs{}
	jobs := app.NewJobService(repo, clock, ids, app.FixedDelayRetryPolicy{Delay: time.Second}, discardLog(), repo)
	queues := app.NewQueueService(repo, repo, repo, clock)
	schedules := app.NewScheduleService(repo, jobs, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: bootstrap}, discardLog())

	env := &testEnv{
		jobs: jobs, queues: queues, schedules: schedules, apiKeys: apiKeys,
		auth: auth, repo: repo, clock: clock,
	}

	lis := bufconn.Listen(bufSize)
	srv := adaptergrpc.NewServer(adaptergrpc.Services{
		Jobs: jobs, Queues: queues, Schedules: schedules, APIKeys: apiKeys,
	}, adaptergrpc.Options{
		Auth:       auth,
		Logger:     discardLog(),
		Reflection: true,
		Ready:      env.ready,
	})
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		adaptergrpc.GracefulStop(srv, time.Second)
		_ = lis.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	env.conn = conn
	return env
}

func ctxWithKey(ctx context.Context, key string) context.Context {
	if key == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
}

func TestGRPC_JobLifecycle(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	jobs := relayv1.NewJobServiceClient(env.conn)

	enq, err := jobs.Enqueue(ctx, &relayv1.EnqueueRequest{
		Queue:   "default",
		Type:    "demo.job",
		Payload: []byte(`{"n":1}`),
	})
	require.NoError(t, err)
	require.True(t, enq.Created)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_PENDING, enq.Job.Status)
	jobID := enq.Job.Id

	got, err := jobs.GetJob(ctx, &relayv1.GetJobRequest{Id: jobID})
	require.NoError(t, err)
	require.Equal(t, jobID, got.Job.Id)

	lease, err := jobs.Lease(ctx, &relayv1.LeaseRequest{
		Queue:        "default",
		WorkerId:     "w1",
		MaxJobs:      ptrInt32(1),
		LeaseSeconds: ptrInt32(30),
	})
	require.NoError(t, err)
	require.Len(t, lease.Jobs, 1)

	hb, err := jobs.Heartbeat(ctx, &relayv1.HeartbeatRequest{
		Id: jobID, WorkerId: "w1", LeaseSeconds: ptrInt32(60),
	})
	require.NoError(t, err)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_RUNNING, hb.Job.Status)

	done, err := jobs.Complete(ctx, &relayv1.CompleteRequest{Id: jobID, WorkerId: "w1"})
	require.NoError(t, err)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_SUCCEEDED, done.Job.Status)

	attempts, err := jobs.ListAttempts(ctx, &relayv1.ListAttemptsRequest{Id: jobID})
	require.NoError(t, err)
	require.NotEmpty(t, attempts.Attempts)
}

func TestGRPC_JobFailReleaseCancelRetry(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	cli := relayv1.NewJobServiceClient(env.conn)

	enq, err := cli.Enqueue(ctx, &relayv1.EnqueueRequest{Queue: "default", Type: "t"})
	require.NoError(t, err)
	id := enq.Job.Id

	_, err = cli.Lease(ctx, &relayv1.LeaseRequest{Queue: "default", WorkerId: "w1", MaxJobs: ptrInt32(1), LeaseSeconds: ptrInt32(30)})
	require.NoError(t, err)

	failed, err := cli.Fail(ctx, &relayv1.FailRequest{Id: id, WorkerId: "w1", Error: "boom", Permanent: true})
	require.NoError(t, err)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_DEAD, failed.Job.Status)

	retried, err := cli.Retry(ctx, &relayv1.RetryRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_PENDING, retried.Job.Status)

	_, err = cli.Lease(ctx, &relayv1.LeaseRequest{Queue: "default", WorkerId: "w1", MaxJobs: ptrInt32(1), LeaseSeconds: ptrInt32(30)})
	require.NoError(t, err)
	released, err := cli.Release(ctx, &relayv1.ReleaseRequest{Id: id, WorkerId: "w1"})
	require.NoError(t, err)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_PENDING, released.Job.Status)

	cancelled, err := cli.Cancel(ctx, &relayv1.CancelRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, relayv1.JobStatus_JOB_STATUS_CANCELLED, cancelled.Job.Status)
}

func TestGRPC_QueueRPCs(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	jobs := relayv1.NewJobServiceClient(env.conn)
	queues := relayv1.NewQueueServiceClient(env.conn)

	_, err := jobs.Enqueue(ctx, &relayv1.EnqueueRequest{Queue: "default", Type: "t"})
	require.NoError(t, err)

	list, err := queues.ListQueues(ctx, &relayv1.ListQueuesRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, list.Queues)

	stats, err := queues.GetQueueStats(ctx, &relayv1.GetQueueStatsRequest{Queue: "default"})
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Stats.Pending)

	paused, err := queues.PauseQueue(ctx, &relayv1.PauseQueueRequest{Queue: "default"})
	require.NoError(t, err)
	require.True(t, paused.Queue.Paused)

	resumed, err := queues.ResumeQueue(ctx, &relayv1.ResumeQueueRequest{Queue: "default"})
	require.NoError(t, err)
	require.False(t, resumed.Queue.Paused)

	page, err := queues.ListQueueJobs(ctx, &relayv1.ListQueueJobsRequest{
		Queue:  "default",
		Status: relayv1.JobStatus_JOB_STATUS_PENDING,
		Limit:  ptrInt32(10),
	})
	require.NoError(t, err)
	require.Len(t, page.Jobs, 1)

	// permanent fail then retry-dead
	enq, err := jobs.Enqueue(ctx, &relayv1.EnqueueRequest{Queue: "default", Type: "dead"})
	require.NoError(t, err)
	_, err = jobs.Lease(ctx, &relayv1.LeaseRequest{Queue: "default", WorkerId: "w1", MaxJobs: ptrInt32(10), LeaseSeconds: ptrInt32(30)})
	require.NoError(t, err)
	_, err = jobs.Fail(ctx, &relayv1.FailRequest{Id: enq.Job.Id, WorkerId: "w1", Error: "x", Permanent: true})
	require.NoError(t, err)

	retry, err := queues.RetryDeadJobs(ctx, &relayv1.RetryDeadJobsRequest{Queue: "default", Limit: ptrInt32(10)})
	require.NoError(t, err)
	require.GreaterOrEqual(t, retry.Retried, int32(1))
}

func TestGRPC_ScheduleAndAPIKeyRPCs(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	sched := relayv1.NewScheduleServiceClient(env.conn)
	keys := relayv1.NewAPIKeyServiceClient(env.conn)

	created, err := sched.CreateSchedule(ctx, &relayv1.CreateScheduleRequest{
		Name:    "hourly-demo",
		Cron:    "@hourly",
		Queue:   "default",
		Type:    "sched.job",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	id := created.Schedule.Id

	byName, err := sched.GetSchedule(ctx, &relayv1.GetScheduleRequest{IdOrName: "hourly-demo"})
	require.NoError(t, err)
	require.Equal(t, id, byName.Schedule.Id)

	list, err := sched.ListSchedules(ctx, &relayv1.ListSchedulesRequest{Limit: ptrInt32(10)})
	require.NoError(t, err)
	require.NotEmpty(t, list.Schedules)

	updated, err := sched.UpdateSchedule(ctx, &relayv1.UpdateScheduleRequest{
		Id:                id,
		ExpectedUpdatedAt: created.Schedule.UpdatedAt,
		Cron:              ptrString("@daily"),
	})
	require.NoError(t, err)
	require.Equal(t, "@daily", updated.Schedule.Cron)

	disabled, err := sched.DisableSchedule(ctx, &relayv1.DisableScheduleRequest{Id: id})
	require.NoError(t, err)
	require.False(t, disabled.Schedule.Enabled)

	enabled, err := sched.EnableSchedule(ctx, &relayv1.EnableScheduleRequest{Id: id})
	require.NoError(t, err)
	require.True(t, enabled.Schedule.Enabled)

	trig, err := sched.TriggerSchedule(ctx, &relayv1.TriggerScheduleRequest{Id: id})
	require.NoError(t, err)
	require.NotEmpty(t, trig.Job.Id)

	_, err = sched.DeleteSchedule(ctx, &relayv1.DeleteScheduleRequest{Id: id})
	require.NoError(t, err)

	key, err := keys.CreateAPIKey(ctx, &relayv1.CreateAPIKeyRequest{
		Name:   "ops",
		Scopes: []relayv1.Scope{relayv1.Scope_SCOPE_ADMIN},
	})
	require.NoError(t, err)
	require.NotEmpty(t, key.Secret)
	require.NotEmpty(t, key.Key.Prefix)

	listed, err := keys.ListAPIKeys(ctx, &relayv1.ListAPIKeysRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.Keys)

	revoked, err := keys.RevokeAPIKey(ctx, &relayv1.RevokeAPIKeyRequest{Id: key.Key.Id})
	require.NoError(t, err)
	require.NotNil(t, revoked.Key.RevokedAt)
}

func TestGRPC_ErrorMapping(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	jobs := relayv1.NewJobServiceClient(env.conn)
	sched := relayv1.NewScheduleServiceClient(env.conn)

	_, err := jobs.GetJob(ctx, &relayv1.GetJobRequest{Id: uuid.Must(uuid.NewV7()).String()})
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = jobs.Enqueue(ctx, &relayv1.EnqueueRequest{Queue: "", Type: "t"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	st, ok := status.FromError(err)
	require.True(t, ok)
	var foundBadReq bool
	for _, d := range st.Details() {
		if _, ok := d.(*errdetails.BadRequest); ok {
			foundBadReq = true
		}
	}
	require.True(t, foundBadReq, "expected BadRequest details")

	enq, err := jobs.Enqueue(ctx, &relayv1.EnqueueRequest{Queue: "default", Type: "t"})
	require.NoError(t, err)
	_, err = jobs.Lease(ctx, &relayv1.LeaseRequest{Queue: "default", WorkerId: "w1", MaxJobs: ptrInt32(1), LeaseSeconds: ptrInt32(30)})
	require.NoError(t, err)
	_, err = jobs.Complete(ctx, &relayv1.CompleteRequest{Id: enq.Job.Id, WorkerId: "other"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	requireReason(t, err, "LEASE_LOST")

	_, err = jobs.Complete(ctx, &relayv1.CompleteRequest{Id: enq.Job.Id, WorkerId: "w1"})
	require.NoError(t, err)
	_, err = jobs.Retry(ctx, &relayv1.RetryRequest{Id: enq.Job.Id})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	requireReason(t, err, "INVALID_TRANSITION")

	_, err = sched.CreateSchedule(ctx, &relayv1.CreateScheduleRequest{
		Name: "dup", Cron: "@hourly", Queue: "default", Type: "t",
	})
	require.NoError(t, err)
	_, err = sched.CreateSchedule(ctx, &relayv1.CreateScheduleRequest{
		Name: "dup", Cron: "@hourly", Queue: "default", Type: "t",
	})
	require.Equal(t, codes.Aborted, status.Code(err))
}

func TestGRPC_AuthAndHealth(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	jobs := relayv1.NewJobServiceClient(env.conn)
	health := grpc_health_v1.NewHealthClient(env.conn)

	_, err := jobs.Enqueue(context.Background(), &relayv1.EnqueueRequest{Queue: "default", Type: "t"})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = jobs.Enqueue(ctxWithKey(context.Background(), "wrong"), &relayv1.EnqueueRequest{Queue: "default", Type: "t"})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// producer cannot list schedules
	adminCtx := ctxWithKey(context.Background(), "bootstrap-secret")
	keys := relayv1.NewAPIKeyServiceClient(env.conn)
	created, err := keys.CreateAPIKey(adminCtx, &relayv1.CreateAPIKeyRequest{
		Name:   "prod",
		Scopes: []relayv1.Scope{relayv1.Scope_SCOPE_PRODUCER},
	})
	require.NoError(t, err)
	prodCtx := ctxWithKey(context.Background(), created.Secret)
	sched := relayv1.NewScheduleServiceClient(env.conn)
	_, err = sched.ListSchedules(prodCtx, &relayv1.ListSchedulesRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	ok, err := health.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ok.Status)

	env.setReadyErr(errors.New("storage down"))
	bad, err := health.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, bad.Status)
}

func TestGRPC_IdempotentEnqueue(t *testing.T) {
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	jobs := relayv1.NewJobServiceClient(env.conn)

	first, err := jobs.Enqueue(ctx, &relayv1.EnqueueRequest{
		Queue: "default", Type: "t", IdempotencyKey: "k1",
	})
	require.NoError(t, err)
	require.True(t, first.Created)

	second, err := jobs.Enqueue(ctx, &relayv1.EnqueueRequest{
		Queue: "default", Type: "t", IdempotencyKey: "k1",
	})
	require.NoError(t, err)
	require.False(t, second.Created)
	require.Equal(t, first.Job.Id, second.Job.Id)
}

func TestMapError_Table(t *testing.T) {
	// Exercise mapError via RPCs already; also ensure UpdateSchedule missing expected_updated_at.
	env := startTestServer(t, "bootstrap-secret")
	ctx := ctxWithKey(context.Background(), "bootstrap-secret")
	sched := relayv1.NewScheduleServiceClient(env.conn)
	created, err := sched.CreateSchedule(ctx, &relayv1.CreateScheduleRequest{
		Name: "u1", Cron: "@hourly", Queue: "default", Type: "t",
	})
	require.NoError(t, err)

	_, err = sched.UpdateSchedule(ctx, &relayv1.UpdateScheduleRequest{Id: created.Schedule.Id})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = sched.UpdateSchedule(ctx, &relayv1.UpdateScheduleRequest{
		Id:                created.Schedule.Id,
		ExpectedUpdatedAt: timestamppb.New(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		Cron:              ptrString("@daily"),
	})
	require.Equal(t, codes.Aborted, status.Code(err))
}

func requireReason(t *testing.T, err error, reason string) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok)
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			require.Equal(t, reason, info.Reason)
			return
		}
	}
	t.Fatalf("missing ErrorInfo reason %q in %v", reason, st.Details())
}

func ptrInt32(v int32) *int32    { return &v }
func ptrString(v string) *string { return &v }
