package sdk_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	adaptergrpc "github.com/MEHRDADsmj/relay/internal/adapter/grpc"
	relayhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

const (
	dualBufSize   = 1 << 20
	dualBootstrap = "sdk-dual-bootstrap"
)

type dualFixedClock struct{ t time.Time }

func (c dualFixedClock) Now() time.Time { return c.t }

func dualDiscardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type dualEnv struct {
	Client *sdk.Client
	Name   string
}

func dualTransports(t *testing.T) []dualEnv {
	t.Helper()
	return []dualEnv{
		{Name: "http", Client: startDualHTTP(t)},
		{Name: "grpc", Client: startDualGRPC(t)},
	}
}

func startDualStack(t *testing.T) (*app.JobService, *app.QueueService, *app.ScheduleService, *app.APIKeyService, *app.AuthService, *memory.Repository) {
	t.Helper()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := dualFixedClock{t: now}
	log := dualDiscardLog()
	jobs := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, log, repo)
	queues := app.NewQueueService(repo, repo, repo, clock)
	schedules := app.NewScheduleService(repo, jobs, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: dualBootstrap}, log)
	return jobs, queues, schedules, apiKeys, auth, repo
}

func startDualHTTP(t *testing.T) *sdk.Client {
	t.Helper()
	jobs, queues, schedules, apiKeys, auth, _ := startDualStack(t)
	h := relayhttp.NewRouter(jobs, queues, schedules, apiKeys, relayhttp.Options{
		APIKey: dualBootstrap,
		Auth:   auth,
		Logger: dualDiscardLog(),
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := sdk.NewClient(srv.URL, sdk.WithAPIKey(dualBootstrap))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func startDualGRPC(t *testing.T) *sdk.Client {
	t.Helper()
	jobs, queues, schedules, apiKeys, auth, _ := startDualStack(t)
	lis := bufconn.Listen(dualBufSize)
	srv := adaptergrpc.NewServer(adaptergrpc.Services{
		Jobs: jobs, Queues: queues, Schedules: schedules, APIKeys: apiKeys,
	}, adaptergrpc.Options{
		Auth:   auth,
		Logger: dualDiscardLog(),
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		adaptergrpc.GracefulStop(srv, time.Second)
		_ = lis.Close()
	})
	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	c := sdk.NewClient("passthrough:///bufnet",
		sdk.WithTransport(sdk.TransportGRPC),
		sdk.WithAPIKey(dualBootstrap),
		sdk.WithGRPCDialOptions(grpc.WithContextDialer(dialer)),
	)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDualTransport_JobsQueuesSchedulesAPIKeys(t *testing.T) {
	for _, env := range dualTransports(t) {
		env := env
		t.Run(env.Name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c := env.Client

			job, err := c.Enqueue(ctx, sdk.EnqueueRequest{
				Queue:   "default",
				Type:    "demo.job",
				Payload: map[string]any{"n": 1},
			})
			require.NoError(t, err)
			require.NotEmpty(t, job.ID)
			require.Equal(t, "pending", job.Status)

			got, err := c.GetJob(ctx, job.ID)
			require.NoError(t, err)
			require.Equal(t, job.ID, got.ID)

			queues, err := c.ListQueues(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, queues)

			stats, err := c.QueueStats(ctx, "default")
			require.NoError(t, err)
			require.Equal(t, "default", stats.Queue)
			require.GreaterOrEqual(t, stats.Pending, int64(1))

			sched, err := c.CreateSchedule(ctx, sdk.CreateScheduleRequest{
				Name:  "dual-" + env.Name,
				Cron:  "@hourly",
				Queue: "default",
				Type:  "demo.job",
			})
			require.NoError(t, err)
			require.True(t, sched.Enabled)
			require.Equal(t, "dual-"+env.Name, sched.Name)

			byName, err := c.GetSchedule(ctx, sched.Name)
			require.NoError(t, err)
			require.Equal(t, sched.ID, byName.ID)

			list, next, err := c.ListSchedules(ctx, sdk.ListSchedulesOptions{Limit: 50})
			require.NoError(t, err)
			require.Empty(t, next)
			require.NotEmpty(t, list)

			cron := "0 * * * *"
			updated, err := c.UpdateSchedule(ctx, sched.ID, sdk.UpdateScheduleRequest{
				ExpectedUpdatedAt: sched.UpdatedAt,
				Cron:              &cron,
			})
			require.NoError(t, err)
			require.Equal(t, cron, updated.Cron)

			disabled, err := c.DisableSchedule(ctx, sched.ID)
			require.NoError(t, err)
			require.False(t, disabled.Enabled)

			enabled, err := c.EnableSchedule(ctx, sched.ID)
			require.NoError(t, err)
			require.True(t, enabled.Enabled)

			triggered, err := c.TriggerSchedule(ctx, sched.ID)
			require.NoError(t, err)
			require.NotEmpty(t, triggered.ID)

			created, err := c.CreateAPIKey(ctx, sdk.CreateAPIKeyRequest{
				Name:   "dual-key-" + env.Name,
				Scopes: []string{sdk.ScopeAdmin},
			})
			require.NoError(t, err)
			require.NotEmpty(t, created.Secret)
			require.NotEmpty(t, created.Key.ID)

			keys, err := c.ListAPIKeys(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, keys)

			revoked, err := c.RevokeAPIKey(ctx, created.Key.ID)
			require.NoError(t, err)
			require.NotNil(t, revoked.RevokedAt)

			require.NoError(t, c.DeleteSchedule(ctx, sched.ID))
			_, err = c.GetSchedule(ctx, sched.ID)
			require.Error(t, err)
			require.ErrorIs(t, err, sdk.ErrNotFound)
		})
	}
}

func TestDualTransport_WorkerLeaseComplete(t *testing.T) {
	for _, env := range dualTransports(t) {
		env := env
		t.Run(env.Name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c := env.Client

			_, err := c.Enqueue(ctx, sdk.EnqueueRequest{Queue: "default", Type: "echo", Payload: map[string]any{"x": 1}})
			require.NoError(t, err)

			w, err := sdk.NewWorker(c, sdk.WorkerOptions{
				Queue:           "default",
				WorkerID:        "dual-w-" + env.Name,
				Concurrency:     1,
				LeaseDuration:   5 * time.Second,
				LeaseWait:       100 * time.Millisecond,
				PollMinInterval: 10 * time.Millisecond,
				PollMaxInterval: 50 * time.Millisecond,
				ShutdownTimeout: 2 * time.Second,
				Logger:          dualDiscardLog(),
			})
			require.NoError(t, err)

			done := make(chan error, 1)
			runCtx, cancel := context.WithCancel(ctx)
			sdk.Handle(w, "echo", func(context.Context, sdk.Job[map[string]any]) error {
				cancel()
				return nil
			})
			go func() { done <- w.Run(runCtx) }()

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				cancel()
				t.Fatal("timeout waiting for worker")
			}
		})
	}
}
