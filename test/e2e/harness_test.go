//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	adaptergrpc "github.com/MEHRDADsmj/relay/internal/adapter/grpc"
	adapterhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres"
	redisstore "github.com/MEHRDADsmj/relay/internal/adapter/storage/redis"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

const (
	apiKey       = "e2e-test-key"
	pollInterval = 50 * time.Millisecond
)

type backend struct {
	repo    app.JobRepository
	cleanup func()
}

type stackOpts struct {
	retry           app.RetryPolicy
	reapInterval    time.Duration // default 5s
	purgeInterval   time.Duration // default 1m
	retention       time.Duration // default 0 = never purge (reapInterval always set)
	scheduler       bool          // run cluster scheduler + leader election
	schedulerTick   time.Duration // default 200ms in e2e when scheduler on
	instanceID      string
	disableMaint    bool // skip maintenance loop (useful for dual-instance tests)
}

type stack struct {
	BaseURL    string
	GRPCTarget string // grpc://host:port
	Client     *sdk.Client
	GRPCClient *sdk.Client
	Log        *slog.Logger
	Stop       func() // resign scheduler, drain HTTP/gRPC, stop maintenance
}

func startPostgresBackend(t *testing.T, ctx context.Context) backend {
	t.Helper()
	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("relay"),
		tcpostgres.WithUsername("relay"),
		tcpostgres.WithPassword("relay"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(termCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, postgres.Migrate(ctx, dsn))

	repo, err := postgres.Open(ctx, dsn)
	require.NoError(t, err)
	return backend{
		repo: repo,
		cleanup: func() {
			repo.Close()
		},
	}
}

func startRedisBackend(t *testing.T, ctx context.Context) backend {
	t.Helper()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(termCtx)
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	repo, err := redisstore.Open(ctx, uri, "relay")
	require.NoError(t, err)
	return backend{
		repo: repo,
		cleanup: func() {
			_ = repo.Close()
		},
	}
}

func startStack(t *testing.T, b backend, opts stackOpts) *stack {
	t.Helper()
	if opts.retry == nil {
		opts.retry = app.FixedDelayRetryPolicy{Delay: 50 * time.Millisecond}
	}
	if opts.reapInterval <= 0 {
		opts.reapInterval = 5 * time.Second
	}
	if opts.purgeInterval <= 0 {
		opts.purgeInterval = time.Minute
	}
	if opts.schedulerTick <= 0 {
		opts.schedulerTick = 200 * time.Millisecond
	}
	if opts.instanceID == "" {
		opts.instanceID = fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	}
	// Retention defaults to 0 with an explicit ReapInterval → never purge in e2e.

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	clock := app.SystemClock{}

	notifier, ok := b.repo.(app.JobNotifier)
	require.True(t, ok, "backend must implement JobNotifier")
	maintRepo, ok := b.repo.(app.MaintenanceRepository)
	require.True(t, ok, "backend must implement MaintenanceRepository")
	queueRepo, ok := b.repo.(app.QueueRepository)
	require.True(t, ok, "backend must implement QueueRepository")

	svc := app.NewJobService(b.repo, clock, app.UUIDv7Generator{}, opts.retry, log, notifier)
	queues := app.NewQueueService(queueRepo, b.repo, clock)
	scheduleRepo, ok := b.repo.(app.ScheduleRepository)
	require.True(t, ok, "backend must implement ScheduleRepository")
	apiKeyRepo, ok := b.repo.(app.APIKeyRepository)
	require.True(t, ok, "backend must implement APIKeyRepository")
	auth := app.NewAuthService(apiKeyRepo, clock, app.AuthServiceConfig{BootstrapKey: apiKey}, log)
	cronParser := adaptercron.NewParser()
	schedules := app.NewScheduleService(scheduleRepo, svc, cronParser, clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(apiKeyRepo, clock)
	handler := adapterhttp.NewRouter(svc, queues, schedules, apiKeys, adapterhttp.Options{
		APIKey: apiKey,
		Auth:   auth,
		Logger: log,
	})

	maint := app.NewMaintenance(maintRepo, clock, app.MaintenanceConfig{
		ReapInterval:  opts.reapInterval,
		ReapBatch:     500,
		PurgeInterval: opts.purgeInterval,
		PurgeBatch:    1000,
		Retention:     opts.retention,
	}, log)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      45 * time.Second,
	}

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcSrv := adaptergrpc.NewServer(adaptergrpc.Services{
		Jobs:      svc,
		Queues:    queues,
		Schedules: schedules,
		APIKeys:   apiKeys,
	}, adaptergrpc.Options{
		Auth:   auth,
		Logger: log,
		Ready:  b.repo.Ping,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	schedCtx, schedCancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	grpcErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	go func() {
		grpcErr <- grpcSrv.Serve(grpcLis)
	}()
	if !opts.disableMaint {
		go func() {
			_ = maint.Run(runCtx)
		}()
	}
	if opts.scheduler {
		elector := newE2ELeaderElector(t, b, opts.instanceID)
		scheduler := app.NewScheduler(scheduleRepo, svc, elector, cronParser, clock, app.SchedulerConfig{
			Tick: opts.schedulerTick,
		}, log)
		go func() {
			_ = scheduler.Run(schedCtx)
		}()
	}

	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			schedCancel()
			cancel()
			shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shCancel()
			_ = srv.Shutdown(shCtx)
			adaptergrpc.GracefulStop(grpcSrv, 5*time.Second)
			_ = grpcLis.Close()
			if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("http serve: %v", err)
			}
			<-grpcErr
		})
	}
	t.Cleanup(stop)

	baseURL := "http://" + ln.Addr().String()
	grpcTarget := "grpc://" + grpcLis.Addr().String()
	httpClient := sdk.NewClient(baseURL, sdk.WithAPIKey(apiKey))
	grpcClient := sdk.NewClient(grpcTarget, sdk.WithAPIKey(apiKey))
	t.Cleanup(func() {
		_ = httpClient.Close()
		_ = grpcClient.Close()
	})
	return &stack{
		BaseURL:    baseURL,
		GRPCTarget: grpcTarget,
		Client:     httpClient,
		GRPCClient: grpcClient,
		Log:        log,
		Stop:       stop,
	}
}

func newE2ELeaderElector(t *testing.T, b backend, instanceID string) app.LeaderElector {
	t.Helper()
	switch r := b.repo.(type) {
	case *postgres.Repository:
		return postgres.NewLeaderElector(r.Pool())
	case *redisstore.Repository:
		return redisstore.NewLeaderElector(r.Client(), r.KeyPrefix()).WithInstanceID(instanceID)
	default:
		t.Fatalf("no leader elector for %T", b.repo)
		return nil
	}
}

// abandonLease claims one job as workerID and returns without release/complete (crash simulation).
func abandonLease(t *testing.T, ctx context.Context, baseURL, queue, workerID string, leaseSeconds int) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"worker_id":     workerID,
		"max_jobs":      1,
		"lease_seconds": leaseSeconds,
	})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/queues/"+queue+"/leases", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, "lease: %s", data)
	var out struct {
		Jobs []struct {
			ID string `json:"id"`
		} `json:"jobs"`
	}
	require.NoError(t, json.Unmarshal(data, &out))
	require.Len(t, out.Jobs, 1)
	return out.Jobs[0].ID
}

func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}
