//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

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
	retry         app.RetryPolicy
	reapInterval  time.Duration // default 5s
	purgeInterval time.Duration // default 1m
	retention     time.Duration // default 0 = never purge (reapInterval always set)
}

type stack struct {
	BaseURL string
	Client  *sdk.Client
	Log     *slog.Logger
	cancel  context.CancelFunc
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
	handler := adapterhttp.NewRouter(svc, queues, adapterhttp.Options{
		APIKey: apiKey,
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

	runCtx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	go func() {
		_ = maint.Run(runCtx)
	}()

	t.Cleanup(func() {
		cancel()
		shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shCancel()
		_ = srv.Shutdown(shCtx)
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("http serve: %v", err)
		}
	})

	baseURL := "http://" + ln.Addr().String()
	return &stack{
		BaseURL: baseURL,
		Client:  sdk.NewClient(baseURL, sdk.WithAPIKey(apiKey)),
		Log:     log,
		cancel:  cancel,
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
