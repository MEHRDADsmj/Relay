//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
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
	jobCount       = 2000
	workerCount    = 3
	concurrency    = 20
	failEveryN     = 10 // 10% fail on first attempt
	queueName      = "e2e"
	jobType        = "e2e_work"
	maxTestDur     = 60 * time.Second
	retryDelay     = 50 * time.Millisecond
	apiKey         = "e2e-test-key"
	pollInterval   = 50 * time.Millisecond
)

type jobPayload struct {
	Index     int  `json:"index"`
	FailFirst bool `json:"fail_first"`
}

func TestE2E_BothBackends(t *testing.T) {
	// Ryuk is flaky on Docker Desktop for Windows.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	t.Run("postgres", func(t *testing.T) {
		runE2E(t, startPostgresBackend)
	})
	t.Run("redis", func(t *testing.T) {
		runE2E(t, startRedisBackend)
	})
}

type backend struct {
	repo    app.JobRepository
	cleanup func()
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

func runE2E(t *testing.T, start func(*testing.T, context.Context) backend) {
	t.Helper()
	ctx := context.Background()
	b := start(t, ctx)
	t.Cleanup(b.cleanup)

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc := app.NewJobService(
		b.repo,
		app.SystemClock{},
		app.UUIDv7Generator{},
		app.FixedDelayRetryPolicy{Delay: retryDelay},
		log,
		nil,
	)
	handler := adapterhttp.NewRouter(svc, adapterhttp.Options{
		APIKey: apiKey,
		Logger: log,
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	t.Cleanup(func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("http serve: %v", err)
		}
	})

	baseURL := "http://" + ln.Addr().String()
	client := sdk.NewClient(baseURL, sdk.WithAPIKey(apiKey))

	deadline := time.Now().Add(maxTestDur)
	startAt := time.Now()

	var (
		execMu     sync.Mutex
		execCount  = make(map[string]int, jobCount)
		failIDs    = make(map[string]struct{}, jobCount/failEveryN)
		jobIDs     = make([]string, jobCount)
		failMu     sync.Mutex
		succeededN atomic.Int64
	)

	// Enqueue 2000 jobs in parallel; every 10th fails on first attempt.
	var (
		enqWG  sync.WaitGroup
		enqErr atomic.Value
	)
	enqSem := make(chan struct{}, 32)
	for i := 0; i < jobCount; i++ {
		enqWG.Add(1)
		go func(i int) {
			defer enqWG.Done()
			enqSem <- struct{}{}
			defer func() { <-enqSem }()

			failFirst := i%failEveryN == 0
			info, err := client.Enqueue(ctx, sdk.EnqueueRequest{
				Queue: queueName,
				Type:  jobType,
				Payload: jobPayload{
					Index:     i,
					FailFirst: failFirst,
				},
				MaxAttempts:    5,
				IdempotencyKey: fmt.Sprintf("e2e-%d", i),
			})
			if err != nil {
				enqErr.Store(err)
				return
			}
			jobIDs[i] = info.ID
			if failFirst {
				failMu.Lock()
				failIDs[info.ID] = struct{}{}
				failMu.Unlock()
			}
		}(i)
	}
	enqWG.Wait()
	if v := enqErr.Load(); v != nil {
		require.NoError(t, v.(error))
	}
	require.Len(t, failIDs, jobCount/failEveryN)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	var workerErr atomic.Value
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			wkr, err := sdk.NewWorker(client, sdk.WorkerOptions{
				Queue:           queueName,
				WorkerID:        fmt.Sprintf("e2e-worker-%d", workerIdx),
				Concurrency:     concurrency,
				LeaseDuration:   30 * time.Second,
				PollMinInterval: 20 * time.Millisecond,
				PollMaxInterval: 200 * time.Millisecond,
				ShutdownTimeout: 15 * time.Second,
				Logger:          log,
			})
			if err != nil {
				workerErr.Store(err)
				return
			}
			sdk.Handle(wkr, jobType, func(_ context.Context, job sdk.Job[jobPayload]) error {
				execMu.Lock()
				execCount[job.ID]++
				execMu.Unlock()
				if job.Args.FailFirst && job.Attempt == 1 {
					return errors.New("intentional first-attempt failure")
				}
				succeededN.Add(1)
				return nil
			})
			if err := wkr.Run(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
				workerErr.Store(err)
			}
		}(w)
	}

	// Wait for handlers to succeed, then confirm API status (complete may lag the handler return).
	require.Eventually(t, func() bool {
		return succeededN.Load() >= int64(jobCount)
	}, time.Until(deadline), pollInterval, "handlers did not succeed all jobs before deadline")

	require.Eventually(t, func() bool {
		ok := 0
		for _, id := range jobIDs {
			info, err := client.GetJob(ctx, id)
			if err != nil || info.Status != "succeeded" {
				return false
			}
			ok++
		}
		return ok == jobCount
	}, time.Until(deadline), pollInterval, "API status not all succeeded before deadline")

	elapsed := time.Since(startAt)
	t.Logf("backend finished in %s (%d jobs, %d flaky)", elapsed.Round(time.Millisecond), jobCount, len(failIDs))
	require.Less(t, elapsed, maxTestDur, "e2e must finish under %s (took %s)", maxTestDur, elapsed)

	cancelWorkers()
	wg.Wait()
	if v := workerErr.Load(); v != nil {
		require.NoError(t, v.(error))
	}

	execMu.Lock()
	defer execMu.Unlock()
	for _, id := range jobIDs {
		info, err := client.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "succeeded", info.Status, "job %s", id)
		require.GreaterOrEqual(t, execCount[id], 1, "job %s execution count", id)

		_, flaky := failIDs[id]
		if flaky {
			require.Equal(t, 2, info.Attempts, "flaky job %s attempts", id)
			require.Equal(t, 2, execCount[id], "flaky job %s exec count", id)
		} else {
			require.Equal(t, 1, info.Attempts, "stable job %s attempts", id)
			require.Equal(t, 1, execCount[id], "stable job %s exec count", id)
		}
	}
}
