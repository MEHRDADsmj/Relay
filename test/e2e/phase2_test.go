//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func TestE2E_Phase2_BothBackends(t *testing.T) {
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	t.Run("postgres", func(t *testing.T) {
		runPhase2(t, startPostgresBackend)
	})
	t.Run("redis", func(t *testing.T) {
		runPhase2(t, startRedisBackend)
	})
}

func runPhase2(t *testing.T, start func(*testing.T, context.Context) backend) {
	t.Helper()
	ctx := context.Background()
	b := start(t, ctx)
	t.Cleanup(b.cleanup)

	t.Run("a_reaper_after_abandon", func(t *testing.T) {
		testReaperAfterAbandon(t, ctx, b)
	})
	t.Run("b_cancel_100_running", func(t *testing.T) {
		testCancel100Running(t, ctx, b)
	})
	t.Run("c_permanent_dead_bulk_retry", func(t *testing.T) {
		testPermanentDeadBulkRetry(t, ctx, b)
	})
	t.Run("d_enqueue_to_start_p95", func(t *testing.T) {
		testEnqueueToStartP95(t, ctx, b)
	})
}

func testReaperAfterAbandon(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const (
		queue = "e2e-reap"
		typ   = "reap_work"
	)
	st := startStack(t, b, stackOpts{
		retry:        app.FixedDelayRetryPolicy{Delay: 50 * time.Millisecond},
		reapInterval: 500 * time.Millisecond,
	})

	info, err := st.Client.Enqueue(ctx, sdk.EnqueueRequest{
		Queue:       queue,
		Type:        typ,
		Payload:     map[string]any{"n": 1},
		MaxAttempts: 5,
	})
	require.NoError(t, err)

	// Simulate worker crash: claim without release/complete/heartbeat.
	abandonedID := abandonLease(t, ctx, st.BaseURL, queue, "zombie-worker", 5)
	require.Equal(t, info.ID, abandonedID)

	running, err := st.Client.GetJob(ctx, info.ID)
	require.NoError(t, err)
	require.Equal(t, "running", running.Status)

	var started atomic.Bool
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wkr, err := sdk.NewWorker(st.Client, sdk.WorkerOptions{
		Queue:           queue,
		WorkerID:        "reaper-rescue",
		Concurrency:     2,
		LeaseDuration:   30 * time.Second,
		LeaseWait:       5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
		Logger:          st.Log,
	})
	require.NoError(t, err)
	sdk.Handle(wkr, typ, func(_ context.Context, _ sdk.Job[map[string]any]) error {
		started.Store(true)
		return nil
	})
	go func() { _ = wkr.Run(workerCtx) }()

	require.Eventually(t, func() bool {
		j, err := st.Client.GetJob(ctx, info.ID)
		return err == nil && j.Status == "succeeded"
	}, 20*time.Second, pollInterval, "job should succeed on another worker after reaper")
	require.True(t, started.Load(), "rescue worker should have started the job")
	cancel()
}

func testCancel100Running(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const (
		n     = 100
		queue = "e2e-cancel"
		typ   = "cancel_work"
	)
	st := startStack(t, b, stackOpts{
		reapInterval: time.Second,
	})

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		info, err := st.Client.Enqueue(ctx, sdk.EnqueueRequest{
			Queue:          queue,
			Type:           typ,
			Payload:        map[string]int{"i": i},
			MaxAttempts:    3,
			IdempotencyKey: fmt.Sprintf("cancel-%d", i),
		})
		require.NoError(t, err)
		ids[i] = info.ID
	}

	var runningN atomic.Int64
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wkr, err := sdk.NewWorker(st.Client, sdk.WorkerOptions{
				Queue:           queue,
				WorkerID:        fmt.Sprintf("cancel-w-%d", idx),
				Concurrency:     40,
				LeaseDuration:   5 * time.Second, // heartbeat every ~1.6s
				LeaseWait:       5 * time.Second,
				ShutdownTimeout: 10 * time.Second,
				Logger:          st.Log,
			})
			if err != nil {
				t.Errorf("worker: %v", err)
				return
			}
			sdk.Handle(wkr, typ, func(jobCtx context.Context, _ sdk.Job[map[string]int]) error {
				runningN.Add(1)
				<-jobCtx.Done()
				return jobCtx.Err()
			})
			_ = wkr.Run(workerCtx)
		}(w)
	}

	require.Eventually(t, func() bool {
		return runningN.Load() >= int64(n)
	}, 30*time.Second, pollInterval, "expected all jobs to enter handlers")

	for _, id := range ids {
		_, err := st.Client.CancelJob(ctx, id)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		for _, id := range ids {
			info, err := st.Client.GetJob(ctx, id)
			if err != nil || info.Status != "cancelled" {
				return false
			}
		}
		return true
	}, 30*time.Second, pollInterval, "all jobs should be cancelled")

	cancelWorkers()
	wg.Wait()
}

func testPermanentDeadBulkRetry(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const (
		n     = 50
		queue = "e2e-perm"
		typ   = "perm_work"
	)
	st := startStack(t, b, stackOpts{
		reapInterval: time.Second,
	})

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		info, err := st.Client.Enqueue(ctx, sdk.EnqueueRequest{
			Queue:          queue,
			Type:           typ,
			Payload:        map[string]int{"i": i},
			MaxAttempts:    5,
			IdempotencyKey: fmt.Sprintf("perm-%d", i),
		})
		require.NoError(t, err)
		ids[i] = info.ID
	}

	var mode atomic.Int32 // 0 = permanent fail, 1 = succeed
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		wkr, err := sdk.NewWorker(st.Client, sdk.WorkerOptions{
			Queue:           queue,
			WorkerID:        "perm-worker",
			Concurrency:     20,
			LeaseDuration:   30 * time.Second,
			LeaseWait:       5 * time.Second,
			ShutdownTimeout: 10 * time.Second,
			Logger:          st.Log,
		})
		require.NoError(t, err)
		sdk.Handle(wkr, typ, func(_ context.Context, _ sdk.Job[map[string]int]) error {
			if mode.Load() == 0 {
				return sdk.Permanent(errors.New("poison"))
			}
			return nil
		})
		_ = wkr.Run(workerCtx)
	}()

	require.Eventually(t, func() bool {
		for _, id := range ids {
			info, err := st.Client.GetJob(ctx, id)
			if err != nil || info.Status != "dead" || info.Attempts != 1 {
				return false
			}
		}
		return true
	}, 30*time.Second, pollInterval, "all jobs should be dead after one permanent fail")

	mode.Store(1)
	retried, err := st.Client.RetryDead(ctx, queue, n)
	require.NoError(t, err)
	require.Equal(t, n, retried)

	require.Eventually(t, func() bool {
		for _, id := range ids {
			info, err := st.Client.GetJob(ctx, id)
			if err != nil || info.Status != "succeeded" {
				return false
			}
		}
		return true
	}, 30*time.Second, pollInterval, "bulk-retried jobs should succeed")

	cancelWorkers()
	wg.Wait()
}

func testEnqueueToStartP95(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const (
		n     = 100
		queue = "e2e-latency"
		typ   = "lat_work"
	)
	st := startStack(t, b, stackOpts{
		reapInterval: time.Second,
	})

	var (
		startMu  sync.Mutex
		startAt  = make(map[string]time.Time, n)
		doneN    atomic.Int64
	)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wkr, err := sdk.NewWorker(st.Client, sdk.WorkerOptions{
				Queue:           queue,
				WorkerID:        fmt.Sprintf("lat-w-%d", idx),
				Concurrency:     50,
				LeaseDuration:   30 * time.Second,
				LeaseWait:       20 * time.Second,
				ShutdownTimeout: 5 * time.Second,
				Logger:          st.Log,
			})
			if err != nil {
				t.Errorf("worker: %v", err)
				return
			}
			sdk.Handle(wkr, typ, func(_ context.Context, job sdk.Job[map[string]int]) error {
				now := time.Now()
				startMu.Lock()
				startAt[job.ID] = now
				startMu.Unlock()
				doneN.Add(1)
				return nil
			})
			_ = wkr.Run(workerCtx)
		}(w)
	}

	// Let workers enter long-poll before the first enqueue.
	time.Sleep(500 * time.Millisecond)

	enqAt := make([]time.Time, n)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		info, err := st.Client.Enqueue(ctx, sdk.EnqueueRequest{
			Queue:          queue,
			Type:           typ,
			Payload:        map[string]int{"i": i},
			MaxAttempts:    3,
			IdempotencyKey: fmt.Sprintf("lat-%d", i),
		})
		require.NoError(t, err)
		enqAt[i] = t0
		ids[i] = info.ID
	}

	require.Eventually(t, func() bool {
		return doneN.Load() >= int64(n)
	}, 30*time.Second, pollInterval, "all latency jobs should start")

	latencies := make([]time.Duration, n)
	startMu.Lock()
	for i, id := range ids {
		stAt, ok := startAt[id]
		require.True(t, ok, "missing start for %s", id)
		latencies[i] = stAt.Sub(enqAt[i])
	}
	startMu.Unlock()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := percentileDuration(latencies, 0.95)
	t.Logf("enqueue-to-start p50=%s p95=%s max=%s",
		percentileDuration(latencies, 0.50).Round(time.Millisecond),
		p95.Round(time.Millisecond),
		latencies[len(latencies)-1].Round(time.Millisecond))
	require.Less(t, p95, 200*time.Millisecond, "p95 enqueue-to-start must be < 200ms (got %s)", p95)

	cancelWorkers()
	wg.Wait()
}
