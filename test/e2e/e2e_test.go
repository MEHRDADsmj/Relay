//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

const (
	jobCount     = 2000
	workerCount  = 3
	concurrency  = 20
	failEveryN   = 10 // 10% fail on first attempt
	queueName    = "e2e"
	jobType      = "e2e_work"
	maxTestDur = 60 * time.Second
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

func runE2E(t *testing.T, start func(*testing.T, context.Context) backend) {
	t.Helper()
	ctx := context.Background()
	b := start(t, ctx)
	t.Cleanup(b.cleanup)

	st := startStack(t, b, stackOpts{
		retry:        nil, // default fixed 50ms
		reapInterval: time.Second,
	})
	client := st.Client

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
				LeaseWait:       -1, // client poll; load test does not require long-poll
				PollMinInterval: 20 * time.Millisecond,
				PollMaxInterval: 200 * time.Millisecond,
				ShutdownTimeout: 15 * time.Second,
				Logger:          st.Log,
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
