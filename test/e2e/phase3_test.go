//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func TestE2E_Phase3_BothBackends(t *testing.T) {
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	t.Run("postgres", func(t *testing.T) {
		runPhase3(t, startPostgresBackend)
	})
	t.Run("redis", func(t *testing.T) {
		runPhase3(t, startRedisBackend)
	})
}

func runPhase3(t *testing.T, start func(*testing.T, context.Context) backend) {
	t.Helper()
	ctx := context.Background()
	b := start(t, ctx)
	t.Cleanup(b.cleanup)

	t.Run("a_dual_scheduler_no_dupes_leader_kill", func(t *testing.T) {
		testDualSchedulerNoDupes(t, ctx, b)
	})
	t.Run("b_grpc_worker_1000_jobs", func(t *testing.T) {
		testGRPCWorker1000(t, ctx, b)
	})
	t.Run("c_producer_cannot_lease", func(t *testing.T) {
		testProducerCannotLease(t, ctx, b)
	})
}

func testDualSchedulerNoDupes(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const (
		queue = "e2e-sched"
		typ   = "sched_tick"
		runFor = 30 * time.Second
	)

	st1 := startStack(t, b, stackOpts{
		scheduler:     true,
		instanceID:    "e2e-sched-1",
		schedulerTick: 200 * time.Millisecond,
	})
	st2 := startStack(t, b, stackOpts{
		scheduler:     true,
		instanceID:    "e2e-sched-2",
		schedulerTick: 200 * time.Millisecond,
		disableMaint:  true,
	})

	sched, err := st1.Client.CreateSchedule(ctx, sdk.CreateScheduleRequest{
		Name:        fmt.Sprintf("dual-%d", time.Now().UnixNano()),
		Cron:        "@every 2s",
		Timezone:    "UTC",
		Queue:       queue,
		Type:        typ,
		Payload:     map[string]any{"src": "e2e"},
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	startAt := time.Now()
	// Let both instances run; kill one mid-window (leader resigns on Stop).
	time.Sleep(12 * time.Second)
	st1.Stop()
	remaining := runFor - time.Since(startAt)
	if remaining > 0 {
		time.Sleep(remaining)
	}

	jobs := collectAllJobs(t, ctx, st2.Client, queue)
	st2.Stop()

	firePrefix := "schedule:" + sched.ID + ":"
	fireKeys := make(map[string]struct{})
	var fireTimes []time.Time
	for _, j := range jobs {
		if !strings.HasPrefix(j.IdempotencyKey, firePrefix) {
			continue
		}
		if _, dup := fireKeys[j.IdempotencyKey]; dup {
			t.Fatalf("duplicate fire key %q", j.IdempotencyKey)
		}
		fireKeys[j.IdempotencyKey] = struct{}{}
		fireTimes = append(fireTimes, j.CreatedAt)
	}

	n := len(fireKeys)
	// @every 2s for 30s → ~15 fires; allow ±1 plus at most one missed during failover.
	require.GreaterOrEqual(t, n, 14, "jobs=%d keys=%v", n, fireKeys)
	require.LessOrEqual(t, n, 16, "jobs=%d", n)

	if len(fireTimes) >= 2 {
		sortTimes(fireTimes)
		maxGap := time.Duration(0)
		for i := 1; i < len(fireTimes); i++ {
			gap := fireTimes[i].Sub(fireTimes[i-1])
			if gap > maxGap {
				maxGap = gap
			}
		}
		// One missed fire (@every 2s) ⇒ gap ≤ ~4s; allow small scheduler/jitter slack.
		require.LessOrEqual(t, maxGap, 5*time.Second, "max inter-fire gap %s exceeds 1-fire allowance", maxGap)
	}
}

func testGRPCWorker1000(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const (
		queue   = "e2e-grpc"
		typ     = "grpc_work"
		jobN    = 1000
		maxWait = 120 * time.Second
	)
	st := startStack(t, b, stackOpts{
		reapInterval: 2 * time.Second,
	})
	client := st.GRPCClient

	var (
		enqMu  sync.Mutex
		enqErr error
	)
	sem := make(chan struct{}, 32)
	doneEnq := make(chan struct{})
	go func() {
		defer close(doneEnq)
		var wg sync.WaitGroup
		for i := 0; i < jobN; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				_, err := client.Enqueue(ctx, sdk.EnqueueRequest{
					Queue:          queue,
					Type:           typ,
					Payload:        map[string]any{"i": i},
					IdempotencyKey: fmt.Sprintf("grpc-%d", i),
				})
				if err != nil {
					enqMu.Lock()
					if enqErr == nil {
						enqErr = err
					}
					enqMu.Unlock()
				}
			}(i)
		}
		wg.Wait()
	}()

	wkr, err := sdk.NewWorker(client, sdk.WorkerOptions{
		Queue:           queue,
		WorkerID:        "grpc-e2e",
		Concurrency:     32,
		LeaseDuration:   30 * time.Second,
		LeaseWait:       5 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		Logger:          st.Log,
	})
	require.NoError(t, err)
	sdk.Handle(wkr, typ, func(context.Context, sdk.Job[map[string]any]) error {
		return nil
	})

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = wkr.Run(wctx) }()

	<-doneEnq
	require.NoError(t, enqErr)

	var succeeded int64
	require.Eventually(t, func() bool {
		stats, err := client.QueueStats(ctx, queue)
		if err != nil {
			return false
		}
		succeeded = stats.Succeeded
		return stats.Succeeded >= int64(jobN) && stats.Pending == 0 && stats.Running == 0
	}, maxWait, 250*time.Millisecond, "succeeded=%d want=%d", succeeded, jobN)
	cancel()
}

func testProducerCannotLease(t *testing.T, ctx context.Context, b backend) {
	t.Helper()
	const queue = "e2e-scope"
	st := startStack(t, b, stackOpts{})

	created, err := st.Client.CreateAPIKey(ctx, sdk.CreateAPIKeyRequest{
		Name:   "producer-only",
		Scopes: []string{sdk.ScopeProducer},
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.Secret)

	_, err = st.Client.Enqueue(ctx, sdk.EnqueueRequest{
		Queue:   queue,
		Type:    "scope_probe",
		Payload: map[string]any{"ok": true},
	})
	require.NoError(t, err)

	body, err := json.Marshal(map[string]any{
		"worker_id":     "producer-worker",
		"max_jobs":      1,
		"lease_seconds": 30,
	})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, st.BaseURL+"/v1/queues/"+queue+"/leases", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+created.Secret)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, res.StatusCode, "body=%s", data)
}

func collectAllJobs(t *testing.T, ctx context.Context, c *sdk.Client, queue string) []sdk.JobInfo {
	t.Helper()
	statuses := []string{"pending", "running", "succeeded", "dead", "cancelled"}
	var out []sdk.JobInfo
	for _, st := range statuses {
		for j, err := range c.ListJobs(ctx, queue, st, sdk.ListJobsOptions{Limit: 100}) {
			require.NoError(t, err)
			out = append(out, j)
		}
	}
	return out
}

func sortTimes(ts []time.Time) {
	for i := 1; i < len(ts); i++ {
		for j := i; j > 0 && ts[j].Before(ts[j-1]); j-- {
			ts[j], ts[j-1] = ts[j-1], ts[j]
		}
	}
}
