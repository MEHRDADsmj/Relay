package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func TestWorker_LeaseWaitSendsWaitSeconds(t *testing.T) {
	t.Parallel()
	var gotWait atomic.Int32
	api := &fakeAPI{}
	api.leaseFn = func(_ string, body map[string]any) (int, []map[string]any) {
		if v, ok := body["wait_seconds"].(float64); ok {
			gotWait.Store(int32(v))
		} else {
			gotWait.Store(-1)
		}
		return http.StatusOK, nil
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		LeaseWait:       7 * time.Second,
		PollMinInterval: time.Hour, // must not be used for empty leases
		PollMaxInterval: time.Hour,
		ShutdownTimeout: time.Second,
	})
	require.NoError(t, err)
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for gotWait.Load() == 0 && api.leaseCalls.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for lease")
		case <-time.After(10 * time.Millisecond):
		}
	}
	require.Equal(t, int32(7), gotWait.Load())
	require.GreaterOrEqual(t, api.leaseCalls.Load(), int32(2), "empty long-poll should re-lease without client backoff")
	cancel()
	require.NoError(t, <-done)
}

func TestWorker_LeaseWaitDisabledUsesClientBackoff(t *testing.T) {
	t.Parallel()
	var leaseTimes []time.Time
	var mu sync.Mutex
	api := &fakeAPI{}
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		mu.Lock()
		leaseTimes = append(leaseTimes, time.Now())
		mu.Unlock()
		return http.StatusOK, nil
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	minPoll := 40 * time.Millisecond
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		LeaseWait:       -1, // disable long-poll → client-side backoff
		PollMinInterval: minPoll,
		PollMaxInterval: 200 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	require.NoError(t, err)
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(leaseTimes)
		times := append([]time.Time(nil), leaseTimes...)
		mu.Unlock()
		if n >= 3 {
			gap := times[2].Sub(times[1])
			require.GreaterOrEqual(t, gap, minPoll/2)
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected spaced empty leases, got %d", n)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	require.NoError(t, <-done)
}

func TestWorker_CancelRequestedCancelsHandlerAndReports(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	jobID := "aaaaaaaa-bbbb-cccc-dddd-000000000021"
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire(jobID, "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		j := sampleJob(jobID, nil)
		j["status"] = "running"
		j["cancel_requested"] = true
		return http.StatusOK, j
	}
	completed := make(chan struct{}, 1)
	api.completeFn = func(string, map[string]any) int {
		completed <- struct{}{}
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   60 * time.Millisecond,
		LeaseWait:       -1,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	cancelled := make(chan struct{})
	sdk.Handle(w, "demo", func(ctx context.Context, _ sdk.Job[demoArgs]) error {
		<-ctx.Done()
		close(cancelled)
		return nil // still reported; server resolves to cancelled
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled on cancel_requested")
	}
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected complete after cancel_requested (handler result reported)")
	}
	cancel()
}

func TestWorker_PermanentFailSendsFlag(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000022", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000022", nil)
	}
	got := make(chan map[string]any, 1)
	api.failFn = func(_ string, body map[string]any) int {
		got <- body
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		LeaseWait:       -1,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error {
		return sdk.Permanent(errors.New("poison message"))
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case body := <-got:
		require.Equal(t, "poison message", body["error"])
		require.Equal(t, true, body["permanent"])
	case <-time.After(2 * time.Second):
		t.Fatal("expected permanent fail")
	}
	cancel()
}

func TestWorker_ShutdownTimeoutReleasesInFlight(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	jobID := "aaaaaaaa-bbbb-cccc-dddd-000000000023"
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire(jobID, "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob(jobID, nil)
	}
	released := make(chan string, 1)
	api.releaseFn = func(id string, body map[string]any) int {
		released <- id
		require.Equal(t, "w1", body["worker_id"])
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		LeaseWait:       -1,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 40 * time.Millisecond,
	})
	require.NoError(t, err)

	started := make(chan struct{})
	sdk.Handle(w, "demo", func(ctx context.Context, _ sdk.Job[demoArgs]) error {
		close(started)
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond) // exceed shutdown timeout
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	<-started
	cancel()
	err = <-done
	require.True(t, errors.Is(err, sdk.ErrShutdownTimeout), "got %v", err)

	select {
	case id := <-released:
		require.Equal(t, jobID, id)
	case <-time.After(2 * time.Second):
		t.Fatal("expected Release on shutdown timeout")
	}
}

func TestClient_Phase2JobAndQueueMethods(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	api.cancelFn = func(id string) (int, map[string]any) {
		j := sampleJob(id, nil)
		j["status"] = "cancelled"
		return http.StatusOK, j
	}
	api.retryFn = func(id string) (int, map[string]any) {
		j := sampleJob(id, nil)
		j["status"] = "pending"
		j["attempts"] = 0
		return http.StatusOK, j
	}
	api.attemptsFn = func(id string) (int, []map[string]any) {
		return http.StatusOK, []map[string]any{{
			"job_id":     id,
			"number":     1,
			"worker_id":  "w1",
			"started_at": time.Now().UTC(),
			"outcome":    "succeeded",
		}}
	}
	api.listQueuesFn = func() (int, []map[string]any) {
		return http.StatusOK, []map[string]any{{
			"name":       "default",
			"paused":     false,
			"created_at": time.Now().UTC(),
			"updated_at": time.Now().UTC(),
		}}
	}
	api.queueStatsFn = func(q string) (int, map[string]any) {
		return http.StatusOK, map[string]any{
			"queue":                      q,
			"paused":                     false,
			"pending":                    int64(1),
			"scheduled":                  int64(0),
			"running":                    int64(2),
			"succeeded":                  int64(3),
			"dead":                       int64(4),
			"cancelled":                  int64(5),
			"oldest_pending_age_seconds": int64(42),
		}
	}
	api.pauseFn = func(q string) (int, map[string]any) {
		return http.StatusOK, map[string]any{
			"name": q, "paused": true,
			"created_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
		}
	}
	api.resumeFn = func(q string) (int, map[string]any) {
		return http.StatusOK, map[string]any{
			"name": q, "paused": false,
			"created_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
		}
	}
	api.retryDeadFn = func(q string, body map[string]any) (int, int) {
		require.Equal(t, "default", q)
		require.EqualValues(t, 50, body["limit"])
		return http.StatusOK, 7
	}

	pages := []struct {
		jobs   []map[string]any
		cursor string
	}{
		{
			jobs: []map[string]any{
				jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000031", "default", "demo", map[string]any{"n": 1}, 1),
				jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000032", "default", "demo", map[string]any{"n": 2}, 1),
			},
			cursor: "cursor-page-2",
		},
		{
			jobs: []map[string]any{
				jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000033", "default", "demo", map[string]any{"n": 3}, 1),
			},
			cursor: "",
		},
	}
	var pageIdx atomic.Int32
	api.listJobsFn = func(queue, status, cursor string, limit int) (int, []map[string]any, string) {
		require.Equal(t, "default", queue)
		require.Equal(t, "dead", status)
		require.Equal(t, 2, limit)
		i := int(pageIdx.Add(1) - 1)
		require.Less(t, i, len(pages))
		if i == 0 {
			require.Equal(t, "", cursor)
		} else {
			require.Equal(t, "cursor-page-2", cursor)
		}
		p := pages[i]
		return http.StatusOK, p.jobs, p.cursor
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	ctx := context.Background()

	cancelled, err := c.CancelJob(ctx, "aaaaaaaa-bbbb-cccc-dddd-000000000030")
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelled.Status)

	retried, err := c.RetryJob(ctx, "aaaaaaaa-bbbb-cccc-dddd-000000000030")
	require.NoError(t, err)
	require.Equal(t, "pending", retried.Status)
	require.Equal(t, 0, retried.Attempts)

	attempts, err := c.ListAttempts(ctx, "aaaaaaaa-bbbb-cccc-dddd-000000000030")
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	require.Equal(t, "succeeded", attempts[0].Outcome)

	queues, err := c.ListQueues(ctx)
	require.NoError(t, err)
	require.Len(t, queues, 1)
	require.Equal(t, "default", queues[0].Name)

	stats, err := c.QueueStats(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Pending)
	require.Equal(t, 42*time.Second, stats.OldestPendingAge)

	q, err := c.PauseQueue(ctx, "default")
	require.NoError(t, err)
	require.True(t, q.Paused)
	q, err = c.ResumeQueue(ctx, "default")
	require.NoError(t, err)
	require.False(t, q.Paused)

	n, err := c.RetryDead(ctx, "default", 50)
	require.NoError(t, err)
	require.Equal(t, 7, n)

	var ids []string
	for job, iterErr := range c.ListJobs(ctx, "default", "dead", sdk.ListJobsOptions{Limit: 2}) {
		require.NoError(t, iterErr)
		ids = append(ids, job.ID)
	}
	require.Equal(t, []string{
		"aaaaaaaa-bbbb-cccc-dddd-000000000031",
		"aaaaaaaa-bbbb-cccc-dddd-000000000032",
		"aaaaaaaa-bbbb-cccc-dddd-000000000033",
	}, ids)
}

func TestClient_InvalidTransition(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	api.cancelFn = func(string) (int, map[string]any) {
		return http.StatusConflict, nil
	}
	api.conflictType = "https://relay.dev/problems/invalid-transition"
	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	_, err := c.CancelJob(context.Background(), "aaaaaaaa-bbbb-cccc-dddd-000000000040")
	require.Error(t, err)
	require.True(t, errors.Is(err, sdk.ErrInvalidTransition))
	require.False(t, errors.Is(err, sdk.ErrLeaseLost))
}

func TestPermanent_Unwrap(t *testing.T) {
	t.Parallel()
	base := errors.New("nope")
	err := sdk.Permanent(base)
	require.True(t, errors.Is(err, base))
	require.Equal(t, "nope", err.Error())
	require.NotEqual(t, base, err)
}

func TestWorker_DefaultLeaseWaitIs20s(t *testing.T) {
	t.Parallel()
	var gotWait atomic.Int32
	gotWait.Store(-1)
	api := &fakeAPI{}
	api.leaseFn = func(_ string, body map[string]any) (int, []map[string]any) {
		if v, ok := body["wait_seconds"].(float64); ok {
			gotWait.Store(int32(v))
		}
		return http.StatusOK, nil
	}
	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 5 * time.Millisecond,
		ShutdownTimeout: time.Second,
		// LeaseWait zero value should become 20s default (not "disabled")
	})
	require.NoError(t, err)
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for gotWait.Load() < 0 {
		select {
		case <-deadline:
			t.Fatal("timeout")
		case <-time.After(5 * time.Millisecond):
		}
	}
	require.Equal(t, int32(20), gotWait.Load())
	cancel()
	require.NoError(t, <-done)
}

func TestListJobs_StopsOnError(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	api.listJobsFn = func(string, string, string, int) (int, []map[string]any, string) {
		return http.StatusInternalServerError, nil, ""
	}
	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	var sawErr error
	for _, err := range c.ListJobs(context.Background(), "default", "dead", sdk.ListJobsOptions{}) {
		if err != nil {
			sawErr = err
			break
		}
	}
	require.Error(t, sawErr)
	_ = fmt.Sprintf("%v", sawErr)
}
