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

type demoArgs struct {
	N int `json:"n"`
}

func TestWorker_ConcurrencyLimit(t *testing.T) {
	t.Parallel()
	const conc = 3
	const total = 8

	var (
		mu       sync.Mutex
		leased   int
		started  = make(chan struct{}, total)
		release  = make(chan struct{})
		inflight atomic.Int32
		maxSeen  atomic.Int32
	)

	api := &fakeAPI{}
	api.leaseFn = func(_ string, body map[string]any) (int, []map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		maxJobs := int(body["max_jobs"].(float64))
		var jobs []map[string]any
		for len(jobs) < maxJobs && leased < total {
			leased++
			id := fmt.Sprintf("aaaaaaaa-bbbb-cccc-dddd-%012d", leased)
			jobs = append(jobs, jobWire(id, "default", "demo", map[string]any{"n": leased}, 1))
		}
		return http.StatusOK, jobs
	}
	api.completeFn = func(string, map[string]any) int { return http.StatusOK }
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("x", nil)
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     conc,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "demo", func(ctx context.Context, job sdk.Job[demoArgs]) error {
		cur := inflight.Add(1)
		for {
			m := maxSeen.Load()
			if cur <= m || maxSeen.CompareAndSwap(m, cur) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		inflight.Add(-1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	for i := 0; i < conc; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for workers to start")
		}
	}
	require.LessOrEqual(t, int(maxSeen.Load()), conc)
	require.LessOrEqual(t, int(api.maxInFlight.Load()), conc)

	close(release)
	// Wait for remaining starts
	deadline := time.After(3 * time.Second)
	for i := conc; i < total; i++ {
		select {
		case <-started:
		case <-deadline:
			t.Fatalf("timeout waiting for job %d", i)
		}
	}
	cancel()
	require.NoError(t, <-done)
	require.LessOrEqual(t, int(maxSeen.Load()), conc)
}

func TestWorker_HeartbeatCadence(t *testing.T) {
	t.Parallel()
	heartbeats := make(chan time.Time, 8)
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000001", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		heartbeats <- time.Now()
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000001", nil)
	}
	api.completeFn = func(string, map[string]any) int { return http.StatusOK }

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	lease := 90 * time.Millisecond
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   lease,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "demo", func(ctx context.Context, _ sdk.Job[demoArgs]) error {
		select {
		case <-time.After(250 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	var times []time.Time
	deadline := time.After(2 * time.Second)
	for len(times) < 2 {
		select {
		case ts := <-heartbeats:
			times = append(times, ts)
		case <-deadline:
			t.Fatalf("expected >=2 heartbeats, got %d (calls=%d)", len(times), api.heartbeatCalls.Load())
		}
	}
	cancel()
	interval := times[1].Sub(times[0])
	want := lease / 3
	require.InDelta(t, float64(want), float64(interval), float64(want)*0.8)
}

func TestWorker_LeaseLostCancelsHandler(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000002", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusConflict, nil
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   60 * time.Millisecond,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	cancelled := make(chan struct{})
	sdk.Handle(w, "demo", func(ctx context.Context, _ sdk.Job[demoArgs]) error {
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled on lease lost")
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), api.completeCalls.Load())
	require.Equal(t, int32(0), api.failCalls.Load())
	cancel()
}

func TestWorker_PanicRecovery(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000003", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000003", nil)
	}
	failed := make(chan string, 1)
	api.failFn = func(_ string, body map[string]any) int {
		failed <- body["error"].(string)
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error {
		panic("boom")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case msg := <-failed:
		require.Contains(t, msg, "panic: boom")
		require.Contains(t, msg, "goroutine")
	case <-time.After(2 * time.Second):
		t.Fatal("expected fail after panic")
	}
	cancel()
}

func TestWorker_HandlerErrorCallsFail(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000009", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000009", nil)
	}
	failed := make(chan string, 1)
	api.failFn = func(_ string, body map[string]any) int {
		failed <- body["error"].(string)
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error {
		return errors.New("handler blew up")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case msg := <-failed:
		require.Equal(t, "handler blew up", msg)
		require.Equal(t, int32(0), api.completeCalls.Load())
	case <-time.After(2 * time.Second):
		t.Fatal("expected fail with handler error message")
	}
	cancel()
}

func TestWorker_UnmarshalPayloadFails(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000008", "default", "demo", "not-an-object", 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000008", nil)
	}
	failed := make(chan string, 1)
	api.failFn = func(_ string, body map[string]any) int {
		failed <- body["error"].(string)
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	called := atomic.Bool{}
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error {
		called.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case msg := <-failed:
		require.Contains(t, msg, "unmarshal payload")
		require.False(t, called.Load())
	case <-time.After(2 * time.Second):
		t.Fatal("expected fail on bad payload")
	}
	cancel()
}

func TestWorker_UnknownType(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000004", "default", "unknown", map[string]any{}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000004", nil)
	}
	failed := make(chan string, 1)
	api.failFn = func(_ string, body map[string]any) int {
		failed <- body["error"].(string)
		return http.StatusOK
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case msg := <-failed:
		require.Equal(t, `no handler registered for type "unknown"`, msg)
	case <-time.After(2 * time.Second):
		t.Fatal("expected fail for unknown type")
	}
	cancel()
}

func TestWorker_GracefulDrain(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000005", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000005", nil)
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
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	started := make(chan struct{})
	sdk.Handle(w, "demo", func(ctx context.Context, _ sdk.Job[demoArgs]) error {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	<-started
	cancel()
	require.NoError(t, <-done)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("expected complete during graceful drain")
	}
}

func TestWorker_ShutdownTimeout(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000006", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000006", nil)
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	started := make(chan struct{})
	sdk.Handle(w, "demo", func(ctx context.Context, _ sdk.Job[demoArgs]) error {
		close(started)
		<-ctx.Done()
		time.Sleep(20 * time.Millisecond)
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	<-started
	cancel()
	err = <-done
	require.True(t, errors.Is(err, sdk.ErrShutdownTimeout), "got %v", err)
}

func TestWorker_ShutdownTimeoutDespiteIgnoringCancel(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	var once sync.Once
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		var jobs []map[string]any
		once.Do(func() {
			jobs = []map[string]any{jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000010", "default", "demo", map[string]any{"n": 1}, 1)}
		})
		return http.StatusOK, jobs
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000010", nil)
	}

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 5 * time.Millisecond,
		PollMaxInterval: 20 * time.Millisecond,
		ShutdownTimeout: 40 * time.Millisecond,
	})
	require.NoError(t, err)

	release := make(chan struct{})
	started := make(chan struct{})
	var handlerDone sync.WaitGroup
	handlerDone.Add(1)
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error {
		defer handlerDone.Done()
		close(started)
		<-release // ignores job context cancel on purpose
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	<-started
	cancel()

	select {
	case err := <-done:
		require.True(t, errors.Is(err, sdk.ErrShutdownTimeout), "got %v", err)
	case <-time.After(500 * time.Millisecond):
		close(release)
		handlerDone.Wait()
		t.Fatal("Run hung after ShutdownTimeout while handler ignored cancel")
	}
	close(release) // let abandoned handler exit so goleak passes
	handlerDone.Wait()
}

func TestWorker_LeaseTransportErrorsKeepRunning(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		return http.StatusInternalServerError, nil
	}

	srv := api.start(t)
	// Force transport/API errors on every lease via 500 problem responses.
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: 10 * time.Millisecond,
		PollMaxInterval: 30 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	require.NoError(t, err)
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(500 * time.Millisecond)
	for api.leaseCalls.Load() < 3 {
		select {
		case err := <-done:
			t.Fatalf("Run exited on transport errors: %v", err)
		case <-deadline:
			t.Fatalf("expected repeated lease attempts, got %d", api.leaseCalls.Load())
		case <-time.After(15 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	require.GreaterOrEqual(t, api.leaseCalls.Load(), int32(3))
}

func TestWorker_BackoffResetOnJobs(t *testing.T) {
	t.Parallel()
	var leaseTimes []time.Time
	var mu sync.Mutex
	phase := atomic.Int32{} // 0 empty, 1 jobs, 2 empty again

	api := &fakeAPI{}
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		mu.Lock()
		leaseTimes = append(leaseTimes, time.Now())
		mu.Unlock()
		switch phase.Load() {
		case 0:
			if len(leaseTimes) >= 3 {
				phase.Store(1)
			}
			return http.StatusOK, nil
		case 1:
			phase.Store(2)
			return http.StatusOK, []map[string]any{
				jobWire("aaaaaaaa-bbbb-cccc-dddd-000000000007", "default", "demo", map[string]any{"n": 1}, 1),
			}
		default:
			return http.StatusOK, nil
		}
	}
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("aaaaaaaa-bbbb-cccc-dddd-000000000007", nil)
	}
	api.completeFn = func(string, map[string]any) int { return http.StatusOK }

	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	minPoll := 30 * time.Millisecond
	maxPoll := 200 * time.Millisecond
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		Concurrency:     1,
		LeaseDuration:   time.Second,
		PollMinInterval: minPoll,
		PollMaxInterval: maxPoll,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := len(leaseTimes)
		times := append([]time.Time(nil), leaseTimes...)
		mu.Unlock()
		if n >= 5 && phase.Load() >= 2 {
			// times[0..2]: empty leases with growing backoff; then a job; then empty again.
			require.GreaterOrEqual(t, n, 5)
			preGap := times[2].Sub(times[1])
			postGap := times[4].Sub(times[3])
			require.Greater(t, preGap, minPoll)
			require.Less(t, postGap, preGap)
			require.InDelta(t, float64(minPoll), float64(postGap), float64(minPoll)*1.5)
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for backoff pattern; leases=%d phase=%d", n, phase.Load())
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	require.NoError(t, <-done)
}

func TestWorker_RegisterAfterRunPanics(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	api.leaseFn = func(string, map[string]any) (int, []map[string]any) {
		return http.StatusOK, nil
	}
	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	w, err := sdk.NewWorker(c, sdk.WorkerOptions{
		Queue:           "default",
		WorkerID:        "w1",
		PollMinInterval: 50 * time.Millisecond,
		PollMaxInterval: 50 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)

	require.Panics(t, func() {
		sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error { return nil })
	})
	cancel()
}

func TestNewWorker_RequiresQueue(t *testing.T) {
	t.Parallel()
	_, err := sdk.NewWorker(sdk.NewClient("http://example"), sdk.WorkerOptions{})
	require.Error(t, err)
}
