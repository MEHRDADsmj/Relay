package sdk_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

// BenchmarkWorkerDispatchLoop measures the SDK worker dispatch path against fakeAPI.
func BenchmarkWorkerDispatchLoop(b *testing.B) {
	var leased atomic.Int64
	api := &fakeAPI{}
	api.leaseFn = func(_ string, body map[string]any) (int, []map[string]any) {
		maxJobs := 1
		if v, ok := body["max_jobs"].(float64); ok && int(v) > 0 {
			maxJobs = int(v)
		}
		jobs := make([]map[string]any, 0, maxJobs)
		for len(jobs) < maxJobs {
			n := leased.Add(1)
			id := fmt.Sprintf("aaaaaaaa-bbbb-cccc-dddd-%012d", n)
			jobs = append(jobs, jobWire(id, "bench", "demo", map[string]any{"n": n}, 1))
		}
		return http.StatusOK, jobs
	}
	api.completeFn = func(string, map[string]any) int { return http.StatusOK }
	api.heartbeatFn = func(string, map[string]any) (int, map[string]any) {
		return http.StatusOK, sampleJob("x", nil)
	}

	srv := api.start(b)
	client := sdk.NewClient(srv.URL)
	defer func() { _ = client.Close() }()

	w, err := sdk.NewWorker(client, sdk.WorkerOptions{
		Queue:           "bench",
		WorkerID:        "bench-w",
		Concurrency:     8,
		LeaseDuration:   time.Minute, // avoid heartbeats during the short bench
		LeaseWait:       -1,
		PollMinInterval: time.Millisecond,
		PollMaxInterval: 5 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatal(err)
	}

	var handled atomic.Int64
	doneN := make(chan struct{})
	sdk.Handle(w, "demo", func(context.Context, sdk.Job[demoArgs]) error {
		if handled.Add(1) == int64(b.N) {
			close(doneN)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- w.Run(ctx) }()

	b.ReportAllocs()
	b.ResetTimer()
	select {
	case <-doneN:
	case <-time.After(2 * time.Minute):
		b.Fatalf("timeout waiting for %d completions (got %d)", b.N, handled.Load())
	}
	b.StopTimer()
	cancel()
	<-runErr
}
