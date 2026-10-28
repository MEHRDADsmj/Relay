package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	defaultConcurrency     = 10
	defaultLeaseDuration   = 30 * time.Second
	defaultPollMinInterval = 100 * time.Millisecond
	defaultPollMaxInterval = 2 * time.Second
	defaultShutdownTimeout = 30 * time.Second
	transportBackoffMax    = 5 * time.Second
	maxLeaseBatch          = 100
	panicStackLimit        = 4096
)

// Worker leases jobs from a queue and runs registered handlers.
type Worker struct {
	client *Client
	opts   WorkerOptions

	mu       sync.Mutex
	handlers map[string]jobHandler
	running  atomic.Bool
}

type jobHandler func(ctx context.Context, job apiJob) error

// NewWorker validates options and returns a Worker.
func NewWorker(c *Client, opts WorkerOptions) (*Worker, error) {
	if c == nil {
		return nil, fmt.Errorf("sdk: client is required")
	}
	if opts.Queue == "" {
		return nil, fmt.Errorf("sdk: WorkerOptions.Queue is required")
	}
	if opts.WorkerID == "" {
		host, _ := os.Hostname()
		opts.WorkerID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultConcurrency
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = defaultLeaseDuration
	}
	if opts.PollMinInterval <= 0 {
		opts.PollMinInterval = defaultPollMinInterval
	}
	if opts.PollMaxInterval <= 0 {
		opts.PollMaxInterval = defaultPollMaxInterval
	}
	if opts.PollMaxInterval < opts.PollMinInterval {
		opts.PollMaxInterval = opts.PollMinInterval
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = defaultShutdownTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Worker{
		client:   c,
		opts:     opts,
		handlers: make(map[string]jobHandler),
	}, nil
}

// Handle registers a typed handler for jobType. Panics if Run has already started.
func Handle[T any](w *Worker, jobType string, h func(ctx context.Context, job Job[T]) error) {
	w.register(jobType, func(ctx context.Context, raw apiJob) error {
		var args T
		if len(raw.Payload) > 0 {
			if err := json.Unmarshal(raw.Payload, &args); err != nil {
				return fmt.Errorf("unmarshal payload: %w", err)
			}
		}
		return h(ctx, Job[T]{
			ID:      raw.ID,
			Queue:   raw.Queue,
			Type:    raw.Type,
			Attempt: raw.Attempts,
			Args:    args,
		})
	})
}

func (w *Worker) register(jobType string, h jobHandler) {
	if w.running.Load() {
		panic("sdk: cannot register handlers after Worker.Run has started")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = h
}

func (w *Worker) handlerFor(jobType string) (jobHandler, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	h, ok := w.handlers[jobType]
	return h, ok
}

// Run leases and processes jobs until ctx is cancelled, then drains in-flight work.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("sdk: Worker.Run already in progress")
	}
	defer w.running.Store(false)

	// WithoutCancel: stop leasing when ctx ends, but drain in-flight jobs first.
	workerCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	var (
		wg       sync.WaitGroup
		inFlight atomic.Int32
		slotCh   = make(chan struct{}, 1)
	)
	signalSlot := func() {
		select {
		case slotCh <- struct{}{}:
		default:
		}
	}

	backoff := w.opts.PollMinInterval
	leaseSeconds := int(w.opts.LeaseDuration / time.Second)
	if leaseSeconds < 1 {
		leaseSeconds = 1
	}

	for ctx.Err() == nil {
		free := w.opts.Concurrency - int(inFlight.Load())
		if free <= 0 {
			select {
			case <-ctx.Done():
				goto shutdown
			case <-slotCh:
			}
			continue
		}
		if free > maxLeaseBatch {
			free = maxLeaseBatch
		}

		jobs, err := w.client.lease(ctx, w.opts.Queue, w.opts.WorkerID, free, leaseSeconds)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.opts.Logger.ErrorContext(ctx, "lease failed", "err", err)
			sleep := backoff
			if sleep > transportBackoffMax {
				sleep = transportBackoffMax
			}
			if !sleepWait(ctx, sleepWithJitter(sleep)) {
				break
			}
			backoff = nextBackoff(backoff, w.opts.PollMinInterval, transportBackoffMax)
			continue
		}

		if len(jobs) == 0 {
			if !sleepWait(ctx, sleepWithJitter(backoff)) {
				break
			}
			backoff = nextBackoff(backoff, w.opts.PollMinInterval, w.opts.PollMaxInterval)
			continue
		}

		backoff = w.opts.PollMinInterval
		for _, job := range jobs {
			inFlight.Add(1)
			wg.Add(1)
			go func(j apiJob) {
				defer func() {
					inFlight.Add(-1)
					wg.Done()
					signalSlot()
				}()
				w.processJob(workerCtx, j, leaseSeconds)
			}(job)
		}
	}

shutdown:
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	timer := time.NewTimer(w.opts.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-waitDone:
		return nil
	case <-timer.C:
		// Force-cancel job contexts, then wait a secondary bound for watchers to
		// release. processJob must not block forever on handlers that ignore
		// cancel; those invoke goroutines may be abandoned until they return.
		cancelJobs()
		secondary := time.NewTimer(w.opts.ShutdownTimeout)
		defer secondary.Stop()
		select {
		case <-waitDone:
		case <-secondary.C:
			w.opts.Logger.Warn("shutdown: abandoning wait for in-flight jobs that ignore cancel")
		}
		return ErrShutdownTimeout
	}
}

func (w *Worker) processJob(workerCtx context.Context, job apiJob, leaseSeconds int) {
	jobCtx, cancel := context.WithCancel(workerCtx)
	defer cancel()

	h, hasHandler := w.handlerFor(job.Type)

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- w.invokeOrUnknown(jobCtx, job, hasHandler, h)
	}()

	interval := w.opts.LeaseDuration / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case err := <-resultCh:
			if workerCtx.Err() != nil {
				// Shutdown cancelled the job context; skip reporting if we did not finish cleanly.
				return
			}
			w.reportResult(workerCtx, job, err)
			return
		case <-ticker.C:
			hbCtx, hbCancel := context.WithTimeout(context.WithoutCancel(workerCtx), 5*time.Second)
			err := w.client.heartbeat(hbCtx, job.ID, w.opts.WorkerID, leaseSeconds)
			hbCancel()
			if err == nil {
				continue
			}
			if errors.Is(err, ErrLeaseLost) {
				cancel()
				drainResult(resultCh, w.opts.ShutdownTimeout)
				return
			}
			w.opts.Logger.Error("heartbeat failed", "job_id", job.ID, "err", err)
		case <-workerCtx.Done():
			// Bound the wait so a handler that ignores cancel cannot hang Run.
			drainResult(resultCh, w.opts.ShutdownTimeout)
			return
		}
	}
}

// drainResult waits up to bound for the handler result, then returns so the
// job watcher can release WaitGroup slots. If the handler is still running,
// its goroutine is abandoned until it returns (buffered send will not block).
func drainResult(resultCh <-chan error, bound time.Duration) {
	if bound <= 0 {
		bound = time.Second
	}
	t := time.NewTimer(bound)
	defer t.Stop()
	select {
	case <-resultCh: // draining abandoned or finishing handler
	case <-t.C:
	}
}

func (w *Worker) invokeOrUnknown(ctx context.Context, job apiJob, hasHandler bool, h jobHandler) error {
	if !hasHandler {
		return fmt.Errorf("no handler registered for type %q", job.Type)
	}
	return w.invoke(ctx, h, job)
}

func (w *Worker) invoke(ctx context.Context, h jobHandler, job apiJob) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			if len(stack) > panicStackLimit {
				stack = stack[:panicStackLimit]
			}
			err = fmt.Errorf("panic: %v\n%s", r, stack)
		}
	}()
	return h(ctx, job)
}

func (w *Worker) reportResult(ctx context.Context, job apiJob, result error) {
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if result == nil {
		if err := w.client.complete(reportCtx, job.ID, w.opts.WorkerID); err != nil {
			w.opts.Logger.Error("complete failed", "job_id", job.ID, "err", err)
		}
		return
	}
	if err := w.client.fail(reportCtx, job.ID, w.opts.WorkerID, result.Error()); err != nil {
		w.opts.Logger.Error("fail failed", "job_id", job.ID, "err", err)
	}
}

func nextBackoff(current, min, max time.Duration) time.Duration {
	next := current * 2
	if next < min {
		next = min
	}
	if next > max {
		next = max
	}
	return next
}

func sleepWithJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// Deterministic ±10% spread from duration bits (avoids crypto/math rand lint noise).
	j := time.Duration(int64(d) / 10)
	if int64(d)%2 == 0 {
		return d + j
	}
	if d > j {
		return d - j
	}
	return d
}

func sleepWait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
