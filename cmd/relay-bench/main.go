// Command relay-bench measures Relay enqueue/processing throughput and latency.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

const (
	jobTypeBench      = "bench"
	histLowestNs      = 1_000          // 1µs
	histHighestNs     = 60_000_000_000 // 60s
	histSigFigs       = 3
	drainPollInterval = 200 * time.Millisecond
)

type benchPayload struct {
	EnqueuedAtUnixNs int64  `json:"enqueued_at_unix_ns"`
	Pad              string `json:"pad,omitempty"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("relay-bench failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := parseFlags()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clientOpts := []sdk.ClientOption{}
	if cfg.APIKey != "" {
		clientOpts = append(clientOpts, sdk.WithAPIKey(cfg.APIKey))
	}
	switch cfg.Transport {
	case "http":
		clientOpts = append(clientOpts, sdk.WithTransport(sdk.TransportHTTP))
	case "grpc":
		clientOpts = append(clientOpts, sdk.WithTransport(sdk.TransportGRPC))
	}

	client := sdk.NewClient(cfg.Server, clientOpts...)
	defer func() { _ = client.Close() }()

	env := collectEnvironment(cfg)
	logger.Info("starting benchmark",
		"server", cfg.Server,
		"transport", cfg.Transport,
		"queue", cfg.Queue,
		"jobs", cfg.Jobs,
		"producers", cfg.Producers,
		"workers", cfg.Workers,
		"concurrency", cfg.Concurrency,
	)

	stats := newRunStats()
	pad := makePad(cfg.PayloadBytes)

	// Start workers before enqueue so e2e latency is meaningful.
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerEG errgroup.Group
	var completed atomic.Int64
	for i := 0; i < cfg.Workers; i++ {
		workerID := fmt.Sprintf("bench-w-%d-%s", i, uuid.NewString()[:8])
		workerEG.Go(func() error {
			return runWorker(workerCtx, client, cfg, workerID, stats, &completed, logger)
		})
	}

	if cfg.Warmup > 0 {
		logger.Info("warmup", "duration", cfg.Warmup)
		if err := enqueueBatch(ctx, client, cfg, pad, warmupJobCount(cfg), stats, true); err != nil {
			cancelWorkers()
			_ = workerEG.Wait()
			return fmt.Errorf("warmup enqueue: %w", err)
		}
		deadline := time.Now().Add(cfg.Warmup)
		for time.Now().Before(deadline) {
			if ctx.Err() != nil {
				cancelWorkers()
				_ = workerEG.Wait()
				return ctx.Err()
			}
			time.Sleep(50 * time.Millisecond)
		}
		stats.reset()
		completed.Store(0)
		logger.Info("warmup done; measuring")
	}

	enqueueStart := time.Now()
	if err := enqueueBatch(ctx, client, cfg, pad, cfg.Jobs, stats, false); err != nil {
		cancelWorkers()
		_ = workerEG.Wait()
		return fmt.Errorf("enqueue: %w", err)
	}
	enqueueWall := time.Since(enqueueStart)

	logger.Info("enqueue finished; draining", "enqueued", cfg.Jobs, "wall", enqueueWall)
	if err := waitForCompletions(ctx, &completed, int64(cfg.Jobs), cfg.DrainTimeout); err != nil {
		cancelWorkers()
		_ = workerEG.Wait()
		return err
	}
	processWall := time.Since(enqueueStart)

	cancelWorkers()
	if err := workerEG.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("worker exit", "err", err)
	}

	rep := stats.report(cfg, env, enqueueWall, processWall, completed.Load())
	if err := writeReport(os.Stdout, cfg.Output, rep); err != nil {
		return err
	}

	if cfg.Cleanup {
		n, cerr := cleanupQueue(ctx, client, cfg.Queue)
		if cerr != nil {
			return fmt.Errorf("cleanup: %w", cerr)
		}
		logger.Info("cleanup cancelled pending/scheduled jobs", "cancelled", n)
	}
	return nil
}

type config struct {
	Server       string
	Admin        string
	Transport    string
	APIKey       string
	Producers    int
	Workers      int
	Concurrency  int
	Jobs         int
	PayloadBytes int
	HandlerSleep time.Duration
	Queue        string
	Warmup       time.Duration
	DrainTimeout time.Duration
	Output       string
	Cleanup      bool
	StorageHint  string
}

func parseFlags() (config, error) {
	cfg := config{}
	flag.StringVar(&cfg.Server, "server", envOr("RELAY_URL", "http://localhost:7420"), "Relay server URL (http(s):// or grpc(s)://)")
	flag.StringVar(&cfg.Admin, "admin", envOr("RELAY_ADMIN_URL", "http://localhost:7429"), "Admin metrics URL for build info (empty to skip)")
	flag.StringVar(&cfg.Transport, "transport", "http", "transport: http|grpc")
	flag.StringVar(&cfg.APIKey, "api-key", os.Getenv("RELAY_API_KEY"), "Bearer API key (or RELAY_API_KEY)")
	flag.IntVar(&cfg.Producers, "producers", 4, "concurrent producer goroutines")
	flag.IntVar(&cfg.Workers, "workers", 4, "SDK worker processes (each with -concurrency)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 50, "per-worker handler concurrency")
	flag.IntVar(&cfg.Jobs, "jobs", 10000, "jobs to enqueue in the measured phase")
	flag.IntVar(&cfg.PayloadBytes, "payload-bytes", 512, "approximate JSON payload size")
	flag.DurationVar(&cfg.HandlerSleep, "handler-sleep", 0, "artificial sleep inside the bench handler")
	flag.StringVar(&cfg.Queue, "queue", "", "queue name (default bench-<random>)")
	flag.DurationVar(&cfg.Warmup, "warmup", 5*time.Second, "warmup duration before measurement")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", 10*time.Minute, "max wait for all completions after enqueue")
	flag.StringVar(&cfg.Output, "output", "text", "report format: text|json")
	flag.BoolVar(&cfg.Cleanup, "cleanup", false, "cancel remaining pending/scheduled jobs after the run")
	flag.StringVar(&cfg.StorageHint, "storage", os.Getenv("RELAY_STORAGE"), "storage backend label when admin scrape unavailable")
	flag.Parse()

	cfg.Transport = strings.ToLower(strings.TrimSpace(cfg.Transport))
	if cfg.Transport != "http" && cfg.Transport != "grpc" {
		return cfg, fmt.Errorf("--transport must be http or grpc")
	}
	cfg.Output = strings.ToLower(strings.TrimSpace(cfg.Output))
	if cfg.Output != "text" && cfg.Output != "json" {
		return cfg, fmt.Errorf("--output must be text or json")
	}
	if cfg.Producers <= 0 || cfg.Workers <= 0 || cfg.Concurrency <= 0 || cfg.Jobs <= 0 {
		return cfg, fmt.Errorf("--producers, --workers, --concurrency, and --jobs must be positive")
	}
	if cfg.PayloadBytes < 32 {
		return cfg, fmt.Errorf("--payload-bytes must be >= 32")
	}
	if cfg.Queue == "" {
		cfg.Queue = "bench-" + uuid.NewString()[:8]
	}
	if err := validateQueueName(cfg.Queue); err != nil {
		return cfg, err
	}
	if cfg.Transport == "grpc" && strings.HasPrefix(cfg.Server, "http") {
		// Convenience: map host from HTTP URL to grpc:// when user passes localhost:7420 style HTTP.
		cfg.Server = rewriteHTTPToGRPC(cfg.Server)
	}
	return cfg, nil
}

func validateQueueName(q string) error {
	if len(q) == 0 || len(q) > 128 {
		return fmt.Errorf("invalid queue name length")
	}
	for _, r := range q {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if !ok {
			return fmt.Errorf("invalid queue name %q", q)
		}
	}
	return nil
}

func rewriteHTTPToGRPC(server string) string {
	s := strings.TrimPrefix(server, "https://")
	s = strings.TrimPrefix(s, "http://")
	// Default compose maps gRPC on 7421; if user left :7420, swap.
	if strings.HasSuffix(s, ":7420") {
		s = strings.TrimSuffix(s, ":7420") + ":7421"
	}
	return "grpc://" + s
}

func warmupJobCount(cfg config) int {
	// Roughly enough work to keep the pipeline warm without dominating the run.
	n := cfg.Producers * cfg.Concurrency
	if n < 100 {
		n = 100
	}
	if n > cfg.Jobs {
		n = cfg.Jobs
	}
	return n
}

func makePad(payloadBytes int) string {
	const overhead = 64 // JSON keys / timestamps
	n := payloadBytes - overhead
	if n < 0 {
		n = 0
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[i%len(alphabet)]
	}
	return string(b)
}

func enqueueBatch(ctx context.Context, client *sdk.Client, cfg config, pad string, n int, stats *runStats, warmup bool) error {
	var next atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for p := 0; p < cfg.Producers; p++ {
		g.Go(func() error {
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return nil
				}
				if err := gctx.Err(); err != nil {
					return err
				}
				payload := benchPayload{
					EnqueuedAtUnixNs: time.Now().UnixNano(),
					Pad:              pad,
				}
				start := time.Now()
				_, err := client.Enqueue(gctx, sdk.EnqueueRequest{
					Queue:          cfg.Queue,
					Type:           jobTypeBench,
					Payload:        payload,
					IdempotencyKey: uuid.NewString(),
				})
				elapsed := time.Since(start)
				if err != nil {
					stats.recordError("enqueue", err)
					return fmt.Errorf("enqueue #%d: %w", i, err)
				}
				if !warmup {
					stats.recordEnqueue(elapsed)
				}
			}
		})
	}
	return g.Wait()
}

func runWorker(ctx context.Context, client *sdk.Client, cfg config, workerID string, stats *runStats, completed *atomic.Int64, logger *slog.Logger) error {
	w, err := sdk.NewWorker(client, sdk.WorkerOptions{
		Queue:           cfg.Queue,
		WorkerID:        workerID,
		Concurrency:     cfg.Concurrency,
		LeaseDuration:   30 * time.Second,
		LeaseWait:       5 * time.Second,
		PollMinInterval: 20 * time.Millisecond,
		PollMaxInterval: 200 * time.Millisecond,
		ShutdownTimeout: 15 * time.Second,
		Logger:          logger,
	})
	if err != nil {
		return err
	}

	sdk.Handle(w, jobTypeBench, func(ctx context.Context, job sdk.Job[benchPayload]) error {
		now := time.Now()
		if job.Args.EnqueuedAtUnixNs > 0 {
			e2e := now.Sub(time.Unix(0, job.Args.EnqueuedAtUnixNs))
			if e2e > 0 {
				stats.recordE2E(e2e)
			}
		}
		if cfg.HandlerSleep > 0 {
			t := time.NewTimer(cfg.HandlerSleep)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
		completed.Add(1)
		stats.recordComplete()
		return nil
	})

	err = w.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func waitForCompletions(ctx context.Context, completed *atomic.Int64, want int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()
	for {
		if completed.Load() >= want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("drain timeout: completed %d/%d", completed.Load(), want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func cleanupQueue(ctx context.Context, client *sdk.Client, queue string) (int, error) {
	cancelled := 0
	for job, err := range client.ListJobs(ctx, queue, "pending", sdk.ListJobsOptions{Limit: 100}) {
		if err != nil {
			return cancelled, err
		}
		if _, err := client.CancelJob(ctx, job.ID); err != nil {
			return cancelled, err
		}
		cancelled++
	}
	return cancelled, nil
}

type runStats struct {
	mu          sync.Mutex
	enqueueHist *hdrhistogram.Histogram
	e2eHist     *hdrhistogram.Histogram
	enqueued    atomic.Int64
	completions atomic.Int64
	errors      sync.Map // string → *atomic.Int64
}

func newRunStats() *runStats {
	return &runStats{
		enqueueHist: hdrhistogram.New(histLowestNs, histHighestNs, histSigFigs),
		e2eHist:     hdrhistogram.New(histLowestNs, histHighestNs, histSigFigs),
	}
}

func (s *runStats) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enqueueHist.Reset()
	s.e2eHist.Reset()
	s.enqueued.Store(0)
	s.completions.Store(0)
	s.errors = sync.Map{}
}

func (s *runStats) recordEnqueue(d time.Duration) {
	ns := d.Nanoseconds()
	if ns < histLowestNs {
		ns = histLowestNs
	}
	s.mu.Lock()
	_ = s.enqueueHist.RecordValue(ns)
	s.mu.Unlock()
	s.enqueued.Add(1)
}

func (s *runStats) recordE2E(d time.Duration) {
	ns := d.Nanoseconds()
	if ns < histLowestNs {
		ns = histLowestNs
	}
	if ns > histHighestNs {
		ns = histHighestNs
	}
	s.mu.Lock()
	_ = s.e2eHist.RecordValue(ns)
	s.mu.Unlock()
}

func (s *runStats) recordComplete() {
	s.completions.Add(1)
}

func (s *runStats) recordError(kind string, err error) {
	key := kind
	if err != nil {
		key = kind + ":" + classifyErr(err)
	}
	v, _ := s.errors.LoadOrStore(key, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

func classifyErr(err error) string {
	var api *sdk.APIError
	if errors.As(err, &api) {
		return fmt.Sprintf("api_%d", api.StatusCode)
	}
	msg := err.Error()
	if len(msg) > 64 {
		msg = msg[:64]
	}
	return msg
}

func (s *runStats) report(cfg config, env Environment, enqueueWall, processWall time.Duration, completed int64) Report {
	s.mu.Lock()
	defer s.mu.Unlock()

	errCounts := map[string]int64{}
	s.errors.Range(func(k, v any) bool {
		errCounts[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})

	enqN := s.enqueued.Load()
	var enqueueRate float64
	if enqueueWall > 0 {
		enqueueRate = float64(enqN) / enqueueWall.Seconds()
	}
	var processRate float64
	if processWall > 0 {
		processRate = float64(completed) / processWall.Seconds()
	}

	return Report{
		Environment: env,
		Config: ReportConfig{
			Server:       cfg.Server,
			Transport:    cfg.Transport,
			Queue:        cfg.Queue,
			Jobs:         cfg.Jobs,
			Producers:    cfg.Producers,
			Workers:      cfg.Workers,
			Concurrency:  cfg.Concurrency,
			PayloadBytes: cfg.PayloadBytes,
			HandlerSleep: cfg.HandlerSleep.String(),
			Warmup:       cfg.Warmup.String(),
		},
		Enqueue: ThroughputBlock{
			Count:      enqN,
			Wall:       enqueueWall.String(),
			JobsPerSec: enqueueRate,
			Latency:    histSummary(s.enqueueHist),
		},
		Processing: ThroughputBlock{
			Count:      completed,
			Wall:       processWall.String(),
			JobsPerSec: processRate,
		},
		EndToEnd:   histSummary(s.e2eHist),
		Errors:     errCounts,
		Completed:  completed,
		MeasuredAt: time.Now().UTC(),
	}
}
