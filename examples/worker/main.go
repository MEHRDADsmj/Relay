// Command worker demonstrates processing jobs via the Relay SDK worker.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"

	"github.com/MEHRDADsmj/relay/internal/platform/telemetry"
	"github.com/MEHRDADsmj/relay/pkg/sdk"
	"github.com/MEHRDADsmj/relay/pkg/sdk/sdkprom"
)

type welcomeEmailArgs struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type reportArgs struct {
	ReportID string `json:"report_id"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("worker exited", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	baseURL := envOr("RELAY_URL", "http://localhost:7420")
	queue := envOr("RELAY_QUEUE", "default")
	apiKey := os.Getenv("RELAY_API_KEY")
	concurrency := envInt("WORKER_CONCURRENCY", 10)
	metricsAddr := envOr("WORKER_METRICS_ADDR", ":9464")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	telShutdown, err := telemetry.Setup(ctx, telemetry.Config{
		Endpoint:       telemetry.EndpointFromEnv(),
		SampleRatio:    envFloat("RELAY_TRACE_SAMPLE_RATIO", 1.0),
		ServiceName:    "example-worker",
		ServiceVersion: "dev",
		InstanceID:     envOr("RELAY_INSTANCE_ID", hostnamePID()),
		Environment:    envOr("RELAY_ENVIRONMENT", "development"),
	})
	if err != nil {
		return fmt.Errorf("telemetry setup: %w", err)
	}
	defer func() { _ = telShutdown(context.WithoutCancel(ctx)) }()

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	hooks := sdkprom.NewHooks(reg)

	metricsSrv := &http.Server{
		Addr:              metricsAddr,
		Handler:           promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("worker metrics listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "err", err)
			stop()
		}
	}()
	defer func() {
		shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shCtx)
	}()

	opts := []sdk.ClientOption{
		sdk.WithTracerProvider(otel.GetTracerProvider()),
	}
	if apiKey != "" {
		opts = append(opts, sdk.WithAPIKey(apiKey))
	}
	client := sdk.NewClient(baseURL, opts...)
	defer func() { _ = client.Close() }()

	w, err := sdk.NewWorker(client, sdk.WorkerOptions{
		Queue:       queue,
		Concurrency: concurrency,
		Logger:      logger,
		Hooks:       hooks,
	})
	if err != nil {
		return fmt.Errorf("create worker: %w", err)
	}

	sdk.Handle(w, "send_welcome_email", func(ctx context.Context, job sdk.Job[welcomeEmailArgs]) error {
		logger.Info("processing send_welcome_email",
			"job_id", job.ID,
			"attempt", job.Attempt,
			"user_id", job.Args.UserID,
			"email", job.Args.Email,
		)
		if err := sleep(ctx, 200*time.Millisecond+randDuration(600*time.Millisecond)); err != nil {
			return err
		}
		if randFloat64() < 0.20 {
			return errors.New("transient send failure (demo 20%)")
		}
		logger.Info("send_welcome_email succeeded", "job_id", job.ID)
		return nil
	})

	sdk.Handle(w, "generate_report", func(ctx context.Context, job sdk.Job[reportArgs]) error {
		logger.Info("processing generate_report",
			"job_id", job.ID,
			"attempt", job.Attempt,
			"report_id", job.Args.ReportID,
		)
		if err := sleep(ctx, time.Second+randDuration(2*time.Second)); err != nil {
			return err
		}
		logger.Info("generate_report succeeded", "job_id", job.ID)
		return nil
	})

	logger.Info("worker starting",
		"relay_url", baseURL,
		"queue", queue,
		"concurrency", concurrency,
		"metrics_addr", metricsAddr,
	)
	return w.Run(ctx)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func randDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

func randFloat64() float64 {
	const scale = 1_000_000
	n, err := rand.Int(rand.Reader, big.NewInt(scale))
	if err != nil {
		return 0
	}
	return float64(n.Int64()) / scale
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func hostnamePID() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		h = "worker"
	}
	return fmt.Sprintf("%s-%d", h, os.Getpid())
}
