// Command worker demonstrates processing jobs via the Relay SDK worker.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
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

	opts := []sdk.ClientOption{}
	if apiKey != "" {
		opts = append(opts, sdk.WithAPIKey(apiKey))
	}
	client := sdk.NewClient(baseURL, opts...)

	w, err := sdk.NewWorker(client, sdk.WorkerOptions{
		Queue:       queue,
		Concurrency: concurrency,
		Logger:      logger,
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("worker starting",
		"relay_url", baseURL,
		"queue", queue,
		"concurrency", concurrency,
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
