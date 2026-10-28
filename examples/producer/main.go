// Command producer demonstrates enqueueing jobs via the Relay SDK.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/MEHRDADsmj/relay/internal/platform/telemetry"
	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("producer exited", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	n := flag.Int("n", 100, "number of jobs to enqueue")
	queue := flag.String("queue", envOr("RELAY_QUEUE", "default"), "target queue")
	rate := flag.Float64("rate", 20, "enqueue rate (jobs per second)")
	flag.Parse()

	if *n <= 0 {
		return fmt.Errorf("-n must be positive")
	}
	if *rate <= 0 {
		return fmt.Errorf("-rate must be positive")
	}

	baseURL := envOr("RELAY_URL", "http://localhost:7420")
	apiKey := os.Getenv("RELAY_API_KEY")

	ctx := context.Background()
	telShutdown, err := telemetry.Setup(ctx, telemetry.Config{
		Endpoint:       telemetry.EndpointFromEnv(),
		SampleRatio:    envFloat("RELAY_TRACE_SAMPLE_RATIO", 1.0),
		ServiceName:    "example-producer",
		ServiceVersion: "dev",
		Environment:    envOr("RELAY_ENVIRONMENT", "development"),
	})
	if err != nil {
		return fmt.Errorf("telemetry setup: %w", err)
	}
	defer func() { _ = telShutdown(context.Background()) }()

	opts := []sdk.ClientOption{
		sdk.WithTracerProvider(otel.GetTracerProvider()),
	}
	if apiKey != "" {
		opts = append(opts, sdk.WithAPIKey(apiKey))
	}
	client := sdk.NewClient(baseURL, opts...)
	defer func() { _ = client.Close() }()

	interval := time.Duration(float64(time.Second) / *rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("enqueueing jobs",
		"n", *n,
		"queue", *queue,
		"rate", *rate,
		"relay_url", baseURL,
	)

	tracer := otel.Tracer("example-producer")
	ctx, root := tracer.Start(ctx, "demo.enqueue_batch")
	root.SetAttributes(
		attribute.String("relay.queue", *queue),
		attribute.Int("demo.job_count", *n),
	)
	defer root.End()

	for i := 0; i < *n; i++ {
		if i > 0 {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				root.RecordError(ctx.Err())
				root.SetStatus(codes.Error, ctx.Err().Error())
				return ctx.Err()
			}
		}

		var (
			jobType string
			payload any
		)
		idem := uuid.NewString()
		if i%2 == 0 {
			jobType = "send_welcome_email"
			payload = map[string]string{
				"user_id": fmt.Sprintf("user-%d", i),
				"email":   fmt.Sprintf("user-%d@example.com", i),
			}
		} else {
			jobType = "generate_report"
			payload = map[string]string{
				"report_id": fmt.Sprintf("report-%d", i),
			}
		}

		info, err := client.Enqueue(ctx, sdk.EnqueueRequest{
			Queue:          *queue,
			Type:           jobType,
			Payload:        payload,
			IdempotencyKey: idem,
		})
		if err != nil {
			root.RecordError(err)
			root.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("enqueue %s #%d: %w", jobType, i, err)
		}
		fmt.Printf("%s %s %s\n", info.ID, info.Type, info.Status)
	}

	logger.Info("done", "enqueued", *n)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
