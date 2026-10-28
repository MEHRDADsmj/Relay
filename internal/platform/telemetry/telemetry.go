package telemetry

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const serviceName = "relay-server"

// Config holds tracing setup options. Endpoint empty disables tracing (noop provider).
type Config struct {
	Endpoint       string
	SampleRatio    float64
	ServiceVersion string
	InstanceID     string
	Environment    string
}

// Setup installs a global TracerProvider and TextMapPropagator.
// When Endpoint is empty, a noop provider is installed and shutdown is a no-op.
// OTEL_EXPORTER_OTLP_INSECURE is honored (default true for local plaintext OTLP).
func Setup(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	ratio := cfg.SampleRatio
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(stripEndpointScheme(endpoint)),
	}
	if otlpInsecure() {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			semconv.ServiceInstanceID(cfg.InstanceID),
			semconv.DeploymentEnvironmentName(cfg.Environment),
		),
	)
	if err != nil {
		_ = exp.Shutdown(ctx)
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// EndpointFromEnv returns OTEL_EXPORTER_OTLP_ENDPOINT (may be empty).
func EndpointFromEnv() string {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

func stripEndpointScheme(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	return endpoint
}

func otlpInsecure() bool {
	v := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")
	if v == "" {
		return true
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return true
	}
	return b
}
