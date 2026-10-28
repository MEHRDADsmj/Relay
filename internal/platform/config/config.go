package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// defaultInstanceID returns hostname-pid for RELAY_INSTANCE_ID.
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "relay"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// Storage backend identifiers.
const (
	StoragePostgres = "postgres"
	StorageRedis    = "redis"
)

// Retry policy identifiers.
const (
	RetryPolicyExponential = "exponential"
	RetryPolicyFixed       = "fixed"
)

// Config holds relay-server settings loaded from the environment.
type Config struct {
	HTTPAddr         string
	GRPCAddr         string
	AdminAddr        string
	MetricsMaxSeries int
	GRPCReflection   bool
	Storage          string
	PostgresURL      string
	RedisURL         string
	RedisKeyPrefix   string
	MigrateOnStart   bool
	APIKey           string
	AuthCacheTTL     time.Duration
	RetryPolicy      string
	RetryBase        time.Duration
	RetryMax         time.Duration
	RetryDelay       time.Duration // legacy alias for fixed delay / RetryBase override
	ReapInterval     time.Duration
	ReapBatch        int
	PurgeInterval    time.Duration
	PurgeBatch       int
	Retention        time.Duration
	StatsRetention   time.Duration
	LeaseMaxWait     time.Duration
	ShutdownTimeout  time.Duration
	SchedulerEnabled bool
	SchedulerTick    time.Duration
	InstanceID       string
	LogLevel         string
	LogFormat        string
	Environment      string
	TraceSampleRatio float64
	OTLPEndpoint     string
}

// Load reads configuration from environment variables and validates it.
// All validation errors are collected and returned together.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:         envOr("RELAY_HTTP_ADDR", ":7420"),
		GRPCAddr:         envOr("RELAY_GRPC_ADDR", ":7421"),
		AdminAddr:        envOr("RELAY_ADMIN_ADDR", ":7429"),
		MetricsMaxSeries: 1000,
		Storage:          strings.ToLower(envOr("RELAY_STORAGE", StoragePostgres)),
		PostgresURL:      os.Getenv("RELAY_POSTGRES_URL"),
		RedisURL:         os.Getenv("RELAY_REDIS_URL"),
		RedisKeyPrefix:   envOr("RELAY_REDIS_KEY_PREFIX", "relay"),
		APIKey:           os.Getenv("RELAY_API_KEY"),
		AuthCacheTTL:     30 * time.Second,
		LogLevel:         strings.ToLower(envOr("RELAY_LOG_LEVEL", "info")),
		LogFormat:        strings.ToLower(envOr("RELAY_LOG_FORMAT", "json")),
		RetryPolicy:      strings.ToLower(envOr("RELAY_RETRY_POLICY", RetryPolicyExponential)),
		RetryBase:        time.Second,
		RetryMax:         time.Hour,
		RetryDelay:       5 * time.Second, // Phase 1 default; remapped below if unset overrides
		ReapInterval:     5 * time.Second,
		ReapBatch:        500,
		PurgeInterval:    time.Minute,
		PurgeBatch:       1000,
		Retention:        168 * time.Hour,
		StatsRetention:   48 * time.Hour,
		LeaseMaxWait:     30 * time.Second,
		// ShutdownTimeout default = LeaseMaxWait + 15s margin (BaseContext still cancels long-polls on SIGTERM).
		ShutdownTimeout:  45 * time.Second,
		SchedulerEnabled: true,
		SchedulerTick:    time.Second,
		InstanceID:       envOr("RELAY_INSTANCE_ID", defaultInstanceID()),
		Environment:      envOr("RELAY_ENVIRONMENT", "development"),
		TraceSampleRatio: 0.1,
		OTLPEndpoint:     os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}

	var errs []string

	if v := os.Getenv("RELAY_MIGRATE_ON_START"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_MIGRATE_ON_START: invalid bool %q", v))
		} else {
			cfg.MigrateOnStart = b
		}
	}

	if v := os.Getenv("RELAY_GRPC_REFLECTION"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_GRPC_REFLECTION: invalid bool %q", v))
		} else {
			cfg.GRPCReflection = b
		}
	}

	if v := os.Getenv("RELAY_SCHEDULER_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_SCHEDULER_ENABLED: invalid bool %q", v))
		} else {
			cfg.SchedulerEnabled = b
		}
	}

	if v := os.Getenv("RELAY_AUTH_CACHE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_AUTH_CACHE_TTL: %v", err))
		} else {
			cfg.AuthCacheTTL = d
		}
	}

	if v := os.Getenv("RELAY_METRICS_MAX_SERIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_METRICS_MAX_SERIES: %v", err))
		} else {
			cfg.MetricsMaxSeries = n
		}
	}

	if v := os.Getenv("RELAY_TRACE_SAMPLE_RATIO"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_TRACE_SAMPLE_RATIO: %v", err))
		} else {
			cfg.TraceSampleRatio = f
		}
	}

	if v := os.Getenv("RELAY_SCHEDULER_TICK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_SCHEDULER_TICK: %v", err))
		} else {
			cfg.SchedulerTick = d
		}
	}

	retryBaseSet := false
	if v := os.Getenv("RELAY_RETRY_BASE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_RETRY_BASE: %v", err))
		} else {
			cfg.RetryBase = d
			retryBaseSet = true
		}
	}

	if v := os.Getenv("RELAY_RETRY_MAX"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_RETRY_MAX: %v", err))
		} else {
			cfg.RetryMax = d
		}
	}

	// Legacy RELAY_RETRY_DELAY: overrides RetryBase when RELAY_RETRY_BASE is unset,
	// and always populates RetryDelay for fixed-policy callers.
	if v := os.Getenv("RELAY_RETRY_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_RETRY_DELAY: %v", err))
		} else {
			cfg.RetryDelay = d
			if !retryBaseSet {
				cfg.RetryBase = d
			}
		}
	} else {
		cfg.RetryDelay = cfg.RetryBase
	}

	if v := os.Getenv("RELAY_REAP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_REAP_INTERVAL: %v", err))
		} else {
			cfg.ReapInterval = d
		}
	}
	if v := os.Getenv("RELAY_REAP_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_REAP_BATCH: %v", err))
		} else {
			cfg.ReapBatch = n
		}
	}
	if v := os.Getenv("RELAY_PURGE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_PURGE_INTERVAL: %v", err))
		} else {
			cfg.PurgeInterval = d
		}
	}
	if v := os.Getenv("RELAY_PURGE_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_PURGE_BATCH: %v", err))
		} else {
			cfg.PurgeBatch = n
		}
	}
	if v := os.Getenv("RELAY_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_RETENTION: %v", err))
		} else {
			cfg.Retention = d
		}
	}
	if v := os.Getenv("RELAY_STATS_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_STATS_RETENTION: %v", err))
		} else {
			cfg.StatsRetention = d
		}
	}
	if v := os.Getenv("RELAY_LEASE_MAX_WAIT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_LEASE_MAX_WAIT: %v", err))
		} else {
			cfg.LeaseMaxWait = d
		}
	}

	if v := os.Getenv("RELAY_SHUTDOWN_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_SHUTDOWN_TIMEOUT: %v", err))
		} else {
			cfg.ShutdownTimeout = d
		}
	}

	switch cfg.Storage {
	case StoragePostgres:
		if cfg.PostgresURL == "" {
			errs = append(errs, "RELAY_POSTGRES_URL: required when RELAY_STORAGE=postgres")
		}
	case StorageRedis:
		if cfg.RedisURL == "" {
			errs = append(errs, "RELAY_REDIS_URL: required when RELAY_STORAGE=redis")
		}
	default:
		errs = append(errs, fmt.Sprintf("RELAY_STORAGE: must be %q or %q, got %q", StoragePostgres, StorageRedis, cfg.Storage))
	}

	switch cfg.RetryPolicy {
	case RetryPolicyExponential, RetryPolicyFixed:
	default:
		errs = append(errs, fmt.Sprintf("RELAY_RETRY_POLICY: must be %q or %q, got %q", RetryPolicyExponential, RetryPolicyFixed, cfg.RetryPolicy))
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("RELAY_LOG_LEVEL: must be debug|info|warn|error, got %q", cfg.LogLevel))
	}

	switch cfg.LogFormat {
	case "json", "text":
	default:
		errs = append(errs, fmt.Sprintf("RELAY_LOG_FORMAT: must be json|text, got %q", cfg.LogFormat))
	}

	if cfg.RetryBase <= 0 {
		errs = append(errs, "RELAY_RETRY_BASE: must be greater than 0")
	}
	if cfg.RetryMax <= 0 {
		errs = append(errs, "RELAY_RETRY_MAX: must be greater than 0")
	}
	if cfg.RetryDelay <= 0 {
		errs = append(errs, "RELAY_RETRY_DELAY: must be greater than 0")
	}
	if cfg.ReapInterval <= 0 {
		errs = append(errs, "RELAY_REAP_INTERVAL: must be greater than 0")
	}
	if cfg.ReapBatch <= 0 {
		errs = append(errs, "RELAY_REAP_BATCH: must be greater than 0")
	}
	if cfg.PurgeInterval <= 0 {
		errs = append(errs, "RELAY_PURGE_INTERVAL: must be greater than 0")
	}
	if cfg.PurgeBatch <= 0 {
		errs = append(errs, "RELAY_PURGE_BATCH: must be greater than 0")
	}
	if cfg.Retention < 0 {
		errs = append(errs, "RELAY_RETENTION: must be >= 0")
	}
	if cfg.StatsRetention < 0 {
		errs = append(errs, "RELAY_STATS_RETENTION: must be >= 0")
	}
	if cfg.LeaseMaxWait <= 0 {
		errs = append(errs, "RELAY_LEASE_MAX_WAIT: must be greater than 0")
	}
	if cfg.ShutdownTimeout <= 0 {
		errs = append(errs, "RELAY_SHUTDOWN_TIMEOUT: must be greater than 0")
	}
	if cfg.AuthCacheTTL <= 0 {
		errs = append(errs, "RELAY_AUTH_CACHE_TTL: must be greater than 0")
	}
	if cfg.SchedulerTick <= 0 {
		errs = append(errs, "RELAY_SCHEDULER_TICK: must be greater than 0")
	}
	if cfg.GRPCAddr == "" {
		errs = append(errs, "RELAY_GRPC_ADDR: must not be empty")
	}
	if cfg.AdminAddr == "" {
		errs = append(errs, "RELAY_ADMIN_ADDR: must not be empty")
	}
	if cfg.MetricsMaxSeries <= 0 {
		errs = append(errs, "RELAY_METRICS_MAX_SERIES: must be greater than 0")
	}
	if cfg.TraceSampleRatio < 0 || cfg.TraceSampleRatio > 1 {
		errs = append(errs, "RELAY_TRACE_SAMPLE_RATIO: must be between 0 and 1 inclusive")
	}
	if cfg.Environment == "" {
		errs = append(errs, "RELAY_ENVIRONMENT: must not be empty")
	}
	if cfg.InstanceID == "" {
		errs = append(errs, "RELAY_INSTANCE_ID: must not be empty")
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
