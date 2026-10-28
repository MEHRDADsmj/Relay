package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

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
	HTTPAddr        string
	Storage         string
	PostgresURL     string
	RedisURL        string
	RedisKeyPrefix  string
	MigrateOnStart  bool
	APIKey          string
	RetryPolicy     string
	RetryBase       time.Duration
	RetryMax        time.Duration
	RetryDelay      time.Duration // legacy alias for fixed delay / RetryBase override
	ReapInterval    time.Duration
	ReapBatch       int
	PurgeInterval   time.Duration
	PurgeBatch      int
	Retention       time.Duration
	LeaseMaxWait    time.Duration
	ShutdownTimeout time.Duration
	LogLevel        string
	LogFormat       string
}

// Load reads configuration from environment variables and validates it.
// All validation errors are collected and returned together.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:       envOr("RELAY_HTTP_ADDR", ":7420"),
		Storage:        strings.ToLower(envOr("RELAY_STORAGE", StoragePostgres)),
		PostgresURL:    os.Getenv("RELAY_POSTGRES_URL"),
		RedisURL:       os.Getenv("RELAY_REDIS_URL"),
		RedisKeyPrefix: envOr("RELAY_REDIS_KEY_PREFIX", "relay"),
		APIKey:         os.Getenv("RELAY_API_KEY"),
		LogLevel:       strings.ToLower(envOr("RELAY_LOG_LEVEL", "info")),
		LogFormat:      strings.ToLower(envOr("RELAY_LOG_FORMAT", "json")),
		RetryPolicy:    strings.ToLower(envOr("RELAY_RETRY_POLICY", RetryPolicyExponential)),
		RetryBase:      time.Second,
		RetryMax:       time.Hour,
		RetryDelay:     5 * time.Second, // Phase 1 default; remapped below if unset overrides
		ReapInterval:   5 * time.Second,
		ReapBatch:      500,
		PurgeInterval:  time.Minute,
		PurgeBatch:     1000,
		Retention:      168 * time.Hour,
		LeaseMaxWait:   30 * time.Second,
		// ShutdownTimeout default = LeaseMaxWait + 15s margin (BaseContext still cancels long-polls on SIGTERM).
		ShutdownTimeout: 45 * time.Second,
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
	if cfg.LeaseMaxWait <= 0 {
		errs = append(errs, "RELAY_LEASE_MAX_WAIT: must be greater than 0")
	}
	if cfg.ShutdownTimeout <= 0 {
		errs = append(errs, "RELAY_SHUTDOWN_TIMEOUT: must be greater than 0")
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
