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

// Config holds relay-server settings loaded from the environment.
type Config struct {
	HTTPAddr        string
	Storage         string
	PostgresURL     string
	RedisURL        string
	RedisKeyPrefix  string
	MigrateOnStart  bool
	APIKey          string
	RetryDelay      time.Duration
	ShutdownTimeout time.Duration
	LogLevel        string
	LogFormat       string
}

// Load reads configuration from environment variables and validates it.
// All validation errors are collected and returned together.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        envOr("RELAY_HTTP_ADDR", ":7420"),
		Storage:         strings.ToLower(envOr("RELAY_STORAGE", StoragePostgres)),
		PostgresURL:     os.Getenv("RELAY_POSTGRES_URL"),
		RedisURL:        os.Getenv("RELAY_REDIS_URL"),
		RedisKeyPrefix:  envOr("RELAY_REDIS_KEY_PREFIX", "relay"),
		APIKey:          os.Getenv("RELAY_API_KEY"),
		LogLevel:        strings.ToLower(envOr("RELAY_LOG_LEVEL", "info")),
		LogFormat:       strings.ToLower(envOr("RELAY_LOG_FORMAT", "json")),
		RetryDelay:      5 * time.Second,
		ShutdownTimeout: 15 * time.Second,
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

	if v := os.Getenv("RELAY_RETRY_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("RELAY_RETRY_DELAY: %v", err))
		} else {
			cfg.RetryDelay = d
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

	if cfg.RetryDelay <= 0 {
		errs = append(errs, "RELAY_RETRY_DELAY: must be greater than 0")
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
