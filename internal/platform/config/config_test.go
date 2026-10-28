package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("RELAY_POSTGRES_URL", "postgres://relay:relay@localhost:54329/relay?sslmode=disable")
	clearOptional(t)

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, ":7420", cfg.HTTPAddr)
	require.Equal(t, StoragePostgres, cfg.Storage)
	require.Equal(t, "relay", cfg.RedisKeyPrefix)
	require.False(t, cfg.MigrateOnStart)
	require.Empty(t, cfg.APIKey)
	require.Equal(t, 5*time.Second, cfg.RetryDelay)
	require.Equal(t, 15*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, "json", cfg.LogFormat)
}

func TestLoad_RedisRequiresURL(t *testing.T) {
	t.Setenv("RELAY_STORAGE", "redis")
	t.Setenv("RELAY_REDIS_URL", "")
	t.Setenv("RELAY_POSTGRES_URL", "")

	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "RELAY_REDIS_URL")
}

func TestLoad_PostgresRequiresURL(t *testing.T) {
	t.Setenv("RELAY_STORAGE", "postgres")
	t.Setenv("RELAY_POSTGRES_URL", "")

	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "RELAY_POSTGRES_URL")
}

func TestLoad_CollectsAllErrors(t *testing.T) {
	t.Setenv("RELAY_STORAGE", "mongo")
	t.Setenv("RELAY_POSTGRES_URL", "")
	t.Setenv("RELAY_LOG_LEVEL", "trace")
	t.Setenv("RELAY_LOG_FORMAT", "xml")
	t.Setenv("RELAY_RETRY_DELAY", "0s")
	t.Setenv("RELAY_SHUTDOWN_TIMEOUT", "-1s")

	_, err := Load()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "RELAY_STORAGE")
	require.Contains(t, msg, "RELAY_LOG_LEVEL")
	require.Contains(t, msg, "RELAY_LOG_FORMAT")
	require.Contains(t, msg, "RELAY_RETRY_DELAY")
	require.Contains(t, msg, "RELAY_SHUTDOWN_TIMEOUT")
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("RELAY_HTTP_ADDR", ":9000")
	t.Setenv("RELAY_STORAGE", "redis")
	t.Setenv("RELAY_REDIS_URL", "redis://localhost:63799/0")
	t.Setenv("RELAY_REDIS_KEY_PREFIX", "acme")
	t.Setenv("RELAY_MIGRATE_ON_START", "true")
	t.Setenv("RELAY_API_KEY", "secret")
	t.Setenv("RELAY_RETRY_DELAY", "2s")
	t.Setenv("RELAY_SHUTDOWN_TIMEOUT", "30s")
	t.Setenv("RELAY_LOG_LEVEL", "debug")
	t.Setenv("RELAY_LOG_FORMAT", "text")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, ":9000", cfg.HTTPAddr)
	require.Equal(t, StorageRedis, cfg.Storage)
	require.Equal(t, "redis://localhost:63799/0", cfg.RedisURL)
	require.Equal(t, "acme", cfg.RedisKeyPrefix)
	require.True(t, cfg.MigrateOnStart)
	require.Equal(t, "secret", cfg.APIKey)
	require.Equal(t, 2*time.Second, cfg.RetryDelay)
	require.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, "debug", cfg.LogLevel)
	require.Equal(t, "text", cfg.LogFormat)
}

func clearOptional(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"RELAY_HTTP_ADDR",
		"RELAY_STORAGE",
		"RELAY_REDIS_URL",
		"RELAY_REDIS_KEY_PREFIX",
		"RELAY_MIGRATE_ON_START",
		"RELAY_API_KEY",
		"RELAY_RETRY_DELAY",
		"RELAY_SHUTDOWN_TIMEOUT",
		"RELAY_LOG_LEVEL",
		"RELAY_LOG_FORMAT",
	} {
		t.Setenv(k, "")
	}
}
