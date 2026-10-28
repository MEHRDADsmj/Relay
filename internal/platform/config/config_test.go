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
	require.Equal(t, ":7421", cfg.GRPCAddr)
	require.Equal(t, ":7429", cfg.AdminAddr)
	require.Equal(t, 1000, cfg.MetricsMaxSeries)
	require.False(t, cfg.GRPCReflection)
	require.Equal(t, StoragePostgres, cfg.Storage)
	require.Equal(t, "relay", cfg.RedisKeyPrefix)
	require.False(t, cfg.MigrateOnStart)
	require.Empty(t, cfg.APIKey)
	require.Equal(t, 30*time.Second, cfg.AuthCacheTTL)
	require.Equal(t, RetryPolicyExponential, cfg.RetryPolicy)
	require.Equal(t, time.Second, cfg.RetryBase)
	require.Equal(t, time.Hour, cfg.RetryMax)
	require.Equal(t, time.Second, cfg.RetryDelay) // mirrors RetryBase when RELAY_RETRY_DELAY unset
	require.Equal(t, 5*time.Second, cfg.ReapInterval)
	require.Equal(t, 500, cfg.ReapBatch)
	require.Equal(t, time.Minute, cfg.PurgeInterval)
	require.Equal(t, 1000, cfg.PurgeBatch)
	require.Equal(t, 168*time.Hour, cfg.Retention)
	require.Equal(t, 30*time.Second, cfg.LeaseMaxWait)
	require.Equal(t, 45*time.Second, cfg.ShutdownTimeout)
	require.True(t, cfg.SchedulerEnabled)
	require.Equal(t, time.Second, cfg.SchedulerTick)
	require.NotEmpty(t, cfg.InstanceID)
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
	t.Setenv("RELAY_RETRY_POLICY", "linear")
	t.Setenv("RELAY_RETRY_BASE", "0s")
	t.Setenv("RELAY_RETRY_MAX", "0s")
	t.Setenv("RELAY_RETRY_DELAY", "0s")
	t.Setenv("RELAY_REAP_INTERVAL", "0s")
	t.Setenv("RELAY_REAP_BATCH", "0")
	t.Setenv("RELAY_PURGE_INTERVAL", "0s")
	t.Setenv("RELAY_PURGE_BATCH", "0")
	t.Setenv("RELAY_LEASE_MAX_WAIT", "0s")
	t.Setenv("RELAY_SHUTDOWN_TIMEOUT", "-1s")

	_, err := Load()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "RELAY_STORAGE")
	require.Contains(t, msg, "RELAY_LOG_LEVEL")
	require.Contains(t, msg, "RELAY_LOG_FORMAT")
	require.Contains(t, msg, "RELAY_RETRY_POLICY")
	require.Contains(t, msg, "RELAY_RETRY_BASE")
	require.Contains(t, msg, "RELAY_RETRY_MAX")
	require.Contains(t, msg, "RELAY_RETRY_DELAY")
	require.Contains(t, msg, "RELAY_REAP_INTERVAL")
	require.Contains(t, msg, "RELAY_REAP_BATCH")
	require.Contains(t, msg, "RELAY_PURGE_INTERVAL")
	require.Contains(t, msg, "RELAY_PURGE_BATCH")
	require.Contains(t, msg, "RELAY_LEASE_MAX_WAIT")
	require.Contains(t, msg, "RELAY_SHUTDOWN_TIMEOUT")
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("RELAY_HTTP_ADDR", ":9000")
	t.Setenv("RELAY_STORAGE", "redis")
	t.Setenv("RELAY_REDIS_URL", "redis://localhost:63799/0")
	t.Setenv("RELAY_REDIS_KEY_PREFIX", "acme")
	t.Setenv("RELAY_MIGRATE_ON_START", "true")
	t.Setenv("RELAY_API_KEY", "secret")
	t.Setenv("RELAY_RETRY_POLICY", "fixed")
	t.Setenv("RELAY_RETRY_BASE", "2s")
	t.Setenv("RELAY_RETRY_MAX", "10m")
	t.Setenv("RELAY_RETRY_DELAY", "3s") // base already set; Delay still recorded
	t.Setenv("RELAY_REAP_INTERVAL", "1s")
	t.Setenv("RELAY_REAP_BATCH", "100")
	t.Setenv("RELAY_PURGE_INTERVAL", "30s")
	t.Setenv("RELAY_PURGE_BATCH", "50")
	t.Setenv("RELAY_RETENTION", "24h")
	t.Setenv("RELAY_LEASE_MAX_WAIT", "15s")
	t.Setenv("RELAY_SHUTDOWN_TIMEOUT", "30s")
	t.Setenv("RELAY_GRPC_ADDR", ":9421")
	t.Setenv("RELAY_ADMIN_ADDR", ":9429")
	t.Setenv("RELAY_METRICS_MAX_SERIES", "50")
	t.Setenv("RELAY_GRPC_REFLECTION", "true")
	t.Setenv("RELAY_SCHEDULER_ENABLED", "false")
	t.Setenv("RELAY_SCHEDULER_TICK", "2s")
	t.Setenv("RELAY_INSTANCE_ID", "node-a")
	t.Setenv("RELAY_AUTH_CACHE_TTL", "15s")
	t.Setenv("RELAY_LOG_LEVEL", "debug")
	t.Setenv("RELAY_LOG_FORMAT", "text")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, ":9000", cfg.HTTPAddr)
	require.Equal(t, ":9421", cfg.GRPCAddr)
	require.Equal(t, ":9429", cfg.AdminAddr)
	require.Equal(t, 50, cfg.MetricsMaxSeries)
	require.True(t, cfg.GRPCReflection)
	require.Equal(t, StorageRedis, cfg.Storage)
	require.Equal(t, "redis://localhost:63799/0", cfg.RedisURL)
	require.Equal(t, "acme", cfg.RedisKeyPrefix)
	require.True(t, cfg.MigrateOnStart)
	require.Equal(t, "secret", cfg.APIKey)
	require.Equal(t, 15*time.Second, cfg.AuthCacheTTL)
	require.Equal(t, RetryPolicyFixed, cfg.RetryPolicy)
	require.Equal(t, 2*time.Second, cfg.RetryBase)
	require.Equal(t, 10*time.Minute, cfg.RetryMax)
	require.Equal(t, 3*time.Second, cfg.RetryDelay)
	require.Equal(t, time.Second, cfg.ReapInterval)
	require.Equal(t, 100, cfg.ReapBatch)
	require.Equal(t, 30*time.Second, cfg.PurgeInterval)
	require.Equal(t, 50, cfg.PurgeBatch)
	require.Equal(t, 24*time.Hour, cfg.Retention)
	require.Equal(t, 15*time.Second, cfg.LeaseMaxWait)
	require.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	require.False(t, cfg.SchedulerEnabled)
	require.Equal(t, 2*time.Second, cfg.SchedulerTick)
	require.Equal(t, "node-a", cfg.InstanceID)
	require.Equal(t, "debug", cfg.LogLevel)
	require.Equal(t, "text", cfg.LogFormat)
}

func TestLoad_LegacyRetryDelaySetsBase(t *testing.T) {
	t.Setenv("RELAY_POSTGRES_URL", "postgres://relay:relay@localhost:54329/relay?sslmode=disable")
	clearOptional(t)
	t.Setenv("RELAY_RETRY_DELAY", "7s")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 7*time.Second, cfg.RetryBase)
	require.Equal(t, 7*time.Second, cfg.RetryDelay)
}

func TestLoad_RetentionZeroAllowed(t *testing.T) {
	t.Setenv("RELAY_POSTGRES_URL", "postgres://relay:relay@localhost:54329/relay?sslmode=disable")
	clearOptional(t)
	t.Setenv("RELAY_RETENTION", "0s")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), cfg.Retention)
}

func clearOptional(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"RELAY_HTTP_ADDR",
		"RELAY_GRPC_ADDR",
		"RELAY_ADMIN_ADDR",
		"RELAY_METRICS_MAX_SERIES",
		"RELAY_GRPC_REFLECTION",
		"RELAY_STORAGE",
		"RELAY_REDIS_URL",
		"RELAY_REDIS_KEY_PREFIX",
		"RELAY_MIGRATE_ON_START",
		"RELAY_API_KEY",
		"RELAY_AUTH_CACHE_TTL",
		"RELAY_RETRY_POLICY",
		"RELAY_RETRY_BASE",
		"RELAY_RETRY_MAX",
		"RELAY_RETRY_DELAY",
		"RELAY_REAP_INTERVAL",
		"RELAY_REAP_BATCH",
		"RELAY_PURGE_INTERVAL",
		"RELAY_PURGE_BATCH",
		"RELAY_RETENTION",
		"RELAY_LEASE_MAX_WAIT",
		"RELAY_SHUTDOWN_TIMEOUT",
		"RELAY_SCHEDULER_ENABLED",
		"RELAY_SCHEDULER_TICK",
		"RELAY_INSTANCE_ID",
		"RELAY_LOG_LEVEL",
		"RELAY_LOG_FORMAT",
	} {
		t.Setenv(k, "")
	}
}
