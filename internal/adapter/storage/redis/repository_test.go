//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	storageredis "github.com/MEHRDADsmj/relay/internal/adapter/storage/redis"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/storagetest"
	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestRedisRepository_Contract(t *testing.T) {
	// Ryuk (reaper) is flaky on Docker Desktop for Windows; disable so the suite
	// can start Redis reliably. Containers are still terminated in Cleanup.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(terminateCtx)
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	opts, err := goredis.ParseURL(uri)
	require.NoError(t, err)
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })

	require.NoError(t, client.Ping(ctx).Err())

	storagetest.RunContractTests(t, func(t *testing.T) app.JobRepository {
		t.Helper()
		require.NoError(t, client.FlushDB(ctx).Err())
		return storageredis.New(client, "relay")
	})
}
