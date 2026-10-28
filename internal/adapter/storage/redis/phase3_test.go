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

func TestRedisRepository_Phase3Contract(t *testing.T) {
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

	repo := storageredis.New(client, "relay")
	storagetest.RunPhase3Contract(t, func(t *testing.T) storagetest.ScheduleAPI {
		t.Helper()
		require.NoError(t, client.FlushDB(ctx).Err())
		return repo
	})
}

func TestRedisLeaderElector_Contract(t *testing.T) {
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

	storagetest.RunLeaderElectorContract(t, func(t *testing.T) app.LeaderElector {
		t.Helper()
		return storageredis.NewLeaderElector(client, "relay")
	})
}

func TestRedisLeaderElector_LeaseLost(t *testing.T) {
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

	campaignCtx, cancelCampaign := context.WithCancel(context.Background())
	defer cancelCampaign()

	e1 := storageredis.NewLeaderElector(client, "relay")
	e2 := storageredis.NewLeaderElector(client, "relay")

	lead1, resign1, err := e1.Campaign(campaignCtx, "scheduler-kill")
	require.NoError(t, err)
	defer resign1()
	require.NoError(t, lead1.Err())

	second := make(chan struct{})
	go func() {
		lead2, resign2, err := e2.Campaign(campaignCtx, "scheduler-kill")
		require.NoError(t, err)
		resign2()
		_ = lead2
		close(second)
	}()

	select {
	case <-second:
		t.Fatal("second leader acquired while first held lease")
	case <-time.After(100 * time.Millisecond):
	}

	// Simulate hard failure: delete the lease key so renewal fails.
	n, err := client.Del(ctx, "relay:leader:scheduler-kill").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	select {
	case <-lead1.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("leader context was not cancelled after lease key deleted")
	}

	select {
	case <-second:
	case <-time.After(10 * time.Second):
		t.Fatal("second leader did not acquire within 10s after lease loss")
	}
}
