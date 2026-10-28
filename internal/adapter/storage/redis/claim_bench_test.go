//go:build integration

package redis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	storageredis "github.com/MEHRDADsmj/relay/internal/adapter/storage/redis"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func BenchmarkRedisClaim(b *testing.B) {
	b.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(b, err)
	b.Cleanup(func() {
		terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(terminateCtx)
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(b, err)
	opts, err := goredis.ParseURL(uri)
	require.NoError(b, err)
	client := goredis.NewClient(opts)
	b.Cleanup(func() { _ = client.Close() })
	require.NoError(b, client.Ping(ctx).Err())

	repo := storageredis.New(client, "relay")
	const queue = "bench-claim"
	now := time.Now().UTC()

	refill := func(n int) {
		b.Helper()
		for i := 0; i < n; i++ {
			job, err := domain.NewJob(domain.NewJobParams{
				Queue:   queue,
				Type:    "bench",
				Payload: []byte(`{}`),
			}, uuid.New(), now)
			require.NoError(b, err)
			_, _, err = repo.Create(ctx, job)
			require.NoError(b, err)
		}
	}
	refill(5000)

	b.ReportAllocs()
	b.ResetTimer()
	claimed := 0
	for i := 0; claimed < b.N; i++ {
		batch, err := repo.Claim(ctx, queue, fmt.Sprintf("w-%d", i%8), 10, 30*time.Second, now)
		if err != nil {
			b.Fatal(err)
		}
		if len(batch) == 0 {
			b.StopTimer()
			refill(500)
			b.StartTimer()
			continue
		}
		claimed += len(batch)
	}
}
