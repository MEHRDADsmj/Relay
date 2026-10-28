//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func BenchmarkPostgresClaim(b *testing.B) {
	b.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("relay"),
		tcpostgres.WithUsername("relay"),
		tcpostgres.WithPassword("relay"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(b, err)
	b.Cleanup(func() {
		terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(terminateCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(b, err)
	require.NoError(b, postgres.Migrate(ctx, dsn))

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(b, err)
	b.Cleanup(pool.Close)

	repo := postgres.New(pool)
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
