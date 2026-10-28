//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/storagetest"
	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestPostgresRepository_Contract(t *testing.T) {
	// Ryuk (reaper) is flaky on Docker Desktop for Windows; disable so the suite
	// can start Postgres reliably. Containers are still terminated in Cleanup.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("relay"),
		tcpostgres.WithUsername("relay"),
		tcpostgres.WithPassword("relay"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(terminateCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	require.NoError(t, postgres.Migrate(ctx, dsn))

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	storagetest.RunContractTests(t, func(t *testing.T) app.JobRepository {
		t.Helper()
		_, err := pool.Exec(ctx, "TRUNCATE TABLE jobs")
		require.NoError(t, err)
		return postgres.New(pool)
	})
}
