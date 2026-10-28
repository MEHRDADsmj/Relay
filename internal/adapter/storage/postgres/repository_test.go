//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/storagetest"
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

	notifier, err := postgres.NewNotifier(ctx, pool)
	require.NoError(t, err)
	t.Cleanup(notifier.Close)

	repo := postgres.New(pool)
	repo.AttachNotifier(notifier)

	storagetest.RunContractTests(t, func(t *testing.T) storagetest.Backend {
		t.Helper()
		_, err := pool.Exec(ctx, "TRUNCATE TABLE jobs, queues CASCADE")
		require.NoError(t, err)
		return repo
	})
}

func TestPostgresMigration_RoundTripPhase1Seed(t *testing.T) {
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

	// Apply all migrations, roll back to Phase 1 (00001), seed, then up through latest.
	require.NoError(t, postgres.Migrate(ctx, dsn))
	require.NoError(t, postgres.MigrateDown(ctx, dsn)) // down 00003
	require.NoError(t, postgres.MigrateDown(ctx, dsn)) // down 00002 → Phase 1 schema

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	id := uuid.Must(uuid.NewV7())
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id, queue, type, payload, status, priority, run_at, attempts, max_attempts,
			created_at, updated_at
		) VALUES ($1, 'seed', 'email.send', '{}'::jsonb, 'pending', 0, $2, 0, 3, $2, $2)
	`, id, now)
	require.NoError(t, err)

	require.NoError(t, postgres.Migrate(ctx, dsn)) // up 00002 (+00003) with Phase 1 data present

	var (
		cancelRequested bool
		queueExists     bool
	)
	err = pool.QueryRow(ctx, `SELECT cancel_requested FROM jobs WHERE id = $1`, id).Scan(&cancelRequested)
	require.NoError(t, err)
	require.False(t, cancelRequested)

	err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM queues WHERE name = 'seed')`).Scan(&queueExists)
	require.NoError(t, err)
	require.True(t, queueExists, "00002 should backfill queues from existing jobs")

	require.NoError(t, postgres.MigrateDown(ctx, dsn))
	require.NoError(t, postgres.Migrate(ctx, dsn))

	var status string
	err = pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, id).Scan(&status)
	require.NoError(t, err)
	require.Equal(t, "pending", status)
}
