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

func TestPostgresRepository_Phase3Contract(t *testing.T) {
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

	repo := postgres.New(pool)
	storagetest.RunPhase3Contract(t, func(t *testing.T) storagetest.ScheduleAPI {
		t.Helper()
		_, err := pool.Exec(ctx, "TRUNCATE TABLE schedules, api_keys")
		require.NoError(t, err)
		return repo
	})
}

func TestPostgresLeaderElector_Contract(t *testing.T) {
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

	storagetest.RunLeaderElectorContract(t, func(t *testing.T) app.LeaderElector {
		t.Helper()
		return postgres.NewLeaderElector(pool)
	})
}

func TestPostgresLeaderElector_KillConnection(t *testing.T) {
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

	campaignCtx, cancelCampaign := context.WithCancel(context.Background())
	defer cancelCampaign()

	e1 := postgres.NewLeaderElector(pool)
	e2 := postgres.NewLeaderElector(pool)

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
		t.Fatal("second leader acquired while first held lock")
	case <-time.After(100 * time.Millisecond):
	}

	var pid int32
	err = pool.QueryRow(ctx, `
		SELECT l.pid
		FROM pg_locks l
		WHERE l.locktype = 'advisory' AND l.granted
		LIMIT 1
	`).Scan(&pid)
	require.NoError(t, err)
	require.NotZero(t, pid)

	var terminated bool
	err = pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated)
	require.NoError(t, err)
	require.True(t, terminated)

	select {
	case <-lead1.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("leader context was not cancelled after connection kill")
	}

	select {
	case <-second:
	case <-time.After(10 * time.Second):
		t.Fatal("second leader did not acquire within 10s after kill")
	}
}
