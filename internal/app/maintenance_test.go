package app_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/app"
)

type fakeMaintRepo struct {
	mu         sync.Mutex
	reapCalls  int
	purgeCalls int
	reap       app.ReapResult
	purgeN     int
}

func (f *fakeMaintRepo) ReapExpired(context.Context, time.Time, int) (app.ReapResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reapCalls++
	return f.reap, nil
}

func (f *fakeMaintRepo) PurgeFinished(context.Context, time.Time, int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purgeCalls++
	return f.purgeN, nil
}

func TestMaintenance_RunStopsOnCancel(t *testing.T) {
	t.Parallel()
	repo := &fakeMaintRepo{reap: app.ReapResult{Requeued: 1}, purgeN: 2}
	clock := &fixedClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := app.NewMaintenance(repo, clock, app.MaintenanceConfig{
		ReapInterval:  20 * time.Millisecond,
		PurgeInterval: 20 * time.Millisecond,
		ReapBatch:     10,
		PurgeBatch:    10,
		Retention:     time.Hour,
	}, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance did not stop")
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.GreaterOrEqual(t, repo.reapCalls, 1)
	assert.GreaterOrEqual(t, repo.purgeCalls, 1)
}

func TestMaintenance_RetentionZeroSkipsPurge(t *testing.T) {
	t.Parallel()
	repo := &fakeMaintRepo{purgeN: 5}
	clock := &fixedClock{now: time.Now().UTC()}
	m := app.NewMaintenance(repo, clock, app.MaintenanceConfig{
		ReapInterval:  time.Hour,
		PurgeInterval: 10 * time.Millisecond,
		Retention:     0,
		ReapBatch:     1,
		PurgeBatch:    1,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = m.Run(ctx) }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Equal(t, 0, repo.purgeCalls)
}
