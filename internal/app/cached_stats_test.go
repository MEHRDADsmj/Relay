package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestCachedQueueStats_Snapshot(t *testing.T) {
	t.Parallel()
	repo := memory.New()
	clock := &fixedClock{now: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	js := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, nil, nil)
	qs := app.NewQueueService(repo, repo, repo, clock)
	_, _, err := js.Enqueue(adminCtx(), app.EnqueueCommand{Queue: "q1", Type: "t", Payload: []byte(`{}`)})
	require.NoError(t, err)

	cache := app.NewCachedQueueStats(qs, time.Hour)
	ctx := app.WithPrincipal(context.Background(), app.BootstrapAdmin)
	first, err := cache.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, "q1", first[0].Queue)

	_, _, err = js.Enqueue(adminCtx(), app.EnqueueCommand{Queue: "q2", Type: "t", Payload: []byte(`{}`)})
	require.NoError(t, err)
	second, err := cache.Snapshot(ctx)
	require.NoError(t, err)
	assert.Len(t, second, 1, "stale cache should not see q2 yet")

	cache.Invalidate()
	third, err := cache.Snapshot(ctx)
	require.NoError(t, err)
	assert.Len(t, third, 2)
}
