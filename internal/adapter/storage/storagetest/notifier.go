package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunNotifierContract exercises JobNotifier push wakeups.
func RunNotifierContract(t *testing.T, newRepo func(t *testing.T) Backend) {
	t.Helper()

	t.Run("p2_notifier_create_wakeup", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 4, 10, 0, 0, 0, time.UTC)

		chA, unsubA := repo.Subscribe(ctx, "na")
		defer unsubA()
		chB, unsubB := repo.Subscribe(ctx, "nb")
		defer unsubB()

		jobA := sampleJob(mustParse("018f5555-0000-7000-8000-000000000001"), "na", "t", now)
		_, _, err := repo.Create(ctx, jobA)
		require.NoError(t, err)

		select {
		case <-chA:
		case <-time.After(time.Second):
			t.Fatal("expected notification on queue na within 1s")
		}

		select {
		case <-chB:
			t.Fatal("unexpected notification on queue nb")
		case <-time.After(50 * time.Millisecond):
		}

		jobB := sampleJob(mustParse("018f5555-0000-7000-8000-000000000002"), "nb", "t", now)
		jobB.RunAt = now.Add(time.Hour)
		_, _, err = repo.Create(ctx, jobB)
		require.NoError(t, err)

		select {
		case <-chB:
			t.Fatal("scheduled job should not notify")
		case <-time.After(50 * time.Millisecond):
		}

		unsubA()
		// After unsubscribe, channel is closed.
		_, ok := <-chA
		assert.False(t, ok)
	})
}
