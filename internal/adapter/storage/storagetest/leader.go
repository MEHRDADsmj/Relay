package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/app"
)

// RunLeaderElectorContract verifies single-leader semantics for an elector factory.
func RunLeaderElectorContract(t *testing.T, newElector func(t *testing.T) app.LeaderElector) {
	t.Helper()

	t.Run("exactly_one_leader", func(t *testing.T) {
		e1 := newElector(t)
		e2 := newElector(t)
		// For in-memory, both must share state — callers should return the same instance.
		// Contract: Campaign on same elector name yields one leader.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		lead1, resign1, err := e1.Campaign(ctx, "scheduler")
		require.NoError(t, err)
		require.NoError(t, lead1.Err())

		second := make(chan struct{})
		go func() {
			lead2, resign2, err := e2.Campaign(ctx, "scheduler")
			require.NoError(t, err)
			resign2()
			_ = lead2
			close(second)
		}()

		select {
		case <-second:
			t.Fatal("second leader acquired while first held lock")
		case <-time.After(50 * time.Millisecond):
		}

		resign1()
		select {
		case <-second:
		case <-time.After(5 * time.Second):
			t.Fatal("second leader did not acquire within 5s after resign")
		}
	})
}
