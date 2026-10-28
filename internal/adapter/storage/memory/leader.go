package memory

import (
	"context"
	"sync"

	"github.com/MEHRDADsmj/relay/internal/app"
)

// LeaderElector is an in-process mutex-based leader elector for tests.
type LeaderElector struct {
	mu      sync.Mutex
	holders map[string]*leaderHold
}

type leaderHold struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewLeaderElector returns an empty in-memory leader elector.
func NewLeaderElector() *LeaderElector {
	return &LeaderElector{holders: make(map[string]*leaderHold)}
}

var _ app.LeaderElector = (*LeaderElector)(nil)

// Campaign blocks until leadership for name is acquired or ctx is done.
func (e *LeaderElector) Campaign(ctx context.Context, name string) (context.Context, func(), error) {
	for {
		e.mu.Lock()
		if hold, ok := e.holders[name]; !ok {
			leaderCtx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			e.holders[name] = &leaderHold{cancel: cancel, done: done}
			e.mu.Unlock()

			resign := func() {
				e.mu.Lock()
				cur, ok := e.holders[name]
				if ok && cur.done == done {
					delete(e.holders, name)
					cancel()
					close(done)
				}
				e.mu.Unlock()
			}
			go func() {
				select {
				case <-ctx.Done():
					resign()
				case <-done:
				}
			}()
			return leaderCtx, resign, nil
		} else {
			wait := hold.done
			e.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-wait:
			}
		}
	}
}
