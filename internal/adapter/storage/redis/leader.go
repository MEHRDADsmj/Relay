package redis

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/MEHRDADsmj/relay/internal/app"
)

const (
	leaderLeaseTTL    = 15 * time.Second
	leaderRenewEvery  = 5 * time.Second
	leaderAcquirePoll = 2 * time.Second
)

// LeaderElector campaigns for leadership using a Redis key lease
// (SET NX PX). This is a lease, not a fencing-token lock; scheduler
// correctness relies on idempotency keys and MarkFired CAS.
type LeaderElector struct {
	client     *goredis.Client
	keys       Keys
	instanceID string
}

// NewLeaderElector returns a Redis-backed leader elector.
func NewLeaderElector(client *goredis.Client, prefix string) *LeaderElector {
	return &LeaderElector{
		client:     client,
		keys:       NewKeys(prefix),
		instanceID: uuid.NewString(),
	}
}

// WithInstanceID sets the lease holder identity (RELAY_INSTANCE_ID). Empty is ignored.
func (e *LeaderElector) WithInstanceID(id string) *LeaderElector {
	if id != "" {
		e.instanceID = id
	}
	return e
}

var _ app.LeaderElector = (*LeaderElector)(nil)

// Campaign blocks until leadership for name is acquired or ctx is done.
// The returned context is cancelled when leadership is lost (resign,
// parent cancel, or failed lease renewal). resign releases the lease.
func (e *LeaderElector) Campaign(ctx context.Context, name string) (context.Context, func(), error) {
	key := e.keys.Leader(name)
	ttlMs := leaderLeaseTTL.Milliseconds()

	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		ok, err := e.client.SetNX(ctx, key, e.instanceID, leaderLeaseTTL).Result()
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(leaderAcquirePoll):
				continue
			}
		}

		leaderCtx, cancel := context.WithCancel(context.Background())
		var once sync.Once
		resign := func() {
			once.Do(func() {
				cancel()
				_, _ = leaderReleaseScript.Run(context.Background(), e.client, []string{key}, e.instanceID).Result()
			})
		}

		go func() {
			ticker := time.NewTicker(leaderRenewEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					resign()
					return
				case <-leaderCtx.Done():
					return
				case <-ticker.C:
					renewCtx, renewCancel := context.WithTimeout(leaderCtx, leaderRenewEvery)
					res, err := leaderRenewScript.Run(renewCtx, e.client, []string{key}, e.instanceID, ttlMs).Int64()
					renewCancel()
					if err != nil || res == 0 {
						resign()
						return
					}
				}
			}
		}()

		return leaderCtx, resign, nil
	}
}
