package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MEHRDADsmj/relay/internal/app"
)

const (
	leaderAcquirePoll = 2 * time.Second
	leaderPingEvery   = 5 * time.Second
)

// LeaderElector campaigns for leadership using a Postgres session advisory lock
// held on a dedicated pool connection for the leadership term.
type LeaderElector struct {
	pool *pgxpool.Pool
}

// NewLeaderElector returns a Postgres-backed leader elector.
func NewLeaderElector(pool *pgxpool.Pool) *LeaderElector {
	return &LeaderElector{pool: pool}
}

var _ app.LeaderElector = (*LeaderElector)(nil)

// Campaign blocks until leadership for name is acquired or ctx is done.
// The returned context is cancelled when leadership is lost (resign, parent
// cancel, or connection failure). resign releases the advisory lock.
func (e *LeaderElector) Campaign(ctx context.Context, name string) (context.Context, func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		conn, err := e.pool.Acquire(ctx)
		if err != nil {
			return nil, nil, err
		}

		var locked bool
		err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, name).Scan(&locked)
		if err != nil {
			conn.Release()
			return nil, nil, err
		}
		if !locked {
			conn.Release()
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
				// Best-effort unlock; a dead connection releases the lock on disconnect.
				_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, name)
				conn.Release()
			})
		}

		go func() {
			ticker := time.NewTicker(leaderPingEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					resign()
					return
				case <-leaderCtx.Done():
					return
				case <-ticker.C:
					pingCtx, pingCancel := context.WithTimeout(leaderCtx, leaderPingEvery)
					err := conn.Ping(pingCtx)
					pingCancel()
					if err != nil {
						resign()
						return
					}
				}
			}
		}()

		return leaderCtx, resign, nil
	}
}
