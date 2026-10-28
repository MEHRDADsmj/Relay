package app

import (
	"context"
	"sync"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

const defaultCachedStatsTTL = 2 * time.Second

// QueueStatsReader is the read surface cached for the dashboard SSE.
type QueueStatsReader interface {
	ListQueues(ctx context.Context) ([]domain.Queue, error)
	Stats(ctx context.Context, queue string) (domain.QueueStats, error)
}

// CachedQueueStats caches ListQueues+Stats snapshots for live dashboard updates.
// It is independent of the Prometheus collector (plan §3.2).
type CachedQueueStats struct {
	source QueueStatsReader
	ttl    time.Duration
	clock  func() time.Time

	mu       sync.Mutex
	cachedAt time.Time
	cached   []domain.QueueStats
}

// NewCachedQueueStats wraps source with a short TTL cache.
func NewCachedQueueStats(source QueueStatsReader, ttl time.Duration) *CachedQueueStats {
	if ttl <= 0 {
		ttl = defaultCachedStatsTTL
	}
	return &CachedQueueStats{
		source: source,
		ttl:    ttl,
		clock:  time.Now,
	}
}

// Snapshot returns per-queue stats, refreshing when the cache is stale.
func (c *CachedQueueStats) Snapshot(ctx context.Context) ([]domain.QueueStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if len(c.cached) > 0 && now.Sub(c.cachedAt) < c.ttl {
		out := make([]domain.QueueStats, len(c.cached))
		copy(out, c.cached)
		return out, nil
	}
	queues, err := c.source.ListQueues(ctx)
	if err != nil {
		if len(c.cached) > 0 {
			out := make([]domain.QueueStats, len(c.cached))
			copy(out, c.cached)
			return out, nil
		}
		return nil, err
	}
	out := make([]domain.QueueStats, 0, len(queues))
	for _, q := range queues {
		st, err := c.source.Stats(ctx, q.Name)
		if err != nil {
			if len(c.cached) > 0 {
				cached := make([]domain.QueueStats, len(c.cached))
				copy(cached, c.cached)
				return cached, nil
			}
			return nil, err
		}
		out = append(out, st)
	}
	c.cached = out
	c.cachedAt = now
	copied := make([]domain.QueueStats, len(out))
	copy(copied, out)
	return copied, nil
}

// Invalidate drops the cache (e.g. after pause/resume).
func (c *CachedQueueStats) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cached = nil
	c.cachedAt = time.Time{}
}
