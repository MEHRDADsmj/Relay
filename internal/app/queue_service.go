package app

import (
	"context"
	"fmt"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Allowed Throughput windows.
const (
	ThroughputWindow15m = 15 * time.Minute
	ThroughputWindow1h  = time.Hour
	ThroughputWindow24h = 24 * time.Hour
)

// QueueService orchestrates queue registry, stats, pause/resume, and bulk dead retry.
type QueueService struct {
	queues QueueRepository
	jobs   JobRepository
	stats  StatsRepository
	clock  Clock
}

// NewQueueService constructs a QueueService.
func NewQueueService(queues QueueRepository, jobs JobRepository, stats StatsRepository, clock Clock) *QueueService {
	return &QueueService{queues: queues, jobs: jobs, stats: stats, clock: clock}
}

// ListQueues returns all registered queues.
func (s *QueueService) ListQueues(ctx context.Context) ([]domain.Queue, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return nil, err
	}
	return s.queues.ListQueues(ctx)
}

// Stats returns aggregate counts for a queue.
func (s *QueueService) Stats(ctx context.Context, queue string) (domain.QueueStats, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.QueueStats{}, err
	}
	if err := domain.ValidateQueue(queue); err != nil {
		return domain.QueueStats{}, err
	}
	return s.queues.Stats(ctx, queue, s.clock.Now())
}

// Throughput returns zero-filled minutely counters for window ∈ {15m, 1h, 24h}.
func (s *QueueService) Throughput(ctx context.Context, queue string, window time.Duration) ([]domain.ThroughputPoint, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return nil, err
	}
	if err := domain.ValidateQueue(queue); err != nil {
		return nil, err
	}
	if err := validateThroughputWindow(window); err != nil {
		return nil, err
	}
	if s.stats == nil {
		return nil, fmt.Errorf("stats repository not configured")
	}
	to := s.clock.Now().UTC().Truncate(time.Minute)
	from := to.Add(-window)
	raw, err := s.stats.Throughput(ctx, queue, from, to)
	if err != nil {
		return nil, err
	}
	return zeroFillThroughput(raw, from, to), nil
}

func validateThroughputWindow(window time.Duration) error {
	switch window {
	case ThroughputWindow15m, ThroughputWindow1h, ThroughputWindow24h:
		return nil
	default:
		return &domain.ValidationError{Field: "window", Message: "must be 15m, 1h, or 24h"}
	}
}

func zeroFillThroughput(raw []domain.ThroughputPoint, from, to time.Time) []domain.ThroughputPoint {
	byMin := make(map[int64]domain.ThroughputPoint, len(raw))
	for _, p := range raw {
		m := p.Minute.UTC().Truncate(time.Minute)
		byMin[m.Unix()] = domain.ThroughputPoint{
			Minute:    m,
			Enqueued:  p.Enqueued,
			Succeeded: p.Succeeded,
			Failed:    p.Failed,
			Dead:      p.Dead,
		}
	}
	n := int(to.Sub(from)/time.Minute) + 1
	if n < 0 {
		n = 0
	}
	out := make([]domain.ThroughputPoint, 0, n)
	for t := from; !t.After(to); t = t.Add(time.Minute) {
		if p, ok := byMin[t.Unix()]; ok {
			out = append(out, p)
			continue
		}
		out = append(out, domain.ThroughputPoint{Minute: t})
	}
	return out
}

// Pause marks a queue as paused (claims return empty).
func (s *QueueService) Pause(ctx context.Context, queue string) (domain.Queue, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Queue{}, err
	}
	if err := domain.ValidateQueue(queue); err != nil {
		return domain.Queue{}, err
	}
	return s.queues.SetPaused(ctx, queue, true, s.clock.Now())
}

// Resume marks a queue as not paused.
func (s *QueueService) Resume(ctx context.Context, queue string) (domain.Queue, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Queue{}, err
	}
	if err := domain.ValidateQueue(queue); err != nil {
		return domain.Queue{}, err
	}
	return s.queues.SetPaused(ctx, queue, false, s.clock.Now())
}

// RetryAllDead pages through dead jobs and retries each, up to limit.
func (s *QueueService) RetryAllDead(ctx context.Context, cmd RetryAllDeadCommand) (int, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return 0, err
	}
	if err := domain.ValidateQueue(cmd.Queue); err != nil {
		return 0, err
	}
	limit, err := domain.NormalizeRetryAllDeadLimit(cmd.Limit)
	if err != nil {
		return 0, err
	}
	retried := 0
	for retried < limit {
		pageLimit := 50
		remaining := limit - retried
		if remaining < pageLimit {
			pageLimit = remaining
		}
		// Always take the first page: retried jobs leave dead status.
		page, err := s.jobs.ListByStatus(ctx, cmd.Queue, domain.StatusDead, domain.PageRequest{
			Limit: pageLimit,
		})
		if err != nil {
			return retried, fmt.Errorf("list dead: %w", err)
		}
		if len(page.Items) == 0 {
			break
		}
		for _, j := range page.Items {
			if _, err := s.jobs.Retry(ctx, j.ID, s.clock.Now()); err != nil {
				return retried, fmt.Errorf("retry %s: %w", j.ID, err)
			}
			retried++
			if retried >= limit {
				return retried, nil
			}
		}
	}
	return retried, nil
}
