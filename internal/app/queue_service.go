package app

import (
	"context"
	"fmt"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// QueueService orchestrates queue registry, stats, pause/resume, and bulk dead retry.
type QueueService struct {
	queues QueueRepository
	jobs   JobRepository
	clock  Clock
}

// NewQueueService constructs a QueueService.
func NewQueueService(queues QueueRepository, jobs JobRepository, clock Clock) *QueueService {
	return &QueueService{queues: queues, jobs: jobs, clock: clock}
}

// ListQueues returns all registered queues.
func (s *QueueService) ListQueues(ctx context.Context) ([]domain.Queue, error) {
	return s.queues.ListQueues(ctx)
}

// Stats returns aggregate counts for a queue.
func (s *QueueService) Stats(ctx context.Context, queue string) (domain.QueueStats, error) {
	if err := domain.ValidateQueue(queue); err != nil {
		return domain.QueueStats{}, err
	}
	return s.queues.Stats(ctx, queue, s.clock.Now())
}

// Pause marks a queue as paused (claims return empty).
func (s *QueueService) Pause(ctx context.Context, queue string) (domain.Queue, error) {
	if err := domain.ValidateQueue(queue); err != nil {
		return domain.Queue{}, err
	}
	return s.queues.SetPaused(ctx, queue, true, s.clock.Now())
}

// Resume marks a queue as not paused.
func (s *QueueService) Resume(ctx context.Context, queue string) (domain.Queue, error) {
	if err := domain.ValidateQueue(queue); err != nil {
		return domain.Queue{}, err
	}
	return s.queues.SetPaused(ctx, queue, false, s.clock.Now())
}

// RetryAllDead pages through dead jobs and retries each, up to limit.
func (s *QueueService) RetryAllDead(ctx context.Context, cmd RetryAllDeadCommand) (int, error) {
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
