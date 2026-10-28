package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Phase 2 methods are stubbed until P2-T4. They compile against the expanded ports
// but return errors so callers know the backend is not ready.

func (r *Repository) Release(context.Context, domain.JobID, string, time.Time) (domain.Job, error) {
	return domain.Job{}, fmt.Errorf("redis: Release not implemented")
}

func (r *Repository) Cancel(context.Context, domain.JobID, time.Time) (domain.Job, error) {
	return domain.Job{}, fmt.Errorf("redis: Cancel not implemented")
}

func (r *Repository) Retry(context.Context, domain.JobID, time.Time) (domain.Job, error) {
	return domain.Job{}, fmt.Errorf("redis: Retry not implemented")
}

func (r *Repository) ListAttempts(context.Context, domain.JobID) ([]domain.Attempt, error) {
	return nil, fmt.Errorf("redis: ListAttempts not implemented")
}

func (r *Repository) ListByStatus(context.Context, string, domain.Status, domain.PageRequest) (domain.Page[domain.Job], error) {
	return domain.Page[domain.Job]{}, fmt.Errorf("redis: ListByStatus not implemented")
}

func (r *Repository) ListQueues(context.Context) ([]domain.Queue, error) {
	return nil, fmt.Errorf("redis: ListQueues not implemented")
}

func (r *Repository) Stats(context.Context, string, time.Time) (domain.QueueStats, error) {
	return domain.QueueStats{}, fmt.Errorf("redis: Stats not implemented")
}

func (r *Repository) SetPaused(context.Context, string, bool, time.Time) (domain.Queue, error) {
	return domain.Queue{}, fmt.Errorf("redis: SetPaused not implemented")
}

func (r *Repository) ReapExpired(context.Context, time.Time, int) (app.ReapResult, error) {
	return app.ReapResult{}, fmt.Errorf("redis: ReapExpired not implemented")
}

func (r *Repository) PurgeFinished(context.Context, time.Time, int) (int, error) {
	return 0, fmt.Errorf("redis: PurgeFinished not implemented")
}

func (r *Repository) Subscribe(context.Context, string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() {}
}

var (
	_ app.JobRepository         = (*Repository)(nil)
	_ app.QueueRepository       = (*Repository)(nil)
	_ app.MaintenanceRepository = (*Repository)(nil)
	_ app.JobNotifier           = (*Repository)(nil)
)
