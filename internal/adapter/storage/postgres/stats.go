package postgres

import (
	"context"
	"time"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

var (
	_ app.StatsRepository       = (*Repository)(nil)
	_ app.MaintenanceRepository = (*Repository)(nil)
)

func incrQueueStats(ctx context.Context, q *sqlcgen.Queries, queue string, now time.Time, enqueued, succeeded, failed, dead int64) error {
	if queue == "" || (enqueued == 0 && succeeded == 0 && failed == 0 && dead == 0) {
		return nil
	}
	return q.IncrQueueStats(ctx, sqlcgen.IncrQueueStatsParams{
		Queue:     queue,
		Now:       now.UTC(),
		Enqueued:  enqueued,
		Succeeded: succeeded,
		Failed:    failed,
		Dead:      dead,
	})
}

// Throughput implements app.StatsRepository.
func (r *Repository) Throughput(ctx context.Context, queue string, from, to time.Time) ([]domain.ThroughputPoint, error) {
	rows, err := r.q.ListQueueStats(ctx, sqlcgen.ListQueueStatsParams{
		Queue:      queue,
		FromMinute: from.UTC().Truncate(time.Minute),
		ToMinute:   to.UTC().Truncate(time.Minute),
	})
	if err != nil {
		return nil, err
	}
	out := make([]domain.ThroughputPoint, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.ThroughputPoint{
			Minute:    row.Minute.UTC(),
			Enqueued:  row.Enqueued,
			Succeeded: row.Succeeded,
			Failed:    row.Failed,
			Dead:      row.Dead,
		})
	}
	return out, nil
}

// PurgeStats implements app.MaintenanceRepository.
func (r *Repository) PurgeStats(ctx context.Context, before time.Time) (int, error) {
	n, err := r.q.PurgeQueueStats(ctx, before.UTC())
	return int(n), err
}
