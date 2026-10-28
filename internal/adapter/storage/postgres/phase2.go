package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Release implements app.JobRepository.
func (r *Repository) Release(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := r.q.WithTx(tx)
	row, err := qtx.ReleaseJob(ctx, sqlcgen.ReleaseJobParams{
		Now:   now,
		ID:    id,
		Owner: nullStr(owner),
	})
	if err != nil {
		return r.mapConditionalErrTx(ctx, qtx, id, err)
	}
	if err := closeRunningAttempt(ctx, qtx, id, domain.AttemptReleased, "", now); err != nil {
		return domain.Job{}, err
	}
	if err := notifyQueue(ctx, tx, row.Queue); err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return toDomain(row), nil
}

// Cancel implements app.JobRepository.
func (r *Repository) Cancel(ctx context.Context, id domain.JobID, now time.Time) (domain.Job, error) {
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := r.q.WithTx(tx)
	cur, err := qtx.GetJobForUpdate(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrNotFound
		}
		return domain.Job{}, err
	}
	job := toDomain(cur)
	if err := job.CanCancel(); err != nil {
		return domain.Job{}, err
	}

	var row sqlcgen.Job
	switch job.Status {
	case domain.StatusPending:
		row, err = qtx.CancelPendingJob(ctx, sqlcgen.CancelPendingJobParams{Now: now, ID: id})
	case domain.StatusRunning:
		row, err = qtx.CancelRunningJob(ctx, sqlcgen.CancelRunningJobParams{Now: now, ID: id})
	default:
		return domain.Job{}, domain.ErrInvalidTransition
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrInvalidTransition
		}
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return toDomain(row), nil
}

// Retry implements app.JobRepository.
func (r *Repository) Retry(ctx context.Context, id domain.JobID, now time.Time) (domain.Job, error) {
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := r.q.WithTx(tx)
	cur, err := qtx.GetJobForUpdate(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrNotFound
		}
		return domain.Job{}, err
	}
	job := toDomain(cur)
	if err := job.CanRetry(); err != nil {
		return domain.Job{}, err
	}

	row, err := qtx.RetryJob(ctx, sqlcgen.RetryJobParams{Now: now, ID: id})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrInvalidTransition
		}
		return domain.Job{}, err
	}
	if err := notifyQueue(ctx, tx, row.Queue); err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return toDomain(row), nil
}

// ListAttempts implements app.JobRepository.
func (r *Repository) ListAttempts(ctx context.Context, id domain.JobID) ([]domain.Attempt, error) {
	if _, err := r.q.GetJob(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	rows, err := r.q.ListAttempts(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Attempt, len(rows))
	for i := range rows {
		out[i] = toAttempt(rows[i])
	}
	return out, nil
}

// ListByStatus implements app.JobRepository.
func (r *Repository) ListByStatus(ctx context.Context, queue string, status domain.Status, page domain.PageRequest) (domain.Page[domain.Job], error) {
	limit := page.Limit
	if limit <= 0 {
		limit = domain.DefaultPageLimit
	}

	params := sqlcgen.ListJobsByStatusParams{
		Queue:     queue,
		Status:    string(status),
		HasCursor: false,
		PageLimit: clampInt32(limit + 1),
	}
	if page.Cursor != "" {
		t, id, err := app.DecodeCursor(page.Cursor)
		if err != nil {
			return domain.Page[domain.Job]{}, err
		}
		params.HasCursor = true
		params.CursorUpdatedAt = t.UTC()
		params.CursorID = id
	}

	rows, err := r.q.ListJobsByStatus(ctx, params)
	if err != nil {
		return domain.Page[domain.Job]{}, err
	}

	nextCursor := ""
	if len(rows) > limit {
		last := rows[limit-1]
		enc, err := app.EncodeCursor(last.UpdatedAt.UTC(), last.ID)
		if err != nil {
			return domain.Page[domain.Job]{}, err
		}
		nextCursor = enc
		rows = rows[:limit]
	}

	items := make([]domain.Job, len(rows))
	for i := range rows {
		items[i] = toDomain(rows[i])
	}
	return domain.Page[domain.Job]{Items: items, NextCursor: nextCursor}, nil
}

// ListJobsBySchedule implements app.JobRepository.
func (r *Repository) ListJobsBySchedule(ctx context.Context, scheduleID domain.ScheduleID, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := r.q.ListJobsBySchedule(ctx, sqlcgen.ListJobsByScheduleParams{
		Prefix:    domain.FireIdempotencyPrefix(scheduleID),
		PageLimit: clampInt32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]domain.Job, len(rows))
	for i := range rows {
		out[i] = toDomain(rows[i])
	}
	return out, nil
}

// ListQueues implements app.QueueRepository.
func (r *Repository) ListQueues(ctx context.Context) ([]domain.Queue, error) {
	rows, err := r.q.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Queue, len(rows))
	for i := range rows {
		out[i] = toQueue(rows[i])
	}
	return out, nil
}

// Stats implements app.QueueRepository.
func (r *Repository) Stats(ctx context.Context, queue string, now time.Time) (domain.QueueStats, error) {
	now = now.UTC()
	qrow, err := r.q.GetQueue(ctx, queue)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.QueueStats{}, domain.ErrNotFound
		}
		return domain.QueueStats{}, err
	}

	stats := domain.QueueStats{Queue: queue, Paused: qrow.Paused}
	counts, err := r.q.QueueStatusCounts(ctx, queue)
	if err != nil {
		return domain.QueueStats{}, err
	}
	for _, c := range counts {
		switch domain.Status(c.Status) {
		case domain.StatusPending:
			stats.Pending = c.Count
		case domain.StatusRunning:
			stats.Running = c.Count
		case domain.StatusSucceeded:
			stats.Succeeded = c.Count
		case domain.StatusDead:
			stats.Dead = c.Count
		case domain.StatusCancelled:
			stats.Cancelled = c.Count
		}
	}

	scheduled, err := r.q.QueueScheduledCount(ctx, sqlcgen.QueueScheduledCountParams{Queue: queue, Now: now})
	if err != nil {
		return domain.QueueStats{}, err
	}
	stats.Scheduled = scheduled

	oldest, err := r.q.QueueOldestDuePending(ctx, sqlcgen.QueueOldestDuePendingParams{Queue: queue, Now: now})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return domain.QueueStats{}, err
	}
	if err == nil {
		age := now.Sub(oldest.UTC())
		if age < 0 {
			age = 0
		}
		stats.OldestPendingAge = age
	}
	return stats, nil
}

// SetPaused implements app.QueueRepository.
func (r *Repository) SetPaused(ctx context.Context, queue string, paused bool, now time.Time) (domain.Queue, error) {
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Queue{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := r.q.WithTx(tx)
	row, err := qtx.SetQueuePaused(ctx, sqlcgen.SetQueuePausedParams{
		Paused:    paused,
		UpdatedAt: now,
		Name:      queue,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Queue{}, domain.ErrNotFound
		}
		return domain.Queue{}, err
	}
	if !paused {
		if err := notifyQueue(ctx, tx, queue); err != nil {
			return domain.Queue{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Queue{}, err
	}
	return toQueue(row), nil
}

// ReapExpired implements app.MaintenanceRepository.
func (r *Repository) ReapExpired(ctx context.Context, now time.Time, limit int) (app.ReapResult, error) {
	if limit <= 0 {
		return app.ReapResult{}, nil
	}
	now = now.UTC()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return app.ReapResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := r.q.WithTx(tx)
	ids, err := qtx.ReapExpiredSelect(ctx, sqlcgen.ReapExpiredSelectParams{
		Now:        now,
		BatchLimit: clampInt32(limit),
	})
	if err != nil {
		return app.ReapResult{}, err
	}

	var res app.ReapResult
	notified := make(map[string]struct{})
	for _, id := range ids {
		if err := closeRunningAttempt(ctx, qtx, id, domain.AttemptExpired, "lease expired", now); err != nil {
			return app.ReapResult{}, err
		}
		dead, err := qtx.ReapExpiredDead(ctx, sqlcgen.ReapExpiredDeadParams{Now: now, ID: id})
		if err == nil {
			res.Dead++
			if err := incrQueueStats(ctx, qtx, dead.Queue, now, 0, 0, 0, 1); err != nil {
				return app.ReapResult{}, err
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return app.ReapResult{}, err
		}
		row, err := qtx.ReapExpiredRequeue(ctx, sqlcgen.ReapExpiredRequeueParams{Now: now, ID: id})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return app.ReapResult{}, err
		}
		res.Requeued++
		notified[row.Queue] = struct{}{}
	}
	for q := range notified {
		if err := notifyQueue(ctx, tx, q); err != nil {
			return app.ReapResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return app.ReapResult{}, err
	}
	return res, nil
}

// PurgeFinished implements app.MaintenanceRepository.
func (r *Repository) PurgeFinished(ctx context.Context, before time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	before = before.UTC()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := r.q.WithTx(tx)
	ids, err := qtx.PurgeFinishedSelect(ctx, sqlcgen.PurgeFinishedSelectParams{
		Before:     before,
		BatchLimit: clampInt32(limit),
	})
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	// Copy for sqlc []uuid.UUID
	out := make([]uuid.UUID, len(ids))
	copy(out, ids)
	if err := qtx.DeleteJobsByIDs(ctx, out); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

// Subscribe implements app.JobNotifier.
func (r *Repository) Subscribe(ctx context.Context, queue string) (<-chan struct{}, func()) {
	if r.notifier == nil {
		ch := make(chan struct{})
		return ch, func() { close(ch) }
	}
	return r.notifier.Subscribe(ctx, queue)
}
