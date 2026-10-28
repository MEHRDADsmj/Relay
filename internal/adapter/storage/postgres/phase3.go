package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

var (
	_ app.ScheduleRepository = (*Repository)(nil)
	_ app.APIKeyRepository   = (*Repository)(nil)
)

// CreateSchedule implements app.ScheduleRepository.
func (r *Repository) CreateSchedule(ctx context.Context, s domain.Schedule) (domain.Schedule, error) {
	row, err := r.q.CreateSchedule(ctx, toCreateScheduleParams(s))
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Schedule{}, domain.ErrConflict
		}
		return domain.Schedule{}, err
	}
	return toDomainSchedule(row), nil
}

// GetSchedule implements app.ScheduleRepository.
func (r *Repository) GetSchedule(ctx context.Context, id domain.ScheduleID) (domain.Schedule, error) {
	row, err := r.q.GetSchedule(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Schedule{}, domain.ErrNotFound
		}
		return domain.Schedule{}, err
	}
	return toDomainSchedule(row), nil
}

// GetScheduleByName implements app.ScheduleRepository.
func (r *Repository) GetScheduleByName(ctx context.Context, name string) (domain.Schedule, error) {
	row, err := r.q.GetScheduleByName(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Schedule{}, domain.ErrNotFound
		}
		return domain.Schedule{}, err
	}
	return toDomainSchedule(row), nil
}

// ListSchedules implements app.ScheduleRepository (ordered by name ASC).
func (r *Repository) ListSchedules(ctx context.Context, page domain.PageRequest) (domain.Page[domain.Schedule], error) {
	limit := page.Limit
	if limit <= 0 {
		limit = domain.DefaultPageLimit
	}

	params := sqlcgen.ListSchedulesParams{
		PageLimit: clampInt32(limit + 1),
	}
	if page.Cursor != "" {
		name, id, err := app.DecodeNameCursor(page.Cursor)
		if err != nil {
			return domain.Page[domain.Schedule]{}, err
		}
		params.CursorName = &name
		params.CursorID = &id
	}

	rows, err := r.q.ListSchedules(ctx, params)
	if err != nil {
		return domain.Page[domain.Schedule]{}, err
	}

	nextCursor := ""
	if len(rows) > limit {
		last := rows[limit-1]
		enc, err := app.EncodeNameCursor(last.Name, last.ID)
		if err != nil {
			return domain.Page[domain.Schedule]{}, err
		}
		nextCursor = enc
		rows = rows[:limit]
	}

	items := make([]domain.Schedule, len(rows))
	for i := range rows {
		items[i] = toDomainSchedule(rows[i])
	}
	return domain.Page[domain.Schedule]{Items: items, NextCursor: nextCursor}, nil
}

// UpdateSchedule implements app.ScheduleRepository with optimistic concurrency.
func (r *Repository) UpdateSchedule(ctx context.Context, s domain.Schedule, expectedUpdatedAt time.Time) (domain.Schedule, error) {
	row, err := r.q.UpdateSchedule(ctx, toUpdateScheduleParams(s, expectedUpdatedAt))
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Schedule{}, domain.ErrConflict
		}
		if errors.Is(err, pgx.ErrNoRows) {
			_, getErr := r.q.GetSchedule(ctx, s.ID)
			if getErr != nil {
				if errors.Is(getErr, pgx.ErrNoRows) {
					return domain.Schedule{}, domain.ErrNotFound
				}
				return domain.Schedule{}, getErr
			}
			return domain.Schedule{}, domain.ErrConflict
		}
		return domain.Schedule{}, err
	}
	return toDomainSchedule(row), nil
}

// DeleteSchedule implements app.ScheduleRepository.
func (r *Repository) DeleteSchedule(ctx context.Context, id domain.ScheduleID) error {
	n, err := r.q.DeleteSchedule(ctx, id)
	if err != nil {
		return err
	}
	if n == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// DueSchedules implements app.ScheduleRepository.
func (r *Repository) DueSchedules(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.q.DueSchedules(ctx, sqlcgen.DueSchedulesParams{
		Now:        now.UTC(),
		BatchLimit: clampInt32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]domain.Schedule, len(rows))
	for i := range rows {
		out[i] = toDomainSchedule(rows[i])
	}
	return out, nil
}

// MarkFired implements app.ScheduleRepository.
func (r *Repository) MarkFired(ctx context.Context, id domain.ScheduleID, expectedNext, firedAt, next time.Time, jobID domain.JobID) error {
	jid := jobID
	_, err := r.q.MarkFired(ctx, sqlcgen.MarkFiredParams{
		FiredAt:      timePtr(firedAt),
		JobID:        &jid,
		NextRunAt:    next.UTC(),
		ID:           id,
		ExpectedNext: expectedNext.UTC(),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_, getErr := r.q.GetSchedule(ctx, id)
			if getErr != nil {
				if errors.Is(getErr, pgx.ErrNoRows) {
					return domain.ErrNotFound
				}
				return getErr
			}
			return domain.ErrConflict
		}
		return err
	}
	return nil
}

// CreateAPIKey implements app.APIKeyRepository.
func (r *Repository) CreateAPIKey(ctx context.Context, key domain.APIKey) (domain.APIKey, error) {
	params, err := toCreateAPIKeyParams(key)
	if err != nil {
		return domain.APIKey{}, err
	}
	row, err := r.q.CreateAPIKey(ctx, params)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.APIKey{}, domain.ErrConflict
		}
		return domain.APIKey{}, err
	}
	return toDomainAPIKey(row), nil
}

// GetAPIKey implements app.APIKeyRepository.
func (r *Repository) GetAPIKey(ctx context.Context, id string) (domain.APIKey, error) {
	uid, err := parseAPIKeyID(id)
	if err != nil {
		return domain.APIKey{}, err
	}
	row, err := r.q.GetAPIKey(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.APIKey{}, domain.ErrNotFound
		}
		return domain.APIKey{}, err
	}
	return toDomainAPIKey(row), nil
}

// GetAPIKeyByPrefix implements app.APIKeyRepository.
func (r *Repository) GetAPIKeyByPrefix(ctx context.Context, prefix string) (domain.APIKey, error) {
	row, err := r.q.GetAPIKeyByPrefix(ctx, prefix)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.APIKey{}, domain.ErrNotFound
		}
		return domain.APIKey{}, err
	}
	return toDomainAPIKey(row), nil
}

// ListAPIKeys implements app.APIKeyRepository.
func (r *Repository) ListAPIKeys(ctx context.Context) ([]domain.APIKey, error) {
	rows, err := r.q.ListAPIKeys(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.APIKey, len(rows))
	for i := range rows {
		out[i] = toDomainAPIKey(rows[i])
	}
	return out, nil
}

// RevokeAPIKey implements app.APIKeyRepository.
func (r *Repository) RevokeAPIKey(ctx context.Context, id string, revokedAt time.Time) (domain.APIKey, error) {
	uid, err := parseAPIKeyID(id)
	if err != nil {
		return domain.APIKey{}, err
	}
	row, err := r.q.RevokeAPIKey(ctx, sqlcgen.RevokeAPIKeyParams{
		RevokedAt: timePtr(revokedAt),
		ID:        uid,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.APIKey{}, domain.ErrNotFound
		}
		return domain.APIKey{}, err
	}
	return toDomainAPIKey(row), nil
}

// UpdateLastUsed implements app.APIKeyRepository.
func (r *Repository) UpdateLastUsed(ctx context.Context, id string, at time.Time) error {
	uid, err := parseAPIKeyID(id)
	if err != nil {
		return err
	}
	n, err := r.q.UpdateAPIKeyLastUsed(ctx, sqlcgen.UpdateAPIKeyLastUsedParams{
		LastUsedAt: timePtr(at),
		ID:         uid,
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func parseAPIKeyID(id string) (uuid.UUID, error) {
	uid, err := uuid.Parse(id)
	if err != nil || uid == uuid.Nil {
		return uuid.Nil, fmt.Errorf("postgres api key id: %w", domain.ErrNotFound)
	}
	return uid, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
