package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// ErrPhase3NotImplemented is returned by schedule/API-key stubs until P3-T3.
var ErrPhase3NotImplemented = errors.New("postgres: schedules/api keys not implemented (P3-T3)")

var (
	_ app.ScheduleRepository = (*Repository)(nil)
	_ app.APIKeyRepository   = (*Repository)(nil)
)

func (r *Repository) CreateSchedule(context.Context, domain.Schedule) (domain.Schedule, error) {
	return domain.Schedule{}, ErrPhase3NotImplemented
}

func (r *Repository) GetSchedule(context.Context, domain.ScheduleID) (domain.Schedule, error) {
	return domain.Schedule{}, ErrPhase3NotImplemented
}

func (r *Repository) GetScheduleByName(context.Context, string) (domain.Schedule, error) {
	return domain.Schedule{}, ErrPhase3NotImplemented
}

func (r *Repository) ListSchedules(context.Context, domain.PageRequest) (domain.Page[domain.Schedule], error) {
	return domain.Page[domain.Schedule]{}, ErrPhase3NotImplemented
}

func (r *Repository) UpdateSchedule(context.Context, domain.Schedule, time.Time) (domain.Schedule, error) {
	return domain.Schedule{}, ErrPhase3NotImplemented
}

func (r *Repository) DeleteSchedule(context.Context, domain.ScheduleID) error {
	return ErrPhase3NotImplemented
}

func (r *Repository) DueSchedules(context.Context, time.Time, int) ([]domain.Schedule, error) {
	return nil, ErrPhase3NotImplemented
}

func (r *Repository) MarkFired(context.Context, domain.ScheduleID, time.Time, time.Time, time.Time, domain.JobID) error {
	return ErrPhase3NotImplemented
}

func (r *Repository) CreateAPIKey(context.Context, domain.APIKey) (domain.APIKey, error) {
	return domain.APIKey{}, ErrPhase3NotImplemented
}

func (r *Repository) GetAPIKey(context.Context, string) (domain.APIKey, error) {
	return domain.APIKey{}, ErrPhase3NotImplemented
}

func (r *Repository) GetAPIKeyByPrefix(context.Context, string) (domain.APIKey, error) {
	return domain.APIKey{}, ErrPhase3NotImplemented
}

func (r *Repository) ListAPIKeys(context.Context) ([]domain.APIKey, error) {
	return nil, ErrPhase3NotImplemented
}

func (r *Repository) RevokeAPIKey(context.Context, string, time.Time) (domain.APIKey, error) {
	return domain.APIKey{}, ErrPhase3NotImplemented
}

func (r *Repository) UpdateLastUsed(context.Context, string, time.Time) error {
	return ErrPhase3NotImplemented
}
