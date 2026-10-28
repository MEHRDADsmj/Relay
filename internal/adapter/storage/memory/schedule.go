package memory

import (
	"context"
	"sort"
	"time"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func cloneSchedule(s domain.Schedule) domain.Schedule {
	out := s
	if s.Payload != nil {
		out.Payload = append([]byte(nil), s.Payload...)
	}
	if s.LastRunAt != nil {
		t := *s.LastRunAt
		out.LastRunAt = &t
	}
	if s.LastJobID != nil {
		id := *s.LastJobID
		out.LastJobID = &id
	}
	return out
}

// CreateSchedule implements app.ScheduleRepository.
func (r *Repository) CreateSchedule(_ context.Context, s domain.Schedule) (domain.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.schedName[s.Name]; ok {
		return domain.Schedule{}, domain.ErrConflict
	}
	stored := cloneSchedule(s)
	r.schedules[stored.ID] = stored
	r.schedName[stored.Name] = stored.ID
	return cloneSchedule(stored), nil
}

// GetSchedule implements app.ScheduleRepository.
func (r *Repository) GetSchedule(_ context.Context, id domain.ScheduleID) (domain.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.schedules[id]
	if !ok {
		return domain.Schedule{}, domain.ErrNotFound
	}
	return cloneSchedule(s), nil
}

// GetScheduleByName implements app.ScheduleRepository.
func (r *Repository) GetScheduleByName(_ context.Context, name string) (domain.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.schedName[name]
	if !ok {
		return domain.Schedule{}, domain.ErrNotFound
	}
	return cloneSchedule(r.schedules[id]), nil
}

// ListSchedules implements app.ScheduleRepository (ordered by name ASC).
func (r *Repository) ListSchedules(_ context.Context, page domain.PageRequest) (domain.Page[domain.Schedule], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	limit := page.Limit
	if limit <= 0 {
		limit = domain.DefaultPageLimit
	}

	var cursorName string
	var cursorID domain.ScheduleID
	hasCursor := page.Cursor != ""
	if hasCursor {
		name, id, err := app.DecodeNameCursor(page.Cursor)
		if err != nil {
			return domain.Page[domain.Schedule]{}, err
		}
		cursorName, cursorID = name, id
	}

	matched := make([]domain.Schedule, 0, len(r.schedules))
	for _, s := range r.schedules {
		if hasCursor {
			if s.Name < cursorName {
				continue
			}
			if s.Name == cursorName && s.ID.String() <= cursorID.String() {
				continue
			}
		}
		matched = append(matched, s)
	}
	sort.SliceStable(matched, func(i, k int) bool {
		a, b := matched[i], matched[k]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ID.String() < b.ID.String()
	})

	nextCursor := ""
	if len(matched) > limit {
		last := matched[limit-1]
		enc, err := app.EncodeNameCursor(last.Name, last.ID)
		if err != nil {
			return domain.Page[domain.Schedule]{}, err
		}
		nextCursor = enc
		matched = matched[:limit]
	}

	items := make([]domain.Schedule, len(matched))
	for i := range matched {
		items[i] = cloneSchedule(matched[i])
	}
	return domain.Page[domain.Schedule]{Items: items, NextCursor: nextCursor}, nil
}

// UpdateSchedule implements app.ScheduleRepository with optimistic concurrency.
func (r *Repository) UpdateSchedule(_ context.Context, s domain.Schedule, expectedUpdatedAt time.Time) (domain.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.schedules[s.ID]
	if !ok {
		return domain.Schedule{}, domain.ErrNotFound
	}
	if !cur.UpdatedAt.Equal(expectedUpdatedAt) {
		return domain.Schedule{}, domain.ErrConflict
	}
	if cur.Name != s.Name {
		if other, exists := r.schedName[s.Name]; exists && other != s.ID {
			return domain.Schedule{}, domain.ErrConflict
		}
		delete(r.schedName, cur.Name)
		r.schedName[s.Name] = s.ID
	}
	stored := cloneSchedule(s)
	r.schedules[stored.ID] = stored
	return cloneSchedule(stored), nil
}

// DeleteSchedule implements app.ScheduleRepository.
func (r *Repository) DeleteSchedule(_ context.Context, id domain.ScheduleID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.schedules[id]
	if !ok {
		return domain.ErrNotFound
	}
	delete(r.schedName, s.Name)
	delete(r.schedules, id)
	return nil
}

// DueSchedules implements app.ScheduleRepository.
func (r *Repository) DueSchedules(_ context.Context, now time.Time, limit int) ([]domain.Schedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	matched := make([]domain.Schedule, 0)
	for _, s := range r.schedules {
		if !s.Enabled {
			continue
		}
		if s.NextRunAt.After(now) {
			continue
		}
		matched = append(matched, s)
	}
	sort.SliceStable(matched, func(i, k int) bool {
		a, b := matched[i], matched[k]
		if !a.NextRunAt.Equal(b.NextRunAt) {
			return a.NextRunAt.Before(b.NextRunAt)
		}
		return a.ID.String() < b.ID.String()
	})
	if len(matched) > limit {
		matched = matched[:limit]
	}
	out := make([]domain.Schedule, len(matched))
	for i := range matched {
		out[i] = cloneSchedule(matched[i])
	}
	return out, nil
}

// MarkFired implements app.ScheduleRepository.
func (r *Repository) MarkFired(_ context.Context, id domain.ScheduleID, expectedNext, firedAt, next time.Time, jobID domain.JobID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.schedules[id]
	if !ok {
		return domain.ErrNotFound
	}
	if !s.NextRunAt.Equal(expectedNext) {
		return domain.ErrConflict
	}
	t := firedAt
	s.LastRunAt = &t
	jid := jobID
	s.LastJobID = &jid
	s.NextRunAt = next
	s.UpdatedAt = firedAt
	r.schedules[id] = s
	return nil
}
