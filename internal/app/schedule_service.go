package app

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// CreateScheduleCommand is the input for ScheduleService.Create.
type CreateScheduleCommand struct {
	Name        string
	Cron        string
	Timezone    string
	Queue       string
	JobType     string
	Payload     []byte
	Priority    *int
	MaxAttempts *int
	Enabled     *bool
}

// UpdateScheduleCommand is the input for ScheduleService.Update (optimistic concurrency).
type UpdateScheduleCommand struct {
	ID                domain.ScheduleID
	ExpectedUpdatedAt time.Time
	Cron              *string
	Timezone          *string
	Queue             *string
	JobType           *string
	Payload           []byte // nil = leave unchanged; non-nil replaces (use empty slice for {})
	Priority          *int
	MaxAttempts       *int
	Enabled           *bool
	SetPayload        bool // true when Payload should be applied (including empty JSON)
}

// ScheduleService orchestrates schedule CRUD and manual triggers.
type ScheduleService struct {
	repo   ScheduleRepository
	jobs   *JobService
	parser CronParser
	clock  Clock
	ids    IDGenerator
}

// NewScheduleService constructs a ScheduleService.
func NewScheduleService(repo ScheduleRepository, jobs *JobService, parser CronParser, clock Clock, ids IDGenerator) *ScheduleService {
	return &ScheduleService{repo: repo, jobs: jobs, parser: parser, clock: clock, ids: ids}
}

// Create validates and persists a new schedule.
func (s *ScheduleService) Create(ctx context.Context, cmd CreateScheduleCommand) (domain.Schedule, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Schedule{}, err
	}
	now := s.clock.Now()
	sched, err := domain.NewSchedule(domain.NewScheduleParams{
		Name:        cmd.Name,
		Cron:        cmd.Cron,
		Timezone:    cmd.Timezone,
		Queue:       cmd.Queue,
		JobType:     cmd.JobType,
		Payload:     cmd.Payload,
		Priority:    cmd.Priority,
		MaxAttempts: cmd.MaxAttempts,
		Enabled:     cmd.Enabled,
	}, s.ids.NewID(), now, s.parser)
	if err != nil {
		return domain.Schedule{}, err
	}
	stored, err := s.repo.CreateSchedule(ctx, sched)
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("create schedule: %w", err)
	}
	return stored, nil
}

// Get returns a schedule by ID.
func (s *ScheduleService) Get(ctx context.Context, id domain.ScheduleID) (domain.Schedule, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Schedule{}, err
	}
	return s.repo.GetSchedule(ctx, id)
}

// GetByName returns a schedule by unique name.
func (s *ScheduleService) GetByName(ctx context.Context, name string) (domain.Schedule, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Schedule{}, err
	}
	return s.repo.GetScheduleByName(ctx, name)
}

// List returns schedules ordered by name.
func (s *ScheduleService) List(ctx context.Context, page domain.PageRequest) (domain.Page[domain.Schedule], error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Page[domain.Schedule]{}, err
	}
	limit, err := domain.NormalizePageLimit(page.Limit)
	if err != nil {
		return domain.Page[domain.Schedule]{}, err
	}
	page.Limit = limit
	return s.repo.ListSchedules(ctx, page)
}

// Update applies a partial update with optimistic concurrency on UpdatedAt.
func (s *ScheduleService) Update(ctx context.Context, cmd UpdateScheduleCommand) (domain.Schedule, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Schedule{}, err
	}
	cur, err := s.repo.GetSchedule(ctx, cmd.ID)
	if err != nil {
		return domain.Schedule{}, err
	}
	return s.applyUpdate(ctx, cur, cmd)
}

// Enable sets enabled=true.
func (s *ScheduleService) Enable(ctx context.Context, id domain.ScheduleID) (domain.Schedule, error) {
	return s.setEnabled(ctx, id, true)
}

// Disable sets enabled=false.
func (s *ScheduleService) Disable(ctx context.Context, id domain.ScheduleID) (domain.Schedule, error) {
	return s.setEnabled(ctx, id, false)
}

func (s *ScheduleService) setEnabled(ctx context.Context, id domain.ScheduleID, enabled bool) (domain.Schedule, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Schedule{}, err
	}
	cur, err := s.repo.GetSchedule(ctx, id)
	if err != nil {
		return domain.Schedule{}, err
	}
	en := enabled
	return s.applyUpdate(ctx, cur, UpdateScheduleCommand{
		ID:                id,
		ExpectedUpdatedAt: cur.UpdatedAt,
		Enabled:           &en,
	})
}

func (s *ScheduleService) applyUpdate(ctx context.Context, cur domain.Schedule, cmd UpdateScheduleCommand) (domain.Schedule, error) {
	if !cur.UpdatedAt.Equal(cmd.ExpectedUpdatedAt) {
		return domain.Schedule{}, domain.ErrConflict
	}

	params := domain.NewScheduleParams{
		Name:        cur.Name,
		Cron:        cur.Cron,
		Timezone:    cur.Timezone,
		Queue:       cur.Queue,
		JobType:     cur.JobType,
		Payload:     cur.Payload,
		Priority:    &cur.Priority,
		MaxAttempts: &cur.MaxAttempts,
		Enabled:     &cur.Enabled,
	}
	if cmd.Cron != nil {
		params.Cron = *cmd.Cron
	}
	if cmd.Timezone != nil {
		params.Timezone = *cmd.Timezone
	}
	if cmd.Queue != nil {
		params.Queue = *cmd.Queue
	}
	if cmd.JobType != nil {
		params.JobType = *cmd.JobType
	}
	if cmd.SetPayload {
		params.Payload = cmd.Payload
	}
	if cmd.Priority != nil {
		params.Priority = cmd.Priority
	}
	if cmd.MaxAttempts != nil {
		params.MaxAttempts = cmd.MaxAttempts
	}
	if cmd.Enabled != nil {
		params.Enabled = cmd.Enabled
	}

	now := s.clock.Now()
	updated, err := domain.NewSchedule(params, cur.ID, now, s.parser)
	if err != nil {
		return domain.Schedule{}, err
	}
	// Preserve created_at and last-fire metadata; NewSchedule resets them.
	updated.CreatedAt = cur.CreatedAt
	updated.LastRunAt = cur.LastRunAt
	updated.LastJobID = cur.LastJobID
	// Keep NextRunAt from recompute unless disabled→enabled needs future next — NewSchedule already set NextRunAt from now.

	stored, err := s.repo.UpdateSchedule(ctx, updated, cmd.ExpectedUpdatedAt)
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("update schedule: %w", err)
	}
	return stored, nil
}

// Delete removes a schedule.
func (s *ScheduleService) Delete(ctx context.Context, id domain.ScheduleID) error {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return err
	}
	return s.repo.DeleteSchedule(ctx, id)
}

// TriggerNow enqueues a job immediately with a unique manual idempotency key.
func (s *ScheduleService) TriggerNow(ctx context.Context, id domain.ScheduleID) (domain.Job, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.Job{}, err
	}
	sched, err := s.repo.GetSchedule(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	manualID, err := uuid.NewV7()
	if err != nil {
		manualID = uuid.New()
	}
	key := fmt.Sprintf("schedule:%s:manual:%s", id.String(), manualID.String())
	prio := sched.Priority
	maxA := sched.MaxAttempts
	job, _, err := s.jobs.Enqueue(ctx, EnqueueCommand{
		Queue:          sched.Queue,
		Type:           sched.JobType,
		Payload:        sched.Payload,
		Priority:       &prio,
		MaxAttempts:    &maxA,
		IdempotencyKey: key,
	})
	if err != nil {
		return domain.Job{}, fmt.Errorf("trigger enqueue: %w", err)
	}
	return job, nil
}
