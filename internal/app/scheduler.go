package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

const (
	defaultSchedulerTick  = time.Second
	defaultSchedulerBatch = 100
	schedulerLockName     = "scheduler"
)

// SchedulerConfig tunes the scheduler loop.
type SchedulerConfig struct {
	Tick  time.Duration // default 1s
	Batch int           // default 100
}

// Scheduler fires due schedules while holding cluster leadership.
type Scheduler struct {
	schedules ScheduleRepository
	jobs      *JobService
	elector   LeaderElector
	parser    CronParser
	clock     Clock
	log       *slog.Logger
	cfg       SchedulerConfig
}

// NewScheduler constructs a Scheduler. log may be nil.
func NewScheduler(
	schedules ScheduleRepository,
	jobs *JobService,
	elector LeaderElector,
	parser CronParser,
	clock Clock,
	cfg SchedulerConfig,
	log *slog.Logger,
) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Tick <= 0 {
		cfg.Tick = defaultSchedulerTick
	}
	if cfg.Batch <= 0 {
		cfg.Batch = defaultSchedulerBatch
	}
	return &Scheduler{
		schedules: schedules,
		jobs:      jobs,
		elector:   elector,
		parser:    parser,
		clock:     clock,
		log:       log,
		cfg:       cfg,
	}
}

// SystemPrincipal is used by background components (scheduler) when calling scoped services.
var SystemPrincipal = domain.Principal{
	KeyID:  "system",
	Name:   "system",
	Scopes: []domain.Scope{domain.ScopeAdmin},
}

// Run campaigns for leadership and fires due schedules until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		leaderCtx, resign, err := s.elector.Campaign(ctx, schedulerLockName)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			s.log.ErrorContext(ctx, "scheduler campaign failed", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.cfg.Tick):
				continue
			}
		}
		s.log.InfoContext(ctx, "scheduler leadership acquired")
		err = s.lead(leaderCtx)
		resign()
		s.log.InfoContext(ctx, "scheduler leadership lost")
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			s.log.ErrorContext(ctx, "scheduler lead loop error", "error", err)
		}
	}
}

func (s *Scheduler) lead(leaderCtx context.Context) error {
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-leaderCtx.Done():
			return leaderCtx.Err()
		case <-ticker.C:
			if err := s.Tick(leaderCtx, s.clock.Now()); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				s.log.ErrorContext(leaderCtx, "scheduler tick failed", "error", err)
			}
		}
	}
}

// Tick processes due schedules once (exported for tests with a fake clock).
func (s *Scheduler) Tick(ctx context.Context, now time.Time) error {
	due, err := s.schedules.DueSchedules(ctx, now, s.cfg.Batch)
	if err != nil {
		return fmt.Errorf("due schedules: %w", err)
	}
	sysCtx := WithPrincipal(ctx, SystemPrincipal)
	for _, sched := range due {
		if err := s.fireOne(sysCtx, sched, now); err != nil {
			s.log.ErrorContext(ctx, "schedule fire failed", "schedule_id", sched.ID, "error", err)
		}
	}
	return nil
}

func (s *Scheduler) fireOne(ctx context.Context, sched domain.Schedule, now time.Time) error {
	fire := sched.NextRunAt
	loc, err := time.LoadLocation(sched.Timezone)
	if err != nil {
		return fmt.Errorf("timezone: %w", err)
	}
	// Misfire policy: next is relative to now, not to the missed fire.
	next, err := s.parser.Next(sched.Cron, loc, now)
	if err != nil {
		return fmt.Errorf("next fire: %w", err)
	}
	key := domain.FireIdempotencyKey(sched.ID, fire)
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
		return fmt.Errorf("enqueue: %w", err)
	}
	if err := s.schedules.MarkFired(ctx, sched.ID, fire, now, next, job.ID); err != nil {
		if errors.Is(err, domain.ErrConflict) {
			s.log.DebugContext(ctx, "mark fired conflict skipped", "schedule_id", sched.ID)
			return nil
		}
		return fmt.Errorf("mark fired: %w", err)
	}
	return nil
}
