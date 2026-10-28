package domain

import (
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// ScheduleID uniquely identifies a cron schedule.
type ScheduleID = uuid.UUID

// CronParser validates cron expressions and computes next fire times.
// The app-layer adapter wraps robfig; domain depends only on this port.
type CronParser interface {
	// Next returns the first fire time strictly after after, in loc.
	Next(expr string, loc *time.Location, after time.Time) (time.Time, error)
	Validate(expr string) error
}

// Schedule is a named cron definition that enqueues jobs on each fire.
type Schedule struct {
	ID          ScheduleID
	Name        string // unique, regex ^[a-z0-9][a-z0-9_.-]{0,99}$
	Cron        string // standard 5-field + descriptors (@hourly, @daily, @every 5m)
	Timezone    string // IANA name, default "UTC"
	Queue       string
	JobType     string
	Payload     []byte
	Priority    int
	MaxAttempts int
	Enabled     bool
	NextRunAt   time.Time // always computed, even when disabled
	LastRunAt   *time.Time
	LastJobID   *JobID
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewScheduleParams holds caller-supplied fields for NewSchedule.
// Nil optional pointers select domain defaults.
type NewScheduleParams struct {
	Name        string
	Cron        string
	Timezone    string // empty → "UTC"
	Queue       string
	JobType     string
	Payload     []byte
	Priority    *int
	MaxAttempts *int
	Enabled     *bool // nil → true
}

var scheduleNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,99}$`)

// ValidateScheduleName checks the schedule name against the domain regex.
func ValidateScheduleName(name string) error {
	if !scheduleNamePattern.MatchString(name) {
		return validationErrorf("name", "must match ^[a-z0-9][a-z0-9_.-]{0,99}$")
	}
	return nil
}

// NewSchedule validates params, applies defaults, and returns a Schedule with NextRunAt set.
func NewSchedule(params NewScheduleParams, id ScheduleID, now time.Time, parser CronParser) (Schedule, error) {
	if err := ValidateScheduleName(params.Name); err != nil {
		return Schedule{}, err
	}
	if parser == nil {
		return Schedule{}, validationErrorf("cron", "cron parser is required")
	}
	if err := parser.Validate(params.Cron); err != nil {
		return Schedule{}, validationErrorf("cron", err.Error())
	}

	tz := params.Timezone
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Schedule{}, validationErrorf("timezone", "must be a valid IANA time zone")
	}

	if err := ValidateQueue(params.Queue); err != nil {
		return Schedule{}, err
	}
	if err := ValidateType(params.JobType); err != nil {
		return Schedule{}, err
	}
	payload, err := NormalizePayload(params.Payload)
	if err != nil {
		return Schedule{}, err
	}
	priority, err := NormalizePriority(params.Priority)
	if err != nil {
		return Schedule{}, err
	}
	maxAttempts, err := NormalizeMaxAttempts(params.MaxAttempts)
	if err != nil {
		return Schedule{}, err
	}

	next, err := parser.Next(params.Cron, loc, now)
	if err != nil {
		return Schedule{}, validationErrorf("cron", err.Error())
	}

	enabled := true
	if params.Enabled != nil {
		enabled = *params.Enabled
	}

	return Schedule{
		ID:          id,
		Name:        params.Name,
		Cron:        params.Cron,
		Timezone:    tz,
		Queue:       params.Queue,
		JobType:     params.JobType,
		Payload:     payload,
		Priority:    priority,
		MaxAttempts: maxAttempts,
		Enabled:     enabled,
		NextRunAt:   next,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// FireIdempotencyKey returns the at-most-once enqueue key for a schedule fire.
// Format: schedule:{id}:{fire_unix}
func FireIdempotencyKey(id ScheduleID, fireAt time.Time) string {
	return fmt.Sprintf("schedule:%s:%d", id.String(), fireAt.Unix())
}
