package postgres

import (
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func toDomain(j sqlcgen.Job) domain.Job {
	return domain.Job{
		ID:              j.ID,
		Queue:           j.Queue,
		Type:            j.Type,
		Payload:         append([]byte(nil), j.Payload...),
		Status:          domain.Status(j.Status),
		Priority:        int(j.Priority),
		RunAt:           j.RunAt.UTC(),
		Attempts:        int(j.Attempts),
		MaxAttempts:     int(j.MaxAttempts),
		LeaseOwner:      derefStr(j.LeaseOwner),
		LeaseExpiresAt:  utcPtr(j.LeaseExpiresAt),
		IdempotencyKey:  derefStr(j.IdempotencyKey),
		LastError:       derefStr(j.LastError),
		CancelRequested: j.CancelRequested,
		CancelledAt:     utcPtr(j.CancelledAt),
		CreatedAt:       j.CreatedAt.UTC(),
		UpdatedAt:       j.UpdatedAt.UTC(),
		FinishedAt:      utcPtr(j.FinishedAt),
	}
}

func toAttempt(a sqlcgen.JobAttempt) domain.Attempt {
	return domain.Attempt{
		JobID:      a.JobID,
		Number:     int(a.Number),
		WorkerID:   a.WorkerID,
		StartedAt:  a.StartedAt.UTC(),
		FinishedAt: utcPtr(a.FinishedAt),
		Outcome:    domain.AttemptOutcome(a.Outcome),
		Error:      derefStr(a.Error),
	}
}

func toQueue(q sqlcgen.Queue) domain.Queue {
	return domain.Queue{
		Name:      q.Name,
		Paused:    q.Paused,
		CreatedAt: q.CreatedAt.UTC(),
		UpdatedAt: q.UpdatedAt.UTC(),
	}
}

func toDomainSchedule(s sqlcgen.Schedule) domain.Schedule {
	return domain.Schedule{
		ID:          s.ID,
		Name:        s.Name,
		Cron:        s.Cron,
		Timezone:    s.Timezone,
		Queue:       s.Queue,
		JobType:     s.JobType,
		Payload:     append([]byte(nil), s.Payload...),
		Priority:    int(s.Priority),
		MaxAttempts: int(s.MaxAttempts),
		Enabled:     s.Enabled,
		NextRunAt:   s.NextRunAt.UTC(),
		LastRunAt:   utcPtr(s.LastRunAt),
		LastJobID:   s.LastJobID,
		CreatedAt:   s.CreatedAt.UTC(),
		UpdatedAt:   s.UpdatedAt.UTC(),
	}
}

func toCreateScheduleParams(s domain.Schedule) sqlcgen.CreateScheduleParams {
	return sqlcgen.CreateScheduleParams{
		ID:          s.ID,
		Name:        s.Name,
		Cron:        s.Cron,
		Timezone:    s.Timezone,
		Queue:       s.Queue,
		JobType:     s.JobType,
		Payload:     s.Payload,
		Priority:    int16(s.Priority),    //nolint:gosec // G115: domain-bounded
		MaxAttempts: int32(s.MaxAttempts), //nolint:gosec // G115: domain-bounded
		Enabled:     s.Enabled,
		NextRunAt:   s.NextRunAt.UTC(),
		LastRunAt:   utcPtr(s.LastRunAt),
		LastJobID:   s.LastJobID,
		CreatedAt:   s.CreatedAt.UTC(),
		UpdatedAt:   s.UpdatedAt.UTC(),
	}
}

func toUpdateScheduleParams(s domain.Schedule, expectedUpdatedAt time.Time) sqlcgen.UpdateScheduleParams {
	return sqlcgen.UpdateScheduleParams{
		Name:              s.Name,
		Cron:              s.Cron,
		Timezone:          s.Timezone,
		Queue:             s.Queue,
		JobType:           s.JobType,
		Payload:           s.Payload,
		Priority:          int16(s.Priority),    //nolint:gosec // G115: domain-bounded
		MaxAttempts:       int32(s.MaxAttempts), //nolint:gosec // G115: domain-bounded
		Enabled:           s.Enabled,
		NextRunAt:         s.NextRunAt.UTC(),
		LastRunAt:         utcPtr(s.LastRunAt),
		LastJobID:         s.LastJobID,
		UpdatedAt:         s.UpdatedAt.UTC(),
		ID:                s.ID,
		ExpectedUpdatedAt: expectedUpdatedAt.UTC(),
	}
}

func toDomainAPIKey(k sqlcgen.ApiKey) domain.APIKey {
	scopes := make([]domain.Scope, len(k.Scopes))
	for i, s := range k.Scopes {
		scopes[i] = domain.Scope(s)
	}
	return domain.APIKey{
		ID:         k.ID.String(),
		Name:       k.Name,
		Prefix:     k.Prefix,
		Hash:       append([]byte(nil), k.Hash...),
		Scopes:     scopes,
		CreatedAt:  k.CreatedAt.UTC(),
		LastUsedAt: utcPtr(k.LastUsedAt),
		ExpiresAt:  utcPtr(k.ExpiresAt),
		RevokedAt:  utcPtr(k.RevokedAt),
	}
}

func toCreateAPIKeyParams(k domain.APIKey) (sqlcgen.CreateAPIKeyParams, error) {
	id, err := uuid.Parse(k.ID)
	if err != nil || id == uuid.Nil {
		return sqlcgen.CreateAPIKeyParams{}, fmt.Errorf("postgres api key id: invalid")
	}
	scopes := make([]string, len(k.Scopes))
	for i, s := range k.Scopes {
		scopes[i] = string(s)
	}
	return sqlcgen.CreateAPIKeyParams{
		ID:         id,
		Name:       k.Name,
		Prefix:     k.Prefix,
		Hash:       k.Hash,
		Scopes:     scopes,
		CreatedAt:  k.CreatedAt.UTC(),
		LastUsedAt: utcPtr(k.LastUsedAt),
		ExpiresAt:  utcPtr(k.ExpiresAt),
		RevokedAt:  utcPtr(k.RevokedAt),
	}, nil
}

func toCreateParams(job domain.Job) sqlcgen.CreateJobParams {
	// Domain validates priority (-100..100) and attempt counts before persistence.
	return sqlcgen.CreateJobParams{
		ID:              job.ID,
		Queue:           job.Queue,
		Type:            job.Type,
		Payload:         job.Payload,
		Status:          string(job.Status),
		Priority:        int16(job.Priority), //nolint:gosec // G115: domain-bounded
		RunAt:           job.RunAt.UTC(),
		Attempts:        int32(job.Attempts),    //nolint:gosec // G115: non-negative domain field
		MaxAttempts:     int32(job.MaxAttempts), //nolint:gosec // G115: domain-bounded 1..100
		LeaseOwner:      nullStr(job.LeaseOwner),
		LeaseExpiresAt:  utcPtr(job.LeaseExpiresAt),
		IdempotencyKey:  nullStr(job.IdempotencyKey),
		LastError:       nullStr(job.LastError),
		CancelRequested: job.CancelRequested,
		CancelledAt:     utcPtr(job.CancelledAt),
		CreatedAt:       job.CreatedAt.UTC(),
		UpdatedAt:       job.UpdatedAt.UTC(),
		FinishedAt:      utcPtr(job.FinishedAt),
	}
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func timePtr(t time.Time) *time.Time {
	u := t.UTC()
	return &u
}

// clampInt32 bounds n to [0, MaxInt32] for sqlc LIMIT parameters.
func clampInt32(n int) int32 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}
