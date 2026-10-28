package postgres

import (
	"time"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func toDomain(j sqlcgen.Job) domain.Job {
	return domain.Job{
		ID:             j.ID,
		Queue:          j.Queue,
		Type:           j.Type,
		Payload:        append([]byte(nil), j.Payload...),
		Status:         domain.Status(j.Status),
		Priority:       int(j.Priority),
		RunAt:          j.RunAt.UTC(),
		Attempts:       int(j.Attempts),
		MaxAttempts:    int(j.MaxAttempts),
		LeaseOwner:     derefStr(j.LeaseOwner),
		LeaseExpiresAt: utcPtr(j.LeaseExpiresAt),
		IdempotencyKey: derefStr(j.IdempotencyKey),
		LastError:      derefStr(j.LastError),
		CreatedAt:      j.CreatedAt.UTC(),
		UpdatedAt:      j.UpdatedAt.UTC(),
		FinishedAt:     utcPtr(j.FinishedAt),
	}
}

func toCreateParams(job domain.Job) sqlcgen.CreateJobParams {
	// Domain validates priority (-100..100) and attempt counts before persistence.
	return sqlcgen.CreateJobParams{
		ID:             job.ID,
		Queue:          job.Queue,
		Type:           job.Type,
		Payload:        job.Payload,
		Status:         string(job.Status),
		Priority:       int16(job.Priority), //nolint:gosec // G115: domain-bounded
		RunAt:          job.RunAt.UTC(),
		Attempts:       int32(job.Attempts),    //nolint:gosec // G115: non-negative domain field
		MaxAttempts:    int32(job.MaxAttempts), //nolint:gosec // G115: domain-bounded 1..100
		LeaseOwner:     nullStr(job.LeaseOwner),
		LeaseExpiresAt: utcPtr(job.LeaseExpiresAt),
		IdempotencyKey: nullStr(job.IdempotencyKey),
		LastError:      nullStr(job.LastError),
		CreatedAt:      job.CreatedAt.UTC(),
		UpdatedAt:      job.UpdatedAt.UTC(),
		FinishedAt:     utcPtr(job.FinishedAt),
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
