package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Repository implements app.JobRepository against PostgreSQL.
type Repository struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// New wraps an existing pool. The caller owns the pool lifecycle.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool, q: sqlcgen.New(pool)}
}

// Open creates a pgx pool from databaseURL and returns a repository.
// Call Close when finished.
func Open(ctx context.Context, databaseURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres open: ping: %w", err)
	}
	return New(pool), nil
}

// Close closes the underlying pool.
func (r *Repository) Close() {
	r.pool.Close()
}

var _ app.JobRepository = (*Repository)(nil)

// Create implements app.JobRepository.
func (r *Repository) Create(ctx context.Context, job domain.Job) (domain.Job, bool, error) {
	row, err := r.q.CreateJob(ctx, toCreateParams(job))
	if err == nil {
		return toDomain(row), true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, false, err
	}
	if job.IdempotencyKey == "" {
		return domain.Job{}, false, fmt.Errorf("postgres create: insert returned no rows without idempotency key")
	}
	existing, err := r.q.GetJobByIdempotencyKey(ctx, sqlcgen.GetJobByIdempotencyKeyParams{
		Queue:          job.Queue,
		IdempotencyKey: nullStr(job.IdempotencyKey),
	})
	if err != nil {
		return domain.Job{}, false, err
	}
	return toDomain(existing), false, nil
}

// Get implements app.JobRepository.
func (r *Repository) Get(ctx context.Context, id domain.JobID) (domain.Job, error) {
	row, err := r.q.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrNotFound
		}
		return domain.Job{}, err
	}
	return toDomain(row), nil
}

// Claim implements app.JobRepository.
func (r *Repository) Claim(ctx context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error) {
	if limit < 0 {
		limit = 0
	}
	now = now.UTC()
	leaseExp := now.Add(lease)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	qtx := r.q.WithTx(tx)

	if err := qtx.MarkExpiredLeasesDead(ctx, sqlcgen.MarkExpiredLeasesDeadParams{
		Now:   now,
		Queue: queue,
	}); err != nil {
		return nil, err
	}

	rows, err := qtx.ClaimJobs(ctx, sqlcgen.ClaimJobsParams{
		Owner:          nullStr(owner),
		LeaseExpiresAt: &leaseExp,
		Now:            now,
		Queue:          queue,
		BatchLimit:     int32(limit),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	out := make([]domain.Job, len(rows))
	for i := range rows {
		out[i] = toDomain(rows[i])
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if !a.RunAt.Equal(b.RunAt) {
			return a.RunAt.Before(b.RunAt)
		}
		return a.ID.String() < b.ID.String()
	})
	return out, nil
}

// ExtendLease implements app.JobRepository.
func (r *Repository) ExtendLease(ctx context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error) {
	now = now.UTC()
	exp := now.Add(lease)
	row, err := r.q.ExtendLease(ctx, sqlcgen.ExtendLeaseParams{
		LeaseExpiresAt: &exp,
		Now:            now,
		ID:             id,
		Owner:          nullStr(owner),
	})
	if err != nil {
		return r.mapConditionalErr(ctx, id, err)
	}
	return toDomain(row), nil
}

// Complete implements app.JobRepository.
func (r *Repository) Complete(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	now = now.UTC()
	row, err := r.q.CompleteJob(ctx, sqlcgen.CompleteJobParams{
		Now:   now,
		ID:    id,
		Owner: nullStr(owner),
	})
	if err != nil {
		return r.mapConditionalErr(ctx, id, err)
	}
	return toDomain(row), nil
}

// Fail implements app.JobRepository.
func (r *Repository) Fail(ctx context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error) {
	now = now.UTC()
	ownerPtr := nullStr(owner)
	errPtr := nullStr(errMsg)

	var (
		row sqlcgen.Job
		err error
	)
	switch decision.NextStatus {
	case domain.StatusPending:
		runAt := now
		if decision.NextRunAt != nil {
			runAt = decision.NextRunAt.UTC()
		}
		row, err = r.q.FailJobPending(ctx, sqlcgen.FailJobPendingParams{
			RunAt:     runAt,
			LastError: errPtr,
			Now:       now,
			ID:        id,
			Owner:     ownerPtr,
		})
	case domain.StatusDead:
		row, err = r.q.FailJobDead(ctx, sqlcgen.FailJobDeadParams{
			Now:       now,
			LastError: errPtr,
			ID:        id,
			Owner:     ownerPtr,
		})
	default:
		return domain.Job{}, fmt.Errorf("postgres fail: unsupported next status %q", decision.NextStatus)
	}
	if err != nil {
		return r.mapConditionalErr(ctx, id, err)
	}
	return toDomain(row), nil
}

// Ping implements app.JobRepository.
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *Repository) mapConditionalErr(ctx context.Context, id domain.JobID, err error) (domain.Job, error) {
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, err
	}
	_, getErr := r.q.GetJob(ctx, id)
	if getErr != nil {
		if errors.Is(getErr, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrNotFound
		}
		return domain.Job{}, getErr
	}
	return domain.Job{}, domain.ErrLeaseLost
}
