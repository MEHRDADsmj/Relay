package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres/sqlcgen"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Repository implements app.JobRepository / QueueRepository / MaintenanceRepository
// against PostgreSQL. Optional JobNotifier is attached via AttachNotifier.
type Repository struct {
	pool     *pgxpool.Pool
	q        *sqlcgen.Queries
	notifier *Notifier
}

// New wraps an existing pool. The caller owns the pool lifecycle.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool, q: sqlcgen.New(pool)}
}

// AttachNotifier wires a LISTEN/NOTIFY fan-out for Subscribe.
func (r *Repository) AttachNotifier(n *Notifier) {
	r.notifier = n
}

// Open creates a pgx pool from databaseURL, attaches a Notifier, and returns a repository.
// Call Close when finished. Tracing uses otelpgx on the pool config.
func Open(ctx context.Context, databaseURL string) (*Repository, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres open: parse config: %w", err)
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres open: ping: %w", err)
	}
	repo := New(pool)
	n, err := NewNotifier(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres open: notifier: %w", err)
	}
	repo.AttachNotifier(n)
	return repo, nil
}

// Close closes the notifier (if any) and the underlying pool.
func (r *Repository) Close() {
	if r.notifier != nil {
		r.notifier.Close()
		r.notifier = nil
	}
	r.pool.Close()
}

// Pool returns the underlying pgx pool (for LeaderElector wiring).
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

var (
	_ app.JobRepository         = (*Repository)(nil)
	_ app.QueueRepository       = (*Repository)(nil)
	_ app.MaintenanceRepository = (*Repository)(nil)
	_ app.JobNotifier           = (*Repository)(nil)
)

// Create implements app.JobRepository.
func (r *Repository) Create(ctx context.Context, job domain.Job) (domain.Job, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	qtx := r.q.WithTx(tx)
	if err := qtx.UpsertQueue(ctx, sqlcgen.UpsertQueueParams{
		Name:      job.Queue,
		CreatedAt: job.CreatedAt.UTC(),
	}); err != nil {
		return domain.Job{}, false, err
	}

	row, err := qtx.CreateJob(ctx, toCreateParams(job))
	if err == nil {
		if !job.RunAt.After(job.CreatedAt) {
			if err := notifyQueue(ctx, tx, job.Queue); err != nil {
				return domain.Job{}, false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Job{}, false, err
		}
		return toDomain(row), true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, false, err
	}
	if job.IdempotencyKey == "" {
		return domain.Job{}, false, fmt.Errorf("postgres create: insert returned no rows without idempotency key")
	}
	existing, err := qtx.GetJobByIdempotencyKey(ctx, sqlcgen.GetJobByIdempotencyKeyParams{
		Queue:          job.Queue,
		IdempotencyKey: nullStr(job.IdempotencyKey),
	})
	if err != nil {
		return domain.Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
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

	qrow, err := qtx.GetQueue(ctx, queue)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err == nil && qrow.Paused {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	deadRows, err := qtx.MarkExpiredLeasesDead(ctx, sqlcgen.MarkExpiredLeasesDeadParams{
		Now:   now,
		Queue: queue,
	})
	if err != nil {
		return nil, err
	}
	for _, d := range deadRows {
		if err := closeRunningAttempt(ctx, qtx, d.ID, domain.AttemptExpired, "lease expired", now); err != nil {
			return nil, err
		}
	}

	rows, err := qtx.ClaimJobs(ctx, sqlcgen.ClaimJobsParams{
		Owner:          nullStr(owner),
		LeaseExpiresAt: &leaseExp,
		Now:            now,
		Queue:          queue,
		BatchLimit:     clampInt32(limit),
	})
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		// Reclaim path: close previous running attempt (if any) as expired.
		if err := closeRunningAttempt(ctx, qtx, row.ID, domain.AttemptExpired, "", now); err != nil {
			return nil, err
		}
		if err := insertRunningAttempt(ctx, qtx, row.ID, row.Attempts, owner, now); err != nil {
			return nil, err
		}
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
	if err := job.EnsureLeasedBy(owner, now); err != nil {
		return domain.Job{}, err
	}

	var row sqlcgen.Job
	outcome := domain.AttemptSucceeded
	if job.CancelRequested {
		row, err = qtx.CompleteJobCancelled(ctx, sqlcgen.CompleteJobCancelledParams{
			Now:   now,
			ID:    id,
			Owner: nullStr(owner),
		})
		outcome = domain.AttemptCancelled
	} else {
		row, err = qtx.CompleteJobSucceeded(ctx, sqlcgen.CompleteJobSucceededParams{
			Now:   now,
			ID:    id,
			Owner: nullStr(owner),
		})
	}
	if err != nil {
		return r.mapConditionalErrTx(ctx, qtx, id, err)
	}
	if err := closeRunningAttempt(ctx, qtx, id, outcome, "", now); err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return toDomain(row), nil
}

// Fail implements app.JobRepository.
func (r *Repository) Fail(ctx context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error) {
	now = now.UTC()
	ownerPtr := nullStr(owner)
	errPtr := nullStr(errMsg)

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
	if err := job.EnsureLeasedBy(owner, now); err != nil {
		return domain.Job{}, err
	}

	var (
		row     sqlcgen.Job
		outcome = domain.AttemptFailed
	)
	if job.CancelRequested {
		row, err = qtx.FailJobCancelled(ctx, sqlcgen.FailJobCancelledParams{
			Now:       now,
			LastError: errPtr,
			ID:        id,
			Owner:     ownerPtr,
		})
		outcome = domain.AttemptCancelled
	} else {
		switch decision.NextStatus {
		case domain.StatusPending:
			runAt := now
			if decision.NextRunAt != nil {
				runAt = decision.NextRunAt.UTC()
			}
			row, err = qtx.FailJobPending(ctx, sqlcgen.FailJobPendingParams{
				RunAt:     runAt,
				LastError: errPtr,
				Now:       now,
				ID:        id,
				Owner:     ownerPtr,
			})
		case domain.StatusDead:
			row, err = qtx.FailJobDead(ctx, sqlcgen.FailJobDeadParams{
				Now:       now,
				LastError: errPtr,
				ID:        id,
				Owner:     ownerPtr,
			})
		default:
			return domain.Job{}, fmt.Errorf("postgres fail: unsupported next status %q", decision.NextStatus)
		}
	}
	if err != nil {
		return r.mapConditionalErrTx(ctx, qtx, id, err)
	}
	if err := closeRunningAttempt(ctx, qtx, id, outcome, errMsg, now); err != nil {
		return domain.Job{}, err
	}
	stored := toDomain(row)
	if stored.Status == domain.StatusPending && !stored.RunAt.After(now) {
		if err := notifyQueue(ctx, tx, stored.Queue); err != nil {
			return domain.Job{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return stored, nil
}

// Ping implements app.JobRepository.
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func closeRunningAttempt(ctx context.Context, q *sqlcgen.Queries, id domain.JobID, outcome domain.AttemptOutcome, errMsg string, now time.Time) error {
	return q.CloseRunningAttempt(ctx, sqlcgen.CloseRunningAttemptParams{
		Outcome:    string(outcome),
		FinishedAt: timePtr(now),
		Error:      nullStr(errMsg),
		JobID:      id,
	})
}

// insertRunningAttempt inserts a running attempt. When the injected clock is frozen,
// (job_id, number, started_at) can collide after Release/Retry; bump started_at past any prior row.
func insertRunningAttempt(ctx context.Context, q *sqlcgen.Queries, id domain.JobID, number int32, owner string, now time.Time) error {
	started := now.UTC()
	if maxStarted, err := q.MaxAttemptStartedAt(ctx, id); err == nil {
		if !maxStarted.Before(started) {
			started = maxStarted.UTC().Add(time.Microsecond)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	return q.InsertAttempt(ctx, sqlcgen.InsertAttemptParams{
		JobID:     id,
		Number:    number,
		WorkerID:  owner,
		StartedAt: started,
		Outcome:   string(domain.AttemptRunning),
	})
}

func notifyQueue(ctx context.Context, tx pgx.Tx, queue string) error {
	_, err := tx.Exec(ctx, `SELECT pg_notify('relay_jobs', $1)`, queue)
	return err
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

func (r *Repository) mapConditionalErrTx(ctx context.Context, q *sqlcgen.Queries, id domain.JobID, err error) (domain.Job, error) {
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, err
	}
	_, getErr := q.GetJob(ctx, id)
	if getErr != nil {
		if errors.Is(getErr, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrNotFound
		}
		return domain.Job{}, getErr
	}
	return domain.Job{}, domain.ErrLeaseLost
}
