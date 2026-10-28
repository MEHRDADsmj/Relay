package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Repository is an in-memory JobRepository suitable for tests and local use.
type Repository struct {
	mu    sync.Mutex
	jobs  map[domain.JobID]domain.Job
	byKey map[string]domain.JobID // queue\0key → id when key non-empty
}

// New returns an empty in-memory repository.
func New() *Repository {
	return &Repository{
		jobs:  make(map[domain.JobID]domain.Job),
		byKey: make(map[string]domain.JobID),
	}
}

var _ app.JobRepository = (*Repository)(nil)

func idempotencyIndexKey(queue, key string) string {
	return queue + "\x00" + key
}

func cloneJob(j domain.Job) domain.Job {
	out := j
	if j.Payload != nil {
		out.Payload = append([]byte(nil), j.Payload...)
	}
	if j.LeaseExpiresAt != nil {
		t := *j.LeaseExpiresAt
		out.LeaseExpiresAt = &t
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		out.FinishedAt = &t
	}
	return out
}

// Create implements app.JobRepository.
func (r *Repository) Create(_ context.Context, job domain.Job) (domain.Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if job.IdempotencyKey != "" {
		if id, ok := r.byKey[idempotencyIndexKey(job.Queue, job.IdempotencyKey)]; ok {
			return cloneJob(r.jobs[id]), false, nil
		}
	}

	stored := cloneJob(job)
	r.jobs[stored.ID] = stored
	if stored.IdempotencyKey != "" {
		r.byKey[idempotencyIndexKey(stored.Queue, stored.IdempotencyKey)] = stored.ID
	}
	return cloneJob(stored), true, nil
}

// Get implements app.JobRepository.
func (r *Repository) Get(_ context.Context, id domain.JobID) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	return cloneJob(j), nil
}

// Claim implements app.JobRepository.
func (r *Repository) Claim(_ context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.markExpiredDeadLocked(queue, now)

	type candidate struct {
		id  domain.JobID
		job domain.Job
	}
	cands := make([]candidate, 0)
	for id, j := range r.jobs {
		if j.Queue != queue {
			continue
		}
		switch j.Status {
		case domain.StatusPending:
			if !j.RunAt.After(now) {
				cands = append(cands, candidate{id, j})
			}
		case domain.StatusRunning:
			if j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.After(now) && j.Attempts < j.MaxAttempts {
				cands = append(cands, candidate{id, j})
			}
		}
	}

	sort.SliceStable(cands, func(i, k int) bool {
		a, b := cands[i].job, cands[k].job
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if !a.RunAt.Equal(b.RunAt) {
			return a.RunAt.Before(b.RunAt)
		}
		return a.ID.String() < b.ID.String()
	})

	if limit < 0 {
		limit = 0
	}
	if limit > len(cands) {
		limit = len(cands)
	}

	out := make([]domain.Job, 0, limit)
	exp := now.Add(lease)
	for i := 0; i < limit; i++ {
		j := cands[i].job
		j.Status = domain.StatusRunning
		j.LeaseOwner = owner
		le := exp
		j.LeaseExpiresAt = &le
		j.Attempts++
		j.UpdatedAt = now
		r.jobs[cands[i].id] = j
		out = append(out, cloneJob(j))
	}
	return out, nil
}

func (r *Repository) markExpiredDeadLocked(queue string, now time.Time) {
	for id, j := range r.jobs {
		if j.Queue != queue || j.Status != domain.StatusRunning || j.LeaseExpiresAt == nil {
			continue
		}
		if j.LeaseExpiresAt.After(now) {
			continue
		}
		if j.Attempts < j.MaxAttempts {
			continue
		}
		j.Status = domain.StatusDead
		j.LastError = "lease expired"
		fin := now
		j.FinishedAt = &fin
		j.LeaseOwner = ""
		j.LeaseExpiresAt = nil
		j.UpdatedAt = now
		r.jobs[id] = j
	}
}

func (r *Repository) checkOwnedLocked(id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	if j.Status != domain.StatusRunning || j.LeaseOwner != owner || j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.After(now) {
		return domain.Job{}, domain.ErrLeaseLost
	}
	return j, nil
}

// ExtendLease implements app.JobRepository.
func (r *Repository) ExtendLease(_ context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.checkOwnedLocked(id, owner, now)
	if err != nil {
		return domain.Job{}, err
	}
	exp := now.Add(lease)
	j.LeaseExpiresAt = &exp
	j.UpdatedAt = now
	r.jobs[id] = j
	return cloneJob(j), nil
}

// Complete implements app.JobRepository.
func (r *Repository) Complete(_ context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.checkOwnedLocked(id, owner, now)
	if err != nil {
		return domain.Job{}, err
	}
	j.Status = domain.StatusSucceeded
	fin := now
	j.FinishedAt = &fin
	j.LeaseOwner = ""
	j.LeaseExpiresAt = nil
	j.UpdatedAt = now
	r.jobs[id] = j
	return cloneJob(j), nil
}

// Fail implements app.JobRepository.
func (r *Repository) Fail(_ context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.checkOwnedLocked(id, owner, now)
	if err != nil {
		return domain.Job{}, err
	}
	j.Status = decision.NextStatus
	j.LastError = errMsg
	j.LeaseOwner = ""
	j.LeaseExpiresAt = nil
	j.UpdatedAt = now
	switch decision.NextStatus {
	case domain.StatusPending:
		if decision.NextRunAt != nil {
			j.RunAt = *decision.NextRunAt
		}
	case domain.StatusDead:
		fin := now
		j.FinishedAt = &fin
	}
	r.jobs[id] = j
	return cloneJob(j), nil
}

// Ping implements app.JobRepository.
func (r *Repository) Ping(context.Context) error {
	return nil
}
