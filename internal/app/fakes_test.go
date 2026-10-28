package app_test

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// fixedClock returns a controllable now.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fixedClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// seqIDs returns IDs from a fixed sequence, then panics.
type seqIDs struct {
	mu  sync.Mutex
	ids []domain.JobID
	i   int
}

func (g *seqIDs) NewID() domain.JobID {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.i >= len(g.ids) {
		panic("seqIDs: exhausted")
	}
	id := g.ids[g.i]
	g.i++
	return id
}

func mustID(s string) domain.JobID {
	id, err := uuid.Parse(s)
	if err != nil {
		panic(err)
	}
	return id
}

// memRepo is a minimal in-memory JobRepository for app unit tests.
// Production memory adapter lives under internal/adapter/storage/memory.
type memRepo struct {
	mu    sync.Mutex
	jobs  map[domain.JobID]domain.Job
	byKey map[string]domain.JobID // queue\0key -> id
}

func newMemRepo() *memRepo {
	return &memRepo{
		jobs:  make(map[domain.JobID]domain.Job),
		byKey: make(map[string]domain.JobID),
	}
}

func idemKey(queue, key string) string {
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

func (r *memRepo) Create(_ context.Context, job domain.Job) (domain.Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if job.IdempotencyKey != "" {
		if id, ok := r.byKey[idemKey(job.Queue, job.IdempotencyKey)]; ok {
			return cloneJob(r.jobs[id]), false, nil
		}
	}
	stored := cloneJob(job)
	r.jobs[stored.ID] = stored
	if stored.IdempotencyKey != "" {
		r.byKey[idemKey(stored.Queue, stored.IdempotencyKey)] = stored.ID
	}
	return cloneJob(stored), true, nil
}

func (r *memRepo) Get(_ context.Context, id domain.JobID) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	return cloneJob(j), nil
}

func (r *memRepo) Claim(_ context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, j := range r.jobs {
		if j.Queue != queue || j.Status != domain.StatusRunning || j.LeaseExpiresAt == nil {
			continue
		}
		if j.LeaseExpiresAt.After(now) {
			continue
		}
		if j.Attempts >= j.MaxAttempts {
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

	type cand struct {
		id domain.JobID
		j  domain.Job
	}
	var cands []cand
	for id, j := range r.jobs {
		if j.Queue != queue {
			continue
		}
		switch j.Status {
		case domain.StatusPending:
			if !j.RunAt.After(now) {
				cands = append(cands, cand{id, j})
			}
		case domain.StatusRunning:
			if j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.After(now) && j.Attempts < j.MaxAttempts {
				cands = append(cands, cand{id, j})
			}
		}
	}
	// sort: priority DESC, run_at ASC, id ASC
	for i := 0; i < len(cands); i++ {
		for k := i + 1; k < len(cands); k++ {
			a, b := cands[i].j, cands[k].j
			swap := false
			switch {
			case a.Priority < b.Priority:
				swap = true
			case a.Priority == b.Priority && a.RunAt.After(b.RunAt):
				swap = true
			case a.Priority == b.Priority && a.RunAt.Equal(b.RunAt) && a.ID.String() > b.ID.String():
				swap = true
			}
			if swap {
				cands[i], cands[k] = cands[k], cands[i]
			}
		}
	}
	if limit > len(cands) {
		limit = len(cands)
	}
	out := make([]domain.Job, 0, limit)
	exp := now.Add(lease)
	for i := 0; i < limit; i++ {
		j := cands[i].j
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

func (r *memRepo) owned(id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	if j.Status != domain.StatusRunning || j.LeaseOwner != owner || j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.After(now) {
		return domain.Job{}, domain.ErrLeaseLost
	}
	return j, nil
}

func (r *memRepo) ExtendLease(_ context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.owned(id, owner, now)
	if err != nil {
		return domain.Job{}, err
	}
	exp := now.Add(lease)
	j.LeaseExpiresAt = &exp
	j.UpdatedAt = now
	r.jobs[id] = j
	return cloneJob(j), nil
}

func (r *memRepo) Complete(_ context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.owned(id, owner, now)
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

func (r *memRepo) Fail(_ context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.owned(id, owner, now)
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

func (r *memRepo) Ping(context.Context) error { return nil }

var _ app.JobRepository = (*memRepo)(nil)
