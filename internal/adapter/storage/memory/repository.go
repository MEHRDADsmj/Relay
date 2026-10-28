package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Repository is an in-memory storage backend implementing all app ports.
type Repository struct {
	mu        sync.Mutex
	jobs      map[domain.JobID]domain.Job
	byKey     map[string]domain.JobID // queue\0key → id when key non-empty
	queues    map[string]domain.Queue
	attempts  map[domain.JobID][]domain.Attempt
	subs      map[string]map[chan struct{}]struct{} // queue → subscribers
	schedules map[domain.ScheduleID]domain.Schedule
	schedName map[string]domain.ScheduleID // name → id
	apiKeys   map[string]domain.APIKey     // id → key
	apiPrefix map[string]string            // prefix → id
}

// New returns an empty in-memory repository.
func New() *Repository {
	return &Repository{
		jobs:      make(map[domain.JobID]domain.Job),
		byKey:     make(map[string]domain.JobID),
		queues:    make(map[string]domain.Queue),
		attempts:  make(map[domain.JobID][]domain.Attempt),
		subs:      make(map[string]map[chan struct{}]struct{}),
		schedules: make(map[domain.ScheduleID]domain.Schedule),
		schedName: make(map[string]domain.ScheduleID),
		apiKeys:   make(map[string]domain.APIKey),
		apiPrefix: make(map[string]string),
	}
}

var (
	_ app.JobRepository         = (*Repository)(nil)
	_ app.QueueRepository       = (*Repository)(nil)
	_ app.MaintenanceRepository = (*Repository)(nil)
	_ app.JobNotifier           = (*Repository)(nil)
	_ app.ScheduleRepository    = (*Repository)(nil)
	_ app.APIKeyRepository      = (*Repository)(nil)
)

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
	if j.CancelledAt != nil {
		t := *j.CancelledAt
		out.CancelledAt = &t
	}
	if j.TraceContext != nil {
		out.TraceContext = make(map[string]string, len(j.TraceContext))
		for k, v := range j.TraceContext {
			out.TraceContext[k] = v
		}
	}
	return out
}

func cloneAttempt(a domain.Attempt) domain.Attempt {
	out := a
	if a.FinishedAt != nil {
		t := *a.FinishedAt
		out.FinishedAt = &t
	}
	return out
}

func (r *Repository) ensureQueueLocked(name string, now time.Time) {
	if q, ok := r.queues[name]; ok {
		q.UpdatedAt = now
		r.queues[name] = q
		return
	}
	r.queues[name] = domain.Queue{
		Name:      name,
		Paused:    false,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (r *Repository) notifyLocked(queue string) {
	subs := r.subs[queue]
	for ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (r *Repository) closeRunningAttemptLocked(id domain.JobID, outcome domain.AttemptOutcome, errMsg string, now time.Time) {
	atts := r.attempts[id]
	for i := len(atts) - 1; i >= 0; i-- {
		if atts[i].Outcome == domain.AttemptRunning {
			atts[i].Outcome = outcome
			atts[i].Error = errMsg
			fin := now
			atts[i].FinishedAt = &fin
			r.attempts[id] = atts
			return
		}
	}
}

func (r *Repository) appendRunningAttemptLocked(job domain.Job, now time.Time) {
	r.attempts[job.ID] = append(r.attempts[job.ID], domain.Attempt{
		JobID:     job.ID,
		Number:    job.Attempts,
		WorkerID:  job.LeaseOwner,
		StartedAt: now,
		Outcome:   domain.AttemptRunning,
	})
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
	r.ensureQueueLocked(stored.Queue, stored.CreatedAt)
	// Notify when the job is immediately due (run_at <= created/updated "now").
	if !stored.RunAt.After(stored.CreatedAt) {
		r.notifyLocked(stored.Queue)
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

	if q, ok := r.queues[queue]; ok && q.Paused {
		return nil, nil
	}

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
		if j.Status == domain.StatusRunning {
			r.closeRunningAttemptLocked(j.ID, domain.AttemptExpired, "", now)
		}
		j.Status = domain.StatusRunning
		j.LeaseOwner = owner
		le := exp
		j.LeaseExpiresAt = &le
		j.Attempts++
		j.UpdatedAt = now
		r.jobs[cands[i].id] = j
		r.appendRunningAttemptLocked(j, now)
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
		r.closeRunningAttemptLocked(id, domain.AttemptExpired, "lease expired", now)
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
	status := j.ResolveOutcome(domain.StatusSucceeded)
	fin := now
	j.Status = status
	j.FinishedAt = &fin
	j.LeaseOwner = ""
	j.LeaseExpiresAt = nil
	j.UpdatedAt = now
	outcome := domain.AttemptSucceeded
	if status == domain.StatusCancelled {
		j.CancelledAt = &fin
		j.CancelRequested = false
		outcome = domain.AttemptCancelled
	}
	r.closeRunningAttemptLocked(id, outcome, "", now)
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
	j.LastError = errMsg
	j.LeaseOwner = ""
	j.LeaseExpiresAt = nil
	j.UpdatedAt = now

	if j.CancelRequested {
		fin := now
		j.Status = domain.StatusCancelled
		j.FinishedAt = &fin
		j.CancelledAt = &fin
		j.CancelRequested = false
		r.closeRunningAttemptLocked(id, domain.AttemptCancelled, errMsg, now)
		r.jobs[id] = j
		return cloneJob(j), nil
	}

	j.Status = decision.NextStatus
	switch decision.NextStatus {
	case domain.StatusPending:
		if decision.NextRunAt != nil {
			j.RunAt = *decision.NextRunAt
		}
		r.closeRunningAttemptLocked(id, domain.AttemptFailed, errMsg, now)
		if !j.RunAt.After(now) {
			r.notifyLocked(j.Queue)
		}
	case domain.StatusDead:
		fin := now
		j.FinishedAt = &fin
		r.closeRunningAttemptLocked(id, domain.AttemptFailed, errMsg, now)
	default:
		r.closeRunningAttemptLocked(id, domain.AttemptFailed, errMsg, now)
	}
	r.jobs[id] = j
	return cloneJob(j), nil
}

// Release implements app.JobRepository.
func (r *Repository) Release(_ context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, err := r.checkOwnedLocked(id, owner, now)
	if err != nil {
		return domain.Job{}, err
	}
	r.closeRunningAttemptLocked(id, domain.AttemptReleased, "", now)
	j.Status = domain.StatusPending
	if j.Attempts > 0 {
		j.Attempts--
	}
	j.RunAt = now
	j.LeaseOwner = ""
	j.LeaseExpiresAt = nil
	j.UpdatedAt = now
	r.jobs[id] = j
	r.notifyLocked(j.Queue)
	return cloneJob(j), nil
}

// Cancel implements app.JobRepository.
func (r *Repository) Cancel(_ context.Context, id domain.JobID, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	if err := j.CanCancel(); err != nil {
		return domain.Job{}, err
	}
	switch j.Status {
	case domain.StatusPending:
		fin := now
		j.Status = domain.StatusCancelled
		j.FinishedAt = &fin
		j.CancelledAt = &fin
		j.UpdatedAt = now
	case domain.StatusRunning:
		j.CancelRequested = true
		j.UpdatedAt = now
	}
	r.jobs[id] = j
	return cloneJob(j), nil
}

// Retry implements app.JobRepository.
func (r *Repository) Retry(_ context.Context, id domain.JobID, now time.Time) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	if err := j.CanRetry(); err != nil {
		return domain.Job{}, err
	}
	j.Status = domain.StatusPending
	j.Attempts = 0
	j.RunAt = now
	j.FinishedAt = nil
	j.CancelledAt = nil
	j.CancelRequested = false
	j.LeaseOwner = ""
	j.LeaseExpiresAt = nil
	j.UpdatedAt = now
	r.jobs[id] = j
	r.notifyLocked(j.Queue)
	return cloneJob(j), nil
}

// ListAttempts implements app.JobRepository.
func (r *Repository) ListAttempts(_ context.Context, id domain.JobID) ([]domain.Attempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.jobs[id]; !ok {
		return nil, domain.ErrNotFound
	}
	src := r.attempts[id]
	out := make([]domain.Attempt, len(src))
	for i := range src {
		out[i] = cloneAttempt(src[i])
	}
	return out, nil
}

// ListByStatus implements app.JobRepository.
func (r *Repository) ListByStatus(_ context.Context, queue string, status domain.Status, page domain.PageRequest) (domain.Page[domain.Job], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	limit := page.Limit
	if limit <= 0 {
		limit = domain.DefaultPageLimit
	}

	var cursorTime time.Time
	var cursorID domain.JobID
	hasCursor := page.Cursor != ""
	if hasCursor {
		t, id, err := app.DecodeCursor(page.Cursor)
		if err != nil {
			return domain.Page[domain.Job]{}, err
		}
		cursorTime, cursorID = t, id
	}

	matched := make([]domain.Job, 0)
	for _, j := range r.jobs {
		if j.Queue != queue || j.Status != status {
			continue
		}
		if hasCursor {
			// keyset DESC: keep rows strictly after the cursor
			// i.e. (updated_at, id) < (cursor_t, cursor_id)
			if j.UpdatedAt.After(cursorTime) {
				continue
			}
			if j.UpdatedAt.Equal(cursorTime) && j.ID.String() >= cursorID.String() {
				continue
			}
		}
		matched = append(matched, j)
	}
	sort.SliceStable(matched, func(i, k int) bool {
		a, b := matched[i], matched[k]
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
		return a.ID.String() > b.ID.String()
	})

	nextCursor := ""
	if len(matched) > limit {
		last := matched[limit-1]
		enc, err := app.EncodeCursor(last.UpdatedAt, last.ID)
		if err != nil {
			return domain.Page[domain.Job]{}, err
		}
		nextCursor = enc
		matched = matched[:limit]
	}

	items := make([]domain.Job, len(matched))
	for i := range matched {
		items[i] = cloneJob(matched[i])
	}
	return domain.Page[domain.Job]{Items: items, NextCursor: nextCursor}, nil
}

// Ping implements app.JobRepository.
func (r *Repository) Ping(context.Context) error {
	return nil
}

// ListQueues implements app.QueueRepository.
func (r *Repository) ListQueues(context.Context) ([]domain.Queue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.Queue, 0, len(r.queues))
	for _, q := range r.queues {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Stats implements app.QueueRepository.
func (r *Repository) Stats(_ context.Context, queue string, now time.Time) (domain.QueueStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q, ok := r.queues[queue]
	if !ok {
		return domain.QueueStats{}, domain.ErrNotFound
	}
	stats := domain.QueueStats{Queue: queue, Paused: q.Paused}
	var oldestDue *time.Time
	for _, j := range r.jobs {
		if j.Queue != queue {
			continue
		}
		switch j.Status {
		case domain.StatusPending:
			stats.Pending++
			if j.RunAt.After(now) {
				stats.Scheduled++
			} else if oldestDue == nil || j.RunAt.Before(*oldestDue) {
				t := j.RunAt
				oldestDue = &t
			}
		case domain.StatusRunning:
			stats.Running++
		case domain.StatusSucceeded:
			stats.Succeeded++
		case domain.StatusDead:
			stats.Dead++
		case domain.StatusCancelled:
			stats.Cancelled++
		}
	}
	if oldestDue != nil {
		stats.OldestPendingAge = now.Sub(*oldestDue)
		if stats.OldestPendingAge < 0 {
			stats.OldestPendingAge = 0
		}
	}
	return stats, nil
}

// SetPaused implements app.QueueRepository.
func (r *Repository) SetPaused(_ context.Context, queue string, paused bool, now time.Time) (domain.Queue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q, ok := r.queues[queue]
	if !ok {
		return domain.Queue{}, domain.ErrNotFound
	}
	q.Paused = paused
	q.UpdatedAt = now
	r.queues[queue] = q
	if !paused {
		r.notifyLocked(queue)
	}
	return q, nil
}

// ReapExpired implements app.MaintenanceRepository.
func (r *Repository) ReapExpired(_ context.Context, now time.Time, limit int) (app.ReapResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		return app.ReapResult{}, nil
	}

	type cand struct {
		id domain.JobID
		j  domain.Job
	}
	var expired []cand
	for id, j := range r.jobs {
		if j.Status != domain.StatusRunning || j.LeaseExpiresAt == nil || j.LeaseExpiresAt.After(now) {
			continue
		}
		expired = append(expired, cand{id, j})
	}
	sort.SliceStable(expired, func(i, k int) bool {
		return expired[i].id.String() < expired[k].id.String()
	})
	if len(expired) > limit {
		expired = expired[:limit]
	}

	var res app.ReapResult
	for _, c := range expired {
		j := c.j
		r.closeRunningAttemptLocked(c.id, domain.AttemptExpired, "lease expired", now)
		j.LeaseOwner = ""
		j.LeaseExpiresAt = nil
		j.UpdatedAt = now
		if j.Attempts >= j.MaxAttempts {
			j.Status = domain.StatusDead
			j.LastError = "lease expired"
			fin := now
			j.FinishedAt = &fin
			res.Dead++
		} else {
			j.Status = domain.StatusPending
			j.RunAt = now
			res.Requeued++
			r.notifyLocked(j.Queue)
		}
		r.jobs[c.id] = j
	}
	return res, nil
}

// PurgeFinished implements app.MaintenanceRepository.
func (r *Repository) PurgeFinished(_ context.Context, before time.Time, limit int) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		return 0, nil
	}
	type cand struct {
		id domain.JobID
		j  domain.Job
	}
	var victims []cand
	for id, j := range r.jobs {
		switch j.Status {
		case domain.StatusSucceeded, domain.StatusDead, domain.StatusCancelled:
		default:
			continue
		}
		if j.FinishedAt == nil || !j.FinishedAt.Before(before) {
			continue
		}
		victims = append(victims, cand{id, j})
	}
	sort.SliceStable(victims, func(i, k int) bool {
		return victims[i].j.FinishedAt.Before(*victims[k].j.FinishedAt)
	})
	if len(victims) > limit {
		victims = victims[:limit]
	}
	for _, c := range victims {
		j := c.j
		delete(r.jobs, c.id)
		delete(r.attempts, c.id)
		if j.IdempotencyKey != "" {
			delete(r.byKey, idempotencyIndexKey(j.Queue, j.IdempotencyKey))
		}
	}
	return len(victims), nil
}

// Subscribe implements app.JobNotifier.
func (r *Repository) Subscribe(_ context.Context, queue string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	if r.subs[queue] == nil {
		r.subs[queue] = make(map[chan struct{}]struct{})
	}
	r.subs[queue][ch] = struct{}{}
	r.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if m := r.subs[queue]; m != nil {
				delete(m, ch)
				if len(m) == 0 {
					delete(r.subs, queue)
				}
			}
			close(ch)
		})
	}
	return ch, unsub
}
