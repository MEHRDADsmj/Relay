package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Release implements app.JobRepository.
func (r *Repository) Release(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	now = now.UTC()
	res, err := releaseScript.Run(ctx, r.client, []string{r.keys.Job(id.String())},
		owner,
		strconv.FormatInt(toUnixMs(now), 10),
		id.String(),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return domain.Job{}, err
	}
	return mapScriptJobResult(res)
}

// Cancel implements app.JobRepository.
func (r *Repository) Cancel(ctx context.Context, id domain.JobID, now time.Time) (domain.Job, error) {
	now = now.UTC()
	res, err := cancelScript.Run(ctx, r.client, []string{r.keys.Job(id.String())},
		strconv.FormatInt(toUnixMs(now), 10),
		id.String(),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return domain.Job{}, err
	}
	return mapScriptJobResult(res)
}

// Retry implements app.JobRepository.
func (r *Repository) Retry(ctx context.Context, id domain.JobID, now time.Time) (domain.Job, error) {
	now = now.UTC()
	res, err := retryScript.Run(ctx, r.client, []string{r.keys.Job(id.String())},
		strconv.FormatInt(toUnixMs(now), 10),
		id.String(),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return domain.Job{}, err
	}
	return mapScriptJobResult(res)
}

// ListAttempts implements app.JobRepository.
func (r *Repository) ListAttempts(ctx context.Context, id domain.JobID) ([]domain.Attempt, error) {
	if _, err := r.Get(ctx, id); err != nil {
		return nil, err
	}
	raws, err := r.client.LRange(ctx, r.keys.Attempts(id.String()), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.Attempt, 0, len(raws))
	for _, raw := range raws {
		a, err := decodeAttempt(raw)
		if err != nil {
			return nil, fmt.Errorf("redis list attempts: %w", err)
		}
		out = append(out, a)
	}
	return out, nil
}

// ListByStatus implements app.JobRepository.
func (r *Repository) ListByStatus(ctx context.Context, queue string, status domain.Status, page domain.PageRequest) (domain.Page[domain.Job], error) {
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

	var jobs []domain.Job
	var err error
	switch status {
	case domain.StatusPending:
		jobs, err = r.listPendingJobs(ctx, queue)
	case domain.StatusRunning:
		jobs, err = r.listRunningJobs(ctx, queue)
	case domain.StatusSucceeded, domain.StatusDead, domain.StatusCancelled:
		jobs, err = r.listTerminalJobs(ctx, queue, status)
	default:
		return domain.Page[domain.Job]{}, nil
	}
	if err != nil {
		return domain.Page[domain.Job]{}, err
	}

	matched := make([]domain.Job, 0, len(jobs))
	for _, j := range jobs {
		if j.Status != status {
			continue
		}
		if hasCursor {
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
	return domain.Page[domain.Job]{Items: matched, NextCursor: nextCursor}, nil
}

func (r *Repository) listPendingJobs(ctx context.Context, queue string) ([]domain.Job, error) {
	ids, err := r.client.ZRange(ctx, r.keys.Ready(queue), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	sched, err := r.client.ZRange(ctx, r.keys.Scheduled(queue), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	ids = append(ids, sched...)
	return r.loadJobs(ctx, ids)
}

func (r *Repository) listRunningJobs(ctx context.Context, queue string) ([]domain.Job, error) {
	ids, err := r.client.ZRange(ctx, r.keys.Leased(queue), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return r.loadJobs(ctx, ids)
}

func (r *Repository) listTerminalJobs(ctx context.Context, queue string, status domain.Status) ([]domain.Job, error) {
	// Highest updated_at first.
	ids, err := r.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:     r.keys.Status(queue, string(status)),
		Start:   "0",
		Stop:    "-1",
		Rev:     true,
		ByScore: false,
	}).Result()
	if err != nil {
		return nil, err
	}
	return r.loadJobs(ctx, ids)
}

func (r *Repository) loadJobs(ctx context.Context, ids []string) ([]domain.Job, error) {
	out := make([]domain.Job, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		flat, err := r.client.HGetAll(ctx, r.keys.Job(id)).Result()
		if err != nil {
			return nil, err
		}
		if len(flat) == 0 {
			continue
		}
		job, err := mapToJob(flat)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, nil
}

// ListQueues implements app.QueueRepository.
func (r *Repository) ListQueues(ctx context.Context) ([]domain.Queue, error) {
	names, err := r.client.SMembers(ctx, r.keys.Queues()).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	out := make([]domain.Queue, 0, len(names))
	for _, name := range names {
		m, err := r.client.HGetAll(ctx, r.keys.Queue(name)).Result()
		if err != nil {
			return nil, err
		}
		if len(m) == 0 {
			continue
		}
		q, err := mapToQueue(name, m)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

// Stats implements app.QueueRepository.
//
// Counter strategy: running from ZCARD(leased); terminal from counts hash;
// pending/scheduled/OldestPendingAge from pending job hashes (run_at vs now),
// because scheduled zset members are only promoted on Claim/Reap.
func (r *Repository) Stats(ctx context.Context, queue string, now time.Time) (domain.QueueStats, error) {
	now = now.UTC()
	m, err := r.client.HGetAll(ctx, r.keys.Queue(queue)).Result()
	if err != nil {
		return domain.QueueStats{}, err
	}
	if len(m) == 0 {
		return domain.QueueStats{}, domain.ErrNotFound
	}
	q, err := mapToQueue(queue, m)
	if err != nil {
		return domain.QueueStats{}, err
	}

	running, err := r.client.ZCard(ctx, r.keys.Leased(queue)).Result()
	if err != nil {
		return domain.QueueStats{}, err
	}
	counts, err := r.client.HGetAll(ctx, r.keys.Counts(queue)).Result()
	if err != nil {
		return domain.QueueStats{}, err
	}

	pendingJobs, err := r.listPendingJobs(ctx, queue)
	if err != nil {
		return domain.QueueStats{}, err
	}

	stats := domain.QueueStats{
		Queue:     queue,
		Paused:    q.Paused,
		Running:   running,
		Succeeded: parseCount(counts["succeeded"]),
		Dead:      parseCount(counts["dead"]),
		Cancelled: parseCount(counts["cancelled"]),
	}
	var oldestDue *time.Time
	for _, j := range pendingJobs {
		if j.Status != domain.StatusPending {
			continue
		}
		stats.Pending++
		if j.RunAt.After(now) {
			stats.Scheduled++
		} else if oldestDue == nil || j.RunAt.Before(*oldestDue) {
			t := j.RunAt
			oldestDue = &t
		}
	}
	if oldestDue != nil {
		age := now.Sub(*oldestDue)
		if age > 0 {
			stats.OldestPendingAge = age
		}
	}
	return stats, nil
}

func parseCount(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// SetPaused implements app.QueueRepository.
func (r *Repository) SetPaused(ctx context.Context, queue string, paused bool, now time.Time) (domain.Queue, error) {
	now = now.UTC()
	exists, err := r.client.Exists(ctx, r.keys.Queue(queue)).Result()
	if err != nil {
		return domain.Queue{}, err
	}
	if exists == 0 {
		return domain.Queue{}, domain.ErrNotFound
	}
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, r.keys.Queue(queue),
		"paused", boolToRedis(paused),
		"updated_at", msString(now),
	)
	if !paused {
		pipe.Publish(ctx, r.keys.NotifyChannel(queue), queue)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return domain.Queue{}, err
	}
	m, err := r.client.HGetAll(ctx, r.keys.Queue(queue)).Result()
	if err != nil {
		return domain.Queue{}, err
	}
	return mapToQueue(queue, m)
}

// ReapExpired implements app.MaintenanceRepository.
func (r *Repository) ReapExpired(ctx context.Context, now time.Time, limit int) (app.ReapResult, error) {
	now = now.UTC()
	if limit <= 0 {
		return app.ReapResult{}, nil
	}
	res, err := reapScript.Run(ctx, r.client, nil,
		strconv.FormatInt(toUnixMs(now), 10),
		strconv.Itoa(limit),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return app.ReapResult{}, err
	}
	arr, err := interfaceSliceToStrings(res)
	if err != nil || len(arr) < 2 {
		return app.ReapResult{}, fmt.Errorf("redis reap: bad result %v: %w", res, err)
	}
	requeued, _ := strconv.Atoi(arr[0])
	dead, _ := strconv.Atoi(arr[1])
	return app.ReapResult{Requeued: requeued, Dead: dead}, nil
}

// PurgeFinished implements app.MaintenanceRepository.
func (r *Repository) PurgeFinished(ctx context.Context, before time.Time, limit int) (int, error) {
	before = before.UTC()
	if limit <= 0 {
		return 0, nil
	}
	res, err := purgeScript.Run(ctx, r.client, nil,
		strconv.FormatInt(toUnixMs(before), 10),
		strconv.Itoa(limit),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return 0, err
	}
	switch v := res.(type) {
	case int64:
		return int(v), nil
	case string:
		n, err := strconv.Atoi(v)
		return n, err
	default:
		n, err := strconv.Atoi(fmt.Sprint(v))
		return n, err
	}
}

// Subscribe implements app.JobNotifier.
func (r *Repository) Subscribe(ctx context.Context, queue string) (<-chan struct{}, func()) {
	if r.notifier == nil {
		ch := make(chan struct{})
		return ch, func() {}
	}
	return r.notifier.Subscribe(ctx, queue)
}
