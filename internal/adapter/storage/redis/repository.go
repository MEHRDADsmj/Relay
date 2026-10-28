package redis

import (
	"context"
	"embed"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

var (
	enqueueScript  = goredis.NewScript(mustReadScript("scripts/enqueue.lua"))
	claimScript    = goredis.NewScript(mustReadScript("scripts/claim.lua"))
	extendScript   = goredis.NewScript(mustReadScript("scripts/extend.lua"))
	completeScript = goredis.NewScript(mustReadScript("scripts/complete.lua"))
	failScript     = goredis.NewScript(mustReadScript("scripts/fail.lua"))
)

func mustReadScript(name string) string {
	b, err := scriptFS.ReadFile(name)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// Repository implements app.JobRepository against a single Redis node.
type Repository struct {
	client *goredis.Client
	keys   Keys
}

// New wraps an existing client. The caller owns the client lifecycle.
func New(client *goredis.Client, prefix string) *Repository {
	return &Repository{client: client, keys: NewKeys(prefix)}
}

// Open creates a go-redis client from redisURL and returns a repository.
// Call Close when finished.
func Open(ctx context.Context, redisURL, prefix string) (*Repository, error) {
	opts, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("redis open: parse url: %w", err)
	}
	client := goredis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis open: ping: %w", err)
	}
	return New(client, prefix), nil
}

// Close closes the underlying client.
func (r *Repository) Close() error {
	return r.client.Close()
}

var _ app.JobRepository = (*Repository)(nil)

// Create implements app.JobRepository.
//
// Enqueue places the job on ready vs scheduled by comparing run_at to
// job.UpdatedAt (the service clock at insert time).
func (r *Repository) Create(ctx context.Context, job domain.Job) (domain.Job, bool, error) {
	nowMs := toUnixMs(job.UpdatedAt)
	runAtMs := toUnixMs(job.RunAt)
	id := job.ID.String()

	idemKey := ""
	if job.IdempotencyKey != "" {
		idemKey = r.keys.Idempotency(job.Queue, job.IdempotencyKey)
	}

	res, err := enqueueScript.Run(ctx, r.client, []string{
		r.keys.Job(id),
		r.keys.Scheduled(job.Queue),
		r.keys.Ready(job.Queue),
		idemKey,
	},
		id,
		job.Queue,
		job.Type,
		string(job.Payload),
		string(job.Status),
		strconv.Itoa(job.Priority),
		strconv.FormatInt(runAtMs, 10),
		strconv.Itoa(job.Attempts),
		strconv.Itoa(job.MaxAttempts),
		job.LeaseOwner,
		optionalMsString(job.LeaseExpiresAt),
		job.IdempotencyKey,
		job.LastError,
		msString(job.CreatedAt),
		msString(job.UpdatedAt),
		optionalMsString(job.FinishedAt),
		strconv.FormatInt(nowMs, 10),
		strconv.FormatFloat(readyScore(job.Priority, runAtMs), 'f', 0, 64),
	).Result()
	if err != nil {
		return domain.Job{}, false, err
	}

	arr, err := interfaceSliceToStrings(res)
	if err != nil || len(arr) < 2 {
		return domain.Job{}, false, fmt.Errorf("redis enqueue: bad result %v: %w", res, err)
	}
	created := arr[0] == "1"
	if created {
		return job, true, nil
	}
	existingID, err := uuid.Parse(arr[1])
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("redis enqueue: existing id: %w", err)
	}
	existing, err := r.Get(ctx, existingID)
	if err != nil {
		return domain.Job{}, false, err
	}
	return existing, false, nil
}

// Get implements app.JobRepository.
func (r *Repository) Get(ctx context.Context, id domain.JobID) (domain.Job, error) {
	flat, err := r.client.HGetAll(ctx, r.keys.Job(id.String())).Result()
	if err != nil {
		return domain.Job{}, err
	}
	if len(flat) == 0 {
		return domain.Job{}, domain.ErrNotFound
	}
	return mapToJob(flat)
}

// Claim implements app.JobRepository.
func (r *Repository) Claim(ctx context.Context, queue, owner string, limit int, lease time.Duration, now time.Time) ([]domain.Job, error) {
	if limit < 0 {
		limit = 0
	}
	now = now.UTC()
	leaseMs := lease.Milliseconds()
	if leaseMs < 0 {
		leaseMs = 0
	}

	res, err := claimScript.Run(ctx, r.client, []string{
		r.keys.Scheduled(queue),
		r.keys.Ready(queue),
		r.keys.Leased(queue),
	},
		strconv.FormatInt(toUnixMs(now), 10),
		owner,
		strconv.Itoa(limit),
		strconv.FormatInt(leaseMs, 10),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return nil, err
	}
	if res == nil {
		return []domain.Job{}, nil
	}

	rows, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redis claim: expected []interface{}, got %T", res)
	}
	out := make([]domain.Job, 0, len(rows))
	for _, row := range rows {
		flat, err := interfaceSliceToStrings(row)
		if err != nil {
			return nil, err
		}
		job, err := hashToJob(flat)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, nil
}

// ExtendLease implements app.JobRepository.
func (r *Repository) ExtendLease(ctx context.Context, id domain.JobID, owner string, lease time.Duration, now time.Time) (domain.Job, error) {
	now = now.UTC()
	leaseMs := lease.Milliseconds()
	res, err := extendScript.Run(ctx, r.client, []string{r.keys.Job(id.String())},
		owner,
		strconv.FormatInt(toUnixMs(now), 10),
		strconv.FormatInt(leaseMs, 10),
		id.String(),
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return domain.Job{}, err
	}
	return mapScriptJobResult(res)
}

// Complete implements app.JobRepository.
func (r *Repository) Complete(ctx context.Context, id domain.JobID, owner string, now time.Time) (domain.Job, error) {
	now = now.UTC()
	res, err := completeScript.Run(ctx, r.client, []string{r.keys.Job(id.String())},
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

// Fail implements app.JobRepository.
func (r *Repository) Fail(ctx context.Context, id domain.JobID, owner string, errMsg string, decision domain.FailureDecision, now time.Time) (domain.Job, error) {
	now = now.UTC()
	nextRunAt := now
	if decision.NextRunAt != nil {
		nextRunAt = decision.NextRunAt.UTC()
	}
	runAtMs := toUnixMs(nextRunAt)

	res, err := failScript.Run(ctx, r.client, []string{r.keys.Job(id.String())},
		owner,
		strconv.FormatInt(toUnixMs(now), 10),
		id.String(),
		errMsg,
		string(decision.NextStatus),
		strconv.FormatInt(runAtMs, 10),
		"", // empty → Lua recomputes ready score from hash priority
		r.keys.Prefix(),
	).Result()
	if err != nil {
		return domain.Job{}, err
	}
	return mapScriptJobResult(res)
}

// Ping implements app.JobRepository.
func (r *Repository) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func mapScriptJobResult(res any) (domain.Job, error) {
	arr, err := interfaceSliceToStrings(res)
	if err != nil || len(arr) < 1 {
		return domain.Job{}, fmt.Errorf("redis script: bad result %v: %w", res, err)
	}
	switch arr[0] {
	case "NOT_FOUND":
		return domain.Job{}, domain.ErrNotFound
	case "LEASE_LOST":
		return domain.Job{}, domain.ErrLeaseLost
	case "OK":
		if len(arr) < 2 {
			return domain.Job{}, fmt.Errorf("redis script: OK without hash")
		}
		return hashToJob(arr[1:])
	default:
		return domain.Job{}, fmt.Errorf("redis script: unknown code %q", arr[0])
	}
}
