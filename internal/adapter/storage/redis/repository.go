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
	helpersLua = mustReadScript("scripts/helpers.lua")

	enqueueScript  = newScript("scripts/enqueue.lua")
	claimScript    = newScript("scripts/claim.lua")
	extendScript   = newScript("scripts/extend.lua")
	completeScript = newScript("scripts/complete.lua")
	failScript     = newScript("scripts/fail.lua")
	releaseScript  = newScript("scripts/release.lua")
	cancelScript   = newScript("scripts/cancel.lua")
	retryScript    = newScript("scripts/retry.lua")
	reapScript     = newScript("scripts/reap.lua")
	purgeScript    = newScript("scripts/purge.lua")
)

func mustReadScript(name string) string {
	b, err := scriptFS.ReadFile(name)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newScript(name string) *goredis.Script {
	return goredis.NewScript(helpersLua + "\n" + mustReadScript(name))
}

// Repository implements app storage ports against a single Redis node.
type Repository struct {
	client   *goredis.Client
	keys     Keys
	notifier *Notifier
}

// New wraps an existing client. The caller owns the client lifecycle.
func New(client *goredis.Client, prefix string) *Repository {
	return &Repository{client: client, keys: NewKeys(prefix)}
}

// AttachNotifier wires a Pub/Sub fan-out for Subscribe.
func (r *Repository) AttachNotifier(n *Notifier) {
	r.notifier = n
}

// Open creates a go-redis client from redisURL, attaches a Notifier, and returns a repository.
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
	repo := New(client, prefix)
	repo.AttachNotifier(NewNotifier(ctx, client, prefix))
	return repo, nil
}

// Close closes the notifier (if any) and the underlying client.
func (r *Repository) Close() error {
	if r.notifier != nil {
		r.notifier.Close()
		r.notifier = nil
	}
	return r.client.Close()
}

var (
	_ app.JobRepository         = (*Repository)(nil)
	_ app.QueueRepository       = (*Repository)(nil)
	_ app.MaintenanceRepository = (*Repository)(nil)
	_ app.JobNotifier           = (*Repository)(nil)
)

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
		r.keys.Prefix(),
		boolToRedis(job.CancelRequested),
		optionalMsString(job.CancelledAt),
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
		queue,
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
		"", // unused; Lua recomputes ready score
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
