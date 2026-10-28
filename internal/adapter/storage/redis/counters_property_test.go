//go:build integration

package redis_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	storageredis "github.com/MEHRDADsmj/relay/internal/adapter/storage/redis"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// TestCountsHash_PropertyRandomOps asserts terminal status counters in
// relay:q:{queue}:counts match a full recount after a seeded random op sequence.
func TestCountsHash_PropertyRandomOps(t *testing.T) {
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(terminateCtx)
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := goredis.ParseURL(uri)
	require.NoError(t, err)
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(ctx).Err())

	const prefix = "relay"
	const queue = "prop"
	repo := storageredis.New(client, prefix)
	keys := storageredis.NewKeys(prefix)

	rng := rand.New(rand.NewSource(42))
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	lease := 30 * time.Second

	type tracked struct {
		id    domain.JobID
		owner string
	}
	var running []tracked
	var deadIDs []domain.JobID

	newJob := func(i int) domain.Job {
		id := uuid.MustParse("01900000-0000-7000-8000-" + pad12(i))
		return domain.Job{
			ID:          id,
			Queue:       queue,
			Type:        "t",
			Payload:     []byte(`{}`),
			Status:      domain.StatusPending,
			Priority:    rng.Intn(10),
			RunAt:       now,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
	}

	assertCounts := func(t *testing.T) {
		t.Helper()
		want := map[string]int64{"succeeded": 0, "dead": 0, "cancelled": 0}
		// Scan finished index + job hashes for terminal statuses on this queue.
		ids, err := client.ZRange(ctx, keys.Finished(), 0, -1).Result()
		require.NoError(t, err)
		seen := make(map[string]struct{})
		for _, id := range ids {
			seen[id] = struct{}{}
			flat, err := client.HGetAll(ctx, keys.Job(id)).Result()
			require.NoError(t, err)
			if len(flat) == 0 || flat["queue"] != queue {
				continue
			}
			switch domain.Status(flat["status"]) {
			case domain.StatusSucceeded:
				want["succeeded"]++
			case domain.StatusDead:
				want["dead"]++
			case domain.StatusCancelled:
				want["cancelled"]++
			}
		}
		// Also scan status zsets in case finished is missing (should not happen).
		for _, st := range []string{"succeeded", "dead", "cancelled"} {
			zids, err := client.ZRange(ctx, keys.Status(queue, st), 0, -1).Result()
			require.NoError(t, err)
			for _, id := range zids {
				if _, ok := seen[id]; ok {
					continue
				}
				flat, err := client.HGetAll(ctx, keys.Job(id)).Result()
				require.NoError(t, err)
				if len(flat) == 0 {
					continue
				}
				if domain.Status(flat["status"]) == domain.Status(st) {
					want[st]++
				}
			}
		}

		got, err := client.HGetAll(ctx, keys.Counts(queue)).Result()
		require.NoError(t, err)
		for _, st := range []string{"succeeded", "dead", "cancelled"} {
			g := int64(0)
			if s := got[st]; s != "" {
				g, err = strconv.ParseInt(s, 10, 64)
				require.NoError(t, err)
			}
			require.Equal(t, want[st], g, "counts[%s]", st)
		}
	}

	jobSeq := 0
	for step := 0; step < 80; step++ {
		now = now.Add(time.Millisecond)
		op := rng.Intn(7)
		switch op {
		case 0: // enqueue
			jobSeq++
			_, _, err := repo.Create(ctx, newJob(jobSeq))
			require.NoError(t, err)
		case 1: // claim
			jobs, err := repo.Claim(ctx, queue, "w"+strconv.Itoa(rng.Intn(3)), 2, lease, now)
			require.NoError(t, err)
			for _, j := range jobs {
				running = append(running, tracked{id: j.ID, owner: j.LeaseOwner})
			}
		case 2: // complete
			if len(running) == 0 {
				continue
			}
			i := rng.Intn(len(running))
			tr := running[i]
			running = append(running[:i], running[i+1:]...)
			_, err := repo.Complete(ctx, tr.id, tr.owner, now)
			if err != nil {
				continue
			}
		case 3: // fail dead
			if len(running) == 0 {
				continue
			}
			i := rng.Intn(len(running))
			tr := running[i]
			running = append(running[:i], running[i+1:]...)
			j, err := repo.Fail(ctx, tr.id, tr.owner, "boom", domain.FailureDecision{NextStatus: domain.StatusDead}, now)
			if err != nil {
				continue
			}
			deadIDs = append(deadIDs, j.ID)
		case 4: // cancel pending via claim-skip: create + cancel
			jobSeq++
			j := newJob(jobSeq)
			_, _, err := repo.Create(ctx, j)
			require.NoError(t, err)
			_, err = repo.Cancel(ctx, j.ID, now)
			require.NoError(t, err)
		case 5: // retry dead
			if len(deadIDs) == 0 {
				continue
			}
			i := rng.Intn(len(deadIDs))
			id := deadIDs[i]
			deadIDs = append(deadIDs[:i], deadIDs[i+1:]...)
			_, err := repo.Retry(ctx, id, now)
			if err != nil {
				continue
			}
		case 6: // release
			if len(running) == 0 {
				continue
			}
			i := rng.Intn(len(running))
			tr := running[i]
			running = append(running[:i], running[i+1:]...)
			_, err := repo.Release(ctx, tr.id, tr.owner, now)
			if err != nil {
				continue
			}
		}
		if step%10 == 9 {
			assertCounts(t)
		}
	}
	assertCounts(t)
}

func pad12(i int) string {
	s := strconv.Itoa(i)
	for len(s) < 12 {
		s = "0" + s
	}
	if len(s) > 12 {
		s = s[len(s)-12:]
	}
	return s
}
