package app_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeNotifier delivers wakeups on demand.
type fakeNotifier struct {
	mu   sync.Mutex
	subs map[string][]chan struct{}
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{subs: make(map[string][]chan struct{})}
}

func (n *fakeNotifier) Subscribe(_ context.Context, queue string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.subs[queue] = append(n.subs[queue], ch)
	n.mu.Unlock()
	var once sync.Once
	unsub := func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			list := n.subs[queue]
			for i, c := range list {
				if c == ch {
					n.subs[queue] = append(list[:i], list[i+1:]...)
					break
				}
			}
			close(ch)
		})
	}
	return ch, unsub
}

func (n *fakeNotifier) Notify(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ch := range n.subs[queue] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func newServiceWithNotifier(t *testing.T, now time.Time, n app.JobNotifier, ids ...domain.JobID) (*app.JobService, *memRepo, *fixedClock) {
	t.Helper()
	repo := newMemRepo()
	clock := &fixedClock{now: now}
	var gen app.IDGenerator
	if len(ids) > 0 {
		gen = &seqIDs{ids: ids}
	} else {
		gen = app.UUIDv7Generator{}
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := app.NewJobService(repo, clock, gen, app.FixedDelayRetryPolicy{Delay: time.Minute}, log, n)
	return svc, repo, clock
}

func TestLeaseWait_ImmediateHit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	id := mustID("018f0000-0000-7000-8000-000000000100")
	n := newFakeNotifier()
	svc, _, _ := newServiceWithNotifier(t, now, n, id)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)

	jobs, err := svc.LeaseWait(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second, Wait: 5 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
}

func TestLeaseWait_WakeupHit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	id1 := mustID("018f0000-0000-7000-8000-000000000101")
	id2 := mustID("018f0000-0000-7000-8000-000000000102")
	n := newFakeNotifier()
	svc, _, _ := newServiceWithNotifier(t, now, n, id1, id2)

	done := make(chan []domain.Job, 1)
	errCh := make(chan error, 1)
	go func() {
		jobs, err := svc.LeaseWait(context.Background(), app.LeaseCommand{
			Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second, Wait: 3 * time.Second,
		})
		errCh <- err
		done <- jobs
	}()

	time.Sleep(50 * time.Millisecond)
	_, _, err := svc.Enqueue(context.Background(), app.EnqueueCommand{Queue: "default", Type: "t"})
	require.NoError(t, err)
	n.Notify("default")

	select {
	case jobs := <-done:
		require.NoError(t, <-errCh)
		require.Len(t, jobs, 1)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for wakeup claim")
	}
}

func TestLeaseWait_Timeout(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	n := newFakeNotifier()
	svc, _, _ := newServiceWithNotifier(t, now, n)

	start := time.Now()
	jobs, err := svc.LeaseWait(context.Background(), app.LeaseCommand{
		Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second, Wait: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Empty(t, jobs)
	assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
}

func TestLeaseWait_CtxCancel(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	n := newFakeNotifier()
	svc, _, _ := newServiceWithNotifier(t, now, n)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.LeaseWait(ctx, app.LeaseCommand{
			Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second, Wait: 5 * time.Second,
		})
		errCh <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("did not return on ctx cancel")
	}
}

func TestLeaseWait_SpuriousWakeup(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	n := newFakeNotifier()
	svc, _, _ := newServiceWithNotifier(t, now, n)

	done := make(chan struct{})
	go func() {
		defer close(done)
		jobs, err := svc.LeaseWait(context.Background(), app.LeaseCommand{
			Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: 30 * time.Second, Wait: 300 * time.Millisecond,
		})
		require.NoError(t, err)
		assert.Empty(t, jobs)
	}()
	time.Sleep(30 * time.Millisecond)
	n.Notify("default") // spurious — no jobs
	n.Notify("default")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lease wait hung after spurious wakeups")
	}
}
