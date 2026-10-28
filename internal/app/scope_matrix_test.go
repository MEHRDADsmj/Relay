package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// scopeMatrix covers every service method's required scope.
func TestScopeMatrix(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := &fixedClock{now: now}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	js := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, log, nil)
	qs := app.NewQueueService(repo, repo, clock)
	parser := adaptercron.NewParser()
	ss := app.NewScheduleService(repo, js, parser, clock, app.UUIDv7Generator{})
	aks := app.NewAPIKeyService(repo, clock)

	type check struct {
		name     string
		need     domain.Scope
		call     func(ctx context.Context) error
		allowAll bool // Ready has no scope
	}

	jobID := mustID("018f0000-0000-7000-8000-000000000099")
	checks := []check{
		{"JobService.Enqueue", domain.ScopeProducer, func(ctx context.Context) error {
			_, _, err := js.Enqueue(ctx, app.EnqueueCommand{Queue: "default", Type: "t"})
			return err
		}, false},
		{"JobService.Get", domain.ScopeProducer, func(ctx context.Context) error {
			_, err := js.Get(ctx, jobID)
			return err
		}, false},
		{"JobService.Lease", domain.ScopeWorker, func(ctx context.Context) error {
			_, err := js.Lease(ctx, app.LeaseCommand{Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: time.Second})
			return err
		}, false},
		{"JobService.LeaseWait", domain.ScopeWorker, func(ctx context.Context) error {
			_, err := js.LeaseWait(ctx, app.LeaseCommand{Queue: "default", WorkerID: "w1", MaxJobs: 1, LeaseDuration: time.Second})
			return err
		}, false},
		{"JobService.Heartbeat", domain.ScopeWorker, func(ctx context.Context) error {
			_, err := js.Heartbeat(ctx, app.HeartbeatCommand{JobID: jobID, WorkerID: "w1", LeaseDuration: time.Second})
			return err
		}, false},
		{"JobService.Complete", domain.ScopeWorker, func(ctx context.Context) error {
			_, err := js.Complete(ctx, app.CompleteCommand{JobID: jobID, WorkerID: "w1"})
			return err
		}, false},
		{"JobService.Fail", domain.ScopeWorker, func(ctx context.Context) error {
			_, err := js.Fail(ctx, app.FailCommand{JobID: jobID, WorkerID: "w1", Error: "x"})
			return err
		}, false},
		{"JobService.Release", domain.ScopeWorker, func(ctx context.Context) error {
			_, err := js.Release(ctx, app.ReleaseCommand{JobID: jobID, WorkerID: "w1"})
			return err
		}, false},
		{"JobService.Cancel", domain.ScopeProducer, func(ctx context.Context) error {
			_, err := js.Cancel(ctx, jobID)
			return err
		}, false},
		{"JobService.Retry", domain.ScopeProducer, func(ctx context.Context) error {
			_, err := js.Retry(ctx, jobID)
			return err
		}, false},
		{"JobService.ListAttempts", domain.ScopeProducer, func(ctx context.Context) error {
			_, err := js.ListAttempts(ctx, jobID)
			return err
		}, false},
		{"JobService.ListByStatus", domain.ScopeProducer, func(ctx context.Context) error {
			_, err := js.ListByStatus(ctx, app.ListByStatusCommand{Queue: "default", Status: domain.StatusPending})
			return err
		}, false},
		{"JobService.Ready", "", func(ctx context.Context) error {
			return js.Ready(ctx)
		}, true},
		{"QueueService.ListQueues", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := qs.ListQueues(ctx)
			return err
		}, false},
		{"QueueService.Stats", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := qs.Stats(ctx, "default")
			return err
		}, false},
		{"QueueService.Pause", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := qs.Pause(ctx, "default")
			return err
		}, false},
		{"QueueService.Resume", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := qs.Resume(ctx, "default")
			return err
		}, false},
		{"QueueService.RetryAllDead", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := qs.RetryAllDead(ctx, app.RetryAllDeadCommand{Queue: "default"})
			return err
		}, false},
		{"ScheduleService.Create", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.Create(ctx, app.CreateScheduleCommand{Name: "scope-create-" + uuidShort(), Cron: "@hourly", Queue: "default", JobType: "t"})
			return err
		}, false},
		{"ScheduleService.Get", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.Get(ctx, jobID)
			return err
		}, false},
		{"ScheduleService.GetByName", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.GetByName(ctx, "missing")
			return err
		}, false},
		{"ScheduleService.List", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.List(ctx, domain.PageRequest{})
			return err
		}, false},
		{"ScheduleService.Update", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.Update(ctx, app.UpdateScheduleCommand{ID: jobID, ExpectedUpdatedAt: now})
			return err
		}, false},
		{"ScheduleService.Enable", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.Enable(ctx, jobID)
			return err
		}, false},
		{"ScheduleService.Disable", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.Disable(ctx, jobID)
			return err
		}, false},
		{"ScheduleService.Delete", domain.ScopeAdmin, func(ctx context.Context) error {
			return ss.Delete(ctx, jobID)
		}, false},
		{"ScheduleService.TriggerNow", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := ss.TriggerNow(ctx, jobID)
			return err
		}, false},
		{"APIKeyService.Create", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := aks.Create(ctx, app.CreateAPIKeyCommand{Name: "k", Scopes: []domain.Scope{domain.ScopeProducer}})
			return err
		}, false},
		{"APIKeyService.List", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := aks.List(ctx)
			return err
		}, false},
		{"APIKeyService.Revoke", domain.ScopeAdmin, func(ctx context.Context) error {
			_, err := aks.Revoke(ctx, "nope")
			return err
		}, false},
	}

	wrongScope := map[domain.Scope]domain.Scope{
		domain.ScopeProducer: domain.ScopeWorker,
		domain.ScopeWorker:   domain.ScopeProducer,
		domain.ScopeAdmin:    domain.ScopeProducer,
	}

	for _, c := range checks {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if c.allowAll {
				require.NoError(t, c.call(context.Background()))
				return
			}
			err := c.call(context.Background())
			assert.ErrorIs(t, err, domain.ErrUnauthenticated)

			err = c.call(principalCtx(wrongScope[c.need]))
			assert.ErrorIs(t, err, domain.ErrForbidden, "wrong scope must be forbidden")

			err = c.call(principalCtx(c.need))
			if err != nil {
				assert.NotErrorIs(t, err, domain.ErrForbidden)
				assert.NotErrorIs(t, err, domain.ErrUnauthenticated)
			}

			err = c.call(principalCtx(domain.ScopeAdmin))
			if err != nil {
				assert.NotErrorIs(t, err, domain.ErrForbidden)
				assert.NotErrorIs(t, err, domain.ErrUnauthenticated)
			}
		})
	}
}
