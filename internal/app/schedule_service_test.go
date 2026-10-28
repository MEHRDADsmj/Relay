package app_test

import (
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

func TestScheduleService_CRUDAndTrigger(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := &fixedClock{now: now}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	js := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Minute}, log, nil)
	ss := app.NewScheduleService(repo, js, adaptercron.NewParser(), clock, app.UUIDv7Generator{})

	created, err := ss.Create(adminCtx(), app.CreateScheduleCommand{
		Name: "nightly", Cron: "@daily", Queue: "default", JobType: "report", Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "nightly", created.Name)
	assert.True(t, created.Enabled)

	_, err = ss.Create(adminCtx(), app.CreateScheduleCommand{
		Name: "nightly", Cron: "@hourly", Queue: "default", JobType: "t",
	})
	require.ErrorIs(t, err, domain.ErrConflict)

	got, err := ss.GetByName(adminCtx(), "nightly")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)

	disabled, err := ss.Disable(adminCtx(), created.ID)
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	enabled, err := ss.Enable(adminCtx(), created.ID)
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)

	cron := "@hourly"
	_, err = ss.Update(adminCtx(), app.UpdateScheduleCommand{
		ID: created.ID, ExpectedUpdatedAt: time.Time{}, Cron: &cron,
	})
	require.ErrorIs(t, err, domain.ErrConflict)

	updated, err := ss.Update(adminCtx(), app.UpdateScheduleCommand{
		ID: created.ID, ExpectedUpdatedAt: enabled.UpdatedAt, Cron: &cron,
	})
	require.NoError(t, err)
	assert.Equal(t, "@hourly", updated.Cron)

	job, err := ss.TriggerNow(adminCtx(), created.ID)
	require.NoError(t, err)
	assert.Equal(t, "default", job.Queue)
	assert.Contains(t, job.IdempotencyKey, "manual")

	require.NoError(t, ss.Delete(adminCtx(), created.ID))
	_, err = ss.Get(adminCtx(), created.ID)
	require.ErrorIs(t, err, domain.ErrNotFound)
}
