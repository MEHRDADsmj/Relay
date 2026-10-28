package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// ScheduleBackend is a storage adapter that implements schedule + API key ports.
type ScheduleBackend interface {
	Backend
	ScheduleAPI
}

// ScheduleAPI is the Phase 3 schedule/API-key repository surface.
type ScheduleAPI interface {
	CreateSchedule(ctx context.Context, s domain.Schedule) (domain.Schedule, error)
	GetSchedule(ctx context.Context, id domain.ScheduleID) (domain.Schedule, error)
	GetScheduleByName(ctx context.Context, name string) (domain.Schedule, error)
	ListSchedules(ctx context.Context, page domain.PageRequest) (domain.Page[domain.Schedule], error)
	UpdateSchedule(ctx context.Context, s domain.Schedule, expectedUpdatedAt time.Time) (domain.Schedule, error)
	DeleteSchedule(ctx context.Context, id domain.ScheduleID) error
	DueSchedules(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error)
	MarkFired(ctx context.Context, id domain.ScheduleID, expectedNext, firedAt, next time.Time, jobID domain.JobID) error

	CreateAPIKey(ctx context.Context, key domain.APIKey) (domain.APIKey, error)
	GetAPIKey(ctx context.Context, id string) (domain.APIKey, error)
	GetAPIKeyByPrefix(ctx context.Context, prefix string) (domain.APIKey, error)
	ListAPIKeys(ctx context.Context) ([]domain.APIKey, error)
	RevokeAPIKey(ctx context.Context, id string, revokedAt time.Time) (domain.APIKey, error)
	UpdateLastUsed(ctx context.Context, id string, at time.Time) error
}

// RunPhase3Contract runs schedule + API key contract tests (memory in T2; pg/redis in T3/T4).
func RunPhase3Contract(t *testing.T, newRepo func(t *testing.T) ScheduleAPI) {
	t.Helper()
	RunScheduleContract(t, newRepo)
	RunAPIKeyContract(t, newRepo)
}

func sampleSchedule(id domain.ScheduleID, name string, now time.Time) domain.Schedule {
	return domain.Schedule{
		ID:          id,
		Name:        name,
		Cron:        "@hourly",
		Timezone:    "UTC",
		Queue:       "default",
		JobType:     "t",
		Payload:     []byte(`{}`),
		Priority:    0,
		MaxAttempts: 10,
		Enabled:     true,
		NextRunAt:   now.Add(time.Hour),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// RunScheduleContract exercises schedule repository semantics.
func RunScheduleContract(t *testing.T, newRepo func(t *testing.T) ScheduleAPI) {
	t.Helper()

	t.Run("schedule_create_get_round_trip", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		s := sampleSchedule(uuid.Must(uuid.NewV7()), "sched-a", now)
		stored, err := repo.CreateSchedule(ctx, s)
		require.NoError(t, err)
		got, err := repo.GetSchedule(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, stored.Name, got.Name)
		byName, err := repo.GetScheduleByName(ctx, "sched-a")
		require.NoError(t, err)
		assert.Equal(t, stored.ID, byName.ID)
	})

	t.Run("schedule_name_conflict", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		_, err := repo.CreateSchedule(ctx, sampleSchedule(uuid.Must(uuid.NewV7()), "dup", now))
		require.NoError(t, err)
		_, err = repo.CreateSchedule(ctx, sampleSchedule(uuid.Must(uuid.NewV7()), "dup", now))
		require.ErrorIs(t, err, domain.ErrConflict)
	})

	t.Run("schedule_optimistic_concurrency", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		s, err := repo.CreateSchedule(ctx, sampleSchedule(uuid.Must(uuid.NewV7()), "occ", now))
		require.NoError(t, err)
		s.Cron = "@daily"
		s.UpdatedAt = now.Add(time.Second)
		_, err = repo.UpdateSchedule(ctx, s, now)
		require.NoError(t, err)
		s.Cron = "@hourly"
		_, err = repo.UpdateSchedule(ctx, s, now) // stale expected
		require.ErrorIs(t, err, domain.ErrConflict)
	})

	t.Run("schedule_due_ordering_limit_enabled", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		a := sampleSchedule(uuid.Must(uuid.NewV7()), "due-a", now)
		a.NextRunAt = now.Add(-2 * time.Hour)
		b := sampleSchedule(uuid.Must(uuid.NewV7()), "due-b", now)
		b.NextRunAt = now.Add(-time.Hour)
		c := sampleSchedule(uuid.Must(uuid.NewV7()), "due-c", now)
		c.NextRunAt = now.Add(-30 * time.Minute)
		c.Enabled = false
		d := sampleSchedule(uuid.Must(uuid.NewV7()), "due-d", now)
		d.NextRunAt = now.Add(time.Hour)
		for _, s := range []domain.Schedule{a, b, c, d} {
			_, err := repo.CreateSchedule(ctx, s)
			require.NoError(t, err)
		}
		due, err := repo.DueSchedules(ctx, now, 1)
		require.NoError(t, err)
		require.Len(t, due, 1)
		assert.Equal(t, "due-a", due[0].Name)
		due, err = repo.DueSchedules(ctx, now, 10)
		require.NoError(t, err)
		require.Len(t, due, 2)
		assert.Equal(t, "due-a", due[0].Name)
		assert.Equal(t, "due-b", due[1].Name)
	})

	t.Run("schedule_mark_fired_conflict", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		s, err := repo.CreateSchedule(ctx, sampleSchedule(uuid.Must(uuid.NewV7()), "fire", now))
		require.NoError(t, err)
		jobID := uuid.Must(uuid.NewV7())
		expected := s.NextRunAt
		require.NoError(t, repo.MarkFired(ctx, s.ID, expected, now, now.Add(2*time.Hour), jobID))
		err = repo.MarkFired(ctx, s.ID, expected, now, now.Add(3*time.Hour), jobID)
		require.ErrorIs(t, err, domain.ErrConflict)
	})

	t.Run("schedule_list_by_name", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		for _, name := range []string{"z", "a", "m"} {
			_, err := repo.CreateSchedule(ctx, sampleSchedule(uuid.Must(uuid.NewV7()), name, now))
			require.NoError(t, err)
		}
		page, err := repo.ListSchedules(ctx, domain.PageRequest{Limit: 2})
		require.NoError(t, err)
		require.Len(t, page.Items, 2)
		assert.Equal(t, "a", page.Items[0].Name)
		assert.Equal(t, "m", page.Items[1].Name)
		require.NotEmpty(t, page.NextCursor)
		page2, err := repo.ListSchedules(ctx, domain.PageRequest{Limit: 2, Cursor: page.NextCursor})
		require.NoError(t, err)
		require.Len(t, page2.Items, 1)
		assert.Equal(t, "z", page2.Items[0].Name)
	})
}

// RunAPIKeyContract exercises API key repository semantics.
func RunAPIKeyContract(t *testing.T, newRepo func(t *testing.T) ScheduleAPI) {
	t.Helper()

	t.Run("apikey_create_list_lookup_revoke", func(t *testing.T) {
		repo := newRepo(t)
		ctx := context.Background()
		now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		key := domain.APIKey{
			ID:        uuid.NewString(),
			Name:      "k1",
			Prefix:    "abcd1234",
			Hash:      []byte("hash"),
			Scopes:    []domain.Scope{domain.ScopeProducer},
			CreatedAt: now,
		}
		stored, err := repo.CreateAPIKey(ctx, key)
		require.NoError(t, err)
		got, err := repo.GetAPIKeyByPrefix(ctx, "abcd1234")
		require.NoError(t, err)
		assert.Equal(t, stored.ID, got.ID)
		list, err := repo.ListAPIKeys(ctx)
		require.NoError(t, err)
		require.Len(t, list, 1)
		revoked, err := repo.RevokeAPIKey(ctx, stored.ID, now.Add(time.Minute))
		require.NoError(t, err)
		require.NotNil(t, revoked.RevokedAt)
		require.NoError(t, repo.UpdateLastUsed(ctx, stored.ID, now.Add(2*time.Minute)))
		got, err = repo.GetAPIKey(ctx, stored.ID)
		require.NoError(t, err)
		require.NotNil(t, got.LastUsedAt)
	})
}
