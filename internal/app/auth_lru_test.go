package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

type lruTestClock struct{ now time.Time }

func (c *lruTestClock) Now() time.Time { return c.now }

type noopAPIKeyRepo struct{}

func (noopAPIKeyRepo) CreateAPIKey(context.Context, domain.APIKey) (domain.APIKey, error) {
	return domain.APIKey{}, nil
}

func (noopAPIKeyRepo) GetAPIKey(context.Context, string) (domain.APIKey, error) {
	return domain.APIKey{}, domain.ErrNotFound
}

func (noopAPIKeyRepo) GetAPIKeyByPrefix(context.Context, string) (domain.APIKey, error) {
	return domain.APIKey{}, domain.ErrNotFound
}

func (noopAPIKeyRepo) ListAPIKeys(context.Context) ([]domain.APIKey, error) { return nil, nil }

func (noopAPIKeyRepo) RevokeAPIKey(context.Context, string, time.Time) (domain.APIKey, error) {
	return domain.APIKey{}, domain.ErrNotFound
}

func (noopAPIKeyRepo) UpdateLastUsed(context.Context, string, time.Time) error { return nil }

func TestAuthService_LRUEvictionOrder(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clock := &lruTestClock{now: now}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewAuthService(noopAPIKeyRepo{}, clock, AuthServiceConfig{
		CacheTTL:   time.Minute,
		CacheSlots: 2,
	}, log)

	p := domain.Principal{KeyID: "k", Name: "n", Scopes: []domain.Scope{domain.ScopeWorker}}
	s.putCache("a", p, false, now)
	s.putCache("b", p, false, now)

	// Touch "a" so it becomes MRU; "b" remains LRU.
	s.mu.Lock()
	require.NotNil(t, s.cacheGetLocked("a", now))
	s.mu.Unlock()

	s.putCache("c", p, false, now)

	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotNil(t, s.cache["a"], "MRU entry a must survive")
	require.Nil(t, s.cache["b"], "LRU entry b must be evicted")
	require.NotNil(t, s.cache["c"], "new entry c must be present")
	require.Equal(t, 2, s.lru.Len())
}

func TestAuthService_LRURefreshOnPut(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clock := &lruTestClock{now: now}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewAuthService(noopAPIKeyRepo{}, clock, AuthServiceConfig{
		CacheTTL:   time.Minute,
		CacheSlots: 2,
	}, log)

	p := domain.Principal{KeyID: "k", Name: "n", Scopes: []domain.Scope{domain.ScopeWorker}}
	s.putCache("a", p, false, now)
	s.putCache("b", p, false, now)
	// Re-put "a" — should move to front without growing.
	s.putCache("a", p, false, now)
	s.putCache("c", p, false, now)

	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotNil(t, s.cache["a"])
	require.Nil(t, s.cache["b"])
	require.NotNil(t, s.cache["c"])
}
