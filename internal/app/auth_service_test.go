package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestAuthService_BootstrapAndStoredKey(t *testing.T) {
	t.Parallel()
	repo := memory.New()
	clock := &fixedClock{now: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	aks := app.NewAPIKeyService(repo, clock)
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: "bootstrap-secret"}, log)

	_, err := auth.Authenticate(context.Background(), "wrong")
	require.ErrorIs(t, err, domain.ErrUnauthenticated)

	p, err := auth.Authenticate(context.Background(), "bootstrap-secret")
	require.NoError(t, err)
	assert.Equal(t, "bootstrap", p.KeyID)
	assert.True(t, p.Can(domain.ScopeAdmin))

	created, err := aks.Create(adminCtx(), app.CreateAPIKeyCommand{
		Name: "worker", Scopes: []domain.Scope{domain.ScopeWorker},
	})
	require.NoError(t, err)
	require.True(t, len(created.Secret) > 10)
	prefix, ok := app.ParseAPIKeySecret(created.Secret)
	require.True(t, ok)
	assert.Equal(t, created.Key.Prefix, prefix)

	p, err = auth.Authenticate(context.Background(), created.Secret)
	require.NoError(t, err)
	assert.Equal(t, created.Key.ID, p.KeyID)
	assert.True(t, p.Can(domain.ScopeWorker))
	assert.False(t, p.Can(domain.ScopeProducer))

	_, err = aks.Revoke(adminCtx(), created.Key.ID)
	require.NoError(t, err)
	// Cache may still allow until TTL — force new auth service without cache pollution:
	auth2 := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: ""}, log)
	_, err = auth2.Authenticate(context.Background(), created.Secret)
	require.ErrorIs(t, err, domain.ErrUnauthenticated)
}

func TestGenerateAPIKeySecret_Format(t *testing.T) {
	t.Parallel()
	secret, prefix, err := app.GenerateAPIKeySecret()
	require.NoError(t, err)
	assert.Len(t, prefix, 8)
	got, ok := app.ParseAPIKeySecret(secret)
	require.True(t, ok)
	assert.Equal(t, prefix, got)
	sum := app.HashAPIKeySecret(secret)
	assert.Len(t, sum, 32)
}

func TestRequireScope(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, app.RequireScope(context.Background(), domain.ScopeAdmin), domain.ErrUnauthenticated)
	require.ErrorIs(t, app.RequireScope(principalCtx(domain.ScopeProducer), domain.ScopeAdmin), domain.ErrForbidden)
	require.NoError(t, app.RequireScope(principalCtx(domain.ScopeAdmin), domain.ScopeProducer))
}
