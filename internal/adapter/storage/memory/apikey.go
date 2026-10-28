package memory

import (
	"context"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func cloneAPIKey(k domain.APIKey) domain.APIKey {
	out := k
	if k.Hash != nil {
		out.Hash = append([]byte(nil), k.Hash...)
	}
	if k.Scopes != nil {
		out.Scopes = append([]domain.Scope(nil), k.Scopes...)
	}
	if k.LastUsedAt != nil {
		t := *k.LastUsedAt
		out.LastUsedAt = &t
	}
	if k.ExpiresAt != nil {
		t := *k.ExpiresAt
		out.ExpiresAt = &t
	}
	if k.RevokedAt != nil {
		t := *k.RevokedAt
		out.RevokedAt = &t
	}
	return out
}

// CreateAPIKey implements app.APIKeyRepository.
func (r *Repository) CreateAPIKey(_ context.Context, key domain.APIKey) (domain.APIKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.apiKeys[key.ID]; ok {
		return domain.APIKey{}, domain.ErrConflict
	}
	if _, ok := r.apiPrefix[key.Prefix]; ok {
		return domain.APIKey{}, domain.ErrConflict
	}
	stored := cloneAPIKey(key)
	r.apiKeys[stored.ID] = stored
	r.apiPrefix[stored.Prefix] = stored.ID
	return cloneAPIKey(stored), nil
}

// GetAPIKey implements app.APIKeyRepository.
func (r *Repository) GetAPIKey(_ context.Context, id string) (domain.APIKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k, ok := r.apiKeys[id]
	if !ok {
		return domain.APIKey{}, domain.ErrNotFound
	}
	return cloneAPIKey(k), nil
}

// GetAPIKeyByPrefix implements app.APIKeyRepository.
func (r *Repository) GetAPIKeyByPrefix(_ context.Context, prefix string) (domain.APIKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.apiPrefix[prefix]
	if !ok {
		return domain.APIKey{}, domain.ErrNotFound
	}
	return cloneAPIKey(r.apiKeys[id]), nil
}

// ListAPIKeys implements app.APIKeyRepository.
func (r *Repository) ListAPIKeys(context.Context) ([]domain.APIKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.APIKey, 0, len(r.apiKeys))
	for _, k := range r.apiKeys {
		out = append(out, cloneAPIKey(k))
	}
	return out, nil
}

// RevokeAPIKey implements app.APIKeyRepository.
func (r *Repository) RevokeAPIKey(_ context.Context, id string, revokedAt time.Time) (domain.APIKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k, ok := r.apiKeys[id]
	if !ok {
		return domain.APIKey{}, domain.ErrNotFound
	}
	t := revokedAt
	k.RevokedAt = &t
	r.apiKeys[id] = k
	return cloneAPIKey(k), nil
}

// UpdateLastUsed implements app.APIKeyRepository.
func (r *Repository) UpdateLastUsed(_ context.Context, id string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k, ok := r.apiKeys[id]
	if !ok {
		return domain.ErrNotFound
	}
	t := at
	k.LastUsedAt = &t
	r.apiKeys[id] = k
	return nil
}
