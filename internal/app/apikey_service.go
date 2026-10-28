package app

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// CreateAPIKeyCommand is the input for APIKeyService.Create.
type CreateAPIKeyCommand struct {
	Name      string
	Scopes    []domain.Scope
	ExpiresAt *time.Time
}

// CreatedAPIKey is the persisted key plus the plaintext secret (shown once).
type CreatedAPIKey struct {
	Key    domain.APIKey
	Secret string
}

// APIKeyService manages API key lifecycle.
type APIKeyService struct {
	repo  APIKeyRepository
	clock Clock
}

// NewAPIKeyService constructs an APIKeyService.
func NewAPIKeyService(repo APIKeyRepository, clock Clock) *APIKeyService {
	return &APIKeyService{repo: repo, clock: clock}
}

// Create generates a new key secret, stores its hash, and returns the secret once.
func (s *APIKeyService) Create(ctx context.Context, cmd CreateAPIKeyCommand) (CreatedAPIKey, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return CreatedAPIKey{}, err
	}
	if cmd.Name == "" {
		return CreatedAPIKey{}, &domain.ValidationError{Field: "name", Message: "required"}
	}
	if err := validateScopes(cmd.Scopes); err != nil {
		return CreatedAPIKey{}, err
	}
	secret, prefix, err := GenerateAPIKeySecret()
	if err != nil {
		return CreatedAPIKey{}, err
	}
	now := s.clock.Now()
	key := domain.APIKey{
		ID:        uuid.NewString(),
		Name:      cmd.Name,
		Prefix:    prefix,
		Hash:      HashAPIKeySecret(secret),
		Scopes:    append([]domain.Scope(nil), cmd.Scopes...),
		CreatedAt: now,
		ExpiresAt: cmd.ExpiresAt,
	}
	stored, err := s.repo.CreateAPIKey(ctx, key)
	if err != nil {
		return CreatedAPIKey{}, fmt.Errorf("create api key: %w", err)
	}
	return CreatedAPIKey{Key: stored, Secret: secret}, nil
}

// List returns all API keys (secrets never included).
func (s *APIKeyService) List(ctx context.Context) ([]domain.APIKey, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return nil, err
	}
	return s.repo.ListAPIKeys(ctx)
}

// Revoke marks a key as revoked.
func (s *APIKeyService) Revoke(ctx context.Context, id string) (domain.APIKey, error) {
	if err := RequireScope(ctx, domain.ScopeAdmin); err != nil {
		return domain.APIKey{}, err
	}
	if id == "" {
		return domain.APIKey{}, &domain.ValidationError{Field: "id", Message: "required"}
	}
	return s.repo.RevokeAPIKey(ctx, id, s.clock.Now())
}

func validateScopes(scopes []domain.Scope) error {
	if len(scopes) == 0 {
		return &domain.ValidationError{Field: "scopes", Message: "required"}
	}
	for _, sc := range scopes {
		switch sc {
		case domain.ScopeProducer, domain.ScopeWorker, domain.ScopeAdmin:
		default:
			return &domain.ValidationError{Field: "scopes", Message: "unknown scope: " + string(sc)}
		}
	}
	return nil
}
