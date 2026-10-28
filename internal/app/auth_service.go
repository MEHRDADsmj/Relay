package app

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

const (
	defaultAuthCacheTTL   = 30 * time.Second
	lastUsedThrottle      = time.Minute
	defaultAuthCacheSlots = 1024
)

// AuthService verifies API key secrets and returns principals.
type AuthService struct {
	repo         APIKeyRepository
	clock        Clock
	log          *slog.Logger
	bootstrapKey string
	cacheTTL     time.Duration

	mu         sync.Mutex
	cache      map[string]authCacheEntry // secret hash → entry
	lastUsedAt map[string]time.Time      // key id → last persisted touch
}

type authCacheEntry struct {
	principal domain.Principal
	expires   time.Time
	negative  bool
}

// AuthServiceConfig configures AuthService.
type AuthServiceConfig struct {
	BootstrapKey string
	CacheTTL     time.Duration
}

// NewAuthService constructs an AuthService. log may be nil.
func NewAuthService(repo APIKeyRepository, clock Clock, cfg AuthServiceConfig, log *slog.Logger) *AuthService {
	if log == nil {
		log = slog.Default()
	}
	ttl := cfg.CacheTTL
	if ttl <= 0 {
		ttl = defaultAuthCacheTTL
	}
	return &AuthService{
		repo:         repo,
		clock:        clock,
		log:          log,
		bootstrapKey: cfg.BootstrapKey,
		cacheTTL:     ttl,
		cache:        make(map[string]authCacheEntry),
		lastUsedAt:   make(map[string]time.Time),
	}
}

// Authenticate verifies secret and returns a Principal.
func (s *AuthService) Authenticate(ctx context.Context, secret string) (domain.Principal, error) {
	if secret == "" {
		return domain.Principal{}, domain.ErrUnauthenticated
	}
	now := s.clock.Now()
	cacheKey := string(HashAPIKeySecret(secret))

	s.mu.Lock()
	if e, ok := s.cache[cacheKey]; ok && e.expires.After(now) {
		s.mu.Unlock()
		if e.negative {
			return domain.Principal{}, domain.ErrUnauthenticated
		}
		return e.principal, nil
	}
	s.mu.Unlock()

	if s.bootstrapKey != "" && subtle.ConstantTimeCompare([]byte(secret), []byte(s.bootstrapKey)) == 1 {
		p := BootstrapAdmin
		s.putCache(cacheKey, p, false, now)
		return p, nil
	}

	prefix, ok := ParseAPIKeySecret(secret)
	if !ok {
		s.putCache(cacheKey, domain.Principal{}, true, now)
		return domain.Principal{}, domain.ErrUnauthenticated
	}
	key, err := s.repo.GetAPIKeyByPrefix(ctx, prefix)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			s.putCache(cacheKey, domain.Principal{}, true, now)
			return domain.Principal{}, domain.ErrUnauthenticated
		}
		return domain.Principal{}, fmt.Errorf("lookup api key: %w", err)
	}
	want := HashAPIKeySecret(secret)
	if subtle.ConstantTimeCompare(key.Hash, want) != 1 {
		s.putCache(cacheKey, domain.Principal{}, true, now)
		return domain.Principal{}, domain.ErrUnauthenticated
	}
	if key.RevokedAt != nil {
		s.putCache(cacheKey, domain.Principal{}, true, now)
		return domain.Principal{}, domain.ErrUnauthenticated
	}
	if key.ExpiresAt != nil && !key.ExpiresAt.After(now) {
		s.putCache(cacheKey, domain.Principal{}, true, now)
		return domain.Principal{}, domain.ErrUnauthenticated
	}

	p := domain.Principal{KeyID: key.ID, Name: key.Name, Scopes: append([]domain.Scope(nil), key.Scopes...)}
	s.putCache(cacheKey, p, false, now)
	s.touchLastUsed(ctx, key.ID, now)
	return p, nil
}

// AuthDisabled reports whether neither bootstrap nor stored keys are configured.
// hasKeys should be true when at least one stored key exists.
func (s *AuthService) AuthDisabled(hasKeys bool) bool {
	return s.bootstrapKey == "" && !hasKeys
}

func (s *AuthService) putCache(cacheKey string, p domain.Principal, negative bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) >= defaultAuthCacheSlots {
		// drop arbitrary entry (simple bound; not strict LRU)
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
	}
	s.cache[cacheKey] = authCacheEntry{principal: p, expires: now.Add(s.cacheTTL), negative: negative}
}

func (s *AuthService) touchLastUsed(ctx context.Context, id string, now time.Time) {
	s.mu.Lock()
	last, ok := s.lastUsedAt[id]
	if ok && now.Sub(last) < lastUsedThrottle {
		s.mu.Unlock()
		return
	}
	s.lastUsedAt[id] = now
	s.mu.Unlock()
	if err := s.repo.UpdateLastUsed(ctx, id, now); err != nil {
		s.log.DebugContext(ctx, "update api key last_used_at", "error", err, "key_id", id)
	}
}
