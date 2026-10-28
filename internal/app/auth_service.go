package app

import (
	"container/list"
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
	cacheSlots   int

	mu         sync.Mutex
	cache      map[string]*list.Element // secret hash → LRU element
	lru        *list.List               // front = most recently used
	lastUsedAt map[string]time.Time     // key id → last persisted touch
}

type authCacheEntry struct {
	key       string
	principal domain.Principal
	expires   time.Time
	negative  bool
}

// AuthServiceConfig configures AuthService.
type AuthServiceConfig struct {
	BootstrapKey string
	CacheTTL     time.Duration
	// CacheSlots is the max LRU entries (default 1024). Values <= 0 use the default.
	CacheSlots int
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
	slots := cfg.CacheSlots
	if slots <= 0 {
		slots = defaultAuthCacheSlots
	}
	return &AuthService{
		repo:         repo,
		clock:        clock,
		log:          log,
		bootstrapKey: cfg.BootstrapKey,
		cacheTTL:     ttl,
		cacheSlots:   slots,
		cache:        make(map[string]*list.Element),
		lru:          list.New(),
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
	if e := s.cacheGetLocked(cacheKey, now); e != nil {
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

// PrincipalByKeyID re-validates a session by API key ID (no secret).
// Bootstrap sessions use KeyID "bootstrap" and remain valid while the bootstrap key is set.
// Results are cached by key ID for CacheTTL (revocation applies after TTL).
func (s *AuthService) PrincipalByKeyID(ctx context.Context, keyID string) (domain.Principal, error) {
	if keyID == "" {
		return domain.Principal{}, domain.ErrUnauthenticated
	}
	now := s.clock.Now()
	cacheKey := "id:" + keyID

	s.mu.Lock()
	if e := s.cacheGetLocked(cacheKey, now); e != nil {
		s.mu.Unlock()
		if e.negative {
			return domain.Principal{}, domain.ErrUnauthenticated
		}
		return e.principal, nil
	}
	s.mu.Unlock()

	if keyID == BootstrapAdmin.KeyID {
		if s.bootstrapKey == "" {
			s.putCache(cacheKey, domain.Principal{}, true, now)
			return domain.Principal{}, domain.ErrUnauthenticated
		}
		s.putCache(cacheKey, BootstrapAdmin, false, now)
		return BootstrapAdmin, nil
	}

	key, err := s.repo.GetAPIKey(ctx, keyID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			s.putCache(cacheKey, domain.Principal{}, true, now)
			return domain.Principal{}, domain.ErrUnauthenticated
		}
		return domain.Principal{}, fmt.Errorf("lookup api key by id: %w", err)
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
	return p, nil
}

// AuthDisabled reports whether neither bootstrap nor stored keys are configured.
// hasKeys should be true when at least one stored key exists.
func (s *AuthService) AuthDisabled(hasKeys bool) bool {
	return s.bootstrapKey == "" && !hasKeys
}

// IsOpen reports whether requests may proceed without credentials
// (no bootstrap key and no stored keys).
func (s *AuthService) IsOpen(ctx context.Context) (bool, error) {
	if s.bootstrapKey != "" {
		return false, nil
	}
	keys, err := s.repo.ListAPIKeys(ctx)
	if err != nil {
		return false, err
	}
	return len(keys) == 0, nil
}

// cacheGetLocked returns a live cache entry and marks it MRU. Caller must hold s.mu.
func (s *AuthService) cacheGetLocked(cacheKey string, now time.Time) *authCacheEntry {
	el, ok := s.cache[cacheKey]
	if !ok {
		return nil
	}
	e := el.Value.(*authCacheEntry)
	if !e.expires.After(now) {
		s.lru.Remove(el)
		delete(s.cache, cacheKey)
		return nil
	}
	s.lru.MoveToFront(el)
	return e
}

func (s *AuthService) putCache(cacheKey string, p domain.Principal, negative bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.cache[cacheKey]; ok {
		e := el.Value.(*authCacheEntry)
		e.principal = p
		e.expires = now.Add(s.cacheTTL)
		e.negative = negative
		s.lru.MoveToFront(el)
		return
	}
	for s.lru.Len() >= s.cacheSlots {
		back := s.lru.Back()
		if back == nil {
			break
		}
		victim := back.Value.(*authCacheEntry)
		s.lru.Remove(back)
		delete(s.cache, victim.key)
	}
	entry := &authCacheEntry{
		key:       cacheKey,
		principal: p,
		expires:   now.Add(s.cacheTTL),
		negative:  negative,
	}
	s.cache[cacheKey] = s.lru.PushFront(entry)
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
