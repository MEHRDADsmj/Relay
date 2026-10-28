package domain

import "time"

// Scope is an authorization capability granted to an API key.
type Scope string

const (
	ScopeProducer Scope = "producer" // enqueue, get job
	ScopeWorker   Scope = "worker"   // lease, heartbeat, complete, fail, release
	ScopeAdmin    Scope = "admin"    // everything incl. queues, schedules, keys
)

// APIKey is the persisted representation of a credential (secret shown once at creation).
type APIKey struct {
	ID         string
	Name       string
	Prefix     string
	Hash       []byte
	Scopes     []Scope
	CreatedAt  time.Time
	LastUsedAt *time.Time
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
}

// Principal is the authenticated identity attached to a request.
type Principal struct {
	KeyID  string
	Name   string
	Scopes []Scope
}

// Can reports whether the principal holds the required scope.
// ScopeAdmin implies every other scope.
func (p Principal) Can(s Scope) bool {
	for _, scope := range p.Scopes {
		if scope == ScopeAdmin || scope == s {
			return true
		}
	}
	return false
}
