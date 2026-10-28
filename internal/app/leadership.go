package app

import "sync"

// Leadership tracks whether this process currently holds scheduler leadership.
type Leadership struct {
	mu       sync.RWMutex
	instance string
	holding  bool
}

// NewLeadership returns a tracker for instanceID.
func NewLeadership(instanceID string) *Leadership {
	return &Leadership{instance: instanceID}
}

// SetHolding records whether this instance is the scheduler leader.
func (l *Leadership) SetHolding(holding bool) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holding = holding
}

// Snapshot returns the instance id and whether it currently holds leadership.
func (l *Leadership) Snapshot() (instanceID string, holding bool) {
	if l == nil {
		return "", false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.instance, l.holding
}
