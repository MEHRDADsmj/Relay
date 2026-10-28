package sdk

import "time"

// Hooks receives worker lifecycle events for metrics/observability.
// Implementations must be safe for concurrent use.
type Hooks interface {
	OnLease(queue string, n int, d time.Duration, err error)
	OnJobStart(queue, jobType string)
	OnJobEnd(queue, jobType string, d time.Duration, outcome string) // succeeded|failed|permanent|panic|cancelled|lease_lost
	OnHeartbeat(queue string, err error)
}

// NopHooks implements Hooks with no-ops.
type NopHooks struct{}

func (NopHooks) OnLease(string, int, time.Duration, error)      {}
func (NopHooks) OnJobStart(string, string)                      {}
func (NopHooks) OnJobEnd(string, string, time.Duration, string) {}
func (NopHooks) OnHeartbeat(string, error)                      {}
