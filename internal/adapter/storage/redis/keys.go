package redis

import "fmt"

const defaultPrefix = "relay"

// Keys builds Redis key names for a configurable prefix (default "relay").
type Keys struct {
	prefix string
}

// NewKeys returns key helpers. Empty prefix uses "relay".
func NewKeys(prefix string) Keys {
	if prefix == "" {
		prefix = defaultPrefix
	}
	return Keys{prefix: prefix}
}

// Prefix returns the key prefix.
func (k Keys) Prefix() string { return k.prefix }

// Job returns relay:job:{id}.
func (k Keys) Job(id string) string {
	return fmt.Sprintf("%s:job:%s", k.prefix, id)
}

// Scheduled returns relay:q:{queue}:scheduled.
func (k Keys) Scheduled(queue string) string {
	return fmt.Sprintf("%s:q:%s:scheduled", k.prefix, queue)
}

// Ready returns relay:q:{queue}:ready.
func (k Keys) Ready(queue string) string {
	return fmt.Sprintf("%s:q:%s:ready", k.prefix, queue)
}

// Leased returns relay:q:{queue}:leased.
func (k Keys) Leased(queue string) string {
	return fmt.Sprintf("%s:q:%s:leased", k.prefix, queue)
}

// Idempotency returns relay:idem:{queue}:{key}.
func (k Keys) Idempotency(queue, key string) string {
	return fmt.Sprintf("%s:idem:%s:%s", k.prefix, queue, key)
}

// readyScore returns (100 - priority) * 1e13 + run_at_ms.
func readyScore(priority int, runAtMs int64) float64 {
	return float64(100-priority)*1e13 + float64(runAtMs)
}
