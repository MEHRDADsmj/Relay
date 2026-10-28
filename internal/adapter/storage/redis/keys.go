package redis

import "fmt"

const defaultPrefix = "relay"

// Keys builds Redis key names for a configurable prefix (default "relay").
//
// Counter strategy (Phase 2):
//   - pending = ZCARD(scheduled) + ZCARD(ready)
//   - running = ZCARD(leased)
//   - succeeded/dead/cancelled = fields on relay:q:{queue}:counts (maintained in Lua)
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

// Attempts returns relay:job:{id}:attempts.
func (k Keys) Attempts(id string) string {
	return fmt.Sprintf("%s:job:%s:attempts", k.prefix, id)
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

// Counts returns relay:q:{queue}:counts (terminal status counters).
func (k Keys) Counts(queue string) string {
	return fmt.Sprintf("%s:q:%s:counts", k.prefix, queue)
}

// Status returns relay:q:{queue}:status:{status} (terminal status index).
func (k Keys) Status(queue, status string) string {
	return fmt.Sprintf("%s:q:%s:status:%s", k.prefix, queue, status)
}

// Idempotency returns relay:idem:{queue}:{key}.
func (k Keys) Idempotency(queue, key string) string {
	return fmt.Sprintf("%s:idem:%s:%s", k.prefix, queue, key)
}

// Queues returns relay:queues.
func (k Keys) Queues() string {
	return fmt.Sprintf("%s:queues", k.prefix)
}

// Queue returns relay:queue:{queue}.
func (k Keys) Queue(queue string) string {
	return fmt.Sprintf("%s:queue:%s", k.prefix, queue)
}

// LeasedAll returns relay:leased:all.
func (k Keys) LeasedAll() string {
	return fmt.Sprintf("%s:leased:all", k.prefix)
}

// Finished returns relay:finished.
func (k Keys) Finished() string {
	return fmt.Sprintf("%s:finished", k.prefix)
}

// NotifyChannel returns relay:notify:{queue}.
func (k Keys) NotifyChannel(queue string) string {
	return fmt.Sprintf("%s:notify:%s", k.prefix, queue)
}

// NotifyPattern returns relay:notify:* for PSUBSCRIBE.
func (k Keys) NotifyPattern() string {
	return fmt.Sprintf("%s:notify:*", k.prefix)
}

// Schedule returns relay:schedule:{id}.
func (k Keys) Schedule(id string) string {
	return fmt.Sprintf("%s:schedule:%s", k.prefix, id)
}

// SchedulesByName returns relay:schedules:by-name.
func (k Keys) SchedulesByName() string {
	return fmt.Sprintf("%s:schedules:by-name", k.prefix)
}

// SchedulesDue returns relay:schedules:due.
func (k Keys) SchedulesDue() string {
	return fmt.Sprintf("%s:schedules:due", k.prefix)
}

// APIKey returns relay:apikey:{id}.
func (k Keys) APIKey(id string) string {
	return fmt.Sprintf("%s:apikey:%s", k.prefix, id)
}

// APIKeysByPrefix returns relay:apikeys:by-prefix.
func (k Keys) APIKeysByPrefix() string {
	return fmt.Sprintf("%s:apikeys:by-prefix", k.prefix)
}

// Leader returns relay:leader:{name}.
func (k Keys) Leader(name string) string {
	return fmt.Sprintf("%s:leader:%s", k.prefix, name)
}

// Stats returns relay:stats:{queue}:{minute_unix}.
func (k Keys) Stats(queue string, minuteUnix int64) string {
	return fmt.Sprintf("%s:stats:%s:%d", k.prefix, queue, minuteUnix)
}

// StatsScanPattern returns relay:stats:* for SCAN-based purge.
func (k Keys) StatsScanPattern() string {
	return fmt.Sprintf("%s:stats:*", k.prefix)
}

// LeasedAllMember returns {queue}|{id}.
func LeasedAllMember(queue, id string) string {
	return queue + "|" + id
}

// readyScore returns (100 - priority) * 1e13 + run_at_ms.
func readyScore(priority int, runAtMs int64) float64 {
	return float64(100-priority)*1e13 + float64(runAtMs)
}
