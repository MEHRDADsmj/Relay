// Package redis implements storage ports against a single Redis 7+ node.
//
// All state mutations run inside embedded Lua scripts for atomicity. Scripts may
// touch keys not listed in KEYS[] (acceptable on one node; not Cluster-safe).
//
// Counter strategy (Phase 2):
//   - pending / scheduled: derived from job hashes on ready+scheduled zsets (run_at vs now)
//   - running: ZCARD of the per-queue leased zset
//   - succeeded / dead / cancelled: relay:q:{queue}:counts hash, maintained in every
//     script that transitions into or out of a terminal status
//
// Notifier: one PSUBSCRIBE on relay:notify:* fans out by queue; reconnect broadcasts
// a wakeup to all subscribers.
package redis
