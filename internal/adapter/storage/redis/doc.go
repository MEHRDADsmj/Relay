// Package redis implements JobRepository against a single Redis 7+ node.
//
// All state mutations run inside embedded Lua scripts for atomicity. Scripts may
// touch keys not listed in KEYS[] (acceptable on one node; not Cluster-safe).
package redis
