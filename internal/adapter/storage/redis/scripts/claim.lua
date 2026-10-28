-- claim.lua
-- KEYS[1] = scheduled zset
-- KEYS[2] = ready zset
-- KEYS[3] = leased zset
-- ARGV[1] = now_ms
-- ARGV[2] = owner
-- ARGV[3] = limit
-- ARGV[4] = lease_ms
-- ARGV[5] = key prefix (e.g. "relay")
-- Returns: array of HGETALL flat tables (one per claimed job)

local scheduled = KEYS[1]
local ready = KEYS[2]
local leased = KEYS[3]
local now = tonumber(ARGV[1])
local owner = ARGV[2]
local limit = tonumber(ARGV[3])
local lease_ms = tonumber(ARGV[4])
local prefix = ARGV[5]

local function job_key(id)
  return prefix .. ':job:' .. id
end

local function ready_score(priority, run_at)
  return (100 - priority) * 10000000000000 + run_at
end

-- (a) move due members from scheduled to ready
local due = redis.call('ZRANGEBYSCORE', scheduled, '-inf', now)
for _, id in ipairs(due) do
  local jk = job_key(id)
  local priority = tonumber(redis.call('HGET', jk, 'priority') or '0')
  local run_at = tonumber(redis.call('HGET', jk, 'run_at') or '0')
  redis.call('ZREM', scheduled, id)
  redis.call('ZADD', ready, ready_score(priority, run_at), id)
end

-- (b) reclaim or dead-mark expired leases
local expired = redis.call('ZRANGEBYSCORE', leased, '-inf', now)
for _, id in ipairs(expired) do
  local jk = job_key(id)
  local attempts = tonumber(redis.call('HGET', jk, 'attempts') or '0')
  local max_attempts = tonumber(redis.call('HGET', jk, 'max_attempts') or '0')
  if attempts >= max_attempts then
    redis.call('HSET', jk,
      'status', 'dead',
      'last_error', 'lease expired',
      'finished_at', tostring(now),
      'lease_owner', '',
      'lease_expires_at', '',
      'updated_at', tostring(now)
    )
    redis.call('ZREM', leased, id)
  else
    local priority = tonumber(redis.call('HGET', jk, 'priority') or '0')
    local run_at = tonumber(redis.call('HGET', jk, 'run_at') or '0')
    redis.call('ZREM', leased, id)
    redis.call('ZADD', ready, ready_score(priority, run_at), id)
  end
end

if limit < 1 then
  return {}
end

-- (c) claim from ready
local ids = redis.call('ZRANGE', ready, 0, limit - 1)
local lease_exp = now + lease_ms
local results = {}

for _, id in ipairs(ids) do
  redis.call('ZREM', ready, id)
  redis.call('ZADD', leased, lease_exp, id)
  local jk = job_key(id)
  redis.call('HINCRBY', jk, 'attempts', 1)
  redis.call('HSET', jk,
    'status', 'running',
    'lease_owner', owner,
    'lease_expires_at', tostring(lease_exp),
    'updated_at', tostring(now)
  )
  table.insert(results, redis.call('HGETALL', jk))
end

return results
