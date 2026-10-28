-- claim.lua
-- KEYS[1] = scheduled zset
-- KEYS[2] = ready zset
-- KEYS[3] = leased zset
-- ARGV[1] = now_ms
-- ARGV[2] = owner
-- ARGV[3] = limit
-- ARGV[4] = lease_ms
-- ARGV[5] = key prefix
-- ARGV[6] = queue name
-- Returns: array of HGETALL flat tables (one per claimed job)

local scheduled = KEYS[1]
local ready = KEYS[2]
local leased = KEYS[3]
local now = tonumber(ARGV[1])
local owner = ARGV[2]
local limit = tonumber(ARGV[3])
local lease_ms = tonumber(ARGV[4])
local prefix = ARGV[5]
local queue = ARGV[6]

-- (a) move due members from scheduled to ready
local due = redis.call('ZRANGEBYSCORE', scheduled, '-inf', now)
for _, id in ipairs(due) do
  local jk = job_key(prefix, id)
  local priority = tonumber(redis.call('HGET', jk, 'priority') or '0')
  local run_at = tonumber(redis.call('HGET', jk, 'run_at') or '0')
  redis.call('ZREM', scheduled, id)
  redis.call('ZADD', ready, ready_score(priority, run_at), id)
end

-- (b) reclaim or dead-mark expired leases
local expired = redis.call('ZRANGEBYSCORE', leased, '-inf', now)
for _, id in ipairs(expired) do
  local jk = job_key(prefix, id)
  local attempts = tonumber(redis.call('HGET', jk, 'attempts') or '0')
  local max_attempts = tonumber(redis.call('HGET', jk, 'max_attempts') or '0')
  if attempts >= max_attempts then
    close_running_attempt(prefix, id, 'expired', 'lease expired', now)
    redis.call('HSET', jk,
      'status', 'dead',
      'last_error', 'lease expired',
      'finished_at', tostring(now),
      'lease_owner', '',
      'lease_expires_at', '',
      'updated_at', tostring(now),
      'cancel_requested', '0'
    )
    clear_lease_indexes(prefix, queue, id)
    add_terminal(prefix, queue, id, 'dead', now, now)
    incr_throughput(prefix, queue, now, 'dead')
  else
    -- leave on ready for reclaim in step (c); status stays running until claimed
    redis.call('ZREM', leased, id)
    redis.call('ZREM', leased_all(prefix), leased_all_member(queue, id))
    local priority = tonumber(redis.call('HGET', jk, 'priority') or '0')
    local run_at = tonumber(redis.call('HGET', jk, 'run_at') or '0')
    redis.call('ZADD', ready, ready_score(priority, run_at), id)
  end
end

if queue_paused(prefix, queue) then
  return {}
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
  local jk = job_key(prefix, id)
  local prev_status = redis.call('HGET', jk, 'status')
  if prev_status == 'running' then
    close_running_attempt(prefix, id, 'expired', '', now)
  end
  redis.call('HINCRBY', jk, 'attempts', 1)
  redis.call('HSET', jk,
    'status', 'running',
    'lease_owner', owner,
    'lease_expires_at', tostring(lease_exp),
    'updated_at', tostring(now)
  )
  set_lease_indexes(prefix, queue, id, lease_exp)
  local attempts = tonumber(redis.call('HGET', jk, 'attempts') or '0')
  append_running_attempt(prefix, id, attempts, owner, now)
  table.insert(results, redis.call('HGETALL', jk))
end

return results
