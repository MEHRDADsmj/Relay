-- release.lua
-- KEYS[1] = job hash key
-- ARGV[1] = owner
-- ARGV[2] = now_ms
-- ARGV[3] = job id
-- ARGV[4] = key prefix
-- Returns: {"NOT_FOUND"} | {"LEASE_LOST"} | {"OK", hgetall...}

local job_hash = KEYS[1]
local owner = ARGV[1]
local now = tonumber(ARGV[2])
local id = ARGV[3]
local prefix = ARGV[4]

if redis.call('EXISTS', job_hash) == 0 then
  return {'NOT_FOUND'}
end

local status = redis.call('HGET', job_hash, 'status')
local lease_owner = redis.call('HGET', job_hash, 'lease_owner') or ''
local lease_exp = tonumber(redis.call('HGET', job_hash, 'lease_expires_at') or '')
local queue = redis.call('HGET', job_hash, 'queue')
local priority = tonumber(redis.call('HGET', job_hash, 'priority') or '0')
local attempts = tonumber(redis.call('HGET', job_hash, 'attempts') or '0')

if status ~= 'running' or lease_owner ~= owner or lease_exp == nil or lease_exp <= now then
  return {'LEASE_LOST'}
end

close_running_attempt(prefix, id, 'released', '', now)
clear_lease_indexes(prefix, queue, id)

if attempts > 0 then
  attempts = attempts - 1
end

redis.call('HSET', job_hash,
  'status', 'pending',
  'attempts', tostring(attempts),
  'run_at', tostring(now),
  'lease_owner', '',
  'lease_expires_at', '',
  'updated_at', tostring(now)
)
redis.call('ZADD', ready_key(prefix, queue), ready_score(priority, now), id)
publish_due(prefix, queue)

local flat = redis.call('HGETALL', job_hash)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
