-- retry.lua
-- KEYS[1] = job hash key
-- ARGV[1] = now_ms
-- ARGV[2] = job id
-- ARGV[3] = key prefix
-- Returns: {"NOT_FOUND"} | {"INVALID"} | {"OK", hgetall...}

local job_hash = KEYS[1]
local now = tonumber(ARGV[1])
local id = ARGV[2]
local prefix = ARGV[3]

if redis.call('EXISTS', job_hash) == 0 then
  return {'NOT_FOUND'}
end

local status = redis.call('HGET', job_hash, 'status')
local queue = redis.call('HGET', job_hash, 'queue')
local priority = tonumber(redis.call('HGET', job_hash, 'priority') or '0')

if status ~= 'dead' and status ~= 'cancelled' then
  return {'INVALID'}
end

remove_terminal(prefix, queue, id, status)

redis.call('HSET', job_hash,
  'status', 'pending',
  'attempts', '0',
  'run_at', tostring(now),
  'finished_at', '',
  'cancelled_at', '',
  'cancel_requested', '0',
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
