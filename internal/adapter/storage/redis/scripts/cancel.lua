-- cancel.lua
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

if status == 'pending' then
  redis.call('ZREM', scheduled_key(prefix, queue), id)
  redis.call('ZREM', ready_key(prefix, queue), id)
  redis.call('HSET', job_hash,
    'status', 'cancelled',
    'finished_at', tostring(now),
    'cancelled_at', tostring(now),
    'updated_at', tostring(now)
  )
  add_terminal(prefix, queue, id, 'cancelled', now, now)
elseif status == 'running' then
  redis.call('HSET', job_hash,
    'cancel_requested', '1',
    'updated_at', tostring(now)
  )
else
  return {'INVALID'}
end

local flat = redis.call('HGETALL', job_hash)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
