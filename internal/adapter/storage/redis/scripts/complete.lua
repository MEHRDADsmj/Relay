-- complete.lua
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
local cancel_req = redis.call('HGET', job_hash, 'cancel_requested')

if status ~= 'running' or lease_owner ~= owner or lease_exp == nil or lease_exp <= now then
  return {'LEASE_LOST'}
end

local final_status = 'succeeded'
local attempt_outcome = 'succeeded'
if cancel_req == '1' then
  final_status = 'cancelled'
  attempt_outcome = 'cancelled'
end

close_running_attempt(prefix, id, attempt_outcome, '', now)
clear_lease_indexes(prefix, queue, id)

if final_status == 'cancelled' then
  redis.call('HSET', job_hash,
    'status', 'cancelled',
    'finished_at', tostring(now),
    'cancelled_at', tostring(now),
    'cancel_requested', '0',
    'lease_owner', '',
    'lease_expires_at', '',
    'updated_at', tostring(now)
  )
else
  redis.call('HSET', job_hash,
    'status', 'succeeded',
    'finished_at', tostring(now),
    'lease_owner', '',
    'lease_expires_at', '',
    'updated_at', tostring(now)
  )
end

add_terminal(prefix, queue, id, final_status, now, now)

local flat = redis.call('HGETALL', job_hash)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
