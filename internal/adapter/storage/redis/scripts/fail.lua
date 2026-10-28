-- fail.lua
-- KEYS[1] = job hash key
-- ARGV[1] = owner
-- ARGV[2] = now_ms
-- ARGV[3] = job id
-- ARGV[4] = err_msg
-- ARGV[5] = next_status ("pending" | "dead")
-- ARGV[6] = next_run_at_ms
-- ARGV[7] = ready_score (unused; recomputed)
-- ARGV[8] = key prefix
-- Returns: {"NOT_FOUND"} | {"LEASE_LOST"} | {"OK", hgetall...}

local job_hash = KEYS[1]
local owner = ARGV[1]
local now = tonumber(ARGV[2])
local id = ARGV[3]
local err_msg = ARGV[4]
local next_status = ARGV[5]
local next_run_at = tonumber(ARGV[6])
local prefix = ARGV[8]

if redis.call('EXISTS', job_hash) == 0 then
  return {'NOT_FOUND'}
end

local status = redis.call('HGET', job_hash, 'status')
local lease_owner = redis.call('HGET', job_hash, 'lease_owner') or ''
local lease_exp = tonumber(redis.call('HGET', job_hash, 'lease_expires_at') or '')
local queue = redis.call('HGET', job_hash, 'queue')
local priority = tonumber(redis.call('HGET', job_hash, 'priority') or '0')
local cancel_req = redis.call('HGET', job_hash, 'cancel_requested')

if status ~= 'running' or lease_owner ~= owner or lease_exp == nil or lease_exp <= now then
  return {'LEASE_LOST'}
end

clear_lease_indexes(prefix, queue, id)

if cancel_req == '1' then
  close_running_attempt(prefix, id, 'cancelled', err_msg, now)
  redis.call('HSET', job_hash,
    'status', 'cancelled',
    'last_error', err_msg,
    'finished_at', tostring(now),
    'cancelled_at', tostring(now),
    'cancel_requested', '0',
    'lease_owner', '',
    'lease_expires_at', '',
    'updated_at', tostring(now)
  )
  add_terminal(prefix, queue, id, 'cancelled', now, now)
else
  close_running_attempt(prefix, id, 'failed', err_msg, now)
  if next_status == 'pending' then
    local score = ready_score(priority, next_run_at)
    redis.call('HSET', job_hash,
      'status', 'pending',
      'run_at', tostring(next_run_at),
      'last_error', err_msg,
      'lease_owner', '',
      'lease_expires_at', '',
      'updated_at', tostring(now)
    )
    if next_run_at <= now then
      redis.call('ZADD', ready_key(prefix, queue), score, id)
      publish_due(prefix, queue)
    else
      redis.call('ZADD', scheduled_key(prefix, queue), next_run_at, id)
    end
  else
    redis.call('HSET', job_hash,
      'status', 'dead',
      'last_error', err_msg,
      'finished_at', tostring(now),
      'lease_owner', '',
      'lease_expires_at', '',
      'updated_at', tostring(now)
    )
    add_terminal(prefix, queue, id, 'dead', now, now)
  end
end

local flat = redis.call('HGETALL', job_hash)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
