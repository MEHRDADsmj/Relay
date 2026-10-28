-- fail.lua
-- KEYS[1] = job hash key
-- ARGV[1] = owner
-- ARGV[2] = now_ms
-- ARGV[3] = job id
-- ARGV[4] = err_msg
-- ARGV[5] = next_status ("pending" | "dead")
-- ARGV[6] = next_run_at_ms
-- ARGV[7] = ready_score
-- ARGV[8] = key prefix
-- Returns: {"NOT_FOUND"} | {"LEASE_LOST"} | {"OK", hgetall...}

local job_key = KEYS[1]
local owner = ARGV[1]
local now = tonumber(ARGV[2])
local id = ARGV[3]
local err_msg = ARGV[4]
local next_status = ARGV[5]
local next_run_at = tonumber(ARGV[6])
local score = tonumber(ARGV[7])
local prefix = ARGV[8]

if redis.call('EXISTS', job_key) == 0 then
  return {'NOT_FOUND'}
end

local status = redis.call('HGET', job_key, 'status')
local lease_owner = redis.call('HGET', job_key, 'lease_owner') or ''
local lease_exp = tonumber(redis.call('HGET', job_key, 'lease_expires_at') or '')
local queue = redis.call('HGET', job_key, 'queue')
local priority = tonumber(redis.call('HGET', job_key, 'priority') or '0')

if status ~= 'running' or lease_owner ~= owner or lease_exp == nil or lease_exp <= now then
  return {'LEASE_LOST'}
end

local leased = prefix .. ':q:' .. queue .. ':leased'
local scheduled = prefix .. ':q:' .. queue .. ':scheduled'
local ready = prefix .. ':q:' .. queue .. ':ready'

redis.call('ZREM', leased, id)

if next_status == 'pending' then
  if score == nil then
    score = (100 - priority) * 10000000000000 + next_run_at
  end
  redis.call('HSET', job_key,
    'status', 'pending',
    'run_at', tostring(next_run_at),
    'last_error', err_msg,
    'lease_owner', '',
    'lease_expires_at', '',
    'updated_at', tostring(now)
  )
  if next_run_at <= now then
    redis.call('ZADD', ready, score, id)
  else
    redis.call('ZADD', scheduled, next_run_at, id)
  end
else
  redis.call('HSET', job_key,
    'status', 'dead',
    'last_error', err_msg,
    'finished_at', tostring(now),
    'lease_owner', '',
    'lease_expires_at', '',
    'updated_at', tostring(now)
  )
end

local flat = redis.call('HGETALL', job_key)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
