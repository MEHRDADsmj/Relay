-- complete.lua
-- KEYS[1] = job hash key
-- ARGV[1] = owner
-- ARGV[2] = now_ms
-- ARGV[3] = job id
-- ARGV[4] = key prefix
-- Returns: {"NOT_FOUND"} | {"LEASE_LOST"} | {"OK", hgetall...}

local job_key = KEYS[1]
local owner = ARGV[1]
local now = tonumber(ARGV[2])
local id = ARGV[3]
local prefix = ARGV[4]

if redis.call('EXISTS', job_key) == 0 then
  return {'NOT_FOUND'}
end

local status = redis.call('HGET', job_key, 'status')
local lease_owner = redis.call('HGET', job_key, 'lease_owner') or ''
local lease_exp = tonumber(redis.call('HGET', job_key, 'lease_expires_at') or '')
local queue = redis.call('HGET', job_key, 'queue')

if status ~= 'running' or lease_owner ~= owner or lease_exp == nil or lease_exp <= now then
  return {'LEASE_LOST'}
end

redis.call('HSET', job_key,
  'status', 'succeeded',
  'finished_at', tostring(now),
  'lease_owner', '',
  'lease_expires_at', '',
  'updated_at', tostring(now)
)
redis.call('ZREM', prefix .. ':q:' .. queue .. ':leased', id)

local flat = redis.call('HGETALL', job_key)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
