-- extend.lua
-- KEYS[1] = job hash key
-- ARGV[1] = owner
-- ARGV[2] = now_ms
-- ARGV[3] = lease_ms
-- ARGV[4] = job id
-- ARGV[5] = key prefix
-- Returns: {"NOT_FOUND"} | {"LEASE_LOST"} | {"OK", hgetall...}

local job_hash = KEYS[1]
local owner = ARGV[1]
local now = tonumber(ARGV[2])
local lease_ms = tonumber(ARGV[3])
local id = ARGV[4]
local prefix = ARGV[5]

if redis.call('EXISTS', job_hash) == 0 then
  return {'NOT_FOUND'}
end

local status = redis.call('HGET', job_hash, 'status')
local lease_owner = redis.call('HGET', job_hash, 'lease_owner') or ''
local lease_exp = tonumber(redis.call('HGET', job_hash, 'lease_expires_at') or '')
local queue = redis.call('HGET', job_hash, 'queue')

if status ~= 'running' or lease_owner ~= owner or lease_exp == nil or lease_exp <= now then
  return {'LEASE_LOST'}
end

local new_exp = now + lease_ms
redis.call('HSET', job_hash,
  'lease_expires_at', tostring(new_exp),
  'updated_at', tostring(now)
)
set_lease_indexes(prefix, queue, id, new_exp)

local flat = redis.call('HGETALL', job_hash)
local out = {'OK'}
for i = 1, #flat do
  out[i + 1] = flat[i]
end
return out
