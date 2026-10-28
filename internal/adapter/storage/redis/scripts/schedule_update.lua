-- schedule_update.lua
-- KEYS[1] = schedule hash
-- KEYS[2] = schedules:by-name
-- KEYS[3] = schedules:due
-- ARGV: id, name, cron, timezone, queue, job_type, payload, priority,
--       max_attempts, enabled, next_run_at_ms, last_run_at_ms, last_job_id,
--       updated_at_ms, expected_updated_at_ms
-- Returns: { OK | NOT_FOUND | CONFLICT }

local id = ARGV[1]
local new_name = ARGV[2]
local expected = ARGV[15]

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end

local cur_updated = redis.call('HGET', KEYS[1], 'updated_at')
if cur_updated ~= expected then
  return {'CONFLICT'}
end

local old_name = redis.call('HGET', KEYS[1], 'name')
if old_name ~= new_name then
  local other = redis.call('HGET', KEYS[2], new_name)
  if other and other ~= false and other ~= id then
    return {'CONFLICT'}
  end
  redis.call('HDEL', KEYS[2], old_name)
  redis.call('HSET', KEYS[2], new_name, id)
end

redis.call('HSET', KEYS[1],
  'name', new_name,
  'cron', ARGV[3],
  'timezone', ARGV[4],
  'queue', ARGV[5],
  'job_type', ARGV[6],
  'payload', ARGV[7],
  'priority', ARGV[8],
  'max_attempts', ARGV[9],
  'enabled', ARGV[10],
  'next_run_at', ARGV[11],
  'last_run_at', ARGV[12],
  'last_job_id', ARGV[13],
  'updated_at', ARGV[14]
)

if ARGV[10] == '1' then
  redis.call('ZADD', KEYS[3], tonumber(ARGV[11]), id)
else
  redis.call('ZREM', KEYS[3], id)
end

return {'OK'}
