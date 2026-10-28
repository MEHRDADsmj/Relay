-- schedule_create.lua
-- KEYS[1] = schedule hash
-- KEYS[2] = schedules:by-name
-- KEYS[3] = schedules:due
-- ARGV: id, name, cron, timezone, queue, job_type, payload, priority,
--       max_attempts, enabled, next_run_at_ms, last_run_at_ms, last_job_id,
--       created_at_ms, updated_at_ms
-- Returns: { OK | CONFLICT }

local id = ARGV[1]
local name = ARGV[2]

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'CONFLICT'}
end
if redis.call('HEXISTS', KEYS[2], name) == 1 then
  return {'CONFLICT'}
end

redis.call('HSET', KEYS[1],
  'id', id,
  'name', name,
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
  'created_at', ARGV[14],
  'updated_at', ARGV[15]
)
redis.call('HSET', KEYS[2], name, id)

if ARGV[10] == '1' then
  redis.call('ZADD', KEYS[3], tonumber(ARGV[11]), id)
end

return {'OK'}
