-- schedule_mark_fired.lua
-- KEYS[1] = schedule hash
-- KEYS[2] = schedules:due
-- ARGV: id, expected_next_ms, fired_at_ms, next_run_at_ms, job_id
-- Returns: { OK | NOT_FOUND | CONFLICT }

local id = ARGV[1]
local expected = ARGV[2]

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end

local cur_next = redis.call('HGET', KEYS[1], 'next_run_at')
if cur_next ~= expected then
  return {'CONFLICT'}
end

redis.call('HSET', KEYS[1],
  'last_run_at', ARGV[3],
  'last_job_id', ARGV[5],
  'next_run_at', ARGV[4],
  'updated_at', ARGV[3]
)

local enabled = redis.call('HGET', KEYS[1], 'enabled')
if enabled == '1' then
  redis.call('ZADD', KEYS[2], tonumber(ARGV[4]), id)
else
  redis.call('ZREM', KEYS[2], id)
end

return {'OK'}
