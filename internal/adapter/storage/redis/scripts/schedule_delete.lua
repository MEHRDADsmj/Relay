-- schedule_delete.lua
-- KEYS[1] = schedule hash
-- KEYS[2] = schedules:by-name
-- KEYS[3] = schedules:due
-- ARGV[1] = id
-- Returns: { OK | NOT_FOUND }

local id = ARGV[1]

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end

local name = redis.call('HGET', KEYS[1], 'name')
if name then
  redis.call('HDEL', KEYS[2], name)
end
redis.call('ZREM', KEYS[3], id)
redis.call('DEL', KEYS[1])

return {'OK'}
