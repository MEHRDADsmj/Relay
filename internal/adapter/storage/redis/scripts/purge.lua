-- purge.lua
-- ARGV[1] = before_ms (exclusive: finished_at < before)
-- ARGV[2] = limit
-- ARGV[3] = key prefix
-- Returns: purged count

local before = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local prefix = ARGV[3]

if limit < 1 then
  return 0
end

-- ZRANGEBYSCORE max is exclusive via '(' prefix in Redis
local ids = redis.call('ZRANGEBYSCORE', finished_zset(prefix), '-inf', '(' .. tostring(before), 'LIMIT', 0, limit)
local purged = 0

for _, id in ipairs(ids) do
  local jk = job_key(prefix, id)
  local flat = redis.call('HGETALL', jk)
  if #flat > 0 then
    local m = {}
    for i = 1, #flat, 2 do
      m[flat[i]] = flat[i + 1]
    end
    local status = m['status']
    local queue = m['queue']
    if status == 'succeeded' or status == 'dead' or status == 'cancelled' then
      remove_terminal(prefix, queue, id, status)
      redis.call('DEL', jk)
      redis.call('DEL', attempts_key(prefix, id))
      local idem = m['idem_redis_key']
      if idem and idem ~= '' then
        redis.call('DEL', idem)
      end
      purged = purged + 1
    else
      redis.call('ZREM', finished_zset(prefix), id)
    end
  else
    redis.call('ZREM', finished_zset(prefix), id)
  end
end

return purged
