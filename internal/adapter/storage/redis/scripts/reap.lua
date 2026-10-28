-- reap.lua
-- KEYS: none required (uses prefix from ARGV)
-- ARGV[1] = now_ms
-- ARGV[2] = limit
-- ARGV[3] = key prefix
-- Returns: { requeued, dead }

local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local prefix = ARGV[3]

if limit < 1 then
  return {0, 0}
end

local members = redis.call('ZRANGEBYSCORE', leased_all(prefix), '-inf', now, 'LIMIT', 0, limit)
local requeued = 0
local dead = 0

for _, member in ipairs(members) do
  local sep = string.find(member, '|', 1, true)
  if sep then
    local queue = string.sub(member, 1, sep - 1)
    local id = string.sub(member, sep + 1)
    local jk = job_key(prefix, id)
    if redis.call('EXISTS', jk) == 1 then
      local status = redis.call('HGET', jk, 'status')
      local lease_exp = tonumber(redis.call('HGET', jk, 'lease_expires_at') or '')
      if status == 'running' and lease_exp ~= nil and lease_exp <= now then
        local attempts = tonumber(redis.call('HGET', jk, 'attempts') or '0')
        local max_attempts = tonumber(redis.call('HGET', jk, 'max_attempts') or '0')
        local priority = tonumber(redis.call('HGET', jk, 'priority') or '0')
        close_running_attempt(prefix, id, 'expired', 'lease expired', now)
        clear_lease_indexes(prefix, queue, id)
        if attempts >= max_attempts then
          redis.call('HSET', jk,
            'status', 'dead',
            'last_error', 'lease expired',
            'finished_at', tostring(now),
            'lease_owner', '',
            'lease_expires_at', '',
            'updated_at', tostring(now),
            'cancel_requested', '0'
          )
          add_terminal(prefix, queue, id, 'dead', now, now)
          incr_throughput(prefix, queue, now, 'dead')
          dead = dead + 1
        else
          redis.call('HSET', jk,
            'status', 'pending',
            'run_at', tostring(now),
            'lease_owner', '',
            'lease_expires_at', '',
            'updated_at', tostring(now)
          )
          redis.call('ZADD', ready_key(prefix, queue), ready_score(priority, now), id)
          publish_due(prefix, queue)
          requeued = requeued + 1
        end
      else
        redis.call('ZREM', leased_all(prefix), member)
      end
    else
      redis.call('ZREM', leased_all(prefix), member)
    end
  end
end

return {requeued, dead}
