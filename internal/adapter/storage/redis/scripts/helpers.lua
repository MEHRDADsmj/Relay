-- Shared helpers prepended to every Relay Lua script.
-- Expects no KEYS/ARGV of its own; callers pass prefix and queue/id.

local function ready_score(priority, run_at)
  return (100 - tonumber(priority)) * 10000000000000 + tonumber(run_at)
end

local function job_key(prefix, id)
  return prefix .. ':job:' .. id
end

local function attempts_key(prefix, id)
  return prefix .. ':job:' .. id .. ':attempts'
end

local function counts_key(prefix, queue)
  return prefix .. ':q:' .. queue .. ':counts'
end

local function status_zset(prefix, queue, status)
  return prefix .. ':q:' .. queue .. ':status:' .. status
end

local function scheduled_key(prefix, queue)
  return prefix .. ':q:' .. queue .. ':scheduled'
end

local function ready_key(prefix, queue)
  return prefix .. ':q:' .. queue .. ':ready'
end

local function leased_key(prefix, queue)
  return prefix .. ':q:' .. queue .. ':leased'
end

local function queue_hash(prefix, queue)
  return prefix .. ':queue:' .. queue
end

local function queues_set(prefix)
  return prefix .. ':queues'
end

local function leased_all(prefix)
  return prefix .. ':leased:all'
end

local function finished_zset(prefix)
  return prefix .. ':finished'
end

local function notify_channel(prefix, queue)
  return prefix .. ':notify:' .. queue
end

local function leased_all_member(queue, id)
  return queue .. '|' .. id
end

local function counts_incr(prefix, queue, status, delta)
  if status ~= 'succeeded' and status ~= 'dead' and status ~= 'cancelled' then
    return
  end
  local key = counts_key(prefix, queue)
  local n = tonumber(redis.call('HINCRBY', key, status, delta))
  if n <= 0 then
    redis.call('HDEL', key, status)
  end
end

local function add_terminal(prefix, queue, id, status, updated_ms, finished_ms)
  redis.call('ZADD', status_zset(prefix, queue, status), updated_ms, id)
  redis.call('ZADD', finished_zset(prefix), finished_ms, id)
  counts_incr(prefix, queue, status, 1)
end

local function remove_terminal(prefix, queue, id, status)
  redis.call('ZREM', status_zset(prefix, queue, status), id)
  redis.call('ZREM', finished_zset(prefix), id)
  counts_incr(prefix, queue, status, -1)
end

local function clear_lease_indexes(prefix, queue, id)
  redis.call('ZREM', leased_key(prefix, queue), id)
  redis.call('ZREM', leased_all(prefix), leased_all_member(queue, id))
end

local function set_lease_indexes(prefix, queue, id, lease_exp)
  redis.call('ZADD', leased_key(prefix, queue), lease_exp, id)
  redis.call('ZADD', leased_all(prefix), lease_exp, leased_all_member(queue, id))
end

local function ensure_queue(prefix, queue, now_ms)
  redis.call('SADD', queues_set(prefix), queue)
  local qk = queue_hash(prefix, queue)
  if redis.call('EXISTS', qk) == 0 then
    redis.call('HSET', qk,
      'paused', '0',
      'created_at', tostring(now_ms),
      'updated_at', tostring(now_ms)
    )
  else
    redis.call('HSET', qk, 'updated_at', tostring(now_ms))
  end
end

local function queue_paused(prefix, queue)
  local p = redis.call('HGET', queue_hash(prefix, queue), 'paused')
  return p == '1'
end

local function publish_due(prefix, queue)
  redis.call('PUBLISH', notify_channel(prefix, queue), queue)
end

local function close_running_attempt(prefix, id, outcome, err_msg, now_ms)
  local key = attempts_key(prefix, id)
  local n = redis.call('LLEN', key)
  for i = n - 1, 0, -1 do
    local raw = redis.call('LINDEX', key, i)
    if raw then
      local att = cjson.decode(raw)
      if att.outcome == 'running' then
        att.outcome = outcome
        att.error = err_msg or ''
        att.finished_at = now_ms
        redis.call('LSET', key, i, cjson.encode(att))
        return
      end
    end
  end
end

local function append_running_attempt(prefix, id, number, worker_id, now_ms)
  local att = {
    job_id = id,
    number = tonumber(number),
    worker_id = worker_id,
    started_at = tonumber(now_ms),
    finished_at = cjson.null,
    outcome = 'running',
    error = ''
  }
  redis.call('RPUSH', attempts_key(prefix, id), cjson.encode(att))
end

local function store_idem_redis_key(job_hash_key, idem_redis_key)
  if idem_redis_key and idem_redis_key ~= '' then
    redis.call('HSET', job_hash_key, 'idem_redis_key', idem_redis_key)
  end
end
