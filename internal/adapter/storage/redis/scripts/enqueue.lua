-- enqueue.lua
-- KEYS[1] = job hash key
-- KEYS[2] = scheduled zset
-- KEYS[3] = ready zset
-- KEYS[4] = idempotency key (may be empty string → skip)
-- ARGV: id, queue, type, payload, status, priority, run_at_ms, attempts,
--       max_attempts, lease_owner, lease_expires_at, idempotency_key,
--       last_error, created_at_ms, updated_at_ms, finished_at, now_ms, ready_score,
--       prefix, cancel_requested, cancelled_at, trace_context_json
-- Returns: { created (0|1), job_id }

local idem_key = KEYS[4]
local id = ARGV[1]
local queue = ARGV[2]
local now_ms = tonumber(ARGV[17])
local run_at_ms = tonumber(ARGV[7])
local score = tonumber(ARGV[18])
local prefix = ARGV[19]

if idem_key ~= '' then
  local ok = redis.call('SET', idem_key, id, 'NX')
  if not ok then
    local existing = redis.call('GET', idem_key)
    return {0, existing}
  end
end

ensure_queue(prefix, queue, now_ms)

redis.call('HSET', KEYS[1],
  'id', ARGV[1],
  'queue', ARGV[2],
  'type', ARGV[3],
  'payload', ARGV[4],
  'status', ARGV[5],
  'priority', ARGV[6],
  'run_at', ARGV[7],
  'attempts', ARGV[8],
  'max_attempts', ARGV[9],
  'lease_owner', ARGV[10],
  'lease_expires_at', ARGV[11],
  'idempotency_key', ARGV[12],
  'last_error', ARGV[13],
  'created_at', ARGV[14],
  'updated_at', ARGV[15],
  'finished_at', ARGV[16],
  'cancel_requested', ARGV[20],
  'cancelled_at', ARGV[21],
  'trace_context', ARGV[22]
)
store_idem_redis_key(KEYS[1], idem_key)
incr_throughput(prefix, queue, now_ms, 'enqueued')

if run_at_ms <= now_ms then
  redis.call('ZADD', KEYS[3], score, id)
  publish_due(prefix, queue)
else
  redis.call('ZADD', KEYS[2], run_at_ms, id)
end

return {1, id}
