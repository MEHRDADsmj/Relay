-- apikey_create.lua
-- KEYS[1] = apikey hash
-- KEYS[2] = apikeys:by-prefix
-- ARGV: id, name, prefix, hash_hex, scopes, created_at_ms,
--       last_used_at_ms, expires_at_ms, revoked_at_ms
-- Returns: { OK | CONFLICT }

local id = ARGV[1]
local prefix = ARGV[3]

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'CONFLICT'}
end
if redis.call('HEXISTS', KEYS[2], prefix) == 1 then
  return {'CONFLICT'}
end

redis.call('HSET', KEYS[1],
  'id', id,
  'name', ARGV[2],
  'prefix', prefix,
  'hash', ARGV[4],
  'scopes', ARGV[5],
  'created_at', ARGV[6],
  'last_used_at', ARGV[7],
  'expires_at', ARGV[8],
  'revoked_at', ARGV[9]
)
redis.call('HSET', KEYS[2], prefix, id)

return {'OK'}
