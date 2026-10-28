-- leader_renew.lua
-- KEYS[1] = leader key
-- ARGV[1] = instance id
-- ARGV[2] = ttl_ms
-- Returns: 1 if renewed, 0 if lost

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 0
