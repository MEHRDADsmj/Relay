-- leader_release.lua
-- KEYS[1] = leader key
-- ARGV[1] = instance id
-- Returns: 1 if deleted, 0 otherwise

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
