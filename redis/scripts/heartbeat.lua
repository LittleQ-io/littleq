-- heartbeat.lua: refresh the heartbeat TTL for a claimed task.
--
-- KEYS[1] = lq:{type}:hb:{id}   (string, value = worker_id)
--
-- ARGV[1] = expected worker_id
-- ARGV[2] = new TTL in milliseconds
--
-- Returns: 1 on success, 0 if key missing (zombie), -1 if wrong owner.

local owner = redis.call("GET", KEYS[1])
if not owner then
    return 0  -- heartbeat key expired; task already reclaimed or completed
end
if owner ~= ARGV[1] then
    return -1  -- wrong owner
end
redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
return 1
