-- release.lua: return a task to the queue without side effects on retry count.
--
-- Used when a Pop claimed a task that the worker cannot actually service
-- (capability mismatch). The task must return to the queue atomically.
--
-- KEYS[1] = lq:{type}:claimed           (claimed sorted set)
-- KEYS[2] = lq:{type}:queue             (priority sorted set)
-- KEYS[3] = lq:{type}:hb:{id}           (heartbeat key)
-- KEYS[4] = lq:{type}:meta:{id}         (meta hash)
--
-- ARGV[1] = task id
--
-- Returns: 1 if task was released, 0 if it was already gone from claimed set.

local id        = ARGV[1]
local removed   = redis.call("ZREM", KEYS[1], id)
if removed == 0 then
    return 0
end

-- Preferred score: the original Peff recorded in the meta hash.
-- Fall back to 0 if metadata is missing.
local priority  = tonumber(redis.call("HGET", KEYS[4], "priority") or "0")
redis.call("ZADD", KEYS[2], priority, id)
redis.call("DEL", KEYS[3])
return 1
