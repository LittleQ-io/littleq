-- complete.lua: atomically finalize a task (complete or fail).
--
-- KEYS[1] = lq:{type}:hb:{id}          (heartbeat key)
-- KEYS[2] = lq:{type}:claimed           (claimed sorted set)
-- KEYS[3] = lq:{type}:task:{id}         (task hash)
-- KEYS[4] = lq:{type}:metrics:done      (sorted set for completion metrics)
--
-- ARGV[1] = worker_id (must match heartbeat owner)
-- ARGV[2] = new status string ("done" or "failed")
-- ARGV[3] = unix timestamp
--
-- Returns: 1 on success, -1 if wrong owner, 0 if already gone.

local id = string.match(KEYS[1], ":hb:(.+)$")

local owner = redis.call("GET", KEYS[1])
if not owner then
    return 0
end
if owner ~= ARGV[1] then
    return -1
end

-- Remove heartbeat and claimed membership.
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], id)

-- Record completion for metrics.
redis.call("ZADD", KEYS[4], tonumber(ARGV[3]), id)

return 1
