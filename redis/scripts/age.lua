-- age.lua: recalculate Peff for all tasks in the queue.
--
-- KEYS[1] = lq:{type}:queue   (sorted set)
--
-- ARGV[1] = alpha (aging coefficient, float string)
-- ARGV[2] = current unix timestamp (float string)
--
-- For each member we need its original priority and created_at.
-- These are stored in the task JSON. Since Lua cannot parse JSON natively,
-- we fetch them from a parallel hash set maintained by push.lua via
-- a separate metadata key per task (lq:{type}:meta:{id}).
--
-- Returns: number of tasks updated.
--
-- KEYS[2] = lq:{type}:meta:  (prefix for per-task metadata hashes)
-- ARGV[3] = batch size

local alpha    = tonumber(ARGV[1])
local now      = tonumber(ARGV[2])
local batch    = tonumber(ARGV[3]) or 100

-- Scan the queue in score order.
local members = redis.call("ZRANGE", KEYS[1], 0, batch - 1, "WITHSCORES")
local updated = 0

for i = 1, #members, 2 do
    local id         = members[i]
    local meta_key   = KEYS[2] .. id
    local base_prio  = tonumber(redis.call("HGET", meta_key, "priority") or "0")
    local created_at = tonumber(redis.call("HGET", meta_key, "created_at") or tostring(now))
    local age        = now - created_at
    local peff       = base_prio + age * alpha
    redis.call("ZADD", KEYS[1], peff, id)
    updated = updated + 1
end

return updated
