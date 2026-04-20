-- push.lua: atomically enqueue a task.
--
-- KEYS[1] = lq:{type}:queue             (sorted set, score = Peff)
-- KEYS[2] = lq:{type}:task:{id}         (string, serialized task JSON)
-- KEYS[3] = lq:{type}:dedup             (hash, dedup_key → id)
-- KEYS[4] = lq:{type}:metrics:arrivals  (sorted set, score = unix timestamp)
-- KEYS[5] = lq:{type}:meta:{id}         (hash: priority, created_at, alpha)
--
-- ARGV[1] = task id
-- ARGV[2] = initial Peff score (= Priority as float)
-- ARGV[3] = dedup_key (empty string = no dedup)
-- ARGV[4] = serialized task JSON
-- ARGV[5] = unix timestamp (seconds, float)
-- ARGV[6] = aging alpha (float)

local id        = ARGV[1]
local score     = tonumber(ARGV[2])
local dedup_key = ARGV[3]
local payload   = ARGV[4]
local ts        = ARGV[5]
local alpha     = ARGV[6]

-- Dedup check: if dedup_key is set and already mapped, return existing id.
if dedup_key ~= "" then
    local existing = redis.call("HGET", KEYS[3], dedup_key)
    if existing then
        return {"0", existing}  -- "0" = duplicate, existing id
    end
end

-- Enqueue into priority sorted set.
redis.call("ZADD", KEYS[1], score, id)

-- Store task payload (single-key read on Pop).
redis.call("SET", KEYS[2], payload)

-- Record dedup mapping.
if dedup_key ~= "" then
    redis.call("HSET", KEYS[3], dedup_key, id)
end

-- Record arrival for metrics (sorted set, score = timestamp, member = id).
redis.call("ZADD", KEYS[4], ts, id)

-- Per-task meta hash consumed by age.lua and requeue_zombies.lua.
redis.call("HSET", KEYS[5],
    "priority",   tostring(score),
    "created_at", ts,
    "alpha",      alpha)

return {"1", id}  -- "1" = success, new id
