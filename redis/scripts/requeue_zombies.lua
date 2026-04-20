-- requeue_zombies.lua: re-enqueue tasks whose heartbeat has expired.
--
-- KEYS[1] = lq:{type}:claimed          (sorted set, score = claim timestamp)
-- KEYS[2] = lq:{type}:queue            (sorted set, score = Peff)
-- KEYS[3] = lq:{type}:hb:              (heartbeat key prefix)
-- KEYS[4] = lq:{type}:meta:            (metadata key prefix)
--
-- ARGV[1] = current unix timestamp
-- ARGV[2] = max batch size
--
-- Returns: number of tasks re-queued.

local now      = tonumber(ARGV[1])
local batch    = tonumber(ARGV[2]) or 50
local requeued = 0

-- All claimed tasks.
local claimed = redis.call("ZRANGE", KEYS[1], 0, batch - 1, "WITHSCORES")

for i = 1, #claimed, 2 do
    local id     = claimed[i]
    local hb_key = KEYS[3] .. id

    -- If heartbeat key still exists, the worker is alive.
    if redis.call("EXISTS", hb_key) == 0 then
        -- Heartbeat expired: zombie. Move back to queue.
        local meta_key  = KEYS[4] .. id
        local base_prio = tonumber(redis.call("HGET", meta_key, "priority") or "0")
        local created   = tonumber(redis.call("HGET", meta_key, "created_at") or tostring(now))
        local age       = now - created
        local alpha     = tonumber(redis.call("HGET", meta_key, "alpha") or "0")
        local peff      = base_prio + age * alpha

        redis.call("ZREM", KEYS[1], id)
        redis.call("ZADD", KEYS[2], peff, id)
        requeued = requeued + 1
    end
end

return requeued
