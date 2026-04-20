-- pop.lua: atomically claim the highest-pressure task.
--
-- KEYS[1] = lq:{type}:queue          (sorted set, score = Peff, higher = more urgent)
-- KEYS[2] = lq:{type}:claimed        (sorted set, score = claim unix timestamp)
-- KEYS[3] = lq:{type}:hb:            (prefix; actual key = KEYS[3] .. id)
--
-- ARGV[1] = worker id
-- ARGV[2] = heartbeat TTL in milliseconds
-- ARGV[3] = claim unix timestamp
--
-- Returns: {id, score} on success, {} on no match.
--
-- Note: capability filtering happens in Go after Pop because Redis cannot
-- introspect task JSON without loading all fields. The Lua script claims
-- the highest-score task unconditionally; Go re-queues it if capabilities
-- don't match. This keeps the Lua script simple and the hot path fast for
-- the common case (homogeneous workers with no capability filter).

local worker_id  = ARGV[1]
local hb_ttl_ms  = tonumber(ARGV[2])
local claim_ts   = ARGV[3]

-- Pop the single highest-score candidate.
local result = redis.call("ZPOPMAX", KEYS[1], 1)
if #result == 0 then
    return {}
end

local id    = result[1]
local score = result[2]

-- Move to claimed set.
redis.call("ZADD", KEYS[2], claim_ts, id)

-- Start heartbeat TTL key (millisecond precision).
redis.call("SET", KEYS[3] .. id, worker_id, "PX", hb_ttl_ms)

return {id, score}
