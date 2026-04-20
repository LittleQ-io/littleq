-- fail.lua: atomically mark a task as failed, requeueing if retries remain.
--
-- KEYS[1] = lq:{type}:hb:{id}           (heartbeat key, value = owner worker_id)
-- KEYS[2] = lq:{type}:claimed           (claimed sorted set)
-- KEYS[3] = lq:{type}:task:{id}         (task JSON)
-- KEYS[4] = lq:{type}:queue             (priority sorted set, used if requeueing)
-- KEYS[5] = lq:{type}:metrics:done      (completion sorted set)
-- KEYS[6] = lq:{type}:meta:{id}         (per-task meta hash)
--
-- ARGV[1] = expected worker_id
-- ARGV[2] = unix timestamp (float seconds) for completion metric
-- ARGV[3] = error message
--
-- Returns: 1 on requeue, 2 on dead (terminal), 0 if task not found,
-- -1 if caller does not own the task.

local owner = redis.call("GET", KEYS[1])
if not owner then
    return 0
end
if owner ~= ARGV[1] then
    return -1
end

local raw = redis.call("GET", KEYS[3])
if not raw then
    return 0
end

local task = cjson.decode(raw)
local retry_count  = tonumber(task["retry_count"]  or 0)
local max_retries  = tonumber(task["max_retries"]  or 0)
local priority     = tonumber(task["priority"]     or 0)
local err_msg      = ARGV[3]

-- Always release the claim.
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], task["id"])

if retry_count < max_retries then
    task["retry_count"] = retry_count + 1
    task["status"]      = 0            -- StatusQueued
    -- Backoff via priority decay: decrement by attempt count, floor at 0.
    local new_prio = priority - (retry_count + 1)
    if new_prio < 0 then new_prio = 0 end
    redis.call("SET", KEYS[3], cjson.encode(task))
    redis.call("ZADD", KEYS[4], new_prio, task["id"])
    -- Refresh meta priority for aging calculations.
    redis.call("HSET", KEYS[6], "priority", tostring(new_prio))
    return 1
end

-- Terminal: mark dead.
task["status"] = 5  -- StatusDead
redis.call("SET", KEYS[3], cjson.encode(task))
redis.call("ZADD", KEYS[5], tonumber(ARGV[2]), task["id"])
return 2
