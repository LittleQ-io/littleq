CREATE TABLE IF NOT EXISTS lq_tasks (
    id              BIGSERIAL PRIMARY KEY,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    capabilities    TEXT[] NOT NULL DEFAULT '{}',
    priority        INTEGER NOT NULL DEFAULT 0,
    eff_priority    DOUBLE PRECISION NOT NULL DEFAULT 0,
    status          SMALLINT NOT NULL DEFAULT 0,
    policy_name     TEXT NOT NULL DEFAULT '',
    policy_max_wait BIGINT NOT NULL DEFAULT 0,
    policy_alpha    DOUBLE PRECISION NOT NULL DEFAULT 0,
    policy_retries  INTEGER NOT NULL DEFAULT 0,
    policy_scaling  SMALLINT NOT NULL DEFAULT 0,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    dedup_key       TEXT NOT NULL DEFAULT '',
    source_id       TEXT NOT NULL DEFAULT '',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 0,
    worker_id       TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at      TIMESTAMPTZ,
    heartbeat_at    TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error_msg       TEXT NOT NULL DEFAULT ''
);

-- Dispatch: highest eff_priority first among queued tasks.
CREATE INDEX IF NOT EXISTS lq_tasks_queue_idx
    ON lq_tasks (type, eff_priority DESC)
    WHERE status = 0;

-- Dedup: unique among tasks with a non-empty dedup_key per type.
CREATE UNIQUE INDEX IF NOT EXISTS lq_tasks_dedup_idx
    ON lq_tasks (type, dedup_key)
    WHERE dedup_key != '';

-- Zombie detection: claimed tasks by last heartbeat.
CREATE INDEX IF NOT EXISTS lq_tasks_zombie_idx
    ON lq_tasks (type, heartbeat_at)
    WHERE status = 1;

-- Metrics: arrival rate window.
CREATE INDEX IF NOT EXISTS lq_tasks_arrival_idx
    ON lq_tasks (type, created_at);

-- Metrics: completion rate window.
CREATE INDEX IF NOT EXISTS lq_tasks_done_idx
    ON lq_tasks (type, completed_at)
    WHERE status = 3;

-- Leader election via TTL document.
CREATE TABLE IF NOT EXISTS lq_leader (
    id         TEXT PRIMARY KEY DEFAULT 'leader',
    node_id    TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
