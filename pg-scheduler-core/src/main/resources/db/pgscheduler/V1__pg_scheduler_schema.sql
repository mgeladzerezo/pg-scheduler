-- pg-scheduler schema. Applied by the library's own Flyway instance (history table
-- pgs_schema_history), so it never interferes with the host application's migrations.

-- ---------------------------------------------------------------------------------------------------
-- Jobs
-- ---------------------------------------------------------------------------------------------------
CREATE TABLE pgs_job (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type          text        NOT NULL,
    queue         text        NOT NULL DEFAULT 'default',
    payload       jsonb       NOT NULL DEFAULT 'null'::jsonb,
    -- Higher runs first.
    priority      smallint    NOT NULL DEFAULT 0,
    state         text        NOT NULL,
    -- Earliest time the job may start. Also the retry time while state = 'FAILED'.
    run_at        timestamptz NOT NULL,
    -- Number of attempts started so far. Incremented by the claim, so a crashed attempt is counted.
    attempt       integer     NOT NULL DEFAULT 0,
    -- The job is dead-lettered when a failing attempt has this number.
    max_attempts  integer     NOT NULL CHECK (max_attempts >= 1),
    timeout_ms    bigint      NOT NULL CHECK (timeout_ms > 0),
    unique_key    text,
    -- The lease: which worker owns the running attempt, and until when it may assume it still does.
    locked_by     text,
    locked_until  timestamptz,
    result        jsonb,
    last_error    text,
    -- Set for jobs produced by a cron schedule; fire_time is NULL for a manual "trigger now".
    schedule_id   bigint,
    fire_time     timestamptz,
    -- Set for a continuation: the job whose success enqueued this one.
    parent_id     bigint,
    -- Serialised request to enqueue when this job succeeds (job chaining).
    continuation  jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    started_at    timestamptz,
    finished_at   timestamptz,
    CONSTRAINT pgs_job_state_chk CHECK (state IN
        ('SCHEDULED', 'READY', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD', 'CANCELLED')),
    -- A lease exists exactly while the job is RUNNING.
    CONSTRAINT pgs_job_lease_chk CHECK ((state = 'RUNNING') = (locked_by IS NOT NULL AND locked_until IS NOT NULL))
);

-- The claim index. It contains READY rows only, so its size follows the backlog and not the table:
-- finished rows are simply not in it. Column order matches the claim query exactly
-- (queue = ?, ORDER BY priority DESC, run_at, id), so the scan returns rows already in claim order and
-- stops after LIMIT rows.
CREATE INDEX pgs_job_claim_idx ON pgs_job (queue, priority DESC, run_at, id)
    WHERE state = 'READY';

-- Jobs waiting for their time (delayed jobs and retries in backoff). Used to promote due rows to READY
-- and to find the next wake-up time with an index-only min().
CREATE INDEX pgs_job_due_idx ON pgs_job (run_at)
    WHERE state IN ('SCHEDULED', 'FAILED');

-- Running jobs by lease expiry: the reaper's range scan, and the per-queue running count used for
-- cluster-wide concurrency limits. Bounded by the total worker concurrency.
CREATE INDEX pgs_job_lease_idx ON pgs_job (locked_until, queue)
    WHERE state = 'RUNNING';

-- De-duplication: at most one unfinished job per unique_key.
CREATE UNIQUE INDEX pgs_job_unique_key_idx ON pgs_job (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('SCHEDULED', 'READY', 'RUNNING', 'FAILED');

-- Cron exactly-once: one job per schedule and fire time, for as long as the row exists.
CREATE UNIQUE INDEX pgs_job_fire_idx ON pgs_job (schedule_id, fire_time)
    WHERE schedule_id IS NOT NULL AND fire_time IS NOT NULL;

-- Dead-letter view, newest first.
CREATE INDEX pgs_job_dead_idx ON pgs_job (finished_at DESC)
    WHERE state = 'DEAD';

-- Retention purge of finished jobs, oldest first.
CREATE INDEX pgs_job_finished_idx ON pgs_job (finished_at)
    WHERE state IN ('SUCCEEDED', 'CANCELLED');

-- Every state change rewrites the row and its partial-index entries, so dead tuples accumulate quickly.
-- Vacuum this table far more eagerly than the 20 % default.
ALTER TABLE pgs_job SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

-- One row per finished attempt: the timeline shown on the job detail page.
CREATE TABLE pgs_job_attempt (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id      bigint      NOT NULL REFERENCES pgs_job (id) ON DELETE CASCADE,
    attempt     integer     NOT NULL,
    worker_id   text,
    started_at  timestamptz,
    finished_at timestamptz NOT NULL DEFAULT now(),
    outcome     text        NOT NULL CHECK (outcome IN
        ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LEASE_EXPIRED', 'RELEASED', 'CANCELLED')),
    error       text
);

CREATE INDEX pgs_job_attempt_job_idx ON pgs_job_attempt (job_id, id);

-- ---------------------------------------------------------------------------------------------------
-- Queues, rate limits, workers
-- ---------------------------------------------------------------------------------------------------

-- Optional per-queue settings. A queue needs no row to work; a row exists once a worker serves it or an
-- operator pauses or limits it.
CREATE TABLE pgs_queue (
    name            text PRIMARY KEY,
    paused          boolean     NOT NULL DEFAULT false,
    -- Cluster-wide cap on RUNNING jobs of this queue; NULL means only per-worker limits apply.
    max_concurrency integer CHECK (max_concurrency IS NULL OR max_concurrency > 0),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Cluster-wide token bucket per job type. The row lock taken while refilling serialises the workers
-- that claim this type, which is what makes the limit hold across processes.
CREATE TABLE pgs_rate_limit (
    job_type           text PRIMARY KEY,
    permits_per_second double precision NOT NULL CHECK (permits_per_second > 0),
    burst              integer          NOT NULL CHECK (burst >= 1),
    tokens             double precision NOT NULL,
    refilled_at        timestamptz      NOT NULL DEFAULT clock_timestamp()
);

-- Worker registry, for the dashboard and for spotting job types nobody can run.
CREATE TABLE pgs_worker (
    id             text PRIMARY KEY,
    hostname       text        NOT NULL,
    -- {"queue name": local concurrency}
    queues         jsonb       NOT NULL,
    job_types      text[]      NOT NULL,
    lease_ms       bigint      NOT NULL,
    status         text        NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'STOPPING')),
    started_at     timestamptz NOT NULL DEFAULT now(),
    last_heartbeat timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------------------------------
-- Cron schedules
-- ---------------------------------------------------------------------------------------------------
CREATE TABLE pgs_schedule (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name           text        NOT NULL UNIQUE,
    cron           text        NOT NULL,
    zone           text        NOT NULL DEFAULT 'UTC',
    job_type       text        NOT NULL,
    queue          text        NOT NULL DEFAULT 'default',
    payload        jsonb       NOT NULL DEFAULT 'null'::jsonb,
    priority       smallint    NOT NULL DEFAULT 0,
    -- NULL means "use the scheduler defaults at fire time".
    max_attempts   integer,
    timeout_ms     bigint,
    misfire_policy text        NOT NULL DEFAULT 'FIRE_ONCE' CHECK (misfire_policy IN
        ('FIRE_ONCE', 'SKIP', 'CATCH_UP')),
    paused         boolean     NOT NULL DEFAULT false,
    -- The next fire time not yet processed. Advanced under a row lock by whichever instance fires it.
    next_fire_time timestamptz,
    last_fire_time timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX pgs_schedule_due_idx ON pgs_schedule (next_fire_time)
    WHERE NOT paused;

-- ---------------------------------------------------------------------------------------------------
-- Throughput roll-up
-- ---------------------------------------------------------------------------------------------------

-- Outcome counts per 5-second window, flushed by each worker about once a second. Lets the dashboard
-- draw throughput without counting rows in pgs_job. Approximate by design: a crashed worker loses its
-- unflushed second. pgs_job and pgs_job_attempt remain the source of truth.
CREATE TABLE pgs_stat (
    bucket  timestamptz NOT NULL,
    queue   text        NOT NULL,
    outcome text        NOT NULL,
    count   bigint      NOT NULL,
    PRIMARY KEY (bucket, queue, outcome)
);
