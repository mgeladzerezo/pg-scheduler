package io.github.mgeladzerezo.pgscheduler.admin;

import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobState;
import io.github.mgeladzerezo.pgscheduler.SchedulerException;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.store.EnqueueDao;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Read models and operator actions: what the dashboard shows and does. Usable from any application that
 * wants its own admin screen.
 *
 * <p>Actions are single conditional UPDATEs: the allowed source states are part of the WHERE clause, so an
 * action that lost a race with a worker (the job started, finished or was reaped a moment earlier) changes
 * nothing and reports it, rather than overwriting the newer state. All methods join the caller's
 * transaction if one is open.
 */
public final class SchedulerAdmin {

    /** A job row. JSON columns are passed through as text. */
    public record JobView(long id, String type, String queue, String payloadJson, int priority, JobState state,
                          Instant runAt, int attempt, int maxAttempts, long timeoutMs, String uniqueKey,
                          String lockedBy, Instant lockedUntil, String resultJson, String lastError,
                          Long scheduleId, Instant fireTime, Long parentId, boolean hasContinuation,
                          Instant createdAt, Instant updatedAt, Instant startedAt, Instant finishedAt) {
    }

    /** One finished attempt of a job. */
    public record AttemptView(int attempt, String workerId, Instant startedAt, Instant finishedAt, String outcome,
                              String error) {
    }

    /**
     * Filter for {@link #search}. Every field is optional.
     *
     * @param text     a job id or an exact unique key
     * @param beforeId keyset cursor: only jobs with a smaller id (results are newest first)
     */
    public record JobQuery(JobState state, String queue, String type, String text, Long beforeId, int limit) {
    }

    /**
     * A queue with its live counts.
     *
     * @param counts    jobs per state for the states that are cheap to count exactly (everything unfinished,
     *                  plus dead letters)
     * @param lastHour  attempts per outcome in the last hour, from the roll-up
     */
    public record QueueView(String name, boolean paused, Integer maxConcurrency, Map<JobState, Long> counts,
                            Map<String, Long> lastHour) {
    }

    /** A job currently running on a worker. */
    public record RunningJob(long id, String type, String queue, int attempt, Instant startedAt, Instant lockedUntil) {
    }

    /**
     * A registered worker.
     *
     * @param alive whether it sent a heartbeat within its own lease duration
     */
    public record WorkerView(String id, String hostname, Map<String, Integer> queues, List<String> jobTypes,
                             String status, Instant startedAt, Instant lastHeartbeat, long millisSinceHeartbeat,
                             boolean alive, List<RunningJob> running) {
    }

    /** Attempts that ended in one 5-second window, by outcome. */
    public record ThroughputPoint(Instant bucket, Map<String, Long> byOutcome) {
    }

    private static final String JOB_COLUMNS = """
            id, type, queue, payload::text AS payload, priority, state, run_at, attempt, max_attempts, timeout_ms,
            unique_key, locked_by, locked_until, result::text AS result, last_error, schedule_id, fire_time,
            parent_id, continuation IS NOT NULL AS has_continuation, created_at, updated_at, started_at, finished_at
            """;

    private final Db db;
    private final JobPolicies policies;

    public SchedulerAdmin(Db db, JobPolicies policies) {
        this.db = db;
        this.policies = policies;
    }

    // ---- jobs: reading ----------------------------------------------------------------------------------

    /** The job, or {@code null} if it does not exist. */
    public JobView job(long id) {
        return db.joining(connection -> Db.queryOne(connection,
                "SELECT " + JOB_COLUMNS + " FROM pgs_job WHERE id = ?", SchedulerAdmin::mapJob, id));
    }

    /** The attempts of a job, oldest first. */
    public List<AttemptView> attempts(long jobId) {
        return db.joining(connection -> Db.query(connection, """
                SELECT attempt, worker_id, started_at, finished_at, outcome, error
                FROM pgs_job_attempt WHERE job_id = ? ORDER BY id
                """, rs -> new AttemptView(rs.getInt("attempt"), rs.getString("worker_id"),
                Db.instant(rs, "started_at"), Db.instant(rs, "finished_at"), rs.getString("outcome"),
                rs.getString("error")), jobId));
    }

    /** Jobs matching the filter, newest first, at most {@code query.limit()} (capped at 500). */
    public List<JobView> search(JobQuery query) {
        StringBuilder sql = new StringBuilder("SELECT " + JOB_COLUMNS + " FROM pgs_job WHERE true");
        List<Object> params = new ArrayList<>();
        if (query.state() != null) {
            // Inlined (it is an enum constant, not user text) so the planner can match the partial indexes.
            sql.append(" AND state = '").append(query.state().name()).append('\'');
        }
        if (query.queue() != null && !query.queue().isBlank()) {
            sql.append(" AND queue = ?");
            params.add(query.queue());
        }
        if (query.type() != null && !query.type().isBlank()) {
            sql.append(" AND type = ?");
            params.add(query.type());
        }
        if (query.text() != null && !query.text().isBlank()) {
            String text = query.text().trim();
            if (text.chars().allMatch(Character::isDigit) && text.length() <= 18) {
                sql.append(" AND (id = ? OR unique_key = ?)");
                params.add(Long.parseLong(text));
                params.add(text);
            } else {
                sql.append(" AND unique_key = ?");
                params.add(text);
            }
        }
        if (query.beforeId() != null) {
            sql.append(" AND id < ?");
            params.add(query.beforeId());
        }
        sql.append(" ORDER BY id DESC LIMIT ?");
        params.add(Math.max(1, Math.min(query.limit(), 500)));
        return db.joining(connection -> Db.query(connection, sql.toString(), SchedulerAdmin::mapJob, params.toArray()));
    }

    // ---- jobs: actions ----------------------------------------------------------------------------------

    /**
     * Puts a dead-lettered or cancelled job back in the queue with a fresh attempt budget on top of the
     * attempts it has already used.
     *
     * @param extraAttempts attempts to grant, or {@code null} for the job type's default
     * @throws IllegalStateException  if the job is in another state, or its unique key is now held by another pending job
     * @throws NoSuchElementException if there is no such job
     */
    public void retry(long id, Integer extraAttempts) {
        JobView job = require(id);
        int grant = extraAttempts != null ? extraAttempts : policies.forType(job.type()).maxAttempts();
        if (grant < 1) {
            throw new IllegalArgumentException("extraAttempts must be at least 1");
        }
        try {
            mustChange(job, "retried", "DEAD or CANCELLED", db.joining(connection -> Db.query(connection, """
                    UPDATE pgs_job
                    SET state = 'READY', run_at = now(), max_attempts = attempt + ?, finished_at = NULL, updated_at = now()
                    WHERE id = ? AND state IN ('DEAD', 'CANCELLED')
                    RETURNING pg_notify('%s', queue)
                    """.formatted(EnqueueDao.READY_CHANNEL), rs -> true, grant, id).size()));
        } catch (SchedulerException e) {
            if (e.getCause() instanceof SQLException sql && Db.UNIQUE_VIOLATION.equals(sql.getSQLState())) {
                throw new IllegalStateException("Job " + id + " cannot be retried: another pending job holds its "
                        + "unique key '" + job.uniqueKey() + "'");
            }
            throw e;
        }
    }

    /**
     * Cancels a job that has not finished. A running job is cancelled too: its row leaves the RUNNING
     * state, so the worker's next heartbeat finds the lease gone and interrupts the handler, and whatever
     * the handler reports afterwards is fenced off.
     *
     * @throws IllegalStateException  if the job has already finished
     * @throws NoSuchElementException if there is no such job
     */
    public void cancel(long id) {
        JobView job = require(id);
        mustChange(job, "cancelled", "SCHEDULED, READY, FAILED or RUNNING", db.joining(connection -> Db.queryOne(connection, """
                WITH target AS (
                    SELECT id, state, locked_by, started_at, attempt
                    FROM pgs_job
                    WHERE id = ? AND state IN ('SCHEDULED', 'READY', 'FAILED', 'RUNNING')
                    FOR UPDATE
                ), done AS (
                    UPDATE pgs_job j
                    SET state = 'CANCELLED', locked_by = NULL, locked_until = NULL, finished_at = now(), updated_at = now()
                    FROM target
                    WHERE j.id = target.id
                    RETURNING j.id
                ), logged AS (
                    INSERT INTO pgs_job_attempt (job_id, attempt, worker_id, started_at, outcome, error)
                    SELECT id, attempt, locked_by, started_at, 'CANCELLED', 'Cancelled by an operator while running'
                    FROM target
                    WHERE state = 'RUNNING'
                )
                SELECT count(*) FROM done
                """, rs -> rs.getInt(1), id)));
    }

    /**
     * Makes a waiting job (delayed, or backing off before a retry) due immediately.
     *
     * @throws IllegalStateException  if the job is not SCHEDULED or FAILED
     * @throws NoSuchElementException if there is no such job
     */
    public void runNow(long id) {
        JobView job = require(id);
        mustChange(job, "started early", "SCHEDULED or FAILED", db.joining(connection -> Db.query(connection, """
                UPDATE pgs_job
                SET state = 'READY', run_at = now(), updated_at = now()
                WHERE id = ? AND state IN ('SCHEDULED', 'FAILED')
                RETURNING pg_notify('%s', queue)
                """.formatted(EnqueueDao.READY_CHANNEL), rs -> true, id).size()));
    }

    /**
     * Changes when a job that has not started will run.
     *
     * @throws IllegalStateException  if the job is running or finished
     * @throws NoSuchElementException if there is no such job
     */
    public void reschedule(long id, Instant runAt) {
        JobView job = require(id);
        mustChange(job, "rescheduled", "SCHEDULED, READY or FAILED", db.joining(connection -> Db.query(connection, """
                UPDATE pgs_job
                SET run_at = ?::timestamptz,
                    state = CASE WHEN ?::timestamptz <= now() THEN 'READY' ELSE 'SCHEDULED' END,
                    updated_at = now()
                WHERE id = ? AND state IN ('SCHEDULED', 'READY', 'FAILED')
                RETURNING pg_notify(CASE WHEN state = 'READY' THEN '%s' ELSE '%s' END, queue)
                """.formatted(EnqueueDao.READY_CHANNEL, EnqueueDao.SCHEDULED_CHANNEL), rs -> true, runAt, runAt, id).size()));
    }

    private JobView require(long id) {
        JobView job = job(id);
        if (job == null) {
            throw new NoSuchElementException("No job " + id);
        }
        return job;
    }

    private static void mustChange(JobView before, String verb, String allowed, int changed) {
        if (changed == 0) {
            throw new IllegalStateException("Job " + before.id() + " cannot be " + verb + ": it is "
                    + before.state() + " (or changed state just now); allowed from " + allowed);
        }
    }

    // ---- queues -----------------------------------------------------------------------------------------

    /**
     * All known queues with exact counts of their unfinished and dead jobs.
     *
     * <p>The counts are four separate aggregates whose predicates match the partial indexes exactly, so
     * each is answered from an index that contains only the rows being counted. A single
     * {@code GROUP BY state} over the table would have to read every finished row.
     */
    public List<QueueView> queues() {
        return db.joining(connection -> {
            Map<String, Map<JobState, Long>> counts = new TreeMap<>();
            Db.query(connection, """
                    SELECT queue, 'READY' AS state, count(*) AS n FROM pgs_job WHERE state = 'READY' GROUP BY queue
                    UNION ALL
                    SELECT queue, state, count(*) FROM pgs_job WHERE state IN ('SCHEDULED', 'FAILED') GROUP BY queue, state
                    UNION ALL
                    SELECT queue, 'RUNNING', count(*) FROM pgs_job WHERE state = 'RUNNING' GROUP BY queue
                    UNION ALL
                    SELECT queue, 'DEAD', count(*) FROM pgs_job WHERE state = 'DEAD' GROUP BY queue
                    """, rs -> counts.computeIfAbsent(rs.getString("queue"), q -> new EnumMap<>(JobState.class))
                    .put(JobState.valueOf(rs.getString("state")), rs.getLong("n")));

            Map<String, Map<String, Long>> lastHour = new TreeMap<>();
            Db.query(connection, """
                    SELECT queue, outcome, sum(count)::bigint AS n
                    FROM pgs_stat WHERE bucket > now() - interval '1 hour'
                    GROUP BY queue, outcome
                    """, rs -> lastHour.computeIfAbsent(rs.getString("queue"), q -> new TreeMap<>())
                    .put(rs.getString("outcome"), rs.getLong("n")));

            record Settings(boolean paused, Integer maxConcurrency) {
            }
            Map<String, Settings> settings = new TreeMap<>();
            Db.query(connection, "SELECT name, paused, max_concurrency FROM pgs_queue", rs -> settings.put(
                    rs.getString("name"), new Settings(rs.getBoolean("paused"), Db.nullableInt(rs, "max_concurrency"))));

            TreeMap<String, QueueView> views = new TreeMap<>();
            List<String> names = new ArrayList<>(settings.keySet());
            names.addAll(counts.keySet());
            names.addAll(lastHour.keySet());
            for (String name : names) {
                Settings s = settings.getOrDefault(name, new Settings(false, null));
                views.put(name, new QueueView(name, s.paused(), s.maxConcurrency(),
                        counts.getOrDefault(name, Map.of()), lastHour.getOrDefault(name, Map.of())));
            }
            return List.copyOf(views.values());
        });
    }

    /** Stops workers from claiming from the queue. Running jobs finish; enqueueing still works. */
    public void pauseQueue(String queue) {
        setPaused(queue, true);
    }

    public void resumeQueue(String queue) {
        setPaused(queue, false);
    }

    private void setPaused(String queue, boolean paused) {
        db.joining(connection -> {
            Db.update(connection, """
                    INSERT INTO pgs_queue (name, paused) VALUES (?, ?)
                    ON CONFLICT (name) DO UPDATE SET paused = EXCLUDED.paused, updated_at = now()
                    """, queue, paused);
            if (!paused) {
                EnqueueDao.notify(connection, EnqueueDao.READY_CHANNEL, queue);
            }
            return null;
        });
    }

    /**
     * Sets or clears ({@code null}) the cluster-wide limit on concurrently running jobs of a queue.
     * Workers pick the change up within about a second.
     */
    public void setQueueConcurrencyLimit(String queue, Integer maxConcurrency) {
        if (maxConcurrency != null && maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be at least 1");
        }
        db.joining(connection -> Db.update(connection, """
                INSERT INTO pgs_queue (name, max_concurrency) VALUES (?, ?)
                ON CONFLICT (name) DO UPDATE SET max_concurrency = EXCLUDED.max_concurrency, updated_at = now()
                """, queue, maxConcurrency));
    }

    // ---- workers ----------------------------------------------------------------------------------------

    /** Registered workers, with the jobs each is running, ordered by id. */
    public List<WorkerView> workers() {
        return db.joining(connection -> {
            Map<String, List<RunningJob>> running = new LinkedHashMap<>();
            Db.query(connection, """
                    SELECT id, type, queue, attempt, started_at, locked_by, locked_until
                    FROM pgs_job WHERE state = 'RUNNING' ORDER BY started_at
                    """, rs -> running.computeIfAbsent(rs.getString("locked_by"), w -> new ArrayList<>())
                    .add(new RunningJob(rs.getLong("id"), rs.getString("type"), rs.getString("queue"),
                            rs.getInt("attempt"), Db.instant(rs, "started_at"), Db.instant(rs, "locked_until"))));

            Map<String, Map<String, Integer>> queues = new LinkedHashMap<>();
            Db.query(connection, """
                    SELECT w.id, q.key AS queue, q.value::integer AS concurrency
                    FROM pgs_worker w, jsonb_each_text(w.queues) q
                    ORDER BY w.id, q.key
                    """, rs -> queues.computeIfAbsent(rs.getString("id"), w -> new LinkedHashMap<>())
                    .put(rs.getString("queue"), rs.getInt("concurrency")));

            List<WorkerView> workers = new ArrayList<>(Db.query(connection, """
                    SELECT id, hostname, job_types, status, started_at, last_heartbeat, lease_ms,
                           (extract(epoch FROM now() - last_heartbeat) * 1000)::bigint AS silence_ms
                    FROM pgs_worker ORDER BY id
                    """, rs -> new WorkerView(rs.getString("id"), rs.getString("hostname"),
                    queues.getOrDefault(rs.getString("id"), Map.of()), textArray(rs.getArray("job_types")),
                    rs.getString("status"),
                    Db.instant(rs, "started_at"), Db.instant(rs, "last_heartbeat"), rs.getLong("silence_ms"),
                    rs.getLong("silence_ms") < rs.getLong("lease_ms"),
                    running.getOrDefault(rs.getString("id"), List.of()))));

            // Jobs still leased to a worker that is no longer registered: show them until they are reaped.
            running.forEach((workerId, jobs) -> {
                if (workers.stream().noneMatch(w -> w.id().equals(workerId))) {
                    workers.add(new WorkerView(workerId, "?", Map.of(), List.of(), "GONE", null, null, -1, false, jobs));
                }
            });
            return workers;
        });
    }

    /**
     * READY jobs per type for types that no live worker has a handler for. Such jobs are parked: they are
     * never claimed (workers only claim types they can run), never lost, and start as soon as a worker
     * that knows the type joins.
     */
    public Map<String, Long> parkedJobTypes() {
        return db.joining(connection -> {
            Map<String, Long> parked = new TreeMap<>();
            Db.query(connection, """
                    SELECT type, count(*) AS n
                    FROM pgs_job
                    WHERE state = 'READY'
                      AND type <> ALL (ARRAY(SELECT DISTINCT unnest(job_types)
                                             FROM pgs_worker
                                             WHERE last_heartbeat > now() - lease_ms * interval '1 millisecond'))
                    GROUP BY type
                    """, rs -> parked.put(rs.getString("type"), rs.getLong("n")));
            return parked;
        });
    }

    // ---- throughput -------------------------------------------------------------------------------------

    /** Outcome counts per 5-second window over the given period, oldest first; empty windows are omitted. */
    public List<ThroughputPoint> throughput(Duration period) {
        return db.joining(connection -> {
            Map<Instant, Map<String, Long>> buckets = new TreeMap<>();
            Db.query(connection, """
                    SELECT bucket, outcome, sum(count)::bigint AS n
                    FROM pgs_stat
                    WHERE bucket > now() - ? * interval '1 millisecond'
                    GROUP BY bucket, outcome
                    """, rs -> buckets.computeIfAbsent(Db.instant(rs, "bucket"), b -> new TreeMap<>())
                    .put(rs.getString("outcome"), rs.getLong("n")), period.toMillis());
            return buckets.entrySet().stream().map(e -> new ThroughputPoint(e.getKey(), e.getValue())).toList();
        });
    }

    // ---- mapping ----------------------------------------------------------------------------------------

    private static JobView mapJob(ResultSet rs) throws SQLException {
        return new JobView(rs.getLong("id"), rs.getString("type"), rs.getString("queue"), rs.getString("payload"),
                rs.getInt("priority"), JobState.valueOf(rs.getString("state")), Db.instant(rs, "run_at"),
                rs.getInt("attempt"), rs.getInt("max_attempts"), rs.getLong("timeout_ms"), rs.getString("unique_key"),
                rs.getString("locked_by"), Db.instant(rs, "locked_until"), rs.getString("result"),
                rs.getString("last_error"), Db.nullableLong(rs, "schedule_id"), Db.instant(rs, "fire_time"),
                Db.nullableLong(rs, "parent_id"), rs.getBoolean("has_continuation"), Db.instant(rs, "created_at"),
                Db.instant(rs, "updated_at"), Db.instant(rs, "started_at"), Db.instant(rs, "finished_at"));
    }

    private static List<String> textArray(Array array) throws SQLException {
        return array == null ? List.of() : List.of((String[]) array.getArray());
    }
}
