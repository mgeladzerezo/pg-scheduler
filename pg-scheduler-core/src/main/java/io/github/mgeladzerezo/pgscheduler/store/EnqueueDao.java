package io.github.mgeladzerezo.pgscheduler.store;

import io.github.mgeladzerezo.pgscheduler.EnqueueResult;
import io.github.mgeladzerezo.pgscheduler.SchedulerException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Inserts jobs. All public methods join the caller's transaction (see {@link Db#joining}), which is what
 * makes enqueueing transactional with the caller's business change.
 */
public final class EnqueueDao {

    /** Channel notified (payload: queue name) when a job becomes READY. Delivered by PostgreSQL at commit. */
    public static final String READY_CHANNEL = "pgs_job_ready";
    /** Channel notified when a job is inserted for a future time, so sleepers can recompute their wake-up. */
    public static final String SCHEDULED_CHANNEL = "pgs_job_scheduled";

    static final String PENDING_STATES = "('SCHEDULED', 'READY', 'RUNNING', 'FAILED')";

    /**
     * The state is decided by comparing run_at with the database clock: due jobs are born READY and go
     * straight into the claim index, future jobs are born SCHEDULED. ON CONFLICT targets the partial unique
     * index on unique_key, so a pending duplicate makes the insert a no-op instead of an error. The outer
     * SELECT queues a NOTIFY in the same transaction; PostgreSQL delivers it only if and when it commits.
     */
    private static final String INSERT_ONE = """
            WITH inserted AS (
                INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, max_attempts, timeout_ms,
                                     unique_key, continuation, parent_id)
                SELECT ?::text, ?::text, ?::jsonb, ?::smallint,
                       CASE WHEN t.run_at <= clock_timestamp() THEN 'READY' ELSE 'SCHEDULED' END,
                       t.run_at, ?::integer, ?::bigint, ?::text, ?::jsonb, ?::bigint
                FROM (SELECT COALESCE(?::timestamptz,
                                      clock_timestamp() + ?::bigint * interval '1 millisecond') AS run_at) t
                ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state IN %s
                DO NOTHING
                RETURNING id, state, queue
            )
            SELECT id, pg_notify(CASE WHEN state = 'READY' THEN '%s' ELSE '%s' END, queue)
            FROM inserted
            """.formatted(PENDING_STATES, READY_CHANNEL, SCHEDULED_CHANNEL);

    private static final String FIND_PENDING = """
            SELECT id FROM pgs_job
            WHERE unique_key = ? AND state IN %s
            """.formatted(PENDING_STATES);

    private static final String INSERT_MANY = """
            WITH input AS (
                SELECT u.*,
                       COALESCE(to_timestamp(u.run_at_ms / 1000.0),
                                clock_timestamp() + u.delay_ms * interval '1 millisecond') AS run_at
                FROM unnest(?::text[], ?::text[], ?::text[], ?::integer[], ?::bigint[], ?::bigint[], ?::text[],
                            ?::integer[], ?::bigint[], ?::text[])
                     AS u(type, queue, payload, priority, run_at_ms, delay_ms, unique_key,
                          max_attempts, timeout_ms, continuation)
            ), inserted AS (
                INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, max_attempts, timeout_ms,
                                     unique_key, continuation)
                SELECT type, queue, payload::jsonb, priority,
                       CASE WHEN run_at <= clock_timestamp() THEN 'READY' ELSE 'SCHEDULED' END,
                       run_at, max_attempts, timeout_ms, unique_key, continuation::jsonb
                FROM input
                ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state IN %s
                DO NOTHING
                RETURNING state, queue
            )
            SELECT state, queue, count(*) AS n FROM inserted GROUP BY state, queue
            """.formatted(PENDING_STATES);

    private final Db db;

    public EnqueueDao(Db db) {
        this.db = db;
    }

    /** Inserts one job in the caller's transaction, or resolves the pending job that holds its unique key. */
    public EnqueueResult enqueue(JobSpec spec) {
        return db.joining(connection -> {
            // Two rounds are enough in practice: the second covers the pending duplicate finishing between
            // our skipped insert and the lookup. A third guards against that happening twice in a row.
            for (int round = 0; round < 3; round++) {
                Long id = insert(connection, spec);
                if (id != null) {
                    return new EnqueueResult(id, true);
                }
                Long pending = Db.queryOne(connection, FIND_PENDING, rs -> rs.getLong(1), spec.uniqueKey());
                if (pending != null) {
                    return new EnqueueResult(pending, false);
                }
            }
            throw new SchedulerException("Could not enqueue job with unique key '" + spec.uniqueKey()
                    + "': the key kept changing hands");
        });
    }

    /**
     * Inserts one job on the given connection and queues the wake-up notification.
     *
     * @return the new id, or {@code null} if a pending job already holds the unique key
     */
    public static Long insert(Connection connection, JobSpec spec) throws SQLException {
        return Db.queryOne(connection, INSERT_ONE, rs -> rs.getLong(1),
                spec.type(), spec.queue(), spec.payloadJson(), spec.priority(), spec.maxAttempts(),
                spec.timeoutMs(), spec.uniqueKey(), spec.continuationJson(), spec.parentId(),
                spec.runAt(), spec.delayMs());
    }

    /** Inserts many jobs with one statement in the caller's transaction; returns how many were inserted. */
    public int enqueueAll(List<JobSpec> specs) {
        if (specs.isEmpty()) {
            return 0;
        }
        int n = specs.size();
        String[] types = new String[n];
        String[] queues = new String[n];
        String[] payloads = new String[n];
        Integer[] priorities = new Integer[n];
        Long[] runAtMillis = new Long[n];
        Long[] delays = new Long[n];
        String[] uniqueKeys = new String[n];
        Integer[] maxAttempts = new Integer[n];
        Long[] timeouts = new Long[n];
        String[] continuations = new String[n];
        for (int i = 0; i < n; i++) {
            JobSpec spec = specs.get(i);
            types[i] = spec.type();
            queues[i] = spec.queue();
            payloads[i] = spec.payloadJson();
            priorities[i] = spec.priority();
            runAtMillis[i] = spec.runAt() == null ? null : spec.runAt().toEpochMilli();
            delays[i] = spec.delayMs();
            uniqueKeys[i] = spec.uniqueKey();
            maxAttempts[i] = spec.maxAttempts();
            timeouts[i] = spec.timeoutMs();
            continuations[i] = spec.continuationJson();
        }
        record Group(String state, String queue, int count) {
        }
        return db.joining(connection -> {
            List<Group> groups = Db.query(connection, INSERT_MANY,
                    rs -> new Group(rs.getString("state"), rs.getString("queue"), rs.getInt("n")),
                    types, queues, payloads, priorities, runAtMillis, delays, uniqueKeys, maxAttempts, timeouts,
                    continuations);
            int inserted = 0;
            for (Group group : groups) {
                inserted += group.count();
                notify(connection, "READY".equals(group.state()) ? READY_CHANNEL : SCHEDULED_CHANNEL, group.queue());
            }
            return inserted;
        });
    }

    /** Queues a NOTIFY on the connection's current transaction. */
    public static void notify(Connection connection, String channel, String payload) throws SQLException {
        Db.query(connection, "SELECT pg_notify(?, ?)", rs -> null, channel, payload);
    }
}
