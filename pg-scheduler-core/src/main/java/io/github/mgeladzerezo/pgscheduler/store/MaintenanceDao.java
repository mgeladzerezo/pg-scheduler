package io.github.mgeladzerezo.pgscheduler.store;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;

/**
 * Housekeeping statements that any node may run at any time: promote due jobs, reap expired leases, purge
 * old rows. Each takes its rows with {@code FOR UPDATE SKIP LOCKED}, so several nodes running the same
 * statement concurrently split the work instead of blocking or repeating each other.
 */
public final class MaintenanceDao {

    /**
     * Moves due SCHEDULED and FAILED (retry-pending) jobs to READY, which puts them into the claim index.
     * Does not touch updated_at: the claim uses it to measure how long a job really waited.
     */
    private static final String PROMOTE = """
            WITH due AS (
                SELECT id
                FROM pgs_job
                WHERE state IN ('SCHEDULED', 'FAILED') AND run_at <= now()
                ORDER BY run_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            ), promoted AS (
                UPDATE pgs_job j
                SET state = 'READY'
                FROM due
                WHERE j.id = due.id
                RETURNING j.queue
            )
            SELECT queue, count(*) AS n FROM promoted GROUP BY queue
            """;

    private static final String NEXT_DUE = """
            SELECT (extract(epoch FROM min(run_at) - now()) * 1000)::bigint AS ms
            FROM pgs_job
            WHERE state IN ('SCHEDULED', 'FAILED')
            """;

    /**
     * Takes back jobs whose lease ran out: the worker crashed, hung, or lost its connection for longer than
     * the lease. The attempt was already counted when it was claimed, so a job on its last attempt goes to
     * DEAD and any other back to READY for the next worker. The old owner is not asked; if it is still
     * alive, the fence in its completion statement turns its late result into a no-op.
     */
    private static final String REAP = """
            WITH expired AS (
                SELECT id, locked_by, started_at
                FROM pgs_job
                WHERE state = 'RUNNING' AND locked_until < now()
                ORDER BY locked_until
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            ), reaped AS (
                UPDATE pgs_job j
                SET state        = CASE WHEN j.attempt >= j.max_attempts THEN 'DEAD' ELSE 'READY' END,
                    finished_at  = CASE WHEN j.attempt >= j.max_attempts THEN now() END,
                    last_error   = 'Lease expired: worker ' || expired.locked_by
                                   || ' stopped extending it (crashed, hung or partitioned)',
                    locked_by    = NULL,
                    locked_until = NULL,
                    updated_at   = now()
                FROM expired
                WHERE j.id = expired.id
                RETURNING j.id, j.queue, j.type, j.attempt, j.state, expired.locked_by AS worker_id, expired.started_at
            ), logged AS (
                INSERT INTO pgs_job_attempt (job_id, attempt, worker_id, started_at, outcome, error)
                SELECT id, attempt, worker_id, started_at, 'LEASE_EXPIRED',
                       'The worker stopped extending the lease before reporting a result'
                FROM reaped
            )
            SELECT id, queue, type, state, worker_id FROM reaped
            """;

    private final Db db;

    public MaintenanceDao(Db db) {
        this.db = db;
    }

    /**
     * Promotes up to {@code limit} due jobs and notifies the workers of each affected queue.
     *
     * @return number of jobs promoted
     */
    public int promoteDue(int limit) {
        record Promoted(String queue, int count) {
        }
        return db.ownTransaction(connection -> {
            List<Promoted> promoted = Db.query(connection, PROMOTE,
                    rs -> new Promoted(rs.getString("queue"), rs.getInt("n")), limit);
            int total = 0;
            for (Promoted p : promoted) {
                total += p.count();
                EnqueueDao.notify(connection, EnqueueDao.READY_CHANNEL, p.queue());
            }
            return total;
        });
    }

    /** Milliseconds until the earliest waiting job is due (negative if overdue); empty if none is waiting. */
    public OptionalLong millisUntilNextDue() {
        return db.autoCommit(connection -> {
            Long millis = Db.queryOne(connection, NEXT_DUE, rs -> Db.nullableLong(rs, "ms"));
            return millis == null ? OptionalLong.empty() : OptionalLong.of(millis);
        });
    }

    /** A job taken back from a worker whose lease expired. */
    public record ReapedJob(long id, String queue, String type, boolean dead, String workerId) {
    }

    /** Reaps up to {@code limit} expired leases and wakes the workers of the affected queues. */
    public List<ReapedJob> reapExpiredLeases(int limit) {
        return db.ownTransaction(connection -> {
            List<ReapedJob> reaped = Db.query(connection, REAP, rs -> new ReapedJob(rs.getLong("id"),
                    rs.getString("queue"), rs.getString("type"), "DEAD".equals(rs.getString("state")),
                    rs.getString("worker_id")), limit);
            for (String queue : reaped.stream().filter(r -> !r.dead()).map(ReapedJob::queue).distinct().toList()) {
                EnqueueDao.notify(connection, EnqueueDao.READY_CHANNEL, queue);
            }
            return reaped;
        });
    }

    /**
     * Deletes up to {@code limit} SUCCEEDED and CANCELLED jobs that finished before the retention period.
     * Dead letters are never purged automatically: they exist to be looked at.
     */
    public int purgeFinished(Duration retention, int limit) {
        return db.autoCommit(connection -> Db.update(connection, """
                DELETE FROM pgs_job
                WHERE id IN (SELECT id
                             FROM pgs_job
                             WHERE state IN ('SUCCEEDED', 'CANCELLED')
                               AND finished_at < now() - ? * interval '1 millisecond'
                             ORDER BY finished_at
                             LIMIT ?
                             FOR UPDATE SKIP LOCKED)
                """, retention.toMillis(), limit));
    }

    public int purgeStats(Duration retention) {
        return db.autoCommit(connection -> Db.update(connection,
                "DELETE FROM pgs_stat WHERE bucket < now() - ? * interval '1 millisecond'", retention.toMillis()));
    }

    /** Removes registry rows of workers that have been silent for longer than {@code silence}. */
    public int pruneWorkers(Duration silence) {
        return db.autoCommit(connection -> Db.update(connection,
                "DELETE FROM pgs_worker WHERE last_heartbeat < now() - ? * interval '1 millisecond'",
                silence.toMillis()));
    }
}
