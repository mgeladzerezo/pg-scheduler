package io.github.mgeladzerezo.pgscheduler.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The queue path as seen by a worker: claim, heartbeat, and the fenced writes that end an attempt.
 *
 * <p>Nothing here joins an application transaction. Each operation is one short transaction of its own,
 * because a claim must be visible to other workers the moment it is made, and a completion must not be
 * rolled back by anything the handler did.
 */
public final class WorkerDao {

    /**
     * The claim. Explained line by line in the README; in short:
     * <ul>
     *   <li>the CTE walks {@code pgs_job_claim_idx} (READY rows of one queue in claim order) and stops
     *       after LIMIT rows;</li>
     *   <li>{@code FOR UPDATE SKIP LOCKED} locks each row it takes and steps over rows another worker's
     *       claim holds locked right now, so concurrent claimers never wait for each other and never get
     *       the same row;</li>
     *   <li>the UPDATE flips the picked rows to RUNNING and writes the lease in the same statement, then
     *       the transaction commits and the row locks are gone. From here on ownership is the lease
     *       ({@code locked_by}, {@code locked_until}), not a database lock.</li>
     * </ul>
     * {@code %s} is the LIMIT expression: a plain parameter, or one capped by the queue's cluster-wide
     * concurrency limit.
     *
     * <p>{@code wait_ms} measures from when the job became eligible: its {@code run_at}, or the last change
     * of the row if that is later (a reaped, released or manually retried job). Promotion to READY
     * deliberately leaves {@code updated_at} alone so that promoter lag counts as waiting time.
     */
    private static final String CLAIM_TEMPLATE = """
            WITH picked AS (
                SELECT id, greatest(run_at, updated_at) AS eligible_at
                FROM pgs_job
                WHERE state = 'READY'
                  AND queue = ?
                  AND type = ANY (?)
                  AND NOT EXISTS (SELECT 1 FROM pgs_queue q WHERE q.name = ? AND q.paused)
                ORDER BY priority DESC, run_at, id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE pgs_job j
            SET state        = 'RUNNING',
                attempt      = j.attempt + 1,
                locked_by    = ?,
                locked_until = now() + ? * interval '1 millisecond',
                started_at   = now(),
                updated_at   = now()
            FROM picked
            WHERE j.id = picked.id
            RETURNING j.id, j.type, j.queue, j.payload::text AS payload, j.priority, j.attempt, j.max_attempts,
                      j.timeout_ms, j.created_at, j.unique_key, j.continuation::text AS continuation,
                      j.parent_id, j.run_at,
                      (extract(epoch FROM now() - picked.eligible_at) * 1000)::bigint AS wait_ms
            """;

    /**
     * The claim statement. Parameters: queue, job types (text[]), queue, limit, worker id, lease in ms.
     * Public so that its plan can be inspected with EXPLAIN (see {@code ClaimPlanTest} and docs/claim-plan.md).
     */
    public static final String CLAIM = CLAIM_TEMPLATE.formatted("?");

    /**
     * Same claim for a queue with a cluster-wide limit: never take more than the limit minus what is
     * RUNNING. Only correct while holding the queue's advisory lock, which serialises the claimers of that
     * one queue; otherwise two of them could both count "limit - 1" and both take a job.
     */
    static final String CLAIM_LIMITED = CLAIM_TEMPLATE.formatted("""
            greatest(0, least(?,
                    (SELECT q.max_concurrency FROM pgs_queue q WHERE q.name = ?)
                    - (SELECT count(*) FROM pgs_job r WHERE r.state = 'RUNNING' AND r.queue = ?)))""");

    private static final String LOCK_QUEUE = "SELECT pg_advisory_xact_lock(hashtextextended('pgs_queue:' || ?, 0))";

    /**
     * Ends an attempt successfully, but only if the caller still owns it. The WHERE clause is the fence:
     * state, worker id and attempt number must all still match. If the lease expired and the job was
     * reaped (and perhaps claimed again, even by the same worker), nothing matches, nothing is updated and
     * nothing is logged. The attempt row is inserted from the UPDATE's RETURNING, so "rows inserted" tells
     * the caller whether it won.
     */
    private static final String COMPLETE = """
            WITH done AS (
                UPDATE pgs_job
                SET state = 'SUCCEEDED', result = ?::jsonb, last_error = NULL,
                    locked_by = NULL, locked_until = NULL, finished_at = now(), updated_at = now()
                WHERE id = ? AND state = 'RUNNING' AND locked_by = ? AND attempt = ?
                RETURNING id, attempt, started_at
            )
            INSERT INTO pgs_job_attempt (job_id, attempt, worker_id, started_at, outcome)
            SELECT id, attempt, ?, started_at, 'SUCCEEDED' FROM done
            """;

    /**
     * Ends an attempt with a failure, fenced like {@link #COMPLETE}. Target state is FAILED (retry) or DEAD.
     * The notification wakes sleepers so they notice the new retry time.
     */
    private static final String FAIL = """
            WITH done AS (
                UPDATE pgs_job
                SET state = ?,
                    run_at = CASE WHEN ? = 'FAILED' THEN now() + ? * interval '1 millisecond' ELSE run_at END,
                    finished_at = CASE WHEN ? = 'DEAD' THEN now() END,
                    last_error = ?, locked_by = NULL, locked_until = NULL, updated_at = now()
                WHERE id = ? AND state = 'RUNNING' AND locked_by = ? AND attempt = ?
                RETURNING id, attempt, started_at, queue, state
            ), logged AS (
                INSERT INTO pgs_job_attempt (job_id, attempt, worker_id, started_at, outcome, error)
                SELECT id, attempt, ?, started_at, ?, ? FROM done
            )
            SELECT pg_notify('%s', queue) FROM done
            """.formatted(EnqueueDao.SCHEDULED_CHANNEL);

    /**
     * Hands an unfinished attempt back at shutdown: READY again, and the attempt is not counted because
     * the handler was stopped by us and did not fail.
     */
    private static final String RELEASE = """
            WITH done AS (
                UPDATE pgs_job
                SET state = 'READY', attempt = attempt - 1, locked_by = NULL, locked_until = NULL,
                    updated_at = now()
                WHERE id = ? AND state = 'RUNNING' AND locked_by = ? AND attempt = ?
                RETURNING id, attempt + 1 AS attempt, started_at, queue
            ), logged AS (
                INSERT INTO pgs_job_attempt (job_id, attempt, worker_id, started_at, outcome, error)
                SELECT id, attempt, ?, started_at, 'RELEASED', 'Worker shut down before the handler finished' FROM done
            )
            SELECT pg_notify('%s', queue) FROM done
            """.formatted(EnqueueDao.READY_CHANNEL);

    /** Extends the leases this worker still holds and returns which ones those are. */
    private static final String HEARTBEAT = """
            UPDATE pgs_job j
            SET locked_until = now() + ? * interval '1 millisecond'
            FROM unnest(?::bigint[], ?::integer[]) AS mine(id, attempt)
            WHERE j.id = mine.id AND j.attempt = mine.attempt AND j.state = 'RUNNING' AND j.locked_by = ?
            RETURNING j.id
            """;

    private static final String REFILL_BUCKET = """
            UPDATE pgs_rate_limit
            SET tokens = least(burst, tokens + greatest(0, extract(epoch FROM clock_timestamp() - refilled_at))
                                               * permits_per_second),
                refilled_at = clock_timestamp()
            WHERE job_type = ?
            RETURNING floor(tokens)::integer
            """;

    private static final Comparator<ClaimedJob> CLAIM_ORDER = Comparator.comparingInt(ClaimedJob::priority).reversed()
            .thenComparing(ClaimedJob::runAt).thenComparingLong(ClaimedJob::id);

    private final Db db;

    public WorkerDao(Db db) {
        this.db = db;
    }

    /**
     * Claims up to {@code limit} READY jobs of the given types from one queue, in priority order.
     *
     * @param clusterLimited whether the queue has a cluster-wide concurrency limit to honour
     * @return the claimed jobs in claim order; empty if nothing is ready, the queue is paused or the
     *         cluster-wide limit is reached
     */
    public List<ClaimedJob> claim(String queue, String[] types, int limit, String workerId, long leaseMs,
                                  boolean clusterLimited) {
        return db.ownTransaction(connection -> claim(connection, queue, types, limit, workerId, leaseMs, clusterLimited));
    }

    private static List<ClaimedJob> claim(Connection connection, String queue, String[] types, int limit,
                                          String workerId, long leaseMs, boolean clusterLimited) throws SQLException {
        List<ClaimedJob> jobs;
        if (clusterLimited) {
            Db.query(connection, LOCK_QUEUE, rs -> null, queue);
            jobs = Db.query(connection, CLAIM_LIMITED, WorkerDao::mapClaimed,
                    queue, types, queue, limit, queue, queue, workerId, leaseMs);
        } else {
            jobs = Db.query(connection, CLAIM, WorkerDao::mapClaimed, queue, types, queue, limit, workerId, leaseMs);
        }
        // UPDATE ... RETURNING does not promise the order of the CTE; restore claim order for dispatch.
        jobs.sort(CLAIM_ORDER);
        return jobs;
    }

    /** Result of a rate-limited claim. {@code throttled} means the bucket ran dry and jobs may be waiting. */
    public record ThrottledClaim(List<ClaimedJob> jobs, boolean throttled) {
    }

    /**
     * Claims jobs of one rate-limited type. The token-bucket row is locked for the duration of this short
     * transaction, so across the whole cluster no more jobs of the type are started than there are tokens.
     */
    public ThrottledClaim claimRateLimited(String queue, String type, int limit, String workerId, long leaseMs,
                                           boolean clusterLimited) {
        return db.ownTransaction(connection -> {
            Integer tokens = Db.queryOne(connection, REFILL_BUCKET, rs -> rs.getInt(1), type);
            int allowed = tokens == null ? limit : Math.min(limit, tokens);
            if (allowed <= 0) {
                return new ThrottledClaim(List.of(), true);
            }
            List<ClaimedJob> jobs = claim(connection, queue, new String[] {type}, allowed, workerId, leaseMs, clusterLimited);
            if (tokens != null && !jobs.isEmpty()) {
                Db.update(connection, "UPDATE pgs_rate_limit SET tokens = tokens - ? WHERE job_type = ?",
                        jobs.size(), type);
            }
            return new ThrottledClaim(jobs, tokens != null && jobs.size() == tokens && jobs.size() < limit);
        });
    }

    private static ClaimedJob mapClaimed(java.sql.ResultSet rs) throws SQLException {
        return new ClaimedJob(rs.getLong("id"), rs.getString("type"), rs.getString("queue"),
                rs.getString("payload"), rs.getInt("priority"), rs.getInt("attempt"), rs.getInt("max_attempts"),
                rs.getLong("timeout_ms"), Db.instant(rs, "run_at"), Db.instant(rs, "created_at"), rs.getString("unique_key"),
                rs.getString("continuation"), Db.nullableLong(rs, "parent_id"), Math.max(0, rs.getLong("wait_ms")));
    }

    /**
     * Marks the attempt succeeded and, in the same transaction, enqueues the continuation if there is one.
     *
     * @return {@code false} if the caller no longer owns the attempt (fenced off); nothing was written
     */
    public boolean complete(ClaimedJob job, String workerId, String resultJson, JobSpec continuation) {
        return db.ownTransaction(connection -> {
            int logged = Db.update(connection, COMPLETE, resultJson, job.id(), workerId, job.attempt(), workerId);
            if (logged == 0) {
                return false;
            }
            if (continuation != null) {
                EnqueueDao.insert(connection, continuation);
            }
            return true;
        });
    }

    /**
     * Records a failed attempt: back to FAILED with a retry time {@code retryDelayMs} from now, or DEAD.
     *
     * @param outcome attempt outcome for the timeline, {@code FAILED} or {@code TIMED_OUT}
     * @param dead    whether the job is dead-lettered instead of scheduled for a retry
     * @return {@code false} if the caller no longer owns the attempt (fenced off); nothing was written
     */
    public boolean fail(ClaimedJob job, String workerId, String outcome, boolean dead, long retryDelayMs, String error) {
        String target = dead ? "DEAD" : "FAILED";
        // The final SELECT yields one row exactly when the fenced UPDATE matched.
        return db.autoCommit(connection -> !Db.query(connection, FAIL, rs -> Boolean.TRUE,
                target, target, retryDelayMs, target, error, job.id(), workerId, job.attempt(),
                workerId, outcome, error).isEmpty());
    }

    /** Returns an unfinished attempt to the queue at shutdown. Fenced like every other write. */
    public void release(ClaimedJob job, String workerId) {
        db.autoCommit(connection -> Db.query(connection, RELEASE, rs -> null, job.id(), workerId, job.attempt(), workerId));
    }

    /**
     * Extends the lease of every listed attempt this worker still owns.
     *
     * @return ids whose lease was extended; an id that is missing has been taken away (reaped, cancelled)
     */
    public Set<Long> heartbeat(String workerId, List<ClaimedJob> running, long leaseMs) {
        if (running.isEmpty()) {
            return Set.of();
        }
        Long[] ids = running.stream().map(ClaimedJob::id).toArray(Long[]::new);
        Integer[] attempts = running.stream().map(ClaimedJob::attempt).toArray(Integer[]::new);
        return db.autoCommit(connection -> new HashSet<>(
                Db.query(connection, HEARTBEAT, rs -> rs.getLong(1), leaseMs, ids, attempts, workerId)));
    }

    // ---- worker registry and settings -------------------------------------------------------------------

    public void registerWorker(String workerId, String hostname, String queuesJson, String[] jobTypes, long leaseMs) {
        db.autoCommit(connection -> Db.update(connection, """
                INSERT INTO pgs_worker (id, hostname, queues, job_types, lease_ms)
                VALUES (?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (id) DO UPDATE
                SET hostname = EXCLUDED.hostname, queues = EXCLUDED.queues, job_types = EXCLUDED.job_types,
                    lease_ms = EXCLUDED.lease_ms, status = 'RUNNING', last_heartbeat = now()
                """, workerId, hostname, queuesJson, jobTypes, leaseMs));
    }

    /** @return {@code false} if the registry row is gone (pruned while this worker was unreachable) */
    public boolean touchWorker(String workerId) {
        return db.autoCommit(connection ->
                Db.update(connection, "UPDATE pgs_worker SET last_heartbeat = now() WHERE id = ?", workerId) > 0);
    }

    public void markWorkerStopping(String workerId) {
        db.autoCommit(connection ->
                Db.update(connection, "UPDATE pgs_worker SET status = 'STOPPING' WHERE id = ?", workerId));
    }

    public void removeWorker(String workerId) {
        db.autoCommit(connection -> Db.update(connection, "DELETE FROM pgs_worker WHERE id = ?", workerId));
    }

    /** Makes sure a settings row exists for each queue this worker serves, so the dashboard can list it. */
    public void ensureQueues(List<String> queues) {
        db.autoCommit(connection -> Db.update(connection, """
                INSERT INTO pgs_queue (name) SELECT unnest(?::text[]) ORDER BY 1 ON CONFLICT (name) DO NOTHING
                """, (Object) queues.toArray(String[]::new)));
    }

    /** Per-queue settings a worker caches between polls. */
    public record QueueSettings(boolean paused, Integer maxConcurrency) {
    }

    public Map<String, QueueSettings> queueSettings() {
        return db.autoCommit(connection -> {
            Map<String, QueueSettings> settings = new java.util.HashMap<>();
            Db.query(connection, "SELECT name, paused, max_concurrency FROM pgs_queue", rs -> settings.put(
                    rs.getString("name"),
                    new QueueSettings(rs.getBoolean("paused"), Db.nullableInt(rs, "max_concurrency"))));
            return settings;
        });
    }

    /** Creates or updates the token bucket of a rate-limited job type. A shrunk burst trims stored tokens. */
    public void upsertRateLimit(String jobType, double permitsPerSecond, int burst) {
        db.autoCommit(connection -> Db.update(connection, """
                INSERT INTO pgs_rate_limit (job_type, permits_per_second, burst, tokens)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (job_type) DO UPDATE
                SET permits_per_second = EXCLUDED.permits_per_second, burst = EXCLUDED.burst,
                    tokens = least(pgs_rate_limit.tokens, EXCLUDED.burst)
                """, jobType, permitsPerSecond, burst, (double) burst));
    }
}
