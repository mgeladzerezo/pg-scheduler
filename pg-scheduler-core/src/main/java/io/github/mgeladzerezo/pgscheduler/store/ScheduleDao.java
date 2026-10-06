package io.github.mgeladzerezo.pgscheduler.store;

import io.github.mgeladzerezo.pgscheduler.cron.MisfirePolicy;
import io.github.mgeladzerezo.pgscheduler.cron.Schedule;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/**
 * SQL for cron schedules and for the jobs they produce.
 */
public final class ScheduleDao {

    private static final String COLUMNS = """
            id, name, cron, zone, job_type, queue, payload::text AS payload, priority, max_attempts, timeout_ms,
            misfire_policy, paused, next_fire_time, last_fire_time, created_at, updated_at
            """;

    /**
     * Takes the schedules that are due and that no other scheduler instance is processing right now. The
     * row lock is held until the caller's transaction has inserted the jobs and advanced next_fire_time,
     * so two instances can never work on the same fire time at once; SKIP LOCKED makes the second
     * instance move on instead of queueing behind the first.
     */
    private static final String LOCK_DUE = """
            SELECT %s
            FROM pgs_schedule
            WHERE NOT paused AND next_fire_time <= ?
            ORDER BY next_fire_time
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """.formatted(COLUMNS);

    /**
     * One job per (schedule, fire time). The unique index is the actual exactly-once guarantee: even if
     * the locking above were bypassed (a bug, a manual UPDATE of next_fire_time), a second insert for the
     * same fire time conflicts and does nothing.
     */
    private static final String INSERT_FIRE = """
            WITH inserted AS (
                INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, max_attempts, timeout_ms,
                                     schedule_id, fire_time)
                VALUES (?, ?, ?::jsonb, ?, CASE WHEN ?::timestamptz <= now() THEN 'READY' ELSE 'SCHEDULED' END,
                        ?, ?, ?, ?, ?)
                ON CONFLICT (schedule_id, fire_time) WHERE schedule_id IS NOT NULL AND fire_time IS NOT NULL
                DO NOTHING
                RETURNING id, state, queue
            )
            SELECT id, pg_notify(CASE WHEN state = 'READY' THEN '%s' ELSE '%s' END, queue)
            FROM inserted
            """.formatted(EnqueueDao.READY_CHANNEL, EnqueueDao.SCHEDULED_CHANNEL);

    private final Db db;

    public ScheduleDao(Db db) {
        this.db = db;
    }

    /** Values of a new schedule row. */
    public record NewSchedule(String name, String cron, String zone, String jobType, String queue,
                              String payloadJson, int priority, Integer maxAttempts, Long timeoutMs,
                              MisfirePolicy misfirePolicy, Instant nextFireTime) {
    }

    /**
     * Inserts a schedule in the caller's transaction.
     *
     * @return the stored schedule, or {@code null} if one with that name already exists
     */
    public Schedule insertIfAbsent(NewSchedule s) {
        return db.joining(connection -> Db.queryOne(connection, """
                INSERT INTO pgs_schedule (name, cron, zone, job_type, queue, payload, priority, max_attempts,
                                          timeout_ms, misfire_policy, next_fire_time)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)
                ON CONFLICT (name) DO NOTHING
                RETURNING %s
                """.formatted(COLUMNS), ScheduleDao::map,
                s.name(), s.cron(), s.zone(), s.jobType(), s.queue(), s.payloadJson(), s.priority(),
                s.maxAttempts(), s.timeoutMs(), s.misfirePolicy().name(), s.nextFireTime()));
    }

    public List<Schedule> list() {
        return db.joining(connection ->
                Db.query(connection, "SELECT " + COLUMNS + " FROM pgs_schedule ORDER BY name", ScheduleDao::map));
    }

    public Schedule find(long id) {
        return db.joining(connection ->
                Db.queryOne(connection, "SELECT " + COLUMNS + " FROM pgs_schedule WHERE id = ?", ScheduleDao::map, id));
    }

    public Schedule findByName(String name) {
        return db.joining(connection ->
                Db.queryOne(connection, "SELECT " + COLUMNS + " FROM pgs_schedule WHERE name = ?", ScheduleDao::map, name));
    }

    /** Pauses or resumes; on resume the caller passes the next fire time counted from now. */
    public boolean setPaused(long id, boolean paused, Instant nextFireTime) {
        return db.joining(connection -> Db.update(connection, """
                UPDATE pgs_schedule
                SET paused = ?, next_fire_time = COALESCE(?, next_fire_time), updated_at = now()
                WHERE id = ?
                """, paused, nextFireTime, id) > 0);
    }

    public boolean delete(long id) {
        return db.joining(connection -> Db.update(connection, "DELETE FROM pgs_schedule WHERE id = ?", id) > 0);
    }

    /** Inserts a job for a schedule outside its timetable ("trigger now"). Returns the job id. */
    public long insertManualRun(Schedule s, int maxAttempts, long timeoutMs) {
        return db.joining(connection -> {
            Long id = Db.queryOne(connection, """
                    INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, max_attempts, timeout_ms, schedule_id)
                    VALUES (?, ?, ?::jsonb, ?, 'READY', now(), ?, ?, ?)
                    RETURNING id
                    """, rs -> rs.getLong(1), s.jobType(), s.queue(), s.payloadJson(), s.priority(), maxAttempts,
                    timeoutMs, s.id());
            EnqueueDao.notify(connection, EnqueueDao.READY_CHANNEL, s.queue());
            return id;
        });
    }

    // ---- used inside the scheduler's own transaction ----------------------------------------------------

    public static List<Schedule> lockDue(Connection connection, Instant now, int limit) throws SQLException {
        return Db.query(connection, LOCK_DUE, ScheduleDao::map, now, limit);
    }

    /** @return whether a job was inserted; {@code false} means this fire time already has one */
    public static boolean insertFire(Connection connection, Schedule s, Instant fireTime, int maxAttempts,
                                     long timeoutMs) throws SQLException {
        return Db.queryOne(connection, INSERT_FIRE, rs -> rs.getLong(1), s.jobType(), s.queue(), s.payloadJson(),
                s.priority(), fireTime, fireTime, maxAttempts, timeoutMs, s.id(), fireTime) != null;
    }

    public static void advance(Connection connection, long id, Instant lastFireTime, Instant nextFireTime)
            throws SQLException {
        Db.update(connection, """
                UPDATE pgs_schedule
                SET last_fire_time = ?::timestamptz, next_fire_time = ?::timestamptz, updated_at = now()
                WHERE id = ?
                """, lastFireTime, nextFireTime, id);
    }

    public static void pauseBroken(Connection connection, long id) throws SQLException {
        Db.update(connection, "UPDATE pgs_schedule SET paused = true, updated_at = now() WHERE id = ?", id);
    }

    private static Schedule map(ResultSet rs) throws SQLException {
        return new Schedule(rs.getLong("id"), rs.getString("name"), rs.getString("cron"), rs.getString("zone"),
                rs.getString("job_type"), rs.getString("queue"), rs.getString("payload"), rs.getInt("priority"),
                Db.nullableInt(rs, "max_attempts"), Db.nullableLong(rs, "timeout_ms"),
                MisfirePolicy.valueOf(rs.getString("misfire_policy")), rs.getBoolean("paused"),
                Db.instant(rs, "next_fire_time"), Db.instant(rs, "last_fire_time"),
                Db.instant(rs, "created_at"), Db.instant(rs, "updated_at"));
    }
}
