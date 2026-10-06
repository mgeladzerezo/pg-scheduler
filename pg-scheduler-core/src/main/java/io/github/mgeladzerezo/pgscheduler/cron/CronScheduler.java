package io.github.mgeladzerezo.pgscheduler.cron;

import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobPolicy;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.store.ScheduleDao;
import io.github.mgeladzerezo.pgscheduler.store.ScheduleDao.NewSchedule;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

/**
 * Turns cron schedules into jobs, and manages the schedules.
 *
 * <p>Any number of instances may run against the same database. {@link #tick()} takes the due schedules
 * with {@code FOR UPDATE SKIP LOCKED}, inserts one job per fire time, and advances
 * {@code next_fire_time}, all in one transaction; the unique index on {@code (schedule_id, fire_time)}
 * backs that up. So each fire time produces exactly one job no matter how many instances tick at once,
 * and if an instance dies mid-tick its transaction rolls back and another instance fires instead.
 *
 * <p>Time comes from the injected {@link Clock}, which is what makes misfire handling testable. A node
 * with a fast clock only inserts a job early; the job's {@code run_at} is the fire time and workers go by
 * the database clock, so it still does not start early.
 */
public final class CronScheduler {

    /**
     * @param misfireThreshold  how late a fire time may be noticed and still count as on time (relevant to
     *                          {@link MisfirePolicy#SKIP})
     * @param maxCatchUpPerTick upper bound of jobs one schedule may enqueue in one pass under
     *                          {@link MisfirePolicy#CATCH_UP}; the rest follows on the next pass
     * @param pollInterval      longest sleep of the background loop; it also wakes at every minute boundary
     */
    public record Options(Duration misfireThreshold, int maxCatchUpPerTick, Duration pollInterval) {

        public static Options defaults() {
            return new Options(Duration.ofSeconds(60), 500, Duration.ofSeconds(10));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);
    private static final int SCHEDULES_PER_TICK = 100;

    private final Db db;
    private final ScheduleDao dao;
    private final JobPolicies policies;
    private final ObjectMapper json;
    private final Clock clock;
    private final Options options;
    private final AtomicLong suppressedDuplicates = new AtomicLong();
    private volatile boolean running;
    private Thread thread;

    public CronScheduler(Db db, ScheduleDao dao, JobPolicies policies, ObjectMapper json, Clock clock, Options options) {
        this.db = db;
        this.dao = dao;
        this.policies = policies;
        this.json = json;
        this.clock = clock;
        this.options = options;
    }

    // ---- schedule management ----------------------------------------------------------------------------

    /**
     * Creates a schedule. Joins the caller's transaction if there is one.
     *
     * @throws CronParseException       if the expression is invalid
     * @throws IllegalArgumentException if a schedule with that name already exists
     */
    public Schedule create(ScheduleDefinition definition) {
        Schedule created = insert(definition);
        if (created == null) {
            throw new IllegalArgumentException("A schedule named '" + definition.name() + "' already exists");
        }
        return created;
    }

    /**
     * Creates a schedule unless one with the same name exists; safe to call from every instance at start-up.
     *
     * @return whether this call created it
     */
    public boolean createIfAbsent(ScheduleDefinition definition) {
        return insert(definition) != null;
    }

    private Schedule insert(ScheduleDefinition d) {
        if (d.name() == null || d.name().isBlank() || d.jobType() == null || d.jobType().isBlank()) {
            throw new IllegalArgumentException("a schedule needs a name and a job type");
        }
        CronExpression cron = CronExpression.parse(d.cron());
        Instant next = cron.next(clock.instant(), d.zone()).orElse(null);
        return dao.insertIfAbsent(new NewSchedule(d.name().trim(), cron.toString(), d.zone().getId(), d.jobType(),
                d.queue(), json.writeValueAsString(d.payload()), d.priority(), d.maxAttempts(),
                d.timeout() == null ? null : d.timeout().toMillis(), d.misfirePolicy(), next));
    }

    public List<Schedule> list() {
        return dao.list();
    }

    /** Stops a schedule from firing until it is resumed. */
    public void pause(long scheduleId) {
        if (!dao.setPaused(scheduleId, true, null)) {
            throw new NoSuchElementException("No schedule " + scheduleId);
        }
    }

    /**
     * Resumes a paused schedule from now. Fire times that passed during the pause are not misfires (the
     * pause was deliberate) and are not made up for, whatever the misfire policy.
     */
    public void resume(long scheduleId) {
        Schedule schedule = require(scheduleId);
        Instant next = CronExpression.parse(schedule.cron()).next(clock.instant(), ZoneId.of(schedule.zone())).orElse(null);
        dao.setPaused(scheduleId, false, next);
    }

    public void delete(long scheduleId) {
        if (!dao.delete(scheduleId)) {
            throw new NoSuchElementException("No schedule " + scheduleId);
        }
    }

    /** Enqueues one job for the schedule right now, outside its timetable. Returns the job id. */
    public long triggerNow(long scheduleId) {
        Schedule schedule = require(scheduleId);
        JobPolicy policy = policies.forType(schedule.jobType());
        return dao.insertManualRun(schedule, maxAttempts(schedule, policy), timeoutMs(schedule, policy));
    }

    private Schedule require(long scheduleId) {
        Schedule schedule = dao.find(scheduleId);
        if (schedule == null) {
            throw new NoSuchElementException("No schedule " + scheduleId);
        }
        return schedule;
    }

    // ---- firing -----------------------------------------------------------------------------------------

    /**
     * One pass: fires every due schedule that no other instance is working on.
     *
     * @return number of jobs enqueued by this pass
     */
    public int tick() {
        Instant now = clock.instant();
        return db.ownTransaction(connection -> {
            int enqueued = 0;
            for (Schedule schedule : ScheduleDao.lockDue(connection, now, SCHEDULES_PER_TICK)) {
                enqueued += fire(connection, schedule, now);
            }
            return enqueued;
        });
    }

    private int fire(Connection connection, Schedule schedule, Instant now) throws SQLException {
        CronExpression cron;
        ZoneId zone;
        try {
            cron = CronExpression.parse(schedule.cron());
            zone = ZoneId.of(schedule.zone());
        } catch (RuntimeException e) {
            // Only reachable if the row was edited by hand. Park it instead of failing every pass.
            log.error("Schedule '{}' has an invalid definition and was paused: {}", schedule.name(), e.getMessage());
            ScheduleDao.pauseBroken(connection, schedule.id());
            return 0;
        }

        // Walk the fire times from the first unprocessed one up to now and keep those the policy wants.
        List<Instant> toFire = new ArrayList<>();
        Instant cursor = schedule.nextFireTime();
        while (cursor != null && !cursor.isAfter(now)) {
            boolean stop = false;
            switch (schedule.misfirePolicy()) {
                case CATCH_UP -> {
                    if (toFire.size() == options.maxCatchUpPerTick()) {
                        stop = true; // cursor stays on the first fire time not handled; the next pass continues
                    } else {
                        toFire.add(cursor);
                    }
                }
                case FIRE_ONCE -> {
                    toFire.clear();
                    toFire.add(cursor);
                }
                case SKIP -> {
                    if (Duration.between(cursor, now).compareTo(options.misfireThreshold()) <= 0) {
                        toFire.add(cursor);
                    }
                }
            }
            if (stop) {
                break;
            }
            cursor = cron.next(cursor, zone).orElse(null);
        }

        JobPolicy policy = policies.forType(schedule.jobType());
        int enqueued = 0;
        for (Instant fireTime : toFire) {
            if (ScheduleDao.insertFire(connection, schedule, fireTime, maxAttempts(schedule, policy), timeoutMs(schedule, policy))) {
                enqueued++;
            } else {
                suppressedDuplicates.incrementAndGet();
            }
        }
        ScheduleDao.advance(connection, schedule.id(), toFire.isEmpty() ? schedule.lastFireTime() : toFire.getLast(), cursor);
        return enqueued;
    }

    private static int maxAttempts(Schedule schedule, JobPolicy policy) {
        return schedule.maxAttempts() != null ? schedule.maxAttempts() : policy.maxAttempts();
    }

    private static long timeoutMs(Schedule schedule, JobPolicy policy) {
        return schedule.timeoutMs() != null ? schedule.timeoutMs() : policy.timeout().toMillis();
    }

    /**
     * How many inserts were rejected by the unique index because the fire time already had a job. The
     * row locking makes this stay at zero; it is exposed so that tests and operators can see the second
     * line of defence being (or not being) needed.
     */
    public long suppressedDuplicates() {
        return suppressedDuplicates.get();
    }

    // ---- background loop --------------------------------------------------------------------------------

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().daemon().name("pgs-cron").start(this::loop);
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    private void loop() {
        while (running) {
            try {
                tick();
            } catch (RuntimeException e) {
                log.warn("Cron pass failed, will retry: {}", e.toString());
            }
            // Cron fires on minute boundaries, so sleep to the next one; the poll interval is only the
            // safety net for a failed pass or a schedule created with a past fire time.
            long toNextMinute = 60_000 - Math.floorMod(clock.millis(), 60_000L) + 1;
            try {
                Thread.sleep(Math.min(options.pollInterval().toMillis(), toNextMinute));
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
