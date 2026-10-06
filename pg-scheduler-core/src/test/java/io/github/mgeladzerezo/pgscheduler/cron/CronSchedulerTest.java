package io.github.mgeladzerezo.pgscheduler.cron;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.mgeladzerezo.pgscheduler.SchedulerNode;
import io.github.mgeladzerezo.pgscheduler.testsupport.MutableClock;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Cron firing against a real database with an injected clock: exactly one job per fire time with several
 * scheduler instances, the three misfire policies after downtime, and schedule management.
 */
class CronSchedulerTest {

    private static final Instant START = Instant.parse("2026-03-28T22:00:00Z");

    private TestDatabase database;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        database = TestDatabase.create();
        clock = new MutableClock(START);
    }

    @AfterEach
    void tearDown() {
        database.close();
    }

    /** A scheduler instance on its own connection pool, sharing the test clock. Its loop is not started. */
    private CronScheduler instance() {
        return SchedulerNode.builder(database.newPool(3)).clock(clock).build().cron();
    }

    private List<Instant> fireTimes(String scheduleName) {
        return database.list("""
                SELECT j.fire_time FROM pgs_job j JOIN pgs_schedule s ON s.id = j.schedule_id
                WHERE s.name = ? ORDER BY j.fire_time
                """, rs -> rs.getTimestamp(1).toInstant(), scheduleName);
    }

    private static List<Instant> expectedFireTimes(String cron, ZoneId zone, Instant afterExclusive, Instant untilInclusive) {
        CronExpression expression = CronExpression.parse(cron);
        List<Instant> times = new ArrayList<>();
        Instant cursor = afterExclusive;
        while (true) {
            cursor = expression.next(cursor, zone).orElseThrow();
            if (cursor.isAfter(untilInclusive)) {
                return times;
            }
            times.add(cursor);
        }
    }

    @Test
    void threeInstancesFireEveryScheduleExactlyOncePerFireTime() throws Exception {
        ZoneId berlin = ZoneId.of("Europe/Berlin");
        // The simulated period (22:00 UTC on 28 March, 12 hours) contains Berlin's spring-forward night.
        Map<String, ScheduleDefinition> definitions = Map.of(
                "every-minute", ScheduleDefinition.of("every-minute", "* * * * *", "tick"),
                "every-five", ScheduleDefinition.of("every-five", "*/5 * * * *", "tick").withMisfirePolicy(MisfirePolicy.CATCH_UP),
                "hourly-berlin", ScheduleDefinition.of("hourly-berlin", "0 * * * *", "tick").withZone(berlin),
                "in-the-gap", ScheduleDefinition.of("in-the-gap", "30 2 * * *", "tick").withZone(berlin),
                "quarter-past", ScheduleDefinition.of("quarter-past", "15,45 */2 * * *", "tick").withMisfirePolicy(MisfirePolicy.CATCH_UP));

        List<CronScheduler> instances = List.of(instance(), instance(), instance());
        // Every instance tries to create every schedule, as nodes do at start-up; each exists once.
        for (CronScheduler instance : instances) {
            definitions.values().forEach(instance::createIfAbsent);
        }
        assertThat(database.count("SELECT count(*) FROM pgs_schedule")).isEqualTo(definitions.size());

        Instant end = START.plus(Duration.ofHours(12));
        Random random = new Random(7);
        int[] enqueuedBy = new int[instances.size()];
        try (ExecutorService pool = Executors.newFixedThreadPool(instances.size())) {
            while (clock.instant().isBefore(end)) {
                // Uneven steps of 20 to 55 seconds: sometimes several ticks in one minute, sometimes a fire
                // time is first noticed almost a minute late, but never by more than the misfire threshold.
                clock.advance(Duration.ofSeconds(20 + random.nextInt(36)));
                CyclicBarrier barrier = new CyclicBarrier(instances.size());
                List<Future<Integer>> results = new ArrayList<>();
                for (CronScheduler instance : instances) {
                    Callable<Integer> tick = () -> {
                        barrier.await();
                        return instance.tick();
                    };
                    results.add(pool.submit(tick));
                }
                for (int i = 0; i < results.size(); i++) {
                    enqueuedBy[i] += results.get(i).get();
                }
            }
        }
        Instant simulatedEnd = clock.instant();

        int total = 0;
        for (ScheduleDefinition definition : definitions.values()) {
            List<Instant> expected = expectedFireTimes(definition.cron(), definition.zone(), START, simulatedEnd);
            assertThat(fireTimes(definition.name())).as("fire times of %s", definition.name()).isEqualTo(expected);
            total += expected.size();
        }
        assertThat(total).isGreaterThan(800);
        assertThat(database.count("SELECT count(*) FROM pgs_job")).isEqualTo(total);
        assertThat(enqueuedBy[0] + enqueuedBy[1] + enqueuedBy[2]).isEqualTo(total);
        // All three instances took part, and the row lock alone kept them apart: the unique index on
        // (schedule_id, fire_time) never had to reject anything.
        assertThat(enqueuedBy[0]).isPositive();
        assertThat(enqueuedBy[1]).isPositive();
        assertThat(enqueuedBy[2]).isPositive();
        assertThat(instances).allSatisfy(instance -> assertThat(instance.suppressedDuplicates()).isZero());

        // The schedule that names 02:30 Berlin time fired once on the night 02:30 did not exist, at 03:00.
        assertThat(fireTimes("in-the-gap")).containsExactly(Instant.parse("2026-03-29T01:00:00Z"));
    }

    @Test
    void uniqueIndexStopsADuplicateEvenIfTheScheduleRowIsRewound() {
        CronScheduler scheduler = instance();
        scheduler.create(ScheduleDefinition.of("rewound", "*/10 * * * *", "tick").withMisfirePolicy(MisfirePolicy.CATCH_UP));
        clock.advance(Duration.ofMinutes(30));
        assertThat(scheduler.tick()).isEqualTo(3);

        // Someone resets next_fire_time by hand; the same three fire times come up again.
        database.execute("UPDATE pgs_schedule SET next_fire_time = ? WHERE name = 'rewound'", START.plus(Duration.ofMinutes(10)));

        assertThat(scheduler.tick()).isZero();
        assertThat(scheduler.suppressedDuplicates()).isEqualTo(3);
        assertThat(fireTimes("rewound")).hasSize(3).doesNotHaveDuplicates();
    }

    @Test
    void misfireFireOnceCollapsesDowntimeIntoTheLatestMissedFireTime() {
        CronScheduler scheduler = instance();
        scheduler.create(ScheduleDefinition.of("digest", "*/10 * * * *", "tick").withMisfirePolicy(MisfirePolicy.FIRE_ONCE));

        clock.advance(Duration.ofMinutes(10));
        assertThat(scheduler.tick()).isEqualTo(1); // 22:10, on time

        // Three hours of downtime: 18 fire times pass with no scheduler running.
        clock.advance(Duration.ofHours(3).plusMinutes(4));
        assertThat(scheduler.tick()).isEqualTo(1);
        assertThat(scheduler.tick()).isZero();

        assertThat(fireTimes("digest")).containsExactly(
                Instant.parse("2026-03-28T22:10:00Z"),
                Instant.parse("2026-03-29T01:10:00Z")); // the most recent missed one, not the first
        Schedule schedule = scheduler.list().getFirst();
        assertThat(schedule.lastFireTime()).isEqualTo(Instant.parse("2026-03-29T01:10:00Z"));
        assertThat(schedule.nextFireTime()).isEqualTo(Instant.parse("2026-03-29T01:20:00Z"));

        clock.advance(Duration.ofMinutes(6));
        assertThat(scheduler.tick()).isEqualTo(1); // back to normal at 01:20
    }

    @Test
    void misfireSkipDropsFireTimesOlderThanTheThresholdAndResumes() {
        CronScheduler scheduler = instance();
        scheduler.create(ScheduleDefinition.of("heartbeat", "*/10 * * * *", "tick").withMisfirePolicy(MisfirePolicy.SKIP));

        // Noticed 30 seconds late: within the 60 s threshold, so not a misfire.
        clock.advance(Duration.ofMinutes(10).plusSeconds(30));
        assertThat(scheduler.tick()).isEqualTo(1);

        // Downtime until 01:14:30: everything up to and including 01:10 is more than 60 s old.
        clock.set(Instant.parse("2026-03-29T01:14:30Z"));
        assertThat(scheduler.tick()).isZero();
        assertThat(scheduler.list().getFirst().nextFireTime()).isEqualTo(Instant.parse("2026-03-29T01:20:00Z"));

        clock.set(Instant.parse("2026-03-29T01:20:05Z"));
        assertThat(scheduler.tick()).isEqualTo(1);
        assertThat(fireTimes("heartbeat")).containsExactly(
                Instant.parse("2026-03-28T22:10:00Z"),
                Instant.parse("2026-03-29T01:20:00Z"));
    }

    @Test
    void misfireCatchUpEnqueuesEveryMissedFireTimeInBoundedPasses() {
        // A scheduler whose catch-up is capped at 5 jobs per schedule per pass.
        CronScheduler scheduler = SchedulerNode.builder(database.newPool(3)).clock(clock)
                .cron(new CronScheduler.Options(Duration.ofSeconds(60), 5, Duration.ofHours(1))).build().cron();
        scheduler.create(ScheduleDefinition.of("billing", "0 * * * *", "tick").withMisfirePolicy(MisfirePolicy.CATCH_UP));

        // Twelve hours of downtime: 23:00 through 10:00 were missed.
        clock.advance(Duration.ofHours(12).plusMinutes(20));
        assertThat(scheduler.tick()).isEqualTo(5);
        assertThat(scheduler.tick()).isEqualTo(5);
        assertThat(scheduler.tick()).isEqualTo(2);
        assertThat(scheduler.tick()).isZero();

        assertThat(fireTimes("billing")).isEqualTo(
                expectedFireTimes("0 * * * *", ZoneOffset.UTC, START, clock.instant())).hasSize(12);
        // Each job is due at its own fire time, so workers run the backlog oldest first.
        assertThat(database.count("SELECT count(*) FROM pgs_job WHERE run_at = fire_time AND state = 'READY'")).isEqualTo(12);
    }

    @Test
    void pausedScheduleDoesNotFireAndResumeDoesNotMakeUpForThePause() {
        CronScheduler scheduler = instance();
        Schedule schedule = scheduler.create(ScheduleDefinition.of("pausable", "*/10 * * * *", "tick")
                .withMisfirePolicy(MisfirePolicy.CATCH_UP));

        scheduler.pause(schedule.id());
        clock.advance(Duration.ofHours(2));
        assertThat(scheduler.tick()).isZero();

        scheduler.resume(schedule.id());
        assertThat(scheduler.tick()).as("even CATCH_UP ignores fire times that passed during a deliberate pause").isZero();
        assertThat(scheduler.list().getFirst().nextFireTime()).isEqualTo(Instant.parse("2026-03-29T00:10:00Z"));

        clock.advance(Duration.ofMinutes(10));
        assertThat(scheduler.tick()).isEqualTo(1);
    }

    @Test
    void triggerNowEnqueuesOutsideTheTimetableWithTheSchedulesSettings() {
        CronScheduler scheduler = instance();
        Schedule schedule = scheduler.create(ScheduleDefinition.of("report", "0 6 * * MON", "report.build")
                .withQueue("reports").withPayload(Map.of("kind", "weekly")).withPriority(4).withMaxAttempts(2)
                .withTimeout(Duration.ofSeconds(45)).withZone(ZoneId.of("Asia/Tbilisi")));

        long first = scheduler.triggerNow(schedule.id());
        long second = scheduler.triggerNow(schedule.id()); // manual runs are not de-duplicated

        assertThat(second).isNotEqualTo(first);
        assertThat(database.list("""
                SELECT type || '|' || queue || '|' || payload::text || '|' || priority || '|' || max_attempts || '|' ||
                       timeout_ms || '|' || state || '|' || (fire_time IS NULL)
                FROM pgs_job WHERE schedule_id = ?
                """, rs -> rs.getString(1), schedule.id()))
                .containsOnly("report.build|reports|{\"kind\": \"weekly\"}|4|2|45000|READY|true").hasSize(2);
        // The timetable is unaffected: next Monday 06:00 Tbilisi time is 02:00 UTC on 30 March.
        assertThat(schedule.nextFireTime()).isEqualTo(Instant.parse("2026-03-30T02:00:00Z"));
        assertThat(scheduler.list().getFirst().nextFireTime()).isEqualTo(schedule.nextFireTime());
    }

    @Test
    void invalidDefinitionsAreRejectedAndNamesAreUnique() {
        CronScheduler scheduler = instance();
        scheduler.create(ScheduleDefinition.of("unique", "@daily", "tick"));

        assertThatThrownBy(() -> scheduler.create(ScheduleDefinition.of("unique", "@hourly", "tick")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already exists");
        assertThatThrownBy(() -> scheduler.create(ScheduleDefinition.of("bad", "61 * * * *", "tick")))
                .isInstanceOf(CronParseException.class);
        assertThat(scheduler.createIfAbsent(ScheduleDefinition.of("unique", "@hourly", "tick"))).isFalse();
        assertThat(scheduler.list()).singleElement().satisfies(s -> assertThat(s.cron()).isEqualTo("@daily"));

        scheduler.delete(scheduler.list().getFirst().id());
        assertThat(scheduler.list()).isEmpty();
    }
}
