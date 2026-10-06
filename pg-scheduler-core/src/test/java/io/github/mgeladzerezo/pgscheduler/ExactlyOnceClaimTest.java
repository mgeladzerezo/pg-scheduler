package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.RecordingEvents;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The headline claim of the queue: under heavy contention every job is claimed by exactly one worker and
 * none is left behind.
 *
 * <p>Eight workers, each with its own connection pool (as eight processes would have), race over 20,000
 * short jobs. Each handler inserts the job id into a table with a primary key and bumps counters, so a job
 * claimed twice would show up three ways: a unique violation, a counter above one, a second attempt row.
 *
 * <p>No failures are injected here, so "claimed once" and "executed once" coincide. With crashes the
 * guarantee is at-least-once; see {@code CrashRecoveryTest}.
 */
class ExactlyOnceClaimTest {

    private static final int JOBS = Integer.getInteger("pgs.load.jobs", 20_000);
    private static final int WORKERS = 8;
    private static final int CONCURRENCY = 10;

    @Test
    void everyJobIsExecutedExactlyOnceByEightRacingWorkers() {
        try (TestDatabase database = TestDatabase.create()) {
            database.execute("CREATE TABLE executed (job_id bigint PRIMARY KEY, worker_id text NOT NULL)");
            SchedulerNode client = Nodes.client(database.dataSource());

            AtomicLong executions = new AtomicLong();
            AtomicInteger duplicateInserts = new AtomicInteger();
            Map<Long, AtomicInteger> perJob = new ConcurrentHashMap<>();
            List<SchedulerNode> nodes = new ArrayList<>();
            List<RecordingEvents> events = new ArrayList<>();
            for (int i = 0; i < WORKERS; i++) {
                Db db = new Db(database.newPool(CONCURRENCY + 2));
                RecordingEvents nodeEvents = new RecordingEvents();
                SchedulerNode node = Nodes.full(database, db.dataSource(),
                                Nodes.worker(CONCURRENCY).withQueue("load", CONCURRENCY).withBatchSize(CONCURRENCY))
                        .workerId("worker-" + i).events(nodeEvents).build();
                node.handlers().register("load.job", Integer.class, (payload, context) -> {
                    executions.incrementAndGet();
                    perJob.computeIfAbsent(context.jobId(), id -> new AtomicInteger()).incrementAndGet();
                    try {
                        db.autoCommit(c -> Db.update(c, "INSERT INTO executed (job_id, worker_id) VALUES (?, ?)",
                                context.jobId(), context.workerId()));
                    } catch (SchedulerException e) {
                        if (e.getCause() instanceof SQLException sql && Db.UNIQUE_VIOLATION.equals(sql.getSQLState())) {
                            duplicateInserts.incrementAndGet();
                        }
                        throw e;
                    }
                });
                nodes.add(node);
                events.add(nodeEvents);
            }

            List<JobRequest> requests = new ArrayList<>(JOBS);
            for (int i = 0; i < JOBS; i++) {
                requests.add(JobRequest.of("load.job", i).queue("load").priority(i % 5));
            }
            for (int from = 0; from < JOBS; from += 5_000) {
                client.scheduler().enqueueAll(requests.subList(from, Math.min(JOBS, from + 5_000)));
            }
            assertThat(database.count("SELECT count(*) FROM pgs_job WHERE state = 'READY'")).isEqualTo(JOBS);

            long startedAt = System.nanoTime();
            try {
                nodes.parallelStream().forEach(SchedulerNode::start);
                await().atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofMillis(50))
                        .until(() -> executions.get() >= JOBS
                                && database.count("SELECT count(*) FROM pgs_job WHERE state IN ('READY', 'RUNNING')") == 0);
            } finally {
                double seconds = (System.nanoTime() - startedAt) / 1e9;
                System.out.printf("EXACTLY_ONCE_LOAD jobs=%d workers=%d concurrency_per_worker=%d seconds=%.2f jobs_per_second=%.0f%n",
                        JOBS, WORKERS, CONCURRENCY, seconds, JOBS / seconds);
                nodes.forEach(SchedulerNode::close);
            }

            // Exactly once, seen from the handlers ...
            assertThat(executions).hasValue(JOBS);
            assertThat(duplicateInserts).hasValue(0);
            assertThat(perJob).hasSize(JOBS);
            assertThat(perJob.values()).allSatisfy(count -> assertThat(count).hasValue(1));
            // ... and from the database.
            assertThat(database.count("SELECT count(*) FROM executed")).isEqualTo(JOBS);
            assertThat(database.count("SELECT count(*) FROM pgs_job WHERE state = 'SUCCEEDED' AND attempt = 1")).isEqualTo(JOBS);
            assertThat(database.count("SELECT count(*) FROM pgs_job WHERE state <> 'SUCCEEDED'")).as("none left behind").isZero();
            assertThat(database.count("SELECT count(*) FROM pgs_job_attempt WHERE outcome = 'SUCCEEDED'")).isEqualTo(JOBS);
            assertThat(database.count("SELECT count(*) FROM pgs_job_attempt")).isEqualTo(JOBS);
            // The worker that ran a job is the one that claimed it and the one that completed it.
            assertThat(database.count("""
                    SELECT count(*) FROM executed e JOIN pgs_job_attempt a ON a.job_id = e.job_id
                    WHERE a.worker_id <> e.worker_id
                    """)).isZero();
            // It really was a race: every worker got a share.
            assertThat(database.count("SELECT count(DISTINCT worker_id) FROM executed")).isEqualTo(WORKERS);
            assertThat(events).allSatisfy(e -> assertThat(e.fenced).hasValue(0));
            assertThat(events.stream().mapToInt(e -> e.succeeded.get()).sum()).isEqualTo(JOBS);
            // The roll-up, flushed once a second per worker, adds up to the same total.
            assertThat(database.count("SELECT coalesce(sum(count), 0) FROM pgs_stat WHERE outcome = 'SUCCEEDED'")).isEqualTo(JOBS);
        }
    }
}
