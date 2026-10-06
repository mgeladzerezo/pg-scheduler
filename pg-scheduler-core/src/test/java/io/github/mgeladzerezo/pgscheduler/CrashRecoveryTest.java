package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.AttemptView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.WorkerView;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.testsupport.CrashVictimWorker;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.RecordingEvents;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Crash recovery with a real crash: a worker in a separate operating-system process is killed while its
 * handler is running, and another worker picks the job up once the lease has run out.
 */
class CrashRecoveryTest {

    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final String VICTIM = "victim-process";

    private TestDatabase database;
    private Process victim;

    @BeforeEach
    void setUp() {
        database = TestDatabase.create();
        database.execute("""
                CREATE TABLE crash_log (
                    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    job_id bigint NOT NULL, attempt integer NOT NULL, worker_id text NOT NULL, event text NOT NULL,
                    at timestamptz NOT NULL DEFAULT clock_timestamp())
                """);
    }

    @AfterEach
    void tearDown() {
        if (victim != null) {
            victim.destroyForcibly();
        }
        database.close();
    }

    @Test
    void jobOfAKilledWorkerProcessIsRerunByAnotherWorkerAfterTheLeaseExpires() throws Exception {
        victim = startVictimProcess();
        await().atMost(Duration.ofSeconds(60)).until(() -> database.count("SELECT count(*) FROM pgs_worker WHERE id = ?", VICTIM) == 1);

        SchedulerNode client = Nodes.client(database.dataSource());
        long jobId = client.scheduler().enqueue(JobRequest.of("crash.job", "important").queue("crash").maxAttempts(3)).jobId();

        // The victim claims the job and its handler starts.
        await().atMost(Duration.ofSeconds(30)).until(() -> logEvents(jobId).equals(List.of("1:" + VICTIM + ":STARTED")));

        RecordingEvents survivorEvents = new RecordingEvents();
        Db survivorDb = new Db(database.newPool(4));
        try (SchedulerNode survivor = Nodes.full(database, survivorDb.dataSource(),
                Nodes.worker(2).withQueue("crash", 2).withLease(LEASE)).workerId("survivor").events(survivorEvents).build()) {
            survivor.handlers().register("crash.job", String.class, (payload, context) -> {
                log(survivorDb, context, "STARTED");
                log(survivorDb, context, "FINISHED");
            });
            survivor.start();

            // While the victim lives, its heartbeat keeps the lease alive well past one lease duration,
            // and the survivor, although idle and polling, cannot touch the job.
            TimeUnit.MILLISECONDS.sleep(LEASE.toMillis() + 1_500);
            JobView held = client.admin().job(jobId);
            assertThat(held.state()).isEqualTo(JobState.RUNNING);
            assertThat(held.lockedBy()).isEqualTo(VICTIM);
            assertThat(held.attempt()).isEqualTo(1);
            assertThat(survivorEvents.leasesExpired).hasValue(0);

            // Kill -9. No shutdown hook, no release, no final heartbeat.
            long killedAt = System.nanoTime();
            victim.destroyForcibly();
            assertThat(victim.waitFor(30, TimeUnit.SECONDS)).isTrue();

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(client.admin().job(jobId).state()).isEqualTo(JobState.SUCCEEDED));
            Duration recovery = Duration.ofNanos(System.nanoTime() - killedAt);

            // Not before the lease could have expired (last heartbeat at most a third of a lease before the
            // kill), and promptly after.
            assertThat(recovery).isBetween(LEASE.multipliedBy(2).dividedBy(3).minusMillis(200), LEASE.plusSeconds(5));

            assertThat(logEvents(jobId)).containsExactly(
                    "1:" + VICTIM + ":STARTED",
                    "2:survivor:STARTED",
                    "2:survivor:FINISHED");
            JobView done = client.admin().job(jobId);
            assertThat(done.attempt()).isEqualTo(2);
            List<AttemptView> attempts = client.admin().attempts(jobId);
            assertThat(attempts).extracting(AttemptView::outcome).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
            assertThat(attempts).extracting(AttemptView::workerId).containsExactly(VICTIM, "survivor");
            assertThat(survivorEvents.leasesExpired).hasValue(1);

            // The dead worker is still listed, but as not alive.
            assertThat(client.admin().workers()).filteredOn(w -> w.id().equals(VICTIM)).singleElement()
                    .extracting(WorkerView::alive).isEqualTo(false);
            System.out.printf("CRASH_RECOVERY lease=%dms recovered_after_kill=%dms%n", LEASE.toMillis(), recovery.toMillis());
        }
    }

    private List<String> logEvents(long jobId) {
        return database.list("SELECT attempt || ':' || worker_id || ':' || event FROM crash_log WHERE job_id = ? ORDER BY id",
                rs -> rs.getString(1), jobId);
    }

    private static void log(Db db, JobContext context, String event) {
        db.autoCommit(c -> Db.update(c, "INSERT INTO crash_log (job_id, attempt, worker_id, event) VALUES (?, ?, ?, ?)",
                context.jobId(), context.attempt(), context.workerId(), event));
    }

    /** Starts {@link CrashVictimWorker} in a new JVM with this test's class path. */
    private Process startVictimProcess() throws Exception {
        // Surefire may run tests from a manifest-only jar; it then publishes the real class path separately.
        String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Path target = Path.of("target").toAbsolutePath();
        Files.createDirectories(target);
        // An argument file keeps the command line short enough for Windows. Backslashes are escape
        // characters inside a quoted argument-file value, so they are doubled.
        Path argFile = target.resolve("crash-victim.args");
        Files.writeString(argFile, "-cp\n\"" + classPath.replace("\\", "\\\\") + "\"\n");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder builder = new ProcessBuilder(java, "-Xmx128m", "@" + argFile, CrashVictimWorker.class.getName(),
                database.jdbcUrl(), database.username(), database.password(), VICTIM, String.valueOf(LEASE.toMillis()));
        builder.redirectErrorStream(true);
        builder.redirectOutput(new File(target.toFile(), "crash-victim.log"));
        return builder.start();
    }
}
