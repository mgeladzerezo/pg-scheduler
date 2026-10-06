package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.AttemptView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.store.ClaimedJob;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.store.MaintenanceDao;
import io.github.mgeladzerezo.pgscheduler.store.WorkerDao;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.PartitionableDataSource;
import io.github.mgeladzerezo.pgscheduler.testsupport.RecordingEvents;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A worker that lost its lease must not be able to record a result. The dangerous worker is not the dead
 * one but the one that comes back: partitioned or paused long enough to be presumed dead, then alive again
 * with a finished handler and a result to report.
 */
class FencingTest {

    private static final Duration PATIENCE = Duration.ofSeconds(20);
    private static final Duration LEASE = Duration.ofMillis(1_500);

    private static TestDatabase database;
    private static SchedulerNode client;

    @BeforeAll
    static void setUp() {
        database = TestDatabase.create();
        client = Nodes.client(database.dataSource());
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @Test
    void lateCompletionOfAPartitionedWorkerIsRejectedWhileAnotherWorkerRunsTheJob() throws Exception {
        CountDownLatch zombieStarted = new CountDownLatch(1);
        CountDownLatch zombieMayFinish = new CountDownLatch(1);
        CountDownLatch survivorStarted = new CountDownLatch(1);
        CountDownLatch survivorMayFinish = new CountDownLatch(1);
        RecordingEvents zombieEvents = new RecordingEvents();
        RecordingEvents survivorEvents = new RecordingEvents();

        PartitionableDataSource zombieNetwork = new PartitionableDataSource(database.newPool(4));
        try (SchedulerNode zombie = SchedulerNode.builder(zombieNetwork).workerId("zombie").events(zombieEvents)
                .worker(Nodes.worker(1).withQueue("fence", 1).withLease(LEASE)).build();
             SchedulerNode survivor = Nodes.full(database, database.newPool(4), Nodes.worker(1).withQueue("fence", 1).withLease(LEASE))
                     .workerId("survivor").events(survivorEvents).build()) {

            zombie.handlers().register("fence.job", String.class, (payload, context) -> {
                zombieStarted.countDown();
                // Deaf to interrupts: a handler stuck in a long computation or a stop-the-world pause.
                awaitUninterruptibly(zombieMayFinish);
                context.setResult("written by the zombie");
            });
            survivor.handlers().register("fence.job", String.class, (payload, context) -> {
                survivorStarted.countDown();
                awaitUninterruptibly(survivorMayFinish);
                context.setResult("written by the survivor");
            });

            zombie.start();
            long jobId = client.scheduler().enqueue(JobRequest.of("fence.job", "x").queue("fence").maxAttempts(3)).jobId();
            assertThat(zombieStarted.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(client.admin().job(jobId).lockedBy()).isEqualTo("zombie");

            // The zombie drops off the network: no heartbeat gets through any more.
            zombieNetwork.partition();
            survivor.start();

            // The lease runs out, the survivor's reaper takes the job back and the survivor starts attempt 2.
            assertThat(survivorStarted.await(15, TimeUnit.SECONDS)).isTrue();
            JobView takenOver = client.admin().job(jobId);
            assertThat(takenOver.state()).isEqualTo(JobState.RUNNING);
            assertThat(takenOver.lockedBy()).isEqualTo("survivor");
            assertThat(takenOver.attempt()).isEqualTo(2);

            // The partition heals and the zombie's handler finishes: it now tries to report success for
            // a job that is RUNNING on someone else.
            zombieNetwork.heal();
            zombieMayFinish.countDown();
            await().atMost(PATIENCE).until(() -> zombie.worker().inFlightCount() == 0);

            assertThat(zombieEvents.fenced).hasValue(1);
            assertThat(zombieEvents.succeeded).hasValue(0);
            JobView untouched = client.admin().job(jobId);
            assertThat(untouched.state()).isEqualTo(JobState.RUNNING);
            assertThat(untouched.lockedBy()).isEqualTo("survivor");
            assertThat(untouched.attempt()).isEqualTo(2);
            assertThat(untouched.resultJson()).isNull();

            // The rightful owner finishes normally.
            survivorMayFinish.countDown();
            await().atMost(PATIENCE).untilAsserted(() ->
                    assertThat(client.admin().job(jobId).state()).isEqualTo(JobState.SUCCEEDED));
            assertThat(client.admin().job(jobId).resultJson()).isEqualTo("\"written by the survivor\"");
            List<AttemptView> attempts = client.admin().attempts(jobId);
            assertThat(attempts).extracting(AttemptView::outcome).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
            assertThat(attempts).extracting(AttemptView::workerId).containsExactly("zombie", "survivor");
            assertThat(survivorEvents.fenced).hasValue(0);
        }
    }

    @Test
    void workerThatCannotHeartbeatForAWholeLeaseInterruptsItsOwnHandlers() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch handlerExited = new CountDownLatch(1);

        PartitionableDataSource network = new PartitionableDataSource(database.newPool(4));
        try (SchedulerNode isolated = SchedulerNode.builder(network).workerId("isolated")
                .worker(Nodes.worker(1).withQueue("self-fence", 1).withLease(LEASE)).build()) {
            isolated.handlers().register("self.fence", String.class, (payload, context) -> {
                started.countDown();
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    throw e;
                } finally {
                    handlerExited.countDown();
                }
            });
            isolated.start();
            client.scheduler().enqueue(JobRequest.of("self.fence", "x").queue("self-fence"));
            assertThat(started.await(15, TimeUnit.SECONDS)).isTrue();

            long partitionedAt = System.nanoTime();
            network.partition();

            // Nobody tells the worker anything (it is partitioned). It works out by itself that it can
            // no longer prove ownership and stops the handler.
            assertThat(handlerExited.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(interrupted).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - partitionedAt)).isGreaterThanOrEqualTo(LEASE.minusMillis(600));
            network.heal();
        }
    }

    /**
     * The fence at statement level. The worker id alone would not be enough: here the same worker claims
     * the job a second time after its own lease expired, and only the attempt number tells its stale
     * first attempt from its current second one.
     */
    @Test
    void staleAttemptOfTheSameWorkerCannotCompleteFailOrExtend() {
        Db db = new Db(database.dataSource());
        WorkerDao workerDao = new WorkerDao(db);
        MaintenanceDao maintenanceDao = new MaintenanceDao(db);
        String[] types = {"stale.job"};
        long jobId = client.scheduler().enqueue(JobRequest.of("stale.job", "x").queue("stale").maxAttempts(5)).jobId();

        ClaimedJob first = workerDao.claim("stale", types, 1, "w1", 60_000, false).getFirst();
        assertThat(first.attempt()).isEqualTo(1);

        // The lease expires (a long GC pause, say) and the reaper returns the job to the queue.
        database.execute("UPDATE pgs_job SET locked_until = now() - interval '1 second' WHERE id = ?", jobId);
        assertThat(maintenanceDao.reapExpiredLeases(10)).singleElement()
                .satisfies(reaped -> assertThat(reaped.workerId()).isEqualTo("w1"));

        // Nothing the first attempt does counts any more, even before anyone re-claims the job ...
        assertThat(workerDao.complete(first, "w1", "\"stale\"", null)).isFalse();
        assertThat(client.admin().job(jobId).state()).isEqualTo(JobState.READY);

        // ... and not after the same worker has claimed it again either.
        ClaimedJob second = workerDao.claim("stale", types, 1, "w1", 60_000, false).getFirst();
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(workerDao.complete(first, "w1", "\"stale\"", null)).isFalse();
        assertThat(workerDao.fail(first, "w1", "FAILED", true, 0, "stale failure")).isFalse();
        workerDao.release(first, "w1");
        assertThat(workerDao.heartbeat("w1", List.of(first), 60_000)).isEmpty();
        assertThat(workerDao.heartbeat("w1", List.of(first, second), 60_000)).containsExactly(jobId);

        JobView job = client.admin().job(jobId);
        assertThat(job.state()).isEqualTo(JobState.RUNNING);
        assertThat(job.attempt()).isEqualTo(2);
        assertThat(job.resultJson()).isNull();

        // A different worker presenting the right attempt number is fenced as well.
        assertThat(workerDao.complete(second, "w2", "\"impostor\"", null)).isFalse();

        assertThat(workerDao.complete(second, "w1", "\"current\"", null)).isTrue();
        assertThat(client.admin().job(jobId).resultJson()).isEqualTo("\"current\"");
        assertThat(client.admin().attempts(jobId)).extracting(AttemptView::outcome).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
        // And a second completion of the same attempt is a no-op: the job is no longer RUNNING.
        assertThat(workerDao.complete(second, "w1", "\"again\"", null)).isFalse();
    }

    @Test
    void cancellingARunningJobInterruptsItsHandlerAndFencesItsResult() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        RecordingEvents events = new RecordingEvents();
        try (SchedulerNode node = Nodes.full(database, database.newPool(4), Nodes.worker(1).withQueue("cancel", 1).withLease(LEASE))
                .events(events).build()) {
            node.handlers().register("cancel.job", String.class, (payload, context) -> {
                started.countDown();
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    throw e;
                }
            });
            node.start();
            long jobId = client.scheduler().enqueue(JobRequest.of("cancel.job", "x").queue("cancel")).jobId();
            assertThat(started.await(15, TimeUnit.SECONDS)).isTrue();

            client.admin().cancel(jobId);

            // The next heartbeat finds the lease gone and interrupts the handler; its failure is fenced.
            await().atMost(PATIENCE).untilTrue(interrupted);
            await().atMost(PATIENCE).until(() -> node.worker().inFlightCount() == 0);
            assertThat(events.fenced).hasValue(1);
            JobView job = client.admin().job(jobId);
            assertThat(job.state()).isEqualTo(JobState.CANCELLED);
            assertThat(job.lockedBy()).isNull();
            assertThat(client.admin().attempts(jobId)).extracting(AttemptView::outcome).containsExactly("CANCELLED");
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                if (latch.await(30, TimeUnit.SECONDS)) {
                    break;
                }
                throw new IllegalStateException("test latch was never released");
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
