package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.AttemptView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.RecordingEvents;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Graceful shutdown: stop claiming, let handlers finish within the grace period, release the rest. */
class GracefulShutdownTest {

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
    void stopWaitsForRunningHandlersAndClaimsNothingNew() throws Exception {
        CountDownLatch started = new CountDownLatch(2);
        AtomicInteger finished = new AtomicInteger();
        SchedulerNode node = Nodes.full(database, database.newPool(4),
                Nodes.worker(2).withQueue("drain", 2).withShutdownTimeout(Duration.ofSeconds(20))).build();
        node.handlers().register("drain.job", String.class, (payload, context) -> {
            started.countDown();
            Thread.sleep(1_200);
            finished.incrementAndGet();
        });
        node.start();
        long first = client.scheduler().enqueue(JobRequest.of("drain.job", "a").queue("drain")).jobId();
        long second = client.scheduler().enqueue(JobRequest.of("drain.job", "b").queue("drain")).jobId();
        assertThat(started.await(15, TimeUnit.SECONDS)).isTrue();

        // Enqueued while both slots are busy; the stop below begins before a slot frees up.
        long third = client.scheduler().enqueue(JobRequest.of("drain.job", "c").queue("drain")).jobId();
        node.close();

        // close() returned only after both handlers ran to completion ...
        assertThat(finished).hasValue(2);
        assertThat(client.admin().job(first).state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(client.admin().job(second).state()).isEqualTo(JobState.SUCCEEDED);
        // ... and the freed slots were not used to claim the third job.
        JobView untouched = client.admin().job(third);
        assertThat(untouched.state()).isEqualTo(JobState.READY);
        assertThat(untouched.attempt()).isZero();
        assertThat(database.count("SELECT count(*) FROM pgs_worker")).as("worker deregistered").isZero();
    }

    @Test
    void handlersStillRunningAfterTheGracePeriodAreInterruptedAndTheirJobsReleased() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        RecordingEvents events = new RecordingEvents();
        SchedulerNode node = Nodes.full(database, database.newPool(4),
                Nodes.worker(1).withQueue("release", 1).withShutdownTimeout(Duration.ofMillis(300))).events(events).build();
        node.handlers().register("release.job", String.class, (payload, context) -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
        });
        node.start();
        long jobId = client.scheduler().enqueue(JobRequest.of("release.job", "x").queue("release").maxAttempts(1)).jobId();
        assertThat(started.await(15, TimeUnit.SECONDS)).isTrue();

        long stopStarted = System.nanoTime();
        node.close();
        Duration stopTook = Duration.ofNanos(System.nanoTime() - stopStarted);

        assertThat(stopTook).isBetween(Duration.ofMillis(290), Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5)).untilTrue(interrupted);
        assertThat(events.released).hasValue(1);

        // Back in the queue immediately (no waiting for a 30 s lease to expire), and the interrupted
        // attempt is not counted: with maxAttempts = 1 a counted attempt would have dead-lettered the job.
        JobView released = client.admin().job(jobId);
        assertThat(released.state()).isEqualTo(JobState.READY);
        assertThat(released.attempt()).isZero();
        assertThat(released.lockedBy()).isNull();
        assertThat(client.admin().attempts(jobId)).extracting(AttemptView::outcome).containsExactly("RELEASED");
        assertThat(events.failed).as("the handler's InterruptedException is not recorded as a failure").hasValue(0);

        // Another worker runs it as attempt 1.
        try (SchedulerNode next = Nodes.full(database, database.newPool(4), Nodes.worker(1).withQueue("release", 1)).build()) {
            next.handlers().register("release.job", String.class, (payload, context) -> context.setResult(context.attempt()));
            next.start();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(client.admin().job(jobId).state()).isEqualTo(JobState.SUCCEEDED));
            assertThat(client.admin().job(jobId).resultJson()).isEqualTo("1");
            assertThat(client.admin().attempts(jobId)).extracting(AttemptView::outcome).containsExactly("RELEASED", "SUCCEEDED");
        }
    }
}
