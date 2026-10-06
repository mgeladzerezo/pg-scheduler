package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.AttemptView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Retries with backoff, dead-lettering, manual retry, and timeouts, against a real worker. */
class RetryAndTimeoutTest {

    private static final Duration PATIENCE = Duration.ofSeconds(20);

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

    private SchedulerNode node(String queue, int concurrency) {
        return Nodes.full(database, database.newPool(4), Nodes.worker(concurrency).withQueue(queue, concurrency)).build();
    }

    private void awaitState(long jobId, JobState state) {
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(client.admin().job(jobId).state()).isEqualTo(state));
    }

    @Test
    void failedAttemptsAreRetriedOnTheBackoffScheduleUntilSuccess() {
        List<Long> attemptTimes = new CopyOnWriteArrayList<>();
        try (SchedulerNode node = node("retry", 2)) {
            node.policies().set("flaky", JobPolicy.DEFAULT.withMaxAttempts(4)
                    .withBackoff(new Backoff(Duration.ofMillis(300), 2.0, Duration.ofSeconds(5), 0.0)));
            node.handlers().register("flaky", String.class, (payload, context) -> {
                attemptTimes.add(System.nanoTime());
                if (context.attempt() < 4) {
                    throw new IllegalStateException("downstream unavailable (attempt " + context.attempt() + ")");
                }
            });
            node.start();

            long id = client.scheduler().enqueue(JobRequest.of("flaky", "x").queue("retry").maxAttempts(4)).jobId();
            awaitState(id, JobState.SUCCEEDED);

            // 300 ms, 600 ms, 1200 ms between attempts: never earlier, and not much later.
            assertThat(attemptTimes).hasSize(4);
            long[] expected = {300, 600, 1_200};
            for (int i = 0; i < expected.length; i++) {
                long gapMs = (attemptTimes.get(i + 1) - attemptTimes.get(i)) / 1_000_000;
                assertThat(gapMs).as("gap before attempt %d", i + 2).isBetween(expected[i] - 10, expected[i] + 700);
            }

            JobView job = client.admin().job(id);
            assertThat(job.attempt()).isEqualTo(4);
            assertThat(job.lastError()).isNull();
            List<AttemptView> attempts = client.admin().attempts(id);
            assertThat(attempts).extracting(AttemptView::outcome).containsExactly("FAILED", "FAILED", "FAILED", "SUCCEEDED");
            assertThat(attempts).extracting(AttemptView::attempt).containsExactly(1, 2, 3, 4);
            assertThat(attempts.get(1).error())
                    .contains("IllegalStateException", "downstream unavailable (attempt 2)", "\tat ");
        }
    }

    @Test
    void jobIsInFailedStateWithAFutureRunTimeWhileItBacksOff() {
        try (SchedulerNode node = node("backing-off", 1)) {
            node.policies().set("always.fails", JobPolicy.DEFAULT.withBackoff(Backoff.fixed(Duration.ofMinutes(10))));
            node.handlers().register("always.fails", String.class, (payload, context) -> {
                throw new IllegalStateException("nope");
            });
            node.start();

            long id = client.scheduler().enqueue(JobRequest.of("always.fails", "x").queue("backing-off")).jobId();
            awaitState(id, JobState.FAILED);

            JobView job = client.admin().job(id);
            assertThat(job.attempt()).isEqualTo(1);
            assertThat(job.lockedBy()).isNull();
            assertThat(job.finishedAt()).isNull();
            assertThat(Duration.between(job.updatedAt(), job.runAt())).isBetween(Duration.ofMinutes(9), Duration.ofMinutes(11));
            assertThat(job.lastError()).contains("nope");

            // An operator can skip the wait.
            client.admin().runNow(id);
            await().atMost(PATIENCE).untilAsserted(() -> assertThat(client.admin().job(id).attempt()).isEqualTo(2));
        }
    }

    @Test
    void exhaustedAttemptsDeadLetterTheJobAndManualRetryRevivesIt() {
        AtomicBoolean fixed = new AtomicBoolean();
        AtomicInteger calls = new AtomicInteger();
        try (SchedulerNode node = node("dead", 2)) {
            node.policies().set("broken", JobPolicy.DEFAULT.withMaxAttempts(2).withBackoff(Backoff.fixed(Duration.ofMillis(100))));
            node.handlers().register("broken", String.class, (payload, context) -> {
                calls.incrementAndGet();
                if (!fixed.get()) {
                    throw new IllegalStateException("bug in handler");
                }
            });
            node.start();
            SchedulerNode admin = node; // policies for the default retry grant live on this node

            long id = admin.scheduler().enqueue(JobRequest.of("broken", "x").queue("dead")).jobId();
            awaitState(id, JobState.DEAD);

            JobView dead = client.admin().job(id);
            assertThat(calls).hasValue(2);
            assertThat(dead.attempt()).isEqualTo(2);
            assertThat(dead.maxAttempts()).isEqualTo(2);
            assertThat(dead.finishedAt()).isNotNull();
            assertThat(dead.lastError()).contains("bug in handler");
            assertThat(client.admin().queues()).filteredOn(q -> q.name().equals("dead")).singleElement()
                    .satisfies(q -> assertThat(q.counts()).containsEntry(JobState.DEAD, 1L));

            // Dead letters stay dead until someone acts.
            assertThatThrownBy(() -> admin.admin().runNow(id)).isInstanceOf(IllegalStateException.class);

            fixed.set(true);
            admin.admin().retry(id, null);
            awaitState(id, JobState.SUCCEEDED);

            JobView revived = client.admin().job(id);
            assertThat(revived.attempt()).isEqualTo(3);
            assertThat(revived.maxAttempts()).isEqualTo(4); // 2 used + a fresh budget of 2
            assertThat(client.admin().attempts(id)).extracting(AttemptView::outcome)
                    .containsExactly("FAILED", "FAILED", "SUCCEEDED");
            assertThatThrownBy(() -> admin.admin().retry(id, null))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("SUCCEEDED");
        }
    }

    @Test
    void nonRetryableFailuresDeadLetterOnTheFirstAttempt() {
        AtomicInteger calls = new AtomicInteger();
        try (SchedulerNode node = node("non-retryable", 2)) {
            node.policies().set("validate", JobPolicy.DEFAULT.withMaxAttempts(5)
                    .withBackoff(Backoff.fixed(Duration.ofMillis(50)))
                    .withNoRetryFor(Set.of(IllegalArgumentException.class)));
            node.handlers().register("validate", String.class, (payload, context) -> {
                calls.incrementAndGet();
                switch (payload) {
                    case "explicit" -> throw new NonRetryableJobException("payload can never be processed");
                    case "by-type" -> Integer.parseInt("not a number"); // NumberFormatException is an IllegalArgumentException
                    default -> throw new IllegalStateException("unexpected");
                }
            });
            node.start();

            long explicit = node.scheduler().enqueue(JobRequest.of("validate", "explicit").queue("non-retryable")).jobId();
            long byType = node.scheduler().enqueue(JobRequest.of("validate", "by-type").queue("non-retryable")).jobId();
            awaitState(explicit, JobState.DEAD);
            awaitState(byType, JobState.DEAD);

            assertThat(calls).hasValue(2);
            assertThat(client.admin().job(explicit).attempt()).isEqualTo(1);
            assertThat(client.admin().job(byType).attempt()).isEqualTo(1);
            assertThat(client.admin().job(byType).lastError()).contains("NumberFormatException");
        }
    }

    @Test
    void timeoutInterruptsTheHandlerAndTheAttemptIsRetried() {
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicInteger calls = new AtomicInteger();
        try (SchedulerNode node = node("timeout", 2)) {
            node.policies().set("slow", JobPolicy.DEFAULT.withBackoff(Backoff.fixed(Duration.ofMillis(100))));
            node.handlers().register("slow", String.class, (payload, context) -> {
                if (calls.incrementAndGet() == 1) {
                    try {
                        Thread.sleep(30_000);
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                        throw e;
                    }
                }
            });
            node.start();

            long started = System.nanoTime();
            long id = client.scheduler().enqueue(JobRequest.of("slow", "x").queue("timeout")
                    .timeout(Duration.ofMillis(400)).maxAttempts(3)).jobId();
            awaitState(id, JobState.SUCCEEDED);

            assertThat(interrupted).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
            List<AttemptView> attempts = client.admin().attempts(id);
            assertThat(attempts).extracting(AttemptView::outcome).containsExactly("TIMED_OUT", "SUCCEEDED");
            assertThat(attempts.getFirst().error()).contains("JobTimeoutException", "exceeded its timeout of PT0.4S");
            assertThat(Duration.between(attempts.getFirst().startedAt(), attempts.getFirst().finishedAt()))
                    .isBetween(Duration.ofMillis(390), Duration.ofMillis(1_500));
        }
    }

    @Test
    void handlerThatIgnoresTheInterruptCannotHoldUpTheRetryOrOverwriteItsResult() {
        AtomicInteger calls = new AtomicInteger();
        AtomicBoolean stubbornFinished = new AtomicBoolean();
        try (SchedulerNode node = node("stubborn", 2)) {
            node.policies().set("stubborn", JobPolicy.DEFAULT.withBackoff(Backoff.fixed(Duration.ofMillis(100))));
            node.handlers().register("stubborn", String.class, (payload, context) -> {
                if (calls.incrementAndGet() == 1) {
                    long until = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                    while (System.nanoTime() < until) {
                        Thread.onSpinWait(); // deaf to interrupts, like a handler stuck in non-interruptible code
                    }
                    stubbornFinished.set(true);
                    context.setResult("late result of attempt 1");
                } else {
                    context.setResult("result of attempt " + context.attempt());
                }
            });
            node.start();

            long id = client.scheduler().enqueue(JobRequest.of("stubborn", "x").queue("stubborn")
                    .timeout(Duration.ofMillis(400)).maxAttempts(3)).jobId();

            // The second attempt completes while the first handler is still spinning.
            awaitState(id, JobState.SUCCEEDED);
            assertThat(stubbornFinished).isFalse();
            assertThat(client.admin().job(id).resultJson()).isEqualTo("\"result of attempt 2\"");

            // When the first handler finally returns, its result changes nothing.
            await().atMost(PATIENCE).untilTrue(stubbornFinished);
            await().atMost(PATIENCE).until(() -> node.worker().inFlightCount() == 0);
            JobView job = client.admin().job(id);
            assertThat(job.state()).isEqualTo(JobState.SUCCEEDED);
            assertThat(job.resultJson()).isEqualTo("\"result of attempt 2\"");
            assertThat(client.admin().attempts(id)).extracting(AttemptView::outcome).containsExactly("TIMED_OUT", "SUCCEEDED");
        }
    }
}
