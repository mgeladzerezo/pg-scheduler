package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Per-worker and cluster-wide concurrency limits per queue, and the cluster-wide rate limit per job type. */
class ConcurrencyAndRateLimitTest {

    private static final Duration PATIENCE = Duration.ofSeconds(60);

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

    /** Tracks how many handlers run at the same moment across every node in this JVM. */
    private static final class Gauge {
        final AtomicInteger current = new AtomicInteger();
        final AtomicInteger max = new AtomicInteger();
        final AtomicInteger done = new AtomicInteger();

        void run(long millis) throws InterruptedException {
            int now = current.incrementAndGet();
            max.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(millis);
            } finally {
                current.decrementAndGet();
                done.incrementAndGet();
            }
        }
    }

    @Test
    void workerNeverRunsMoreJobsOfAQueueThanItsConcurrency() {
        Gauge gauge = new Gauge();
        try (SchedulerNode node = Nodes.full(database, database.newPool(6), Nodes.worker(3).withQueue("local", 3).withBatchSize(10)).build()) {
            node.handlers().register("local.job", Integer.class, (payload, context) -> gauge.run(40));
            node.start();

            List<JobRequest> requests = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                requests.add(JobRequest.of("local.job", i).queue("local"));
            }
            client.scheduler().enqueueAll(requests);

            await().atMost(PATIENCE).until(() -> gauge.done.get() == 40);
            assertThat(gauge.max).hasValue(3);
        }
    }

    @Test
    void clusterWideLimitHoldsAcrossWorkers() {
        Gauge gauge = new Gauge();
        List<SchedulerNode> nodes = new ArrayList<>();
        try {
            client.admin().setQueueConcurrencyLimit("global", 3);
            // Four workers that could run 5 each: 20 slots, but the queue allows 3 in the whole cluster.
            for (int i = 0; i < 4; i++) {
                SchedulerNode node = Nodes.full(database, database.newPool(7), Nodes.worker(5).withQueue("global", 5)).build();
                node.handlers().register("global.job", Integer.class, (payload, context) -> gauge.run(30));
                nodes.add(node);
            }
            List<JobRequest> requests = new ArrayList<>();
            for (int i = 0; i < 120; i++) {
                requests.add(JobRequest.of("global.job", i).queue("global"));
            }
            client.scheduler().enqueueAll(requests);
            nodes.forEach(SchedulerNode::start);

            await().atMost(PATIENCE).until(() -> gauge.done.get() == 120);
            assertThat(gauge.max).hasValue(3);
            assertThat(database.count("SELECT count(*) FROM pgs_job WHERE queue = 'global' AND state = 'SUCCEEDED'")).isEqualTo(120);
            assertThat(database.count("SELECT count(DISTINCT worker_id) FROM pgs_job_attempt a JOIN pgs_job j ON j.id = a.job_id "
                    + "WHERE j.queue = 'global'")).as("the limit is shared, not owned by one worker").isGreaterThan(1);

            // Lifting the limit lets the same workers use their own capacity again.
            client.admin().setQueueConcurrencyLimit("global", null);
            gauge.max.set(0);
            gauge.done.set(0);
            await().pollDelay(Duration.ofMillis(1_500)).until(() -> true); // workers refresh queue settings every second
            client.scheduler().enqueueAll(requests);
            await().atMost(PATIENCE).until(() -> gauge.done.get() == 120);
            assertThat(gauge.max.get()).isGreaterThan(3);
        } finally {
            nodes.forEach(SchedulerNode::close);
        }
    }

    @Test
    void rateLimitCapsStartsPerSecondAcrossWorkers() {
        double perSecond = 20;
        int burst = 5;
        int jobs = 45;
        List<Long> starts = new CopyOnWriteArrayList<>();
        List<Long> freeStarts = new CopyOnWriteArrayList<>();
        List<SchedulerNode> nodes = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                SchedulerNode node = Nodes.full(database, database.newPool(6), Nodes.worker(20).withQueue("throttled", 20)).build();
                node.policies().set("throttled.job", JobPolicy.DEFAULT.withRateLimit(new RateLimit(perSecond, burst)));
                node.handlers().register("throttled.job", Integer.class, (payload, context) -> starts.add(System.nanoTime()));
                // An unlimited type on the same queue must not be slowed down by its throttled neighbour.
                node.handlers().register("free.job", Integer.class, (payload, context) -> freeStarts.add(System.nanoTime()));
                nodes.add(node);
            }
            List<JobRequest> requests = new ArrayList<>();
            for (int i = 0; i < jobs; i++) {
                requests.add(JobRequest.of("throttled.job", i).queue("throttled"));
                requests.add(JobRequest.of("free.job", i).queue("throttled"));
            }
            client.scheduler().enqueueAll(requests);
            nodes.forEach(SchedulerNode::start);

            await().atMost(PATIENCE).until(() -> starts.size() == jobs && freeStarts.size() == jobs);

            List<Long> sorted = starts.stream().sorted().toList();
            double totalSeconds = (sorted.getLast() - sorted.getFirst()) / 1e9;
            // The first `burst` may start at once; the remaining 40 need 40 / 20 = 2 seconds.
            double expectedSeconds = (jobs - burst) / perSecond;
            System.out.printf("RATE_LIMIT %d jobs at %.0f/s burst %d took %.2fs (theoretical minimum %.2fs)%n",
                    jobs, perSecond, burst, totalSeconds, expectedSeconds);
            assertThat(totalSeconds).isBetween(expectedSeconds * 0.9, expectedSeconds * 3);

            // The unlimited jobs were all started while the throttled ones were still trickling out.
            assertThat(freeStarts.stream().mapToLong(Long::longValue).max().orElseThrow()).isLessThan(sorted.getLast());

            // No one-second window anywhere contains more starts than the bucket allows.
            int maxInWindow = 0;
            for (int i = 0; i < sorted.size(); i++) {
                int j = i;
                while (j < sorted.size() && sorted.get(j) - sorted.get(i) < 1_000_000_000L) {
                    j++;
                }
                maxInWindow = Math.max(maxInWindow, j - i);
            }
            // burst + one second of refill, plus slack for the gap between the claim and the handler's timestamp
            assertThat(maxInWindow).isLessThanOrEqualTo(burst + (int) perSecond + 3);
        } finally {
            nodes.forEach(SchedulerNode::close);
        }
    }
}
