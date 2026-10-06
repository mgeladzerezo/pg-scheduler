package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.AttemptView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import io.github.mgeladzerezo.pgscheduler.worker.HandlerRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Handlers, typed payloads, ordering, delays, pausing, parking and chaining on a single worker. */
class WorkerExecutionTest {

    private static final Duration PATIENCE = Duration.ofSeconds(15);

    record Greeting(String name, int times) {
    }

    record OrderLine(String sku, int quantity) {
    }

    /** Interface-style handler: the payload type is read from {@code JobHandler<Greeting>}. */
    static final class Greeter implements JobHandler<Greeting> {
        final AtomicReference<Greeting> seen = new AtomicReference<>();
        final AtomicReference<JobContext> context = new AtomicReference<>();

        @Override
        public String jobType() {
            return "greet";
        }

        @Override
        public void handle(Greeting payload, JobContext ctx) {
            seen.set(payload);
            context.set(ctx);
            ctx.setResult(Map.of("greeted", payload.name()));
        }
    }

    /** A generic payload type must survive: the elements are {@code OrderLine}s, not maps. */
    static final class OrderPacker implements JobHandler<List<OrderLine>> {
        final AtomicReference<Object> firstElement = new AtomicReference<>();

        @Override
        public String jobType() {
            return "order.pack";
        }

        @Override
        public void handle(List<OrderLine> lines, JobContext ctx) {
            firstElement.set(lines.getFirst());
            ctx.setResult(lines.stream().mapToInt(OrderLine::quantity).sum());
        }
    }

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
    void interfaceHandlerReceivesTypedPayloadAndContextAndStoresResult() {
        Greeter greeter = new Greeter();
        try (SchedulerNode node = node("typed", 2)) {
            node.handlers().register(greeter);
            node.start();

            long id = client.scheduler().enqueue(JobRequest.of("greet", new Greeting("Ada", 3)).queue("typed")
                    .uniqueKey("greet-ada")).jobId();
            awaitState(id, JobState.SUCCEEDED);

            assertThat(greeter.seen.get()).isEqualTo(new Greeting("Ada", 3));
            JobContext context = greeter.context.get();
            assertThat(context.jobId()).isEqualTo(id);
            assertThat(context.attempt()).isEqualTo(1);
            assertThat(context.jobType()).isEqualTo("greet");
            assertThat(context.queue()).isEqualTo("typed");
            assertThat(context.uniqueKey()).isEqualTo("greet-ada");
            assertThat(context.workerId()).isEqualTo(node.worker().workerId());

            JobView job = client.admin().job(id);
            assertThat(job.resultJson()).isEqualTo("{\"greeted\": \"Ada\"}");
            assertThat(job.lockedBy()).isNull();
            assertThat(job.finishedAt()).isNotNull();
            assertThat(client.admin().attempts(id)).singleElement().satisfies(attempt -> {
                assertThat(attempt.outcome()).isEqualTo("SUCCEEDED");
                assertThat(attempt.attempt()).isEqualTo(1);
                assertThat(attempt.workerId()).isEqualTo(node.worker().workerId());
            });
        }
    }

    @Test
    void genericPayloadTypeIsResolvedFromTheHandlerSignature() {
        OrderPacker packer = new OrderPacker();
        try (SchedulerNode node = node("generic", 2)) {
            node.handlers().register(packer);
            node.start();

            long id = client.scheduler().enqueue(JobRequest.of("order.pack",
                    List.of(new OrderLine("A-1", 2), new OrderLine("B-7", 5))).queue("generic")).jobId();
            awaitState(id, JobState.SUCCEEDED);

            assertThat(packer.firstElement.get()).isEqualTo(new OrderLine("A-1", 2));
            assertThat(client.admin().job(id).resultJson()).isEqualTo("7");
        }
    }

    @Test
    void lambdaWithoutResolvablePayloadTypeIsRejectedAtRegistration() {
        JobHandler<Greeting> anonymous = new JobHandler<>() {
            @Override
            public String jobType() {
                return "fine";
            }

            @Override
            public void handle(Greeting payload, JobContext context) {
            }
        };
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(anonymous); // an anonymous class keeps its type argument

        @SuppressWarnings({"rawtypes", "unchecked"})
        JobHandler<?> raw = new JobHandler() {
            @Override
            public String jobType() {
                return "raw";
            }

            @Override
            public void handle(Object payload, JobContext context) {
            }
        };
        assertThatThrownBy(() -> registry.register(raw))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("payload type");
        assertThatThrownBy(() -> registry.register(anonymous))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Two handlers");
    }

    @Test
    void jobsAreClaimedByPriorityThenByRunTime() {
        List<String> order = new CopyOnWriteArrayList<>();
        try (SchedulerNode node = node("prio", 1)) {
            node.handlers().register("prio.job", String.class, (payload, context) -> order.add(payload));
            client.admin().pauseQueue("prio");
            node.start();

            client.scheduler().enqueue(JobRequest.of("prio.job", "low").queue("prio").priority(1));
            client.scheduler().enqueue(JobRequest.of("prio.job", "high-first").queue("prio").priority(5));
            client.scheduler().enqueue(JobRequest.of("prio.job", "medium").queue("prio").priority(3));
            client.scheduler().enqueue(JobRequest.of("prio.job", "high-second").queue("prio").priority(5));
            client.scheduler().enqueue(JobRequest.of("prio.job", "default").queue("prio"));
            client.admin().resumeQueue("prio");

            await().atMost(PATIENCE).untilAsserted(() ->
                    assertThat(order).containsExactly("high-first", "high-second", "medium", "low", "default"));
        }
    }

    @Test
    void delayedJobStartsWhenDueAndNotBefore() {
        AtomicReference<Long> startedNanos = new AtomicReference<>();
        try (SchedulerNode node = node("delayed", 2)) {
            node.handlers().register("delayed.job", String.class, (payload, context) -> startedNanos.set(System.nanoTime()));
            node.start();

            long enqueuedNanos = System.nanoTime();
            long id = client.scheduler().enqueue(JobRequest.of("delayed.job", "x").queue("delayed")
                    .delay(Duration.ofMillis(800))).jobId();
            assertThat(client.admin().job(id).state()).isEqualTo(JobState.SCHEDULED);
            awaitState(id, JobState.SUCCEEDED);

            long waitedMs = TimeUnit.NANOSECONDS.toMillis(startedNanos.get() - enqueuedNanos);
            // Not before it is due; and well before the next maintenance poll would be needed if the
            // promoter were not sleeping until exactly the due time.
            assertThat(waitedMs).isBetween(790L, 1_500L);
        }
    }

    @Test
    void pausedQueueIsNotClaimedFromUntilResumed() throws Exception {
        List<String> ran = new CopyOnWriteArrayList<>();
        try (SchedulerNode node = node("pausable", 2)) {
            node.handlers().register("pausable.job", String.class, (payload, context) -> ran.add(payload));
            node.start();
            client.admin().pauseQueue("pausable");

            long id = client.scheduler().enqueue(JobRequest.of("pausable.job", "x").queue("pausable")).jobId();
            TimeUnit.MILLISECONDS.sleep(800); // several poll intervals
            assertThat(ran).isEmpty();
            assertThat(client.admin().job(id).state()).isEqualTo(JobState.READY);
            assertThat(client.admin().queues()).filteredOn(q -> q.name().equals("pausable")).singleElement()
                    .satisfies(q -> {
                        assertThat(q.paused()).isTrue();
                        assertThat(q.counts()).containsEntry(JobState.READY, 1L);
                    });

            client.admin().resumeQueue("pausable");
            awaitState(id, JobState.SUCCEEDED);
            assertThat(ran).containsExactly("x");
        }
    }

    @Test
    void jobOfUnknownTypeIsParkedUntilAWorkerThatKnowsItJoins() throws Exception {
        List<String> ran = new CopyOnWriteArrayList<>();
        try (SchedulerNode oldVersion = node("rolling", 2)) {
            oldVersion.handlers().register("known.job", String.class, (payload, context) -> ran.add("known"));
            oldVersion.start();

            long known = client.scheduler().enqueue(JobRequest.of("known.job", "x").queue("rolling")).jobId();
            long unknown = client.scheduler().enqueue(JobRequest.of("new.feature", "y").queue("rolling")).jobId();
            awaitState(known, JobState.SUCCEEDED);
            TimeUnit.MILLISECONDS.sleep(600);

            // Not claimed, not failed, not counted as an attempt: simply waiting.
            JobView parked = client.admin().job(unknown);
            assertThat(parked.state()).isEqualTo(JobState.READY);
            assertThat(parked.attempt()).isZero();
            assertThat(client.admin().parkedJobTypes()).containsExactly(Map.entry("new.feature", 1L));

            try (SchedulerNode newVersion = node("rolling", 2)) {
                newVersion.handlers().register("new.feature", String.class, (payload, context) -> ran.add("new"));
                newVersion.start();

                awaitState(unknown, JobState.SUCCEEDED);
                assertThat(ran).containsExactly("known", "new");
                assertThat(client.admin().parkedJobTypes()).isEmpty();
                assertThat(client.admin().attempts(unknown)).extracting(AttemptView::workerId)
                        .containsExactly(newVersion.worker().workerId());
            }
        }
    }

    @Test
    void continuationsRunInOrderAndOnlyAfterSuccess() {
        List<String> ran = new CopyOnWriteArrayList<>();
        try (SchedulerNode node = node("chain", 4)) {
            node.handlers().register("chain.step", String.class, (payload, context) -> {
                ran.add(payload + (context.parentJobId() == null ? "" : "<-" + context.parentJobId()));
                if (payload.equals("boom")) {
                    throw new NonRetryableJobException("step failed for good");
                }
            });
            node.start();

            long first = client.scheduler().enqueue(JobRequest.of("chain.step", "extract").queue("chain")
                    .then(JobRequest.of("chain.step", "transform").queue("chain"))
                    .then(JobRequest.of("chain.step", "load").queue("chain"))).jobId();

            await().atMost(PATIENCE).untilAsserted(() -> assertThat(ran).hasSize(3));
            long second = database.count("SELECT id FROM pgs_job WHERE parent_id = ?", first);
            long third = database.count("SELECT id FROM pgs_job WHERE parent_id = ?", second);
            assertThat(ran).containsExactly("extract", "transform<-" + first, "load<-" + second);
            awaitState(third, JobState.SUCCEEDED);
            assertThat(client.admin().job(first).hasContinuation()).isTrue();
            assertThat(client.admin().job(third).hasContinuation()).isFalse();

            // A step that dead-letters does not enqueue what follows it.
            long failing = client.scheduler().enqueue(JobRequest.of("chain.step", "boom").queue("chain")
                    .then(JobRequest.of("chain.step", "never").queue("chain"))).jobId();
            awaitState(failing, JobState.DEAD);
            assertThat(database.count("SELECT count(*) FROM pgs_job WHERE parent_id = ?", failing)).isZero();
            assertThat(ran).doesNotContain("never<-" + failing);
        }
    }
}
