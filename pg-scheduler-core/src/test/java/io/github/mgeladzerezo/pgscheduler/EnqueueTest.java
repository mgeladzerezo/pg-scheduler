package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** What an enqueue writes, and that a unique key de-duplicates against pending jobs only. */
class EnqueueTest {

    private static TestDatabase database;
    private static SchedulerNode node;

    @BeforeAll
    static void setUp() {
        database = TestDatabase.create();
        node = Nodes.client(database.dataSource());
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @Test
    void jobThatIsDueIsBornReadyWithRequestedAttributes() {
        EnqueueResult result = node.scheduler().enqueue(JobRequest.of("report.build", Map.of("month", "2026-09"))
                .queue("reports").priority(7).maxAttempts(3).timeout(Duration.ofSeconds(90)));

        assertThat(result.created()).isTrue();
        JobView job = node.admin().job(result.jobId());
        assertThat(job.state()).isEqualTo(JobState.READY);
        assertThat(job.type()).isEqualTo("report.build");
        assertThat(job.queue()).isEqualTo("reports");
        assertThat(job.priority()).isEqualTo(7);
        assertThat(job.maxAttempts()).isEqualTo(3);
        assertThat(job.timeoutMs()).isEqualTo(90_000);
        assertThat(job.attempt()).isZero();
        assertThat(job.payloadJson()).isEqualTo("{\"month\": \"2026-09\"}");
        assertThat(job.lockedBy()).isNull();
    }

    @Test
    void defaultsComeFromTheJobTypePolicy() {
        node.policies().set("policy.test", JobPolicy.DEFAULT.withMaxAttempts(9).withTimeout(Duration.ofSeconds(7)));

        JobView job = node.admin().job(node.scheduler().enqueue("policy.test", null).jobId());

        assertThat(job.maxAttempts()).isEqualTo(9);
        assertThat(job.timeoutMs()).isEqualTo(7_000);
        assertThat(job.queue()).isEqualTo("default");
        assertThat(job.payloadJson()).isEqualTo("null");
    }

    @Test
    void delayedAndFutureJobsAreBornScheduled() {
        long delayed = node.scheduler().enqueueIn(Duration.ofHours(1), "later", "x").jobId();
        Instant at = Instant.now().plus(2, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MILLIS);
        long absolute = node.scheduler().enqueueAt(at, "later", "y").jobId();
        long past = node.scheduler().enqueueAt(Instant.now().minusSeconds(60), "later", "z").jobId();

        assertThat(node.admin().job(delayed).state()).isEqualTo(JobState.SCHEDULED);
        assertThat(Duration.between(Instant.now(), node.admin().job(delayed).runAt()))
                .isBetween(Duration.ofMinutes(59), Duration.ofMinutes(61));
        assertThat(node.admin().job(absolute).state()).isEqualTo(JobState.SCHEDULED);
        assertThat(node.admin().job(absolute).runAt()).isEqualTo(at);
        assertThat(node.admin().job(past).state()).isEqualTo(JobState.READY);
    }

    @Test
    void sameUniqueKeyWhilePendingIsANoOp() {
        EnqueueResult first = node.scheduler().enqueue(JobRequest.of("sync.account", 1).uniqueKey("account-1"));
        EnqueueResult second = node.scheduler().enqueue(JobRequest.of("sync.account", 2).uniqueKey("account-1"));
        EnqueueResult other = node.scheduler().enqueue(JobRequest.of("sync.account", 3).uniqueKey("account-2"));

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.jobId()).isEqualTo(first.jobId());
        assertThat(other.created()).isTrue();
        assertThat(database.count("SELECT count(*) FROM pgs_job WHERE unique_key = 'account-1'")).isEqualTo(1);
        // The payload of the pending job is untouched by the ignored request.
        assertThat(node.admin().job(first.jobId()).payloadJson()).isEqualTo("1");
    }

    @Test
    void uniqueKeyIsFreeAgainOnceTheJobHasFinished() {
        long first = node.scheduler().enqueue(JobRequest.of("sync.account", 1).uniqueKey("account-9")).jobId();
        for (String pending : List.of("SCHEDULED", "FAILED")) {
            database.execute("UPDATE pgs_job SET state = ? WHERE id = ?", pending, first);
            assertThat(node.scheduler().enqueue(JobRequest.of("sync.account", 2).uniqueKey("account-9")).created())
                    .as("blocked while %s", pending).isFalse();
        }
        database.execute("UPDATE pgs_job SET state = 'RUNNING', locked_by = 'w', locked_until = now() WHERE id = ?", first);
        assertThat(node.scheduler().enqueue(JobRequest.of("sync.account", 2).uniqueKey("account-9")).created())
                .as("blocked while RUNNING").isFalse();

        database.execute("UPDATE pgs_job SET state = 'SUCCEEDED', locked_by = NULL, locked_until = NULL WHERE id = ?", first);
        EnqueueResult again = node.scheduler().enqueue(JobRequest.of("sync.account", 2).uniqueKey("account-9"));

        assertThat(again.created()).isTrue();
        assertThat(again.jobId()).isNotEqualTo(first);
    }

    @Test
    void concurrentEnqueuesWithOneKeyCreateExactlyOneJob() throws Exception {
        int threads = 16;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Future<EnqueueResult>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            var dataSource = database.newPool(threads);
            SchedulerNode racing = Nodes.client(dataSource);
            for (int i = 0; i < threads; i++) {
                Callable<EnqueueResult> task = () -> {
                    barrier.await();
                    return racing.scheduler().enqueue(JobRequest.of("race", null).uniqueKey("the-one"));
                };
                futures.add(pool.submit(task));
            }
            List<EnqueueResult> results = new ArrayList<>();
            for (Future<EnqueueResult> future : futures) {
                results.add(future.get());
            }
            assertThat(results).filteredOn(EnqueueResult::created).hasSize(1);
            assertThat(results).extracting(EnqueueResult::jobId).containsOnly(results.getFirst().jobId());
        }
        assertThat(database.count("SELECT count(*) FROM pgs_job WHERE unique_key = 'the-one'")).isEqualTo(1);
    }

    @Test
    void bulkEnqueueInsertsInOneStatementAndSkipsPendingDuplicates() {
        node.scheduler().enqueue(JobRequest.of("bulk", 0).uniqueKey("bulk-dup"));
        List<JobRequest> requests = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            requests.add(JobRequest.of("bulk", i).queue("bulk-q").priority(i % 3));
        }
        requests.add(JobRequest.of("bulk", -1).queue("bulk-q").uniqueKey("bulk-dup"));     // already pending
        requests.add(JobRequest.of("bulk", -2).queue("bulk-q").uniqueKey("bulk-new"));
        requests.add(JobRequest.of("bulk", -3).queue("bulk-q").uniqueKey("bulk-new"));     // duplicate within the batch
        requests.add(JobRequest.of("bulk", -4).queue("bulk-q").delay(Duration.ofHours(1)));

        int inserted = node.scheduler().enqueueAll(requests);

        assertThat(inserted).isEqualTo(1_002);
        assertThat(database.count("SELECT count(*) FROM pgs_job WHERE queue = 'bulk-q' AND state = 'READY'")).isEqualTo(1_001);
        assertThat(database.count("SELECT count(*) FROM pgs_job WHERE queue = 'bulk-q' AND state = 'SCHEDULED'")).isEqualTo(1);
        assertThat(database.count("SELECT count(*) FROM pgs_job WHERE queue = 'bulk-q' AND priority = 2")).isEqualTo(333);
    }
}
