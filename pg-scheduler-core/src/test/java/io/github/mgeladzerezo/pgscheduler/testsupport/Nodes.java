package io.github.mgeladzerezo.pgscheduler.testsupport;

import io.github.mgeladzerezo.pgscheduler.SchedulerNode;
import io.github.mgeladzerezo.pgscheduler.worker.Maintenance;
import io.github.mgeladzerezo.pgscheduler.worker.WorkerOptions;
import java.time.Duration;
import javax.sql.DataSource;

/** Node configurations tuned for tests: short intervals so that nothing waits on production defaults. */
public final class Nodes {

    private Nodes() {
    }

    /** Maintenance that promotes and reaps every 100 ms. */
    public static Maintenance.Options fastMaintenance() {
        return Maintenance.Options.defaults().withIntervals(Duration.ofMillis(100), Duration.ofMillis(100));
    }

    /** A worker on the default queue: given concurrency, 200 ms poll, 30 s lease. */
    public static WorkerOptions worker(int concurrency) {
        return WorkerOptions.defaults().withQueue("default", concurrency).withBatchSize(Math.max(1, concurrency))
                .withPollInterval(Duration.ofMillis(200));
    }

    /** A node with worker, maintenance and LISTEN/NOTIFY on its own pool. Call {@code start()} after registering handlers. */
    public static SchedulerNode.Builder full(TestDatabase database, DataSource pool, WorkerOptions options) {
        return SchedulerNode.builder(pool)
                .worker(options)
                .maintenance(fastMaintenance())
                .listen(database::openConnection);
    }

    /** A node that can only enqueue and administer; runs nothing in the background. */
    public static SchedulerNode client(DataSource pool) {
        return SchedulerNode.builder(pool).build();
    }
}
