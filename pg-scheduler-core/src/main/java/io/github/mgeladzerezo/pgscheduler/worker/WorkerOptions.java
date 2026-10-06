package io.github.mgeladzerezo.pgscheduler.worker;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Settings of one worker.
 *
 * @param queues            queues to serve and how many jobs of each this worker runs at once
 * @param batchSize         most jobs claimed by one claim statement
 * @param pollInterval      how often to poll when no notification arrives; the safety net behind LISTEN/NOTIFY
 * @param leaseDuration     how long a claim or heartbeat keeps a job; a crashed worker's jobs become
 *                          available to others this long after its last heartbeat
 * @param heartbeatInterval how often leases are extended; must be well below {@code leaseDuration}
 * @param shutdownTimeout   how long a graceful stop waits for running handlers before releasing their jobs
 */
public record WorkerOptions(Map<String, Integer> queues, int batchSize, Duration pollInterval,
                            Duration leaseDuration, Duration heartbeatInterval, Duration shutdownTimeout) {

    public WorkerOptions {
        if (queues.isEmpty()) {
            throw new IllegalArgumentException("a worker needs at least one queue");
        }
        queues.forEach((queue, concurrency) -> {
            if (concurrency < 1) {
                throw new IllegalArgumentException("concurrency of queue '" + queue + "' must be at least 1");
            }
        });
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1");
        }
        if (heartbeatInterval.multipliedBy(2).compareTo(leaseDuration) > 0) {
            throw new IllegalArgumentException("heartbeatInterval (" + heartbeatInterval
                    + ") must be at most half of leaseDuration (" + leaseDuration
                    + "), otherwise one late heartbeat loses the lease");
        }
        queues = Collections.unmodifiableMap(new LinkedHashMap<>(queues));
    }

    /** One queue named {@code default} with 10 slots, batches of 10, 1 s poll, 30 s lease, 10 s heartbeat. */
    public static WorkerOptions defaults() {
        return new WorkerOptions(Map.of("default", 10), 10, Duration.ofSeconds(1), Duration.ofSeconds(30),
                Duration.ofSeconds(10), Duration.ofSeconds(30));
    }

    public WorkerOptions withQueues(Map<String, Integer> queues) {
        return new WorkerOptions(queues, batchSize, pollInterval, leaseDuration, heartbeatInterval, shutdownTimeout);
    }

    public WorkerOptions withQueue(String queue, int concurrency) {
        return withQueues(Map.of(queue, concurrency));
    }

    public WorkerOptions withBatchSize(int batchSize) {
        return new WorkerOptions(queues, batchSize, pollInterval, leaseDuration, heartbeatInterval, shutdownTimeout);
    }

    public WorkerOptions withPollInterval(Duration pollInterval) {
        return new WorkerOptions(queues, batchSize, pollInterval, leaseDuration, heartbeatInterval, shutdownTimeout);
    }

    /** Sets the lease and a heartbeat interval of one third of it. */
    public WorkerOptions withLease(Duration leaseDuration) {
        return new WorkerOptions(queues, batchSize, pollInterval, leaseDuration, leaseDuration.dividedBy(3), shutdownTimeout);
    }

    public WorkerOptions withShutdownTimeout(Duration shutdownTimeout) {
        return new WorkerOptions(queues, batchSize, pollInterval, leaseDuration, heartbeatInterval, shutdownTimeout);
    }
}
