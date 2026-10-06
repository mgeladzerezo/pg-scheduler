package io.github.mgeladzerezo.pgscheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

/**
 * The enqueue API.
 *
 * <p><b>Transactional enqueue.</b> Every method runs on the connection Spring has bound to the calling
 * thread for the {@code DataSource} the scheduler uses. Called inside {@code @Transactional} code (or a
 * {@code TransactionTemplate}) the insert is part of the surrounding transaction: the job exists if and
 * only if the business change commits, and workers are notified by PostgreSQL at commit, not before.
 * Called outside a transaction each method commits on its own.
 */
public interface JobScheduler {

    /** Enqueues one job, or returns the pending job that already holds the unique key of the request. */
    EnqueueResult enqueue(JobRequest request);

    /**
     * Enqueues many jobs in one statement. Requests whose unique key is already pending are skipped.
     *
     * @return the number of jobs actually inserted
     */
    int enqueueAll(Collection<JobRequest> requests);

    /** Enqueues a job on the default queue to run as soon as a worker is free. */
    default EnqueueResult enqueue(String type, Object payload) {
        return enqueue(JobRequest.of(type, payload));
    }

    /** Enqueues a job that becomes due after {@code delay}. */
    default EnqueueResult enqueueIn(Duration delay, String type, Object payload) {
        return enqueue(JobRequest.of(type, payload).delay(delay));
    }

    /** Enqueues a job that becomes due at {@code runAt}. */
    default EnqueueResult enqueueAt(Instant runAt, String type, Object payload) {
        return enqueue(JobRequest.of(type, payload).runAt(runAt));
    }
}
