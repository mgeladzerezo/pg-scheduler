package io.github.mgeladzerezo.pgscheduler;

/**
 * Lifecycle states of a job. The README contains the full state machine.
 */
public enum JobState {
    /** Waiting for {@code run_at}: a delayed job that is not due yet. */
    SCHEDULED,
    /** Due and waiting for a worker. Only READY rows are in the claim index. */
    READY,
    /** Claimed by a worker that holds a lease on it. */
    RUNNING,
    /** Finished successfully. Terminal. */
    SUCCEEDED,
    /** The last attempt failed and a retry is scheduled at {@code run_at}. */
    FAILED,
    /** Dead letter: attempts exhausted or a non-retryable failure. Stays until retried or purged manually. */
    DEAD,
    /** Cancelled by an operator. Terminal. */
    CANCELLED;

    /** States in which a job still blocks another enqueue with the same unique key. */
    public boolean isPending() {
        return this == SCHEDULED || this == READY || this == RUNNING || this == FAILED;
    }
}
