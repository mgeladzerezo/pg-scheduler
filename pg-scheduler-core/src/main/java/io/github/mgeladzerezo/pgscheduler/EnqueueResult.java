package io.github.mgeladzerezo.pgscheduler;

/**
 * Outcome of an enqueue.
 *
 * @param jobId   id of the job that now represents the request: the new job, or the pending job that already
 *                holds the same unique key
 * @param created {@code false} when the request was de-duplicated against a pending job and nothing was inserted
 */
public record EnqueueResult(long jobId, boolean created) {
}
