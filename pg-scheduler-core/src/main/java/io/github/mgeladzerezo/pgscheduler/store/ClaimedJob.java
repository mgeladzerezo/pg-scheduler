package io.github.mgeladzerezo.pgscheduler.store;

import java.time.Instant;

/**
 * A job as returned by the claim: already RUNNING and leased to the claiming worker.
 *
 * @param attempt the number of this attempt (1-based); together with the worker id it is the fencing token
 *                every later write about this attempt must present
 * @param waitMs  how long the job waited from becoming eligible to being claimed, on the database clock
 */
public record ClaimedJob(long id, String type, String queue, String payloadJson, int priority, int attempt,
                         int maxAttempts, long timeoutMs, Instant runAt, Instant createdAt, String uniqueKey,
                         String continuationJson, Long parentId, long waitMs) {
}
