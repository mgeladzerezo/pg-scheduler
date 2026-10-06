package io.github.mgeladzerezo.pgscheduler.store;

import java.time.Instant;

/**
 * A fully resolved job ready to insert: payload already serialised, defaults already applied.
 *
 * @param runAt            absolute earliest start, or {@code null} to use {@code delayMs}
 * @param delayMs          delay from the moment of the insert on the database clock; used when {@code runAt} is null
 * @param continuationJson serialised chain to enqueue on success, or {@code null}
 * @param parentId         id of the job this one continues, or {@code null}
 */
public record JobSpec(String type, String queue, String payloadJson, int priority, Instant runAt, long delayMs,
                      String uniqueKey, int maxAttempts, long timeoutMs, String continuationJson, Long parentId) {
}
