package io.github.mgeladzerezo.pgscheduler;

import java.time.Instant;

/**
 * What a handler may know about the attempt it is running.
 */
public interface JobContext {

    long jobId();

    String jobType();

    String queue();

    /** 1 for the first attempt. Together with {@link #jobId()} this makes a natural idempotency key. */
    int attempt();

    int maxAttempts();

    /** Whether a failure of this attempt would dead-letter the job. */
    default boolean isLastAttempt() {
        return attempt() >= maxAttempts();
    }

    String workerId();

    /** When the job was first enqueued. */
    Instant createdAt();

    /** The unique key given at enqueue time, or {@code null}. */
    String uniqueKey();

    /** Id of the job this one continues (job chaining), or {@code null}. */
    Long parentJobId();

    /**
     * Stores a result with the job when the attempt succeeds. Serialised to JSON. Annotated handler methods
     * can simply return the value instead.
     */
    void setResult(Object result);
}
