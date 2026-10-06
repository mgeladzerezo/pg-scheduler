package io.github.mgeladzerezo.pgscheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * An immutable description of a job to enqueue. Start with {@link #of(String, Object)} and refine with the
 * fluent methods, each of which returns a new instance.
 *
 * <pre>{@code
 * scheduler.enqueue(JobRequest.of("invoice.render", new Invoice(42))
 *         .queue("documents")
 *         .delay(Duration.ofMinutes(5))
 *         .uniqueKey("invoice-42")
 *         .then(JobRequest.of("email.send", new Email("billing@example.org", "Invoice 42"))));
 * }</pre>
 */
public final class JobRequest {

    /** Queue used when none is given. */
    public static final String DEFAULT_QUEUE = "default";

    private final String type;
    private final Object payload;
    private final String queue;
    private final int priority;
    private final Instant runAt;
    private final Duration delay;
    private final String uniqueKey;
    private final Integer maxAttempts;
    private final Duration timeout;
    private final JobRequest continuation;

    private JobRequest(String type, Object payload, String queue, int priority, Instant runAt, Duration delay,
                       String uniqueKey, Integer maxAttempts, Duration timeout, JobRequest continuation) {
        this.type = type;
        this.payload = payload;
        this.queue = queue;
        this.priority = priority;
        this.runAt = runAt;
        this.delay = delay;
        this.uniqueKey = uniqueKey;
        this.maxAttempts = maxAttempts;
        this.timeout = timeout;
        this.continuation = continuation;
    }

    /**
     * @param type    job type; selects the handler
     * @param payload any object Jackson can serialise, or {@code null}
     */
    public static JobRequest of(String type, Object payload) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("job type must not be blank");
        }
        return new JobRequest(type, payload, DEFAULT_QUEUE, 0, null, null, null, null, null, null);
    }

    public static JobRequest of(String type) {
        return of(type, null);
    }

    public JobRequest queue(String queue) {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("queue must not be blank");
        }
        return new JobRequest(type, payload, queue, priority, runAt, delay, uniqueKey, maxAttempts, timeout, continuation);
    }

    /** Higher priorities are claimed first within a queue. Default 0; range of a {@code smallint}. */
    public JobRequest priority(int priority) {
        if (priority < Short.MIN_VALUE || priority > Short.MAX_VALUE) {
            throw new IllegalArgumentException("priority out of range: " + priority);
        }
        return new JobRequest(type, payload, queue, priority, runAt, delay, uniqueKey, maxAttempts, timeout, continuation);
    }

    /** Earliest start as an absolute instant. Replaces any {@link #delay}. */
    public JobRequest runAt(Instant runAt) {
        return new JobRequest(type, payload, queue, priority, Objects.requireNonNull(runAt), null, uniqueKey,
                maxAttempts, timeout, continuation);
    }

    /**
     * Earliest start relative to the moment of the insert, measured on the database clock so that clock skew
     * between application nodes does not matter. Replaces any {@link #runAt}.
     */
    public JobRequest delay(Duration delay) {
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        return new JobRequest(type, payload, queue, priority, null, delay, uniqueKey, maxAttempts, timeout, continuation);
    }

    /**
     * De-duplication key. While a job with this key is pending (scheduled, ready, running or waiting for a
     * retry), enqueueing another one with the same key inserts nothing and returns the id of the pending job.
     */
    public JobRequest uniqueKey(String uniqueKey) {
        return new JobRequest(type, payload, queue, priority, runAt, delay, uniqueKey, maxAttempts, timeout, continuation);
    }

    /** Overrides the default number of attempts of the job type. */
    public JobRequest maxAttempts(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        return new JobRequest(type, payload, queue, priority, runAt, delay, uniqueKey, maxAttempts, timeout, continuation);
    }

    /** Overrides the default handler timeout of the job type. */
    public JobRequest timeout(Duration timeout) {
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return new JobRequest(type, payload, queue, priority, runAt, delay, uniqueKey, maxAttempts, timeout, continuation);
    }

    /**
     * Chains a continuation: {@code next} is enqueued in the same transaction that marks this job succeeded,
     * so it is enqueued exactly once and only if this job succeeds. Calling {@code then} repeatedly appends
     * to the end of the chain. The {@link #delay} of a continuation counts from the moment its predecessor
     * succeeds; an absolute {@link #runAt} is not allowed on a continuation.
     */
    public JobRequest then(JobRequest next) {
        if (next.runAt != null) {
            throw new IllegalArgumentException("a continuation cannot have an absolute runAt; use delay");
        }
        return new JobRequest(type, payload, queue, priority, runAt, delay, uniqueKey, maxAttempts, timeout,
                continuation == null ? next : continuation.then(next));
    }

    public String type() {
        return type;
    }

    public Object payload() {
        return payload;
    }

    public String queue() {
        return queue;
    }

    public int priority() {
        return priority;
    }

    public Instant runAt() {
        return runAt;
    }

    public Duration delay() {
        return delay;
    }

    public String uniqueKey() {
        return uniqueKey;
    }

    public Integer maxAttempts() {
        return maxAttempts;
    }

    public Duration timeout() {
        return timeout;
    }

    public JobRequest continuation() {
        return continuation;
    }
}
