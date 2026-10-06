package io.github.mgeladzerezo.pgscheduler;

import java.time.Duration;
import java.util.Set;

/**
 * How jobs of one type are retried, timed out and throttled.
 *
 * @param maxAttempts default number of attempts before dead-lettering (a request may override it)
 * @param timeout     default handler timeout (a request may override it)
 * @param backoff     delay schedule between attempts
 * @param noRetryFor  exception types (including subclasses) that dead-letter immediately
 * @param rateLimit   optional cluster-wide start-rate limit, or {@code null}
 */
public record JobPolicy(int maxAttempts, Duration timeout, Backoff backoff,
                        Set<Class<? extends Throwable>> noRetryFor, RateLimit rateLimit) {

    /** 5 attempts, 5 minute timeout, {@link Backoff#DEFAULT}, everything retryable, no rate limit. */
    public static final JobPolicy DEFAULT = new JobPolicy(5, Duration.ofMinutes(5), Backoff.DEFAULT, Set.of(), null);

    public JobPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        noRetryFor = Set.copyOf(noRetryFor);
    }

    public JobPolicy withMaxAttempts(int maxAttempts) {
        return new JobPolicy(maxAttempts, timeout, backoff, noRetryFor, rateLimit);
    }

    public JobPolicy withTimeout(Duration timeout) {
        return new JobPolicy(maxAttempts, timeout, backoff, noRetryFor, rateLimit);
    }

    public JobPolicy withBackoff(Backoff backoff) {
        return new JobPolicy(maxAttempts, timeout, backoff, noRetryFor, rateLimit);
    }

    public JobPolicy withNoRetryFor(Set<Class<? extends Throwable>> noRetryFor) {
        return new JobPolicy(maxAttempts, timeout, backoff, noRetryFor, rateLimit);
    }

    public JobPolicy withRateLimit(RateLimit rateLimit) {
        return new JobPolicy(maxAttempts, timeout, backoff, noRetryFor, rateLimit);
    }

    /**
     * Whether a failure with this exception should be retried (attempts permitting). Walks the cause chain
     * so that a wrapped {@link NonRetryableJobException} is still honoured.
     */
    public boolean isRetryable(Throwable failure) {
        Throwable t = failure;
        for (int depth = 0; t != null && depth < 32; depth++, t = t.getCause()) {
            if (t instanceof NonRetryableJobException) {
                return false;
            }
            for (Class<? extends Throwable> type : noRetryFor) {
                if (type.isInstance(t)) {
                    return false;
                }
            }
        }
        return true;
    }
}
