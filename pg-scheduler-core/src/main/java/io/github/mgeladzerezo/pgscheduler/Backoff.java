package io.github.mgeladzerezo.pgscheduler;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with a cap and jitter.
 *
 * <p>The delay before retrying after failed attempt {@code n} (1-based) is
 * {@code min(max, initial * multiplier^(n-1))}, then reduced by a random fraction of up to {@code jitter}.
 * Jitter only ever shortens the delay, so {@code max} is a hard upper bound, and it spreads out retries of
 * jobs that failed together (a downstream outage) instead of sending them back as one synchronised wave.
 *
 * @param initial    delay after the first failure
 * @param multiplier growth factor per attempt, at least 1
 * @param max        upper bound for any delay
 * @param jitter     fraction of the delay that may be randomly removed, between 0 (none) and 1 (full jitter)
 */
public record Backoff(Duration initial, double multiplier, Duration max, double jitter) {

    /** 2 s, 4 s, 8 s, ... capped at 5 minutes, with 20 % jitter. */
    public static final Backoff DEFAULT = new Backoff(Duration.ofSeconds(2), 2.0, Duration.ofMinutes(5), 0.2);

    public Backoff {
        if (initial.isNegative() || max.isNegative()) {
            throw new IllegalArgumentException("backoff delays must not be negative");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("backoff multiplier must be at least 1");
        }
        if (jitter < 0.0 || jitter > 1.0) {
            throw new IllegalArgumentException("backoff jitter must be between 0 and 1");
        }
    }

    /** A constant delay without jitter; mostly useful in tests. */
    public static Backoff fixed(Duration delay) {
        return new Backoff(delay, 1.0, delay, 0.0);
    }

    /** The delay without jitter after the given failed attempt (1-based). */
    public Duration baseDelay(int failedAttempt) {
        double millis = initial.toMillis() * Math.pow(multiplier, Math.max(0, failedAttempt - 1));
        // Math.pow overflows to infinity for large attempts; the cap handles that as well.
        return millis >= max.toMillis() ? max : Duration.ofMillis((long) millis);
    }

    /** The delay to wait after the given failed attempt (1-based), jitter applied. */
    public Duration delay(int failedAttempt, RandomGenerator random) {
        long base = baseDelay(failedAttempt).toMillis();
        if (jitter == 0.0 || base == 0) {
            return Duration.ofMillis(base);
        }
        return Duration.ofMillis(base - (long) (base * jitter * random.nextDouble()));
    }
}
