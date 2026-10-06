package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

/** The retry delay schedule and the retryable / non-retryable decision, without a database. */
class BackoffTest {

    private static RandomGenerator fixed(double value) {
        return new RandomGenerator() {
            @Override
            public long nextLong() {
                return 0;
            }

            @Override
            public double nextDouble() {
                return value;
            }
        };
    }

    @Test
    void delaysDoubleUntilTheCap() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), 0.0);

        assertThat(List.of(1, 2, 3, 4, 5, 6, 7, 20)).extracting(attempt -> backoff.delay(attempt, fixed(0.5)).toMillis())
                .containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L, 30_000L);
    }

    @Test
    void hugeAttemptNumbersDoNotOverflowPastTheCap() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), 3.0, Duration.ofMinutes(10), 0.0);

        assertThat(backoff.baseDelay(5_000)).isEqualTo(Duration.ofMinutes(10));
        assertThat(backoff.baseDelay(Integer.MAX_VALUE)).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void jitterOnlyShortensTheDelayAndByAtMostItsFraction() {
        Backoff backoff = new Backoff(Duration.ofSeconds(10), 2.0, Duration.ofMinutes(1), 0.25);

        // The two ends of the random range: nothing removed, and (almost) the full fraction removed.
        assertThat(backoff.delay(1, fixed(0.0))).isEqualTo(Duration.ofSeconds(10));
        assertThat(backoff.delay(1, fixed(0.999999))).isBetween(Duration.ofMillis(7_500), Duration.ofMillis(7_501));
        assertThat(backoff.delay(3, fixed(0.5))).isEqualTo(Duration.ofSeconds(35)); // 40 s minus half of 25 %

        Random random = new Random(42);
        Set<Long> distinct = new HashSet<>();
        for (int i = 0; i < 2_000; i++) {
            int attempt = 1 + i % 8;
            long base = backoff.baseDelay(attempt).toMillis();
            long delay = backoff.delay(attempt, random).toMillis();
            assertThat(delay).isBetween((long) (base * 0.75), base);
            assertThat(delay).isLessThanOrEqualTo(60_000);
            distinct.add(delay);
        }
        assertThat(distinct).as("jitter actually spreads the delays").hasSizeGreaterThan(500);
    }

    @Test
    void fixedBackoffIsConstant() {
        Backoff backoff = Backoff.fixed(Duration.ofMillis(250));

        assertThat(backoff.delay(1, fixed(0.9))).isEqualTo(Duration.ofMillis(250));
        assertThat(backoff.delay(9, fixed(0.9))).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void invalidSettingsAreRejected() {
        assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(1), 0.5, Duration.ofSeconds(2), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(1), 2, Duration.ofSeconds(2), 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(-1), 2, Duration.ofSeconds(2), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void policyDecidesWhatIsRetryable() {
        JobPolicy policy = JobPolicy.DEFAULT.withNoRetryFor(Set.of(IllegalArgumentException.class));

        assertThat(policy.isRetryable(new IOException("connection reset"))).isTrue();
        assertThat(policy.isRetryable(new IllegalStateException("try later"))).isTrue();
        assertThat(policy.isRetryable(new NonRetryableJobException("bad payload"))).isFalse();
        assertThat(policy.isRetryable(new IllegalArgumentException("bad argument"))).isFalse();
        // Subclasses of a listed type, and listed types buried in the cause chain, count too.
        assertThat(policy.isRetryable(new NumberFormatException("NaN"))).isFalse();
        assertThat(policy.isRetryable(new UncheckedIOException(new IOException(new NonRetryableJobException("deep"))))).isFalse();
    }
}
