package io.github.mgeladzerezo.pgscheduler;

/**
 * A cluster-wide token-bucket limit on how fast jobs of one type are started.
 *
 * @param permitsPerSecond sustained start rate
 * @param burst            bucket size: how many jobs may start at once after an idle period
 */
public record RateLimit(double permitsPerSecond, int burst) {

    public RateLimit {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (burst < 1) {
            throw new IllegalArgumentException("burst must be at least 1");
        }
    }

    /** A limit whose burst is one second of permits (at least one). */
    public static RateLimit perSecond(double permitsPerSecond) {
        return new RateLimit(permitsPerSecond, Math.max(1, (int) Math.ceil(permitsPerSecond)));
    }
}
