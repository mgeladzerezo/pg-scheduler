package io.github.mgeladzerezo.pgscheduler.cron;

/**
 * What a schedule does about fire times that passed while no scheduler was running (or while the
 * schedulers were too slow to notice them).
 */
public enum MisfirePolicy {

    /**
     * Collapse everything that was missed into one job, for the most recent missed fire time. The default:
     * right for "refresh the cache" or "send the digest", where running once late is useful and running
     * forty times in a row is not.
     */
    FIRE_ONCE,

    /**
     * Drop fire times that are older than the misfire threshold and carry on with the next future one.
     * Right for jobs that are pointless when late. A fire time noticed within the threshold is not a
     * misfire and runs normally.
     */
    SKIP,

    /**
     * Enqueue one job for every missed fire time, oldest first. Right when each fire time stands for a
     * unit of work, such as "bill the hour that just ended". Bounded per pass, so a long outage is worked
     * off over several passes instead of in one huge transaction.
     */
    CATCH_UP
}
