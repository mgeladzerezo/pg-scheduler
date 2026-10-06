package io.github.mgeladzerezo.pgscheduler.cron;

/**
 * What a schedule does about misfires: fire times that are noticed later than the scheduler's misfire
 * threshold (60 seconds by default), typically because no scheduler instance was running.
 *
 * <p>A fire time noticed within the threshold is on time under every policy and simply fires.
 */
public enum MisfirePolicy {

    /**
     * If anything was missed, enqueue a single job, for the most recent fire time that is due, and drop
     * the older ones. The default: right for "refresh the cache" or "send the digest", where running once
     * late is useful and running forty times in a row is not.
     */
    FIRE_ONCE,

    /**
     * Drop every missed fire time and carry on with the next one that is on time. Right for jobs that are
     * pointless when late.
     */
    SKIP,

    /**
     * Enqueue one job for every missed fire time, oldest first. Right when each fire time stands for a
     * unit of work, such as "bill the hour that just ended". Bounded per pass, so a long outage is worked
     * off over several passes instead of in one huge transaction.
     */
    CATCH_UP
}
