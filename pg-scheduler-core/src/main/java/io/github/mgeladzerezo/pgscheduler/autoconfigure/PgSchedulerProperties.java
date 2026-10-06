package io.github.mgeladzerezo.pgscheduler.autoconfigure;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration of the scheduler under the {@code pgscheduler} prefix. Every background role is switched
 * on or off on its own, so one application can be a pure client, a worker, or the one running cron.
 *
 * @param enabled     master switch of the whole auto-configuration
 * @param migrate     apply the library's own Flyway migrations (separate history table) on start-up
 * @param workerId    id shown in the dashboard; generated as {@code host-xxxxxx} when empty
 * @param worker      the worker role
 * @param maintenance promotion of due jobs, lease reaper and purge
 * @param cron        the cron loop
 * @param listen      LISTEN/NOTIFY wake-ups
 * @param defaults    retry, timeout and rate-limit defaults for every job type
 * @param jobTypes    overrides by job type; these win over {@code @JobHandler} attributes
 */
@ConfigurationProperties("pgscheduler")
public record PgSchedulerProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("true") boolean migrate,
        String workerId,
        @DefaultValue Worker worker,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Cron cron,
        @DefaultValue Listen listen,
        @DefaultValue JobType defaults,
        @DefaultValue Map<String, JobType> jobTypes) {

    /**
     * @param enabled         run a worker in this JVM
     * @param queues          queues to serve and how many jobs of each run at once
     * @param batchSize       most jobs taken by one claim
     * @param pollInterval    poll interval when no notification arrives
     * @param lease           how long a claim or heartbeat keeps a job; a crashed worker's jobs are taken
     *                        back this long after its last heartbeat
     * @param heartbeat       heartbeat interval; a third of the lease when empty
     * @param shutdownTimeout how long shutdown waits for running handlers before releasing their jobs
     */
    public record Worker(
            @DefaultValue("true") boolean enabled,
            Map<String, Integer> queues,
            @DefaultValue("10") int batchSize,
            @DefaultValue("1s") Duration pollInterval,
            @DefaultValue("30s") Duration lease,
            Duration heartbeat,
            @DefaultValue("30s") Duration shutdownTimeout) {

        /** An unset {@code queues} means one queue named {@code default} with 10 slots. */
        public Worker {
            if (queues == null || queues.isEmpty()) {
                queues = Map.of("default", 10);
            }
        }
    }

    /**
     * @param enabled           run maintenance in this JVM
     * @param interval          longest sleep between promotion passes
     * @param reapInterval      how often expired leases are looked for
     * @param finishedRetention how long succeeded and cancelled jobs are kept
     */
    public record Maintenance(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("1s") Duration interval,
            @DefaultValue("1s") Duration reapInterval,
            @DefaultValue("7d") Duration finishedRetention) {
    }

    /**
     * @param enabled           run the cron loop in this JVM; any number of instances may, each fire time is
     *                          enqueued once
     * @param misfireThreshold  a fire time noticed later than this is a misfire
     * @param maxCatchUpPerTick jobs one schedule may enqueue per pass under the CATCH_UP policy
     * @param pollInterval      longest sleep of the loop; it also wakes at every minute boundary
     */
    public record Cron(
            boolean enabled,
            @DefaultValue("60s") Duration misfireThreshold,
            @DefaultValue("500") int maxCatchUpPerTick,
            @DefaultValue("10s") Duration pollInterval) {
    }

    /** @param enabled wake workers through LISTEN/NOTIFY; polling continues as a safety net either way */
    public record Listen(@DefaultValue("true") boolean enabled) {
    }

    /**
     * Unset values keep what is below them in the precedence order: library default, then
     * {@code pgscheduler.defaults}, then the {@code @JobHandler} attributes, then {@code pgscheduler.job-types}.
     *
     * @param rateLimitPerSecond jobs of the type started per second across the cluster; empty or 0 is unlimited
     * @param noRetryFor         exceptions that dead-letter a job without retries
     */
    public record JobType(
            Integer maxAttempts,
            Duration timeout,
            Duration backoffInitial,
            Double backoffMultiplier,
            Duration backoffMax,
            Double backoffJitter,
            Double rateLimitPerSecond,
            List<Class<? extends Throwable>> noRetryFor) {
    }
}
