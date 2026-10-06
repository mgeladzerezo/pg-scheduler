package io.github.mgeladzerezo.pgscheduler.cron;

import io.github.mgeladzerezo.pgscheduler.JobRequest;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * What to create a schedule from. Start with {@link #of} and refine with the {@code with...} methods.
 *
 * @param name          unique name of the schedule
 * @param cron          five-field cron expression, see {@link CronExpression}
 * @param zone          time zone the expression is evaluated in
 * @param jobType       type of the jobs to enqueue
 * @param payload       payload of every produced job; anything Jackson can serialise, or {@code null}
 * @param maxAttempts   attempts for produced jobs, or {@code null} for the job type's default
 * @param timeout       handler timeout for produced jobs, or {@code null} for the job type's default
 */
public record ScheduleDefinition(String name, String cron, ZoneId zone, String jobType, String queue,
                                 Object payload, int priority, Integer maxAttempts, Duration timeout,
                                 MisfirePolicy misfirePolicy) {

    /** A schedule in UTC on the default queue with no payload and the {@link MisfirePolicy#FIRE_ONCE} policy. */
    public static ScheduleDefinition of(String name, String cron, String jobType) {
        return new ScheduleDefinition(name, cron, ZoneOffset.UTC, jobType, JobRequest.DEFAULT_QUEUE, null, 0,
                null, null, MisfirePolicy.FIRE_ONCE);
    }

    public ScheduleDefinition withZone(ZoneId zone) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }

    public ScheduleDefinition withQueue(String queue) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }

    public ScheduleDefinition withPayload(Object payload) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }

    public ScheduleDefinition withPriority(int priority) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }

    public ScheduleDefinition withMaxAttempts(Integer maxAttempts) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }

    public ScheduleDefinition withTimeout(Duration timeout) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }

    public ScheduleDefinition withMisfirePolicy(MisfirePolicy misfirePolicy) {
        return new ScheduleDefinition(name, cron, zone, jobType, queue, payload, priority, maxAttempts, timeout, misfirePolicy);
    }
}
