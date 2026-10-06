package io.github.mgeladzerezo.pgscheduler.cron;

import java.time.Instant;

/**
 * A stored cron schedule.
 *
 * @param maxAttempts  attempts for the jobs it produces, or {@code null} for the job type's default
 * @param timeoutMs    handler timeout for the jobs it produces, or {@code null} for the job type's default
 * @param nextFireTime the next fire time that has not been processed yet; {@code null} if there is none
 * @param lastFireTime the most recent fire time for which a job was enqueued
 */
public record Schedule(long id, String name, String cron, String zone, String jobType, String queue,
                       String payloadJson, int priority, Integer maxAttempts, Long timeoutMs,
                       MisfirePolicy misfirePolicy, boolean paused, Instant nextFireTime, Instant lastFireTime,
                       Instant createdAt, Instant updatedAt) {
}
