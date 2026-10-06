package io.github.mgeladzerezo.pgscheduler.metrics;

import io.github.mgeladzerezo.pgscheduler.store.ClaimedJob;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;

/**
 * Publishes job events as Micrometer meters.
 *
 * <ul>
 *   <li>{@code pgscheduler.job.latency} (timer, tags queue, type): from the moment a job was eligible to run
 *       to the moment its handler started</li>
 *   <li>{@code pgscheduler.job.duration} (timer, tags queue, type, outcome): handler run time</li>
 *   <li>{@code pgscheduler.job.failures} (counter, tags queue, type, result): failed attempts; result is
 *       {@code retry}, {@code dead} or {@code timeout}</li>
 *   <li>{@code pgscheduler.lease.expirations} (counter, tags queue, type, result): jobs taken back from a
 *       worker whose lease ran out</li>
 *   <li>{@code pgscheduler.completions.fenced} (counter): results rejected because the lease was lost</li>
 *   <li>{@code pgscheduler.jobs.released} (counter): jobs given back during graceful shutdown</li>
 * </ul>
 */
public final class MicrometerSchedulerEvents implements SchedulerEvents {

    private final MeterRegistry registry;

    public MicrometerSchedulerEvents(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void jobStarted(ClaimedJob job) {
        Timer.builder("pgscheduler.job.latency").tags("queue", job.queue(), "type", job.type())
                .register(registry).record(Duration.ofMillis(Math.max(0, job.waitMs())));
    }

    @Override
    public void jobSucceeded(ClaimedJob job, Duration duration) {
        duration(job, "success").record(duration);
    }

    @Override
    public void jobFailed(ClaimedJob job, Duration duration, boolean dead, boolean timedOut) {
        duration(job, "failure").record(duration);
        String result = timedOut ? "timeout" : dead ? "dead" : "retry";
        registry.counter("pgscheduler.job.failures", "queue", job.queue(), "type", job.type(), "result", result)
                .increment();
    }

    @Override
    public void completionFenced(ClaimedJob job) {
        registry.counter("pgscheduler.completions.fenced").increment();
    }

    @Override
    public void jobReleased(ClaimedJob job) {
        registry.counter("pgscheduler.jobs.released").increment();
    }

    @Override
    public void leaseExpired(String queue, String jobType, boolean dead) {
        registry.counter("pgscheduler.lease.expirations", "queue", queue, "type", jobType,
                "result", dead ? "dead" : "requeued").increment();
    }

    private Timer duration(ClaimedJob job, String outcome) {
        return Timer.builder("pgscheduler.job.duration")
                .tags("queue", job.queue(), "type", job.type(), "outcome", outcome).register(registry);
    }
}
