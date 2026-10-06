package io.github.mgeladzerezo.pgscheduler.metrics;

import io.github.mgeladzerezo.pgscheduler.store.ClaimedJob;
import java.time.Duration;

/**
 * Callbacks for what happens to jobs on this node. The library reports through this interface so that it
 * works without Micrometer on the classpath; {@link MicrometerSchedulerEvents} turns the callbacks into
 * meters. All methods are called on worker or maintenance threads and must be quick and must not throw.
 */
public interface SchedulerEvents {

    /** Does nothing. */
    SchedulerEvents NONE = new SchedulerEvents() {
    };

    /** A claimed job is about to be handed to its handler; {@code job.waitMs()} is its queue latency. */
    default void jobStarted(ClaimedJob job) {
    }

    default void jobSucceeded(ClaimedJob job, Duration duration) {
    }

    /**
     * @param dead     the job was dead-lettered rather than scheduled for a retry
     * @param timedOut the attempt failed because it exceeded the job timeout
     */
    default void jobFailed(ClaimedJob job, Duration duration, boolean dead, boolean timedOut) {
    }

    /** This worker tried to record a result for an attempt it no longer owned; the write was rejected. */
    default void completionFenced(ClaimedJob job) {
    }

    /** This worker returned an unfinished attempt to the queue while shutting down. */
    default void jobReleased(ClaimedJob job) {
    }

    /**
     * This node's reaper took a job back from a worker whose lease expired.
     *
     * @param dead the job had no attempts left and was dead-lettered
     */
    default void leaseExpired(String queue, String jobType, boolean dead) {
    }
}
