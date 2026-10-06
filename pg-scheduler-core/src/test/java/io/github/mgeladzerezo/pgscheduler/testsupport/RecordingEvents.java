package io.github.mgeladzerezo.pgscheduler.testsupport;

import io.github.mgeladzerezo.pgscheduler.metrics.SchedulerEvents;
import io.github.mgeladzerezo.pgscheduler.store.ClaimedJob;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** Counts what a node reports, so tests can assert on fencing and lease expiry as the node saw them. */
public final class RecordingEvents implements SchedulerEvents {

    public final AtomicInteger started = new AtomicInteger();
    public final AtomicInteger succeeded = new AtomicInteger();
    public final AtomicInteger failed = new AtomicInteger();
    public final AtomicInteger fenced = new AtomicInteger();
    public final AtomicInteger released = new AtomicInteger();
    public final AtomicInteger leasesExpired = new AtomicInteger();
    public final List<Long> waitMillis = new CopyOnWriteArrayList<>();

    @Override
    public void jobStarted(ClaimedJob job) {
        started.incrementAndGet();
        waitMillis.add(job.waitMs());
    }

    @Override
    public void jobSucceeded(ClaimedJob job, Duration duration) {
        succeeded.incrementAndGet();
    }

    @Override
    public void jobFailed(ClaimedJob job, Duration duration, boolean dead, boolean timedOut) {
        failed.incrementAndGet();
    }

    @Override
    public void completionFenced(ClaimedJob job) {
        fenced.incrementAndGet();
    }

    @Override
    public void jobReleased(ClaimedJob job) {
        released.incrementAndGet();
    }

    @Override
    public void leaseExpired(String queue, String jobType, boolean dead) {
        leasesExpired.incrementAndGet();
    }
}
