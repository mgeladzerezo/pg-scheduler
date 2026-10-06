package io.github.mgeladzerezo.pgscheduler.autoconfigure;

import io.github.mgeladzerezo.pgscheduler.JobState;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.QueueView;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@code pgscheduler.queue.depth} gauges (tags queue, state) for unfinished and dead-lettered jobs.
 *
 * <p>The numbers come from one query that is refreshed on a timer, never from the gauge callbacks: a scrape
 * must not cost a database round trip per series, and a database outage must not make the scrape fail.
 * A queue's gauges appear the first time the refresh sees the queue.
 */
public final class QueueDepthMetrics implements MeterBinder, AutoCloseable {

    private static final List<JobState> STATES =
            List.of(JobState.READY, JobState.SCHEDULED, JobState.RUNNING, JobState.FAILED, JobState.DEAD);

    private final SchedulerAdmin admin;
    private final Duration refreshInterval;
    private final AtomicReference<List<QueueView>> latest = new AtomicReference<>(List.of());
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private ScheduledExecutorService executor;

    public QueueDepthMetrics(SchedulerAdmin admin, Duration refreshInterval) {
        this.admin = admin;
        this.refreshInterval = refreshInterval;
    }

    @Override
    public synchronized void bindTo(MeterRegistry registry) {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().daemon().name("pgs-metrics").unstarted(r));
        executor.scheduleWithFixedDelay(() -> refresh(registry), 0, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void refresh(MeterRegistry registry) {
        try {
            List<QueueView> views = admin.queues();
            latest.set(views);
            for (QueueView view : views) {
                String queue = view.name();
                for (JobState state : STATES) {
                    if (registered.add(queue + "/" + state)) {
                        Gauge.builder("pgscheduler.queue.depth", () -> depth(queue, state))
                                .tags("queue", queue, "state", state.name()).register(registry);
                    }
                }
            }
        } catch (RuntimeException e) {
            // Keep the last known values; the next refresh tries again.
        }
    }

    private double depth(String queue, JobState state) {
        return latest.get().stream().filter(v -> v.name().equals(queue)).findFirst()
                .map(v -> (double) v.counts().getOrDefault(state, 0L)).orElse(0.0);
    }

    @Override
    public synchronized void close() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }
}
