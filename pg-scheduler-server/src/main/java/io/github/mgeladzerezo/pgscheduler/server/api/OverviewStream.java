package io.github.mgeladzerezo.pgscheduler.server.api;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.QueueView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.ThroughputPoint;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.WorkerView;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Pushes the overview to every connected dashboard once a second.
 *
 * <p>One snapshot is computed per tick and shared by all subscribers, and nothing is computed while nobody
 * is connected, so the cost of the live view does not grow with the number of open browser tabs.
 */
@Component
public class OverviewStream {

    /** Everything the overview page shows. */
    public record Overview(Instant now, List<QueueView> queues, List<WorkerView> workers,
                           List<ThroughputPoint> throughput, Map<String, Long> parkedTypes) {
    }

    private static final Logger log = LoggerFactory.getLogger(OverviewStream.class);
    private static final Duration WINDOW = Duration.ofMinutes(5);

    private final SchedulerAdmin admin;
    private final List<SseEmitter> subscribers = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().daemon().name("overview-stream").unstarted(r));

    public OverviewStream(SchedulerAdmin admin) {
        this.admin = admin;
        executor.scheduleWithFixedDelay(this::tick, 1, 1, TimeUnit.SECONDS);
    }

    public Overview snapshot() {
        return new Overview(Instant.now(), admin.queues(), admin.workers(), admin.throughput(WINDOW), admin.parkedJobTypes());
    }

    /** Registers a subscriber and sends it the current snapshot straight away. */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(Duration.ofMinutes(30).toMillis());
        subscribers.add(emitter);
        emitter.onCompletion(() -> subscribers.remove(emitter));
        emitter.onTimeout(() -> subscribers.remove(emitter));
        emitter.onError(e -> subscribers.remove(emitter));
        executor.execute(() -> send(emitter, safeSnapshot()));
        return emitter;
    }

    private void tick() {
        if (subscribers.isEmpty()) {
            return;
        }
        Overview overview = safeSnapshot();
        if (overview != null) {
            subscribers.forEach(emitter -> send(emitter, overview));
        }
    }

    private Overview safeSnapshot() {
        try {
            return snapshot();
        } catch (RuntimeException e) {
            log.warn("Could not build the overview: {}", e.toString());
            return null;
        }
    }

    private void send(SseEmitter emitter, Overview overview) {
        if (overview == null) {
            return;
        }
        try {
            emitter.send(SseEmitter.event().name("overview").data(overview));
        } catch (IOException | IllegalStateException e) {
            subscribers.remove(emitter);
        }
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
        subscribers.forEach(SseEmitter::complete);
    }
}
