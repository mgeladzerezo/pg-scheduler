package io.github.mgeladzerezo.pgscheduler.worker;

import io.github.mgeladzerezo.pgscheduler.metrics.SchedulerEvents;
import io.github.mgeladzerezo.pgscheduler.store.MaintenanceDao;
import io.github.mgeladzerezo.pgscheduler.store.MaintenanceDao.ReapedJob;
import io.github.mgeladzerezo.pgscheduler.store.StatsDao;
import io.github.mgeladzerezo.pgscheduler.store.StatsDao.StatDelta;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The per-node housekeeping loop: promotes due jobs to READY, reaps expired leases, and purges old rows.
 *
 * <p>Every node runs one and none is special. The statements take their rows with {@code SKIP LOCKED}, so
 * the nodes share the work, and if all but one node die the survivor does all of it. That is what makes
 * crash recovery independent of any particular process: a dead worker's jobs are reaped by whoever is
 * still alive.
 *
 * <p>The loop sleeps until the earliest waiting job is due (at most {@code interval}), and is woken early
 * when a job with a nearer time is inserted, so delayed jobs and retries become READY on time without the
 * loop spinning.
 */
public final class Maintenance {

    /**
     * @param interval          longest sleep between passes
     * @param reapInterval      how often expired leases are looked for
     * @param batchSize         rows handled per statement
     * @param finishedRetention how long SUCCEEDED and CANCELLED jobs are kept; {@code null} keeps them forever
     * @param workerSilence     how long a silent worker stays in the registry before its row is removed
     */
    public record Options(Duration interval, Duration reapInterval, int batchSize, Duration finishedRetention,
                          Duration workerSilence) {

        public static Options defaults() {
            return new Options(Duration.ofSeconds(1), Duration.ofSeconds(1), 500, Duration.ofDays(7), Duration.ofMinutes(5));
        }

        public Options withIntervals(Duration interval, Duration reapInterval) {
            return new Options(interval, reapInterval, batchSize, finishedRetention, workerSilence);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Maintenance.class);
    private static final long HOUSEKEEPING_INTERVAL_NANOS = Duration.ofSeconds(30).toNanos();
    private static final Duration STATS_RETENTION = Duration.ofHours(24);

    private final MaintenanceDao dao;
    private final StatsDao statsDao;
    private final Options options;
    private final SchedulerEvents events;
    private final Object monitor = new Object();
    private boolean woken;
    private volatile boolean running;
    private Thread thread;

    public Maintenance(MaintenanceDao dao, StatsDao statsDao, Options options, SchedulerEvents events) {
        this.dao = dao;
        this.statsDao = statsDao;
        this.options = options;
        this.events = events;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().daemon().name("pgs-maintenance").start(this::loop);
    }

    public synchronized void stop() {
        running = false;
        wake();
        if (thread != null) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    /** Asks the loop to run a pass now; called when a job with a future run time was inserted. */
    public void wake() {
        synchronized (monitor) {
            woken = true;
            monitor.notifyAll();
        }
    }

    private void loop() {
        long lastReap = System.nanoTime() - options.reapInterval().toNanos();
        long lastHousekeeping = System.nanoTime();
        while (running) {
            long sleepMs = options.interval().toMillis();
            try {
                promoteDue();
                long now = System.nanoTime();
                if (now - lastReap >= options.reapInterval().toNanos()) {
                    lastReap = now;
                    reapExpiredLeases();
                }
                if (now - lastHousekeeping >= HOUSEKEEPING_INTERVAL_NANOS) {
                    lastHousekeeping = now;
                    housekeeping();
                }
                OptionalLong untilNextDue = dao.millisUntilNextDue();
                if (untilNextDue.isPresent()) {
                    // At least 1 ms: a job that is due but locked by another node's pass must not make us spin.
                    sleepMs = Math.max(1, Math.min(sleepMs, untilNextDue.getAsLong()));
                }
                sleepMs = Math.min(sleepMs, options.reapInterval().toMillis());
            } catch (RuntimeException e) {
                log.warn("Maintenance pass failed, retrying in {} ms: {}", sleepMs, e.toString());
            }
            synchronized (monitor) {
                if (running && !woken) {
                    try {
                        monitor.wait(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                woken = false;
            }
        }
    }

    /** One promotion pass; public so tests can drive it deterministically. */
    public int promoteDue() {
        int total = 0;
        int promoted;
        do {
            promoted = dao.promoteDue(options.batchSize());
            total += promoted;
        } while (promoted == options.batchSize());
        return total;
    }

    /** One reaper pass; public so tests can drive it deterministically. */
    public List<ReapedJob> reapExpiredLeases() {
        List<ReapedJob> reaped = dao.reapExpiredLeases(options.batchSize());
        if (reaped.isEmpty()) {
            return reaped;
        }
        for (ReapedJob job : reaped) {
            log.warn("Reaped job {} ({}) from worker {}: lease expired, {}", job.id(), job.type(), job.workerId(),
                    job.dead() ? "no attempts left, dead-lettered" : "returned to READY");
            events.leaseExpired(job.queue(), job.type(), job.dead());
        }
        Map<String, Long> perQueue = reaped.stream().collect(Collectors.groupingBy(ReapedJob::queue, Collectors.counting()));
        try {
            statsDao.add(perQueue.entrySet().stream()
                    .map(e -> new StatDelta(e.getKey(), "LEASE_EXPIRED", e.getValue())).toList());
        } catch (RuntimeException e) {
            log.debug("Could not record lease expirations in the roll-up: {}", e.toString());
        }
        return reaped;
    }

    private void housekeeping() {
        if (options.finishedRetention() != null) {
            int purged;
            int rounds = 0;
            do {
                purged = dao.purgeFinished(options.finishedRetention(), options.batchSize());
            } while (purged == options.batchSize() && ++rounds < 20 && running);
        }
        dao.purgeStats(STATS_RETENTION);
        dao.pruneWorkers(options.workerSilence());
    }
}
