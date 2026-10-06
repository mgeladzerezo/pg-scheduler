package io.github.mgeladzerezo.pgscheduler.worker;

import io.github.mgeladzerezo.pgscheduler.JobContext;
import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobPolicy;
import io.github.mgeladzerezo.pgscheduler.JobTimeoutException;
import io.github.mgeladzerezo.pgscheduler.RateLimit;
import io.github.mgeladzerezo.pgscheduler.metrics.SchedulerEvents;
import io.github.mgeladzerezo.pgscheduler.store.ClaimedJob;
import io.github.mgeladzerezo.pgscheduler.store.JobSpec;
import io.github.mgeladzerezo.pgscheduler.store.StatsDao;
import io.github.mgeladzerezo.pgscheduler.store.StatsDao.StatDelta;
import io.github.mgeladzerezo.pgscheduler.store.WorkerDao;
import io.github.mgeladzerezo.pgscheduler.store.WorkerDao.QueueSettings;
import io.github.mgeladzerezo.pgscheduler.store.WorkerDao.ThrottledClaim;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

/**
 * Claims jobs and runs their handlers.
 *
 * <p>One poller thread claims in batches whenever slots are free and it is woken: by a NOTIFY for one of
 * its queues, by a handler finishing, or by the poll interval. Each claimed job runs on its own virtual
 * thread; a semaphore per queue bounds how many run at once. A heartbeat extends the leases of running
 * jobs, and every write that ends an attempt is fenced on (worker id, attempt), so this worker can never
 * record a result for a job it no longer owns.
 *
 * <p><b>Ownership rules this class keeps.</b>
 * <ul>
 *   <li>An attempt is settled exactly once locally ({@link JobRun#settled}): by its handler returning, by
 *       the timeout watchdog, or by shutdown releasing it. Whoever wins the flag writes to the database.</li>
 *   <li>A slot is given back only when the handler thread has really ended. A handler that ignores its
 *       interrupt after a timeout keeps occupying its slot, because it is still using this JVM.</li>
 *   <li>If the heartbeat cannot reach the database for longer than a lease, the worker assumes it has lost
 *       everything it was running and interrupts the handlers, without waiting to be told.</li>
 * </ul>
 */
public final class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int MAX_ERROR_CHARS = 16_000;

    private final String workerId;
    private final WorkerOptions options;
    private final WorkerDao dao;
    private final StatsDao statsDao;
    private final HandlerRegistry handlers;
    private final JobPolicies policies;
    private final ObjectMapper json;
    private final Continuations continuations;
    private final SchedulerEvents events;

    private final List<QueueSlot> slots = new ArrayList<>();
    private final Map<Long, JobRun> inFlight = new ConcurrentHashMap<>();
    private final Map<StatKey, LongAdder> stats = new ConcurrentHashMap<>();
    private final WakeSignal wake = new WakeSignal();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private volatile boolean claiming;
    private volatile Map<String, QueueSettings> queueSettings = Map.of();
    private volatile long lastHeartbeatOkNanos;
    private String[] unlimitedTypes = new String[0];
    private Map<String, RateLimit> rateLimitedTypes = Map.of();
    private Thread poller;
    private ScheduledExecutorService timer;

    /** One served queue: its name and the permits that bound local concurrency. */
    private record QueueSlot(String queue, Semaphore permits) {
    }

    private record StatKey(String queue, String outcome) {
    }

    public Worker(String workerId, WorkerOptions options, WorkerDao dao, StatsDao statsDao, HandlerRegistry handlers,
                  JobPolicies policies, ObjectMapper json, SchedulerEvents events) {
        this.workerId = workerId;
        this.options = options;
        this.dao = dao;
        this.statsDao = statsDao;
        this.handlers = handlers;
        this.policies = policies;
        this.json = json;
        this.continuations = new Continuations(json, policies);
        this.events = events;
        options.queues().forEach((queue, concurrency) -> slots.add(new QueueSlot(queue, new Semaphore(concurrency))));
    }

    /** A worker id that is unique per JVM instance and readable in the dashboard: {@code host-xxxxxx}. */
    public static String generateId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "worker";
        }
        return host + "-" + Long.toString(ThreadLocalRandom.current().nextLong(36L * 36 * 36 * 36 * 36, 36L * 36 * 36 * 36 * 36 * 36), 36);
    }

    public String workerId() {
        return workerId;
    }

    /** Number of jobs whose handler thread is currently alive on this worker. */
    public int inFlightCount() {
        return inFlight.size();
    }

    /** Wakes the poller if it serves {@code queue} ({@code null} wakes it unconditionally). */
    public void wake(String queue) {
        if (queue == null || options.queues().containsKey(queue)) {
            wake.signal();
        }
    }

    // ---- lifecycle --------------------------------------------------------------------------------------

    /**
     * Registers the worker and starts polling. The set of job types is fixed here: the worker only ever
     * claims types it has a handler for, so jobs of other types stay READY for a node that does.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        List<String> unlimited = new ArrayList<>();
        Map<String, RateLimit> limited = new TreeMap<>();
        for (String type : handlers.jobTypes()) {
            RateLimit limit = policies.forType(type).rateLimit();
            if (limit == null) {
                unlimited.add(type);
            } else {
                limited.put(type, limit);
                dao.upsertRateLimit(type, limit.permitsPerSecond(), limit.burst());
            }
        }
        unlimitedTypes = unlimited.toArray(String[]::new);
        rateLimitedTypes = limited;
        if (handlers.jobTypes().isEmpty()) {
            log.warn("Worker {} has no job handlers registered and will not claim anything", workerId);
        }

        dao.ensureQueues(List.copyOf(options.queues().keySet()));
        register();
        queueSettings = dao.queueSettings();
        lastHeartbeatOkNanos = System.nanoTime();

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2,
                Thread.ofPlatform().daemon().name("pgs-timer-" + workerId + "-", 0).factory());
        // Nearly every job cancels its timeout task; do not let the cancelled ones pile up in the queue.
        executor.setRemoveOnCancelPolicy(true);
        timer = executor;
        long heartbeatMs = options.heartbeatInterval().toMillis();
        timer.scheduleWithFixedDelay(this::heartbeat, heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(this::everySecond, 1, 1, TimeUnit.SECONDS);

        claiming = true;
        poller = Thread.ofPlatform().daemon().name("pgs-poller-" + workerId).start(this::pollLoop);
        log.info("Worker {} started: queues {}, job types {}", workerId, options.queues(), handlers.jobTypes());
    }

    private void register() {
        dao.registerWorker(workerId, hostname(), json.writeValueAsString(new LinkedHashMap<>(options.queues())),
                handlers.jobTypes().stream().sorted().toArray(String[]::new), options.leaseDuration().toMillis());
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Graceful shutdown: stop claiming, give running handlers {@code shutdownTimeout} to finish (their
     * leases keep being extended meanwhile), then interrupt what is left and release those jobs so another
     * worker can start them immediately instead of waiting for the lease to expire.
     */
    public void stop() {
        if (!started.get() || !stopped.compareAndSet(false, true)) {
            return;
        }
        claiming = false;
        wake.signal();
        quietly("mark worker stopping", () -> dao.markWorkerStopping(workerId));
        joinQuietly(poller);

        long deadline = System.nanoTime() + options.shutdownTimeout().toNanos();
        while (!inFlight.isEmpty() && System.nanoTime() < deadline) {
            sleepQuietly(20);
        }
        for (JobRun run : inFlight.values()) {
            if (run.settled.compareAndSet(false, true)) {
                log.warn("Releasing job {} ({}) back to the queue: handler still running at shutdown",
                        run.job.id(), run.job.type());
                quietly("release job " + run.job.id(), () -> dao.release(run.job, workerId));
                events.jobReleased(run.job);
            }
            run.interrupt();
        }

        timer.shutdownNow();
        quietly("flush stats", this::flushStats);
        quietly("deregister worker", () -> dao.removeWorker(workerId));
        log.info("Worker {} stopped", workerId);
    }

    // ---- claiming ---------------------------------------------------------------------------------------

    private void pollLoop() {
        int consecutiveErrors = 0;
        while (claiming) {
            long waitMs = options.pollInterval().toMillis();
            try {
                for (QueueSlot slot : slots) {
                    waitMs = Math.min(waitMs, pollQueue(slot));
                }
                consecutiveErrors = 0;
            } catch (RuntimeException e) {
                consecutiveErrors++;
                waitMs = Math.min(30_000, 500L << Math.min(consecutiveErrors, 6));
                log.warn("Worker {} could not claim jobs (attempt {}), retrying in {} ms: {}",
                        workerId, consecutiveErrors, waitMs, e.toString());
            }
            if (claiming && waitMs > 0) {
                wake.await(waitMs);
            }
        }
    }

    /**
     * Claims for one queue as many jobs as it has free slots (at most one batch).
     *
     * @return how long the poller may sleep as far as this queue is concerned
     */
    private long pollQueue(QueueSlot slot) {
        int free = slot.permits.drainPermits();
        if (free == 0) {
            return Long.MAX_VALUE;
        }
        int budget = Math.min(free, options.batchSize());
        int taken = 0;
        long waitHint = Long.MAX_VALUE;
        QueueSettings settings = queueSettings.get(slot.queue);
        boolean clusterLimited = settings != null && settings.maxConcurrency() != null;
        long leaseMs = options.leaseDuration().toMillis();
        try {
            // Rate-limited types first: they are bounded by their bucket anyway, and claiming them last
            // would let a steady stream of unlimited jobs starve them of slots.
            for (Map.Entry<String, RateLimit> limited : rateLimitedTypes.entrySet()) {
                if (taken == budget) {
                    break;
                }
                ThrottledClaim claim = dao.claimRateLimited(slot.queue, limited.getKey(), budget - taken, workerId,
                        leaseMs, clusterLimited);
                for (ClaimedJob job : claim.jobs()) {
                    dispatch(slot, job);
                    taken++;
                }
                if (claim.throttled()) {
                    // Come back when the bucket holds the next token.
                    waitHint = Math.min(waitHint, Math.max(10, (long) (1000 / limited.getValue().permitsPerSecond())));
                }
            }
            if (taken < budget && unlimitedTypes.length > 0) {
                int asked = budget - taken;
                List<ClaimedJob> jobs = dao.claim(slot.queue, unlimitedTypes, asked, workerId, leaseMs, clusterLimited);
                for (ClaimedJob job : jobs) {
                    dispatch(slot, job);
                    taken++;
                }
                if (jobs.size() == asked && free > budget) {
                    waitHint = 0; // a full batch and slots to spare: more is probably waiting
                }
            }
        } finally {
            slot.permits.release(free - taken);
        }
        return waitHint;
    }

    private void dispatch(QueueSlot slot, ClaimedJob job) {
        JobRun run = new JobRun(job, slot);
        inFlight.put(job.id(), run);
        events.jobStarted(job);
        run.thread = Thread.ofVirtual().name("pgs-job-" + job.id()).unstarted(() -> execute(run));
        run.timeout = timer.schedule(() -> onTimeout(run), job.timeoutMs(), TimeUnit.MILLISECONDS);
        run.thread.start();
    }

    // ---- running ----------------------------------------------------------------------------------------

    private void execute(JobRun run) {
        ClaimedJob job = run.job;
        Throwable failure = null;
        Object result = null;
        try {
            HandlerRegistry.Registration handler = handlers.find(job.type());
            if (handler == null) {
                throw new IllegalStateException("No handler registered for job type '" + job.type() + "'");
            }
            Object payload = handler.payloadType() == null ? null
                    : json.readValue(job.payloadJson(), json.constructType(handler.payloadType()));
            Object returned = handler.invoker().invoke(payload, run);
            result = run.result != null ? run.result : returned;
        } catch (Throwable t) {
            failure = t;
        } finally {
            run.timeout.cancel(false);
            // Clear a pending interrupt (timeout, lost lease) so it cannot break the bookkeeping below.
            Thread.interrupted();
        }
        try {
            settle(run, failure, result);
        } finally {
            inFlight.remove(job.id(), run);
            run.slot.permits.release();
            wake.signal();
        }
    }

    /**
     * Runs on a timer thread when a job exceeds its timeout. The attempt is recorded as timed out right
     * away, on a separate thread, instead of when the handler reacts to the interrupt: a handler that
     * swallows interrupts must not be able to hold up the retry.
     */
    private void onTimeout(JobRun run) {
        if (run.settled.get()) {
            return;
        }
        Duration timeout = Duration.ofMillis(run.job.timeoutMs());
        log.warn("Job {} ({}) exceeded its timeout of {}; interrupting the handler", run.job.id(), run.job.type(), timeout);
        run.timedOut = true;
        Thread.startVirtualThread(() -> settle(run, new JobTimeoutException(run.job.id(), timeout), null));
        run.interrupt();
    }

    /** Records the outcome of an attempt, once. Later callers for the same attempt return immediately. */
    private void settle(JobRun run, Throwable failure, Object result) {
        if (!run.settled.compareAndSet(false, true)) {
            return;
        }
        ClaimedJob job = run.job;
        Duration duration = Duration.ofNanos(System.nanoTime() - run.startedNanos);
        if (failure != null && run.timedOut && !(failure instanceof JobTimeoutException)) {
            // The handler reacted to the timeout's interrupt before the watchdog got here. Whatever it
            // threw on the way out (usually InterruptedException), the attempt failed because it timed out.
            JobTimeoutException timeout = new JobTimeoutException(job.id(), Duration.ofMillis(job.timeoutMs()));
            timeout.addSuppressed(failure);
            failure = timeout;
        }

        String resultJson = null;
        if (failure == null && result != null) {
            try {
                resultJson = json.writeValueAsString(result);
            } catch (RuntimeException e) {
                failure = new IllegalStateException("The handler succeeded but its result could not be serialised", e);
            }
        }
        JobSpec continuation = null;
        if (failure == null) {
            try {
                continuation = continuations.next(job);
            } catch (RuntimeException e) {
                failure = new IllegalStateException("The stored continuation of job " + job.id() + " is unreadable", e);
            }
        }

        if (failure == null) {
            String finalResult = resultJson;
            JobSpec next = continuation;
            Boolean recorded = writeWithRetry(job, () -> dao.complete(job, workerId, finalResult, next));
            if (Boolean.TRUE.equals(recorded)) {
                count(job.queue(), "SUCCEEDED");
                events.jobSucceeded(job, duration);
            } else if (recorded != null) {
                fenced(job, "success");
            }
            return;
        }

        JobPolicy policy = policies.forType(job.type());
        boolean timedOut = failure instanceof JobTimeoutException;
        boolean dead = job.attempt() >= job.maxAttempts() || !policy.isRetryable(failure);
        long retryDelayMs = dead ? 0 : policy.backoff().delay(job.attempt(), ThreadLocalRandom.current()).toMillis();
        String error = stackTrace(failure);
        Boolean recorded = writeWithRetry(job, () ->
                dao.fail(job, workerId, timedOut ? "TIMED_OUT" : "FAILED", dead, retryDelayMs, error));
        if (Boolean.TRUE.equals(recorded)) {
            count(job.queue(), dead ? "DEAD" : "FAILED");
            events.jobFailed(job, duration, dead, timedOut);
            if (dead) {
                log.warn("Job {} ({}) dead-lettered after attempt {} of {}: {}", job.id(), job.type(),
                        job.attempt(), job.maxAttempts(), failure.toString());
            } else {
                log.info("Job {} ({}) failed attempt {} of {}, retrying in {} ms: {}", job.id(), job.type(),
                        job.attempt(), job.maxAttempts(), retryDelayMs, failure.toString());
            }
        } else if (recorded != null) {
            fenced(job, "failure");
        }
    }

    /**
     * Tries a fenced write a few times, because a blip of the database should not cost a finished job its
     * result while the lease is still being held.
     *
     * @return the write's answer (true: recorded, false: fenced off), or {@code null} if the database stayed
     *         unreachable; in that case the lease will expire and the job will run again
     */
    private Boolean writeWithRetry(ClaimedJob job, java.util.function.BooleanSupplier write) {
        long[] pauses = {100, 500, 2_000};
        for (int i = 0; ; i++) {
            try {
                return write.getAsBoolean();
            } catch (RuntimeException e) {
                if (i == pauses.length) {
                    log.error("Could not record the outcome of job {} ({}) attempt {}; it will be retried by "
                            + "whichever worker claims it after the lease expires", job.id(), job.type(), job.attempt(), e);
                    return null;
                }
                sleepQuietly(pauses[i]);
            }
        }
    }

    private void fenced(ClaimedJob job, String what) {
        log.warn("Job {} ({}) attempt {}: {} reported by {} was rejected, the attempt is no longer owned by this "
                + "worker (lease expired, job cancelled or released)", job.id(), job.type(), job.attempt(), what, workerId);
        events.completionFenced(job);
    }

    // ---- heartbeat --------------------------------------------------------------------------------------

    private void heartbeat() {
        List<JobRun> active = inFlight.values().stream().filter(run -> !run.settled.get()).toList();
        try {
            Set<Long> owned = dao.heartbeat(workerId, active.stream().map(run -> run.job).toList(),
                    options.leaseDuration().toMillis());
            lastHeartbeatOkNanos = System.nanoTime();
            for (JobRun run : active) {
                if (!owned.contains(run.job.id()) && !run.settled.get() && run.leaseLost.compareAndSet(false, true)) {
                    log.warn("Job {} ({}) attempt {} is no longer leased to {}; interrupting its handler",
                            run.job.id(), run.job.type(), run.job.attempt(), workerId);
                    run.interrupt();
                }
            }
            if (!dao.touchWorker(workerId) && !stopped.get()) {
                register();
            }
        } catch (RuntimeException e) {
            long silentNanos = System.nanoTime() - lastHeartbeatOkNanos;
            log.warn("Worker {} heartbeat failed ({} ms since the last success): {}", workerId,
                    TimeUnit.NANOSECONDS.toMillis(silentNanos), e.toString());
            if (silentNanos > options.leaseDuration().toNanos()) {
                // No proof of ownership for a whole lease: other workers may already be running these jobs.
                for (JobRun run : active) {
                    if (!run.settled.get() && run.leaseLost.compareAndSet(false, true)) {
                        log.warn("Job {} ({}): lease presumed lost after {} ms without a heartbeat; interrupting",
                                run.job.id(), run.job.type(), TimeUnit.NANOSECONDS.toMillis(silentNanos));
                        run.interrupt();
                    }
                }
            }
        }
    }

    /** Housekeeping that should not wait for the heartbeat interval. */
    private void everySecond() {
        try {
            flushStats();
            queueSettings = dao.queueSettings();
        } catch (RuntimeException e) {
            log.debug("Worker {} periodic refresh failed: {}", workerId, e.toString());
        }
    }

    private void count(String queue, String outcome) {
        stats.computeIfAbsent(new StatKey(queue, outcome), key -> new LongAdder()).increment();
    }

    private void flushStats() {
        List<StatDelta> deltas = new ArrayList<>();
        stats.forEach((key, adder) -> {
            long n = adder.sumThenReset();
            if (n > 0) {
                deltas.add(new StatDelta(key.queue(), key.outcome(), n));
            }
        });
        try {
            statsDao.add(deltas);
        } catch (RuntimeException e) {
            // Put the counts back so that a short outage does not lose them.
            deltas.forEach(d -> stats.computeIfAbsent(new StatKey(d.queue(), d.outcome()), key -> new LongAdder()).add(d.count()));
            throw e;
        }
    }

    // ---- helpers ----------------------------------------------------------------------------------------

    private static String stackTrace(Throwable failure) {
        StringWriter writer = new StringWriter();
        failure.printStackTrace(new PrintWriter(writer));
        String text = writer.toString();
        return text.length() <= MAX_ERROR_CHARS ? text : text.substring(0, MAX_ERROR_CHARS) + "\n... (truncated)";
    }

    private void quietly(String what, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            log.warn("Worker {} could not {}: {}", workerId, what, e.toString());
        }
    }

    private static void joinQuietly(Thread thread) {
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** A monitor-based "something changed" flag; signals that arrive while nobody waits are not lost. */
    private static final class WakeSignal {
        private boolean signalled;

        synchronized void signal() {
            signalled = true;
            notifyAll();
        }

        synchronized void await(long millis) {
            try {
                if (!signalled) {
                    wait(millis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            signalled = false;
        }
    }

    /** One attempt running on this worker; also the {@link JobContext} its handler sees. */
    private final class JobRun implements JobContext {
        private final ClaimedJob job;
        private final QueueSlot slot;
        private final long startedNanos = System.nanoTime();
        private final AtomicBoolean settled = new AtomicBoolean();
        private final AtomicBoolean leaseLost = new AtomicBoolean();
        private volatile boolean timedOut;
        private volatile Thread thread;
        private volatile ScheduledFuture<?> timeout;
        private volatile Object result;

        JobRun(ClaimedJob job, QueueSlot slot) {
            this.job = job;
            this.slot = slot;
        }

        void interrupt() {
            Thread t = thread;
            if (t != null) {
                t.interrupt();
            }
        }

        @Override
        public long jobId() {
            return job.id();
        }

        @Override
        public String jobType() {
            return job.type();
        }

        @Override
        public String queue() {
            return job.queue();
        }

        @Override
        public int attempt() {
            return job.attempt();
        }

        @Override
        public int maxAttempts() {
            return job.maxAttempts();
        }

        @Override
        public String workerId() {
            return workerId;
        }

        @Override
        public Instant createdAt() {
            return job.createdAt();
        }

        @Override
        public String uniqueKey() {
            return job.uniqueKey();
        }

        @Override
        public Long parentJobId() {
            return job.parentId();
        }

        @Override
        public void setResult(Object value) {
            this.result = value;
        }
    }
}
