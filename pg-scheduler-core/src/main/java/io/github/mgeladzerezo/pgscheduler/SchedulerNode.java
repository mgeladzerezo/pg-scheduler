package io.github.mgeladzerezo.pgscheduler;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin;
import io.github.mgeladzerezo.pgscheduler.cron.CronScheduler;
import io.github.mgeladzerezo.pgscheduler.metrics.SchedulerEvents;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.store.EnqueueDao;
import io.github.mgeladzerezo.pgscheduler.store.MaintenanceDao;
import io.github.mgeladzerezo.pgscheduler.store.ScheduleDao;
import io.github.mgeladzerezo.pgscheduler.store.StatsDao;
import io.github.mgeladzerezo.pgscheduler.store.WorkerDao;
import io.github.mgeladzerezo.pgscheduler.worker.HandlerRegistry;
import io.github.mgeladzerezo.pgscheduler.worker.Maintenance;
import io.github.mgeladzerezo.pgscheduler.worker.NotificationListener;
import io.github.mgeladzerezo.pgscheduler.worker.PgJobScheduler;
import io.github.mgeladzerezo.pgscheduler.worker.Worker;
import io.github.mgeladzerezo.pgscheduler.worker.WorkerOptions;
import java.time.Clock;
import javax.sql.DataSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * One scheduler instance in one JVM: the enqueue API plus whichever background roles are switched on
 * (worker, maintenance, cron, LISTEN connection). The Spring Boot auto-configuration builds one from
 * configuration properties; tests and non-Boot applications use {@link #builder(DataSource)} directly.
 *
 * <p>Every role is optional and independent. A web application that only enqueues builds a node with no
 * worker; a dedicated worker process leaves cron off; and any number of nodes with any mix of roles can
 * share one database.
 */
public final class SchedulerNode implements AutoCloseable {

    private final JobScheduler scheduler;
    private final SchedulerAdmin admin;
    private final HandlerRegistry handlers;
    private final JobPolicies policies;
    private final CronScheduler cron;
    private final boolean cronLoopEnabled;
    private final Worker worker;
    private final Maintenance maintenance;
    private final NotificationListener listener;
    private boolean started;

    private SchedulerNode(Builder b) {
        Db db = new Db(b.dataSource);
        ObjectMapper json = b.json != null ? b.json : JsonMapper.builder().build();
        this.handlers = b.handlers;
        this.policies = b.policies;
        this.scheduler = new PgJobScheduler(new EnqueueDao(db), policies, json);
        this.admin = new SchedulerAdmin(db, policies);
        this.cron = new CronScheduler(db, new ScheduleDao(db), policies, json, b.clock,
                b.cronOptions != null ? b.cronOptions : CronScheduler.Options.defaults());
        this.cronLoopEnabled = b.cronOptions != null;
        StatsDao statsDao = new StatsDao(db);
        this.worker = b.workerOptions == null ? null : new Worker(
                b.workerId != null ? b.workerId : Worker.generateId(), b.workerOptions, new WorkerDao(db), statsDao,
                handlers, policies, json, b.events);
        this.maintenance = b.maintenanceOptions == null ? null
                : new Maintenance(new MaintenanceDao(db), statsDao, b.maintenanceOptions, b.events);
        this.listener = b.listenSource == null ? null : new NotificationListener(b.listenSource);
        if (listener != null) {
            if (worker != null) {
                listener.subscribe(EnqueueDao.READY_CHANNEL, worker::wake);
            }
            if (maintenance != null) {
                listener.subscribe(EnqueueDao.SCHEDULED_CHANNEL, payload -> maintenance.wake());
            }
        }
    }

    public static Builder builder(DataSource dataSource) {
        return new Builder(dataSource);
    }

    /** Starts the enabled background roles. Register all handlers first. */
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        if (maintenance != null) {
            maintenance.start();
        }
        if (worker != null) {
            worker.start();
        }
        if (listener != null) {
            listener.start();
        }
        if (cronLoopEnabled) {
            cron.start();
        }
    }

    /** Stops the roles in reverse order; the worker shuts down gracefully (see {@link Worker#stop()}). */
    @Override
    public synchronized void close() {
        if (!started) {
            return;
        }
        started = false;
        cron.stop();
        if (worker != null) {
            worker.stop();
        }
        if (listener != null) {
            listener.stop();
        }
        if (maintenance != null) {
            maintenance.stop();
        }
    }

    public JobScheduler scheduler() {
        return scheduler;
    }

    /** Schedule management and manual ticking; the background loop runs only if cron was enabled. */
    public CronScheduler cron() {
        return cron;
    }

    public SchedulerAdmin admin() {
        return admin;
    }

    public HandlerRegistry handlers() {
        return handlers;
    }

    public JobPolicies policies() {
        return policies;
    }

    /** The worker, or {@code null} if this node does not run one. */
    public Worker worker() {
        return worker;
    }

    /** The maintenance loop, or {@code null} if this node does not run one. */
    public Maintenance maintenance() {
        return maintenance;
    }

    /** The LISTEN connection, or {@code null} if this node polls only. */
    public NotificationListener listener() {
        return listener;
    }

    /** Collects the parts of a node. Roles that are not configured are not started. */
    public static final class Builder {
        private final DataSource dataSource;
        private HandlerRegistry handlers = new HandlerRegistry();
        private JobPolicies policies = new JobPolicies();
        private ObjectMapper json;
        private Clock clock = Clock.systemUTC();
        private SchedulerEvents events = SchedulerEvents.NONE;
        private String workerId;
        private WorkerOptions workerOptions;
        private Maintenance.Options maintenanceOptions;
        private CronScheduler.Options cronOptions;
        private NotificationListener.ConnectionSource listenSource;

        private Builder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Builder handlers(HandlerRegistry handlers) {
            this.handlers = handlers;
            return this;
        }

        public Builder policies(JobPolicies policies) {
            this.policies = policies;
            return this;
        }

        /** The mapper for payloads and results; defaults to a plain {@link JsonMapper}. */
        public Builder json(ObjectMapper json) {
            this.json = json;
            return this;
        }

        /** The clock cron firing goes by; inject a controllable one in tests. */
        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder events(SchedulerEvents events) {
            this.events = events;
            return this;
        }

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        /** Enables the worker role. */
        public Builder worker(WorkerOptions options) {
            this.workerOptions = options;
            return this;
        }

        /** Enables the maintenance role (promotion, lease reaper, purge). */
        public Builder maintenance(Maintenance.Options options) {
            this.maintenanceOptions = options;
            return this;
        }

        /** Enables the background cron loop. Without it schedules can still be managed and ticked by hand. */
        public Builder cron(CronScheduler.Options options) {
            this.cronOptions = options;
            return this;
        }

        /** Enables LISTEN/NOTIFY wake-ups using connections from the given source. */
        public Builder listen(NotificationListener.ConnectionSource source) {
            this.listenSource = source;
            return this;
        }

        public SchedulerNode build() {
            return new SchedulerNode(this);
        }
    }
}
