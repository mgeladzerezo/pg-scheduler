package io.github.mgeladzerezo.pgscheduler.testsupport;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mgeladzerezo.pgscheduler.SchedulerNode;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.worker.WorkerOptions;
import java.time.Duration;

/**
 * A worker in its own operating-system process, started by {@code CrashRecoveryTest} and then killed.
 *
 * <p>Its handler logs that it started and then hangs "mid-job", so that when the process is destroyed the
 * job is left RUNNING with a lease nobody will ever extend again. Nothing in here gets a chance to clean
 * up: no shutdown hook runs on a forced kill.
 *
 * <p>Arguments: JDBC URL, user, password, worker id, lease in milliseconds.
 */
public final class CrashVictimWorker {

    private CrashVictimWorker() {
    }

    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(args[0]);
        config.setUsername(args[1]);
        config.setPassword(args[2]);
        config.setMaximumPoolSize(4);
        HikariDataSource pool = new HikariDataSource(config);
        String workerId = args[3];
        Duration lease = Duration.ofMillis(Long.parseLong(args[4]));

        Db db = new Db(pool);
        SchedulerNode node = SchedulerNode.builder(pool)
                .workerId(workerId)
                .worker(WorkerOptions.defaults().withQueue("crash", 2).withPollInterval(Duration.ofMillis(100)).withLease(lease))
                .build();
        node.handlers().register("crash.job", String.class, (payload, context) -> {
            db.autoCommit(c -> Db.update(c, "INSERT INTO crash_log (job_id, attempt, worker_id, event) VALUES (?, ?, ?, 'STARTED')",
                    context.jobId(), context.attempt(), context.workerId()));
            Thread.sleep(Duration.ofMinutes(10)); // the process is killed long before this returns
            db.autoCommit(c -> Db.update(c, "INSERT INTO crash_log (job_id, attempt, worker_id, event) VALUES (?, ?, ?, 'FINISHED')",
                    context.jobId(), context.attempt(), context.workerId()));
        });
        node.start();
        System.out.println("victim worker " + workerId + " running, pid " + ProcessHandle.current().pid());
        Thread.sleep(Duration.ofMinutes(10));
    }
}
