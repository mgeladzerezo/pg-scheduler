package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * LISTEN/NOTIFY wakes the worker, so a job starts milliseconds after it is committed although the worker
 * polls only every 30 seconds here. Also: losing the LISTEN connection degrades to polling and recovers.
 */
class NotifyWakeUpTest {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(30);

    private static TestDatabase database;
    private static SchedulerNode client;

    @BeforeAll
    static void setUp() {
        database = TestDatabase.create();
        client = Nodes.client(database.dataSource());
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @Test
    void enqueuedJobStartsWithinMillisecondsWithoutTightPolling() throws Exception {
        SynchronousQueue<Long> startedAt = new SynchronousQueue<>();
        try (SchedulerNode node = Nodes.full(database, database.newPool(4),
                Nodes.worker(2).withQueue("notify", 2).withPollInterval(POLL_INTERVAL)).build()) {
            node.handlers().register("notify.job", String.class, (payload, context) -> startedAt.put(System.nanoTime()));
            node.start();
            await().atMost(Duration.ofSeconds(10)).until(() -> node.listener().isConnected());
            TimeUnit.MILLISECONDS.sleep(300); // let the start-up poll finish: from here on only NOTIFY can wake it

            List<Long> latenciesMicros = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                long before = System.nanoTime();
                client.scheduler().enqueue(JobRequest.of("notify.job", "x").queue("notify"));
                Long started = startedAt.poll(10, TimeUnit.SECONDS);
                assertThat(started).as("job %d was not started; the worker is not being woken", i).isNotNull();
                latenciesMicros.add((started - before) / 1_000);
            }

            Collections.sort(latenciesMicros);
            long median = latenciesMicros.get(latenciesMicros.size() / 2);
            long p95 = latenciesMicros.get((int) (latenciesMicros.size() * 0.95));
            long max = latenciesMicros.getLast();
            System.out.printf("NOTIFY_WAKEUP enqueue-call-to-handler-start over %d jobs: median=%.1fms p95=%.1fms max=%.1fms (poll interval %ds)%n",
                    latenciesMicros.size(), median / 1000.0, p95 / 1000.0, max / 1000.0, POLL_INTERVAL.toSeconds());
            // Generous bounds for a loaded CI machine; three orders of magnitude below the poll interval.
            assertThat(median).isLessThan(250_000);
            assertThat(max).isLessThan(3_000_000);
        }
    }

    @Test
    void losingTheListenConnectionFallsBackToPollingAndThenRecovers() throws Exception {
        SynchronousQueue<Long> startedAt = new SynchronousQueue<>();
        String applicationName = "pgs-listener-under-test";
        try (SchedulerNode node = SchedulerNode.builder(database.newPool(4))
                .worker(Nodes.worker(2).withQueue("reconnect", 2).withPollInterval(Duration.ofSeconds(2)))
                .listen(() -> listenerConnection(applicationName))
                .build()) {
            node.handlers().register("reconnect.job", String.class, (payload, context) -> startedAt.put(System.nanoTime()));
            node.start();
            await().atMost(Duration.ofSeconds(10)).until(() -> node.listener().isConnected());

            // Kill the LISTEN backend from the server side.
            assertThat(database.count("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = ?",
                    applicationName)).isEqualTo(1);

            // Whether or not the listener has noticed yet, the job still runs: polling is the safety net.
            client.scheduler().enqueue(JobRequest.of("reconnect.job", "x").queue("reconnect"));
            assertThat(startedAt.poll(10, TimeUnit.SECONDS)).isNotNull();

            // The listener reconnects on its own and wake-ups are fast again.
            await().atMost(Duration.ofSeconds(15)).until(() -> node.listener().isConnected()
                    && database.count("SELECT count(*) FROM pg_stat_activity WHERE application_name = ?", applicationName) == 1);
            // Five in a row within half a second each cannot be the 2 s poll getting lucky.
            for (int i = 0; i < 5; i++) {
                long before = System.nanoTime();
                client.scheduler().enqueue(JobRequest.of("reconnect.job", "y" + i).queue("reconnect"));
                Long started = startedAt.poll(10, TimeUnit.SECONDS);
                assertThat(started).isNotNull();
                assertThat(Duration.ofNanos(started - before)).isLessThan(Duration.ofMillis(500));
            }
        }
    }

    private static Connection listenerConnection(String applicationName) throws java.sql.SQLException {
        String url = database.jdbcUrl();
        return DriverManager.getConnection(url + (url.contains("?") ? "&" : "?") + "ApplicationName=" + applicationName,
                database.username(), database.password());
    }
}
