package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.testsupport.Nodes;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The enqueue joins the caller's transaction: the job exists if and only if the business change commits,
 * and no worker can see or start it before the commit.
 */
class TransactionalEnqueueTest {

    record OrderPlaced(long orderId) {
    }

    private static TestDatabase database;
    private static SchedulerNode workerNode;
    private static JobScheduler scheduler;
    private static TransactionTemplate transaction;
    private static JdbcTemplate jdbc;
    private static final List<Long> handled = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void setUp() {
        database = TestDatabase.create();
        database.execute("CREATE TABLE orders (id bigint PRIMARY KEY, status text NOT NULL)");

        // The application side: one pool, one transaction manager, and a scheduler on the same pool.
        DataSource applicationPool = database.newPool(4);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(applicationPool));
        jdbc = new JdbcTemplate(applicationPool);
        scheduler = Nodes.client(applicationPool).scheduler();

        // A worker elsewhere, on its own pool, eager to run whatever becomes visible.
        workerNode = Nodes.full(database, database.newPool(4), Nodes.worker(4)).build();
        workerNode.handlers().register("order.confirm", OrderPlaced.class, (payload, context) -> handled.add(payload.orderId()));
        workerNode.start();
    }

    @AfterAll
    static void tearDown() {
        workerNode.close();
        database.close();
    }

    @BeforeEach
    void reset() {
        handled.clear();
    }

    @Test
    void rollbackDiscardsTheJobTogetherWithTheBusinessChange() throws Exception {
        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO orders (id, status) VALUES (1, 'PLACED')");
            scheduler.enqueue("order.confirm", new OrderPlaced(1));
            throw new IllegalStateException("payment declined");
        })).hasMessage("payment declined");

        assertThat(database.count("SELECT count(*) FROM orders WHERE id = 1")).isZero();
        assertThat(jobsForOrder(1)).isZero();
        // Give a wrongly visible job every chance to be picked up.
        TimeUnit.MILLISECONDS.sleep(600);
        assertThat(handled).isEmpty();
    }

    @Test
    void commitMakesBothVisibleAndOnlyThenDoesTheWorkerStart() throws Exception {
        CountDownLatch enqueued = new CountDownLatch(1);
        CountDownLatch mayCommit = new CountDownLatch(1);

        Thread business = Thread.ofPlatform().start(() -> transaction.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO orders (id, status) VALUES (2, 'PLACED')");
            scheduler.enqueue("order.confirm", new OrderPlaced(2));
            enqueued.countDown();
            awaitQuietly(mayCommit);
        }));

        assertThat(enqueued.await(10, TimeUnit.SECONDS)).isTrue();
        // The transaction is open: neither the order nor the job exists for anyone else.
        TimeUnit.MILLISECONDS.sleep(600);
        assertThat(jobsForOrder(2)).isZero();
        assertThat(handled).isEmpty();

        mayCommit.countDown();
        business.join();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handled).containsExactly(2L));
        assertThat(database.count("SELECT count(*) FROM orders WHERE id = 2")).isEqualTo(1);
        assertThat(jobsForOrder(2)).isEqualTo(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(database.string(
                "SELECT state FROM pgs_job WHERE payload ->> 'orderId' = '2'")).isEqualTo("SUCCEEDED"));
    }

    @Test
    void outsideATransactionTheEnqueueCommitsOnItsOwn() {
        scheduler.enqueue("order.confirm", new OrderPlaced(3));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handled).containsExactly(3L));
    }

    private static long jobsForOrder(long orderId) {
        return database.count("SELECT count(*) FROM pgs_job WHERE payload ->> 'orderId' = ?", String.valueOf(orderId));
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
