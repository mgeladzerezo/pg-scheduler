package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.store.MaintenanceDao;
import io.github.mgeladzerezo.pgscheduler.store.WorkerDao;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The claim must stay an index scan over the backlog no matter how many finished jobs the table holds.
 *
 * <p>The table is filled with finished rows, a small backlog of READY rows and some future SCHEDULED rows;
 * then the real statements are run under {@code EXPLAIN (ANALYZE, BUFFERS)}. The plans are written to
 * {@code target/claim-plan-<rows>.txt}; docs/claim-plan.md quotes them.
 */
class ClaimPlanTest {

    @Test
    void claimPromoteAndReapUsePartialIndexesWithThreeHundredThousandFinishedRows() throws Exception {
        explainWith(300_000);
    }

    /** The same with three million finished rows, for the numbers quoted in the docs. {@code mvnw -Pbench verify}. */
    @Test
    @Tag("bench")
    void claimPromoteAndReapUsePartialIndexesWithThreeMillionFinishedRows() throws Exception {
        explainWith(3_000_000);
    }

    private void explainWith(int finishedRows) throws Exception {
        try (TestDatabase database = TestDatabase.create()) {
            long loadStarted = System.nanoTime();
            // Finished history: the bulk of any long-lived job table.
            database.execute("""
                    INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, attempt, max_attempts, timeout_ms,
                                         created_at, updated_at, started_at, finished_at)
                    SELECT 'history.job', 'queue-' || (g % 4), '{"n": 1}'::jsonb, g % 5, 'SUCCEEDED',
                           now() - g * interval '1 second', 1, 5, 300000,
                           now() - g * interval '1 second', now() - g * interval '1 second',
                           now() - g * interval '1 second', now() - g * interval '1 second'
                    FROM generate_series(1, ?) g
                    """, finishedRows);
            // The backlog: 5,000 READY jobs over four queues and five priorities.
            database.execute("""
                    INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, max_attempts, timeout_ms)
                    SELECT 'backlog.job', 'queue-' || (g % 4), '{"n": 2}'::jsonb, g % 5, 'READY',
                           now() - g * interval '1 millisecond', 5, 300000
                    FROM generate_series(1, 5000) g
                    """);
            // Delayed jobs, some of them already due, and a handful of running ones with expired leases.
            database.execute("""
                    INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, max_attempts, timeout_ms)
                    SELECT 'delayed.job', 'queue-' || (g % 4), 'null'::jsonb, 0, 'SCHEDULED',
                           now() + (g - 100) * interval '1 minute', 5, 300000
                    FROM generate_series(1, 20000) g
                    """);
            database.execute("""
                    INSERT INTO pgs_job (type, queue, payload, priority, state, run_at, attempt, max_attempts, timeout_ms,
                                         locked_by, locked_until, started_at)
                    SELECT 'running.job', 'queue-' || (g % 4), 'null'::jsonb, 0, 'RUNNING', now(), 1, 5, 300000,
                           'worker-' || (g % 8), now() + (g - 20) * interval '1 second', now()
                    FROM generate_series(1, 200) g
                    """);
            database.execute("VACUUM (ANALYZE) pgs_job");
            long loadSeconds = (System.nanoTime() - loadStarted) / 1_000_000_000;

            String sizes = database.string("""
                    SELECT format('table pgs_job: %s rows, %s; claim index: %s; primary key: %s',
                                  (SELECT count(*) FROM pgs_job),
                                  pg_size_pretty(pg_table_size('pgs_job')),
                                  pg_size_pretty(pg_relation_size('pgs_job_claim_idx')),
                                  pg_size_pretty(pg_relation_size('pgs_job_pkey')))
                    """);

            String claim = explain(database, WorkerDao.CLAIM,
                    "queue-1", new String[] {"backlog.job", "other.job"}, "queue-1", 10, "worker-explain", 30_000L);
            String promote = explain(database, MaintenanceDao.PROMOTE, 500);
            String reap = explain(database, MaintenanceDao.REAP, 500);

            String report = "# " + sizes + " (loaded in " + loadSeconds + " s)\n\n"
                    + "## claim (LIMIT 10)\n" + claim + "\n## promote due jobs (LIMIT 500)\n" + promote
                    + "\n## reap expired leases (LIMIT 500)\n" + reap;
            Path file = Path.of("target", "claim-plan-" + finishedRows + ".txt");
            Files.createDirectories(file.getParent());
            Files.writeString(file, report);
            System.out.println(report);

            // The claim walks the READY-only index in claim order and stops after LIMIT rows: no sort, no
            // scan of the table, and it touches a handful of pages however large the table is.
            assertThat(claim).contains("Index Scan using pgs_job_claim_idx on pgs_job");
            assertThat(claim).doesNotContain("Seq Scan on pgs_job").doesNotContain("Sort").doesNotContain("Bitmap");
            assertThat(claim).contains("Index Scan using pgs_job_pkey on pgs_job j");
            assertThat(rowsOfNode(claim, "Index Scan using pgs_job_claim_idx")).isEqualTo(10);
            assertThat(sharedBuffersOfNode(claim, "Index Scan using pgs_job_claim_idx")).isLessThan(50);
            assertThat(database.string("SELECT pg_relation_size('pgs_job_claim_idx') < pg_relation_size('pgs_job_pkey') / 20"))
                    .as("the claim index is a small fraction of the primary key").isEqualTo("t");

            assertThat(promote).contains("Index Scan using pgs_job_due_idx on pgs_job");
            assertThat(promote).doesNotContain("Seq Scan on pgs_job").doesNotContain("Sort (");
            assertThat(reap).contains("pgs_job_lease_idx");
            assertThat(reap).doesNotContain("Seq Scan on pgs_job");
        }
    }

    /** Runs the statement for real under EXPLAIN ANALYZE, then rolls its effects back. */
    private static String explain(TestDatabase database, String sql, Object... params) throws Exception {
        try (Connection connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<String> lines = Db.query(connection, "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) " + sql,
                        rs -> rs.getString(1), params);
                return String.join("\n", lines) + "\n";
            } finally {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        }
    }

    private static long rowsOfNode(String plan, String node) {
        Matcher matcher = Pattern.compile(Pattern.quote(node) + ".*?\\(actual rows=(\\d+)").matcher(plan);
        assertThat(matcher.find()).as("node '%s' in plan", node).isTrue();
        return Long.parseLong(matcher.group(1));
    }

    /** Shared buffers hit plus read, from the Buffers line directly under the node. */
    private static long sharedBuffersOfNode(String plan, String node) {
        Matcher matcher = Pattern.compile(Pattern.quote(node) + "[^\\n]*\\n(?:[^\\n]*\\n){0,4}?\\s*Buffers: shared(?: hit=(\\d+))?(?: read=(\\d+))?")
                .matcher(plan);
        assertThat(matcher.find()).as("buffers of node '%s' in plan", node).isTrue();
        long hit = matcher.group(1) == null ? 0 : Long.parseLong(matcher.group(1));
        long read = matcher.group(2) == null ? 0 : Long.parseLong(matcher.group(2));
        return hit + read;
    }
}
