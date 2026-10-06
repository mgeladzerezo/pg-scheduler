package io.github.mgeladzerezo.pgscheduler.store;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Writes the throughput roll-up ({@code pgs_stat}).
 *
 * <p>Counting inside the completion statement would make every job of a queue update the same counter row
 * and queue up behind its row lock. Instead each node counts in memory and adds its totals here about once
 * a second, so the roll-up costs one small upsert per node per second regardless of throughput.
 */
public final class StatsDao {

    /** How many attempts on a queue ended with an outcome since the last flush. */
    public record StatDelta(String queue, String outcome, long count) {
    }

    private final Db db;

    public StatsDao(Db db) {
        this.db = db;
    }

    /** Adds counters to the current 5-second bucket. Rows are sorted so concurrent flushes cannot deadlock. */
    public void add(List<StatDelta> deltas) {
        if (deltas.isEmpty()) {
            return;
        }
        List<StatDelta> sorted = new ArrayList<>(deltas);
        sorted.sort(Comparator.comparing(StatDelta::queue).thenComparing(StatDelta::outcome));
        String[] queues = sorted.stream().map(StatDelta::queue).toArray(String[]::new);
        String[] outcomes = sorted.stream().map(StatDelta::outcome).toArray(String[]::new);
        Long[] counts = sorted.stream().map(StatDelta::count).toArray(Long[]::new);
        db.autoCommit(connection -> Db.update(connection, """
                INSERT INTO pgs_stat (bucket, queue, outcome, count)
                SELECT date_bin('5 seconds', now(), TIMESTAMPTZ '2000-01-01 00:00:00+00'), t.queue, t.outcome, t.count
                FROM unnest(?::text[], ?::text[], ?::bigint[]) WITH ORDINALITY AS t(queue, outcome, count, ord)
                ORDER BY t.ord
                ON CONFLICT (bucket, queue, outcome) DO UPDATE SET count = pgs_stat.count + EXCLUDED.count
                """, queues, outcomes, counts));
    }
}
