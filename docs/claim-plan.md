# The claim query and its plan

Status: **the plans below are the expected shape, derived from the index design. They were not captured in the final
state of the repository.** `ClaimPlanTest` runs the real statements under `EXPLAIN (ANALYZE, BUFFERS)`, asserts that
they use the partial indexes, and writes the plans to `pg-scheduler-core/target/claim-plan-<rows>.txt`. Capture the real
output with:

```
./mvnw -B -pl pg-scheduler-core test -Dtest=ClaimPlanTest                 # 300,000 finished rows
./mvnw -B -pl pg-scheduler-core -Pbench test -Dtest=ClaimPlanTest         # adds the 3,000,000-row variant
```

## Index design

```sql
-- READY rows only, in claim order: queue, then priority DESC, run_at, id
CREATE INDEX pgs_job_claim_idx ON pgs_job (queue, priority DESC, run_at, id) WHERE state = 'READY';
-- delayed jobs and retries in backoff
CREATE INDEX pgs_job_due_idx   ON pgs_job (run_at) WHERE state IN ('SCHEDULED', 'FAILED');
-- running jobs by lease expiry (the reaper)
CREATE INDEX pgs_job_lease_idx ON pgs_job (locked_until, queue) WHERE state = 'RUNNING';
```

A job that has finished (SUCCEEDED, CANCELLED, DEAD) is in none of these indexes. Their size and the cost of walking
them follow the backlog, not the table. This is why the claim stays an index scan when the table holds millions of
finished rows: the planner never has a reason to look at them, because the partial index predicate
(`state = 'READY'`) is implied by the query's `WHERE state = 'READY'`, and the index order equals the `ORDER BY`, so no
sort is needed and `LIMIT` stops the scan after the first rows.

Two things keep this true. The claim writes `state = 'RUNNING'` in the same statement that selects the row, so rows leave
the index immediately and the index does not accumulate dead entries beyond what autovacuum handles; and the table has
aggressive per-table autovacuum settings (`autovacuum_vacuum_scale_factor = 0.02`) because every state change rewrites a
row.

## The claim

```sql
WITH picked AS (
    SELECT id, greatest(run_at, updated_at) AS eligible_at
    FROM pgs_job
    WHERE state = 'READY'
      AND queue = ?
      AND type = ANY (?)
      AND NOT EXISTS (SELECT 1 FROM pgs_queue q WHERE q.name = ? AND q.paused)
    ORDER BY priority DESC, run_at, id
    LIMIT ?
    FOR UPDATE SKIP LOCKED
)
UPDATE pgs_job j
SET state = 'RUNNING', attempt = j.attempt + 1, locked_by = ?,
    locked_until = now() + ? * interval '1 millisecond', started_at = now(), updated_at = now()
FROM picked
WHERE j.id = picked.id
RETURNING j.id, j.type, ..., (extract(epoch FROM now() - picked.eligible_at) * 1000)::bigint AS wait_ms
```

## Expected plan

```
Update on pgs_job j
  CTE picked
    ->  Limit
          ->  LockRows
                ->  Result  (One-Time Filter: NOT InitPlan: queue is not paused)
                      ->  Index Scan using pgs_job_claim_idx on pgs_job
                            Index Cond: (queue = 'q')
                            Filter: (type = ANY (...))
  ->  Nested Loop
        ->  CTE Scan on picked
        ->  Index Scan using pgs_job_pkey on pgs_job j  (Index Cond: id = picked.id)
```

Notes on reading it:

* `Index Cond: (queue = ...)` is the leading column of the partial index; `state = 'READY'` does not appear in the
  condition because it is the index predicate.
* The `type = ANY (...)` filter is applied to index entries as they are read. A worker only asks for types it has a
  handler for, so a READY job of an unknown type is skipped by the filter (parked, not lost). If a large block of
  parked jobs of one type sits at the front of the queue, the scan reads past them; that is the cost of the design and
  is listed under known limitations.
* `LockRows` with `SKIP LOCKED` is what lets concurrent claimers step over each other's rows instead of waiting.
* The second index scan, on the primary key, is the UPDATE locating the picked rows by id. The number of rows touched is
  the batch size, independent of table size.

The same test explains `promote` (uses `pgs_job_due_idx`) and `reap` (uses `pgs_job_lease_idx`) the same way.
