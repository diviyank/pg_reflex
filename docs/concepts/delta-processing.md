# Delta processing

When a source table is mutated, pg_reflex's statement-level trigger applies the delta in five steps.

## 1. Empty-delta short-circuit

The trigger first checks if the transition table (`new_rows` for INSERT, `old_rows` for DELETE, both for UPDATE) is empty. If so, it returns immediately — no FFI calls, no advisory lock, no MERGE generation. Saves 5–15 ms per fired-but-empty trigger.

## 2. Per-IMV `where_predicate` filter

For every IMV on the source, pg_reflex stores the IMV's `WHERE` clause in `__reflex_ivm_reference.where_predicate`. The trigger evaluates the predicate against the transition rows; if no rows match, the IMV is skipped (no advisory lock, no delta). Most useful for `UNION` IMVs whose operands have disjoint filters.

## 3. Advisory lock

Per-IMV, the trigger takes a `pg_advisory_xact_lock(hash(name), hash(reverse(name)))`. Two sessions flushing the same IMV serialise; two sessions flushing different IMVs do not. The 2-arg hash form (1.1.3+) avoids cross-name collisions.

## 4. Delta MERGE

The Rust function `reflex_build_delta_sql` generates a MERGE statement:

```sql
MERGE INTO __reflex_intermediate_v t USING delta d ON t.region IS NOT DISTINCT FROM d.region
WHEN MATCHED THEN UPDATE SET
    __sum_amount = COALESCE(t.__sum_amount, 0) + COALESCE(d.__sum_amount, 0),
    __ivm_count = COALESCE(t.__ivm_count, 0) + COALESCE(d.__ivm_count, 0),
    -- ...top-K columns get a sorted-merge here when topk is enabled
WHEN NOT MATCHED THEN INSERT (...) VALUES (d.region, COALESCE(d.__sum_amount, 0), ...);
```

The `RETURNING` clause captures affected group keys into `__reflex_affected_<view>`.

## 5. Targeted refresh

Only the groups present in `__reflex_affected_<view>` are deleted from the target and re-inserted from the intermediate:

```sql
DELETE FROM v WHERE (region) IN (SELECT region FROM __reflex_affected_v);
INSERT INTO v SELECT region, __sum_amount AS total
  FROM __reflex_intermediate_v
  WHERE (region) IN (SELECT region FROM __reflex_affected_v)
    AND __ivm_count > 0;
```

The `__ivm_count > 0` filter excludes soft-deleted groups (those whose source row count dropped to zero).

## DEFERRED mode flow

When the IMV is created with `mode='DEFERRED'`, steps 4 and 5 happen at COMMIT time (or on-demand via `reflex_flush_deferred(source)`), not per-statement. The trigger writes a row to `__reflex_deferred_pending` and a deferred constraint trigger drains the queue at COMMIT.

## Locking and reader impact

Two questions matter to operators: **does a source-table write block IMV readers?** and **does a long IMV reader block a writer's flush?**

PostgreSQL lock compatibility decides this. `SELECT` on the IMV takes `AccessShareLock`. The flush path takes one of several locks on the target depending on which code path the dispatcher picks:

| Source operation | IMV path taken | Lock on target | Readers blocked? |
|---|---|---|---|
| `INSERT` / `UPDATE` / `DELETE`, low-selectivity (default) | `MERGE` into intermediate + targeted `DELETE`+`INSERT` on target (`trigger.rs:278`) | `RowExclusiveLock` | **No** — compatible with `AccessShareLock` |
| Bulk `INSERT` qualifying for Path B (`|transition| / |source|` ≥ `wipe_threshold`, `source_join_keys` present) | Pre-scratch bulk path: scratch fill → `INSERT INTO` intermediate (UNIQUE drop+rebuild) → `INSERT INTO` target | `RowExclusiveLock` | **No** — target only ever sees `INSERT` |
| Bulk `DELETE` qualifying for Path B | Pre-scratch bulk path: affected-set scratch → `DELETE FROM` intermediate / target by join keys | `RowExclusiveLock` + row locks | **No** |
| Item α `UPDATE` promoted to `INSERT_PROMOTED` qualifying for Path C (1.5.0, EXPLAIN-estimated rows ≥ `wipe_threshold`) | Smart bulk-INSERT: scratch fill → intermediate UNIQUE drop → `INSERT INTO intermediate` → recreate UNIQUE → `INSERT INTO target` from scratch (`schema_builder.rs:622`) | `RowExclusiveLock` | **No** — target only sees `INSERT`; brief `AccessExclusiveLock` on the *intermediate* during the index drop, but the intermediate is internal |
| Any DML still routed to `reflex_reconcile` (passthrough non-Item-α high-selectivity, or explicit call) | `TRUNCATE` target + bulk `INSERT` | `AccessExclusiveLock` | **Yes** — full table lock for the rebuild's duration |
| `UPDATE` on a self-join source (`is_self_join` branch, `trigger.rs:1435`) | `TRUNCATE` target + full `INSERT` from base query | `AccessExclusiveLock` | **Yes** |
| `TRUNCATE` on source | `TRUNCATE` all dependent IMVs | `AccessExclusiveLock` | **Yes** |
| Outer-join secondary table DML, passthrough shape | `DELETE FROM target` + `INSERT` (no `TRUNCATE`) | `RowExclusiveLock` + row locks | **No**, but produces a wave of dead tuples |

The dispatcher's selectivity check (`trigger.rs:1051`, the `DO` block injected ahead of every grouped MERGE) reads `affected_groups / intermediate_size` and compares against the per-IMV `wipe_threshold` column, then the `reflex.wipe_threshold` GUC, then the compiled default. Below the threshold, the run takes the incremental branch (no reader block); at or above, it delegates to either an inline bulk path (Path B / Path C, 1.5.0+) or `reflex_reconcile`. Only the `reflex_reconcile` branch and the self-join / TRUNCATE-source branches take `AccessExclusiveLock`.

All locks above are held for the writer's entire transaction, not just the trigger body — because the maintenance runs in the writer's `AFTER STATEMENT` trigger. A long writer transaction therefore extends the lock hold time, even on the non-blocking paths (writers wait on the same `RowExclusiveLock`, readers don't).

Two effects matter even when the table-level lock is benign:

- **Reader-induced bloat.** Each in-place UPDATE creates new MVCC tuple versions. Autovacuum cannot reclaim dead tuples while a long reader's snapshot still sees them. Sustained writes plus long dashboards grow the IMV heap until the reader finishes.
- **Index leaf-page contention.** B-tree leaf splits during writes briefly delay concurrent readers traversing the same page. Visible only under heavy concurrency, and mitigated by the fillfactor strategy described in [internals](internals.md#hot-updates-and-fillfactor).

Practical operator notes: if your workload sits below the wipe threshold on every IMV — or your bulk flips qualify for Path B / Path C — readers run uninterrupted. The remaining blocking paths (`reflex_reconcile`, self-join sources, source `TRUNCATE`) are the regimes where in-place IMV maintenance is more expensive than a full rebuild. Lowering `wipe_threshold` per IMV (`reflex_set_wipe_threshold`) routes more workloads through the rebuild branch in exchange for cheaper individual rebuilds; whether that rebuild blocks readers depends on whether it qualifies for Path B / Path C (non-blocking) or falls through to `reflex_reconcile` (blocking).

[Deferred mode :material-arrow-right-bold:](deferred-mode.md){ .md-button }
[Internals :material-arrow-right-bold:](internals.md){ .md-button }
