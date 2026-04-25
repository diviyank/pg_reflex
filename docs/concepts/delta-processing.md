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

[Deferred mode :material-arrow-right-bold:](deferred-mode.md){ .md-button }
