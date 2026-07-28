# 2026-07-28 — an explicit multi-level `partition_by` is impossible on an aggregate IMV, and the rejection prescribes a remedy that cannot work

**Status: untreated. Reproduced** (PostgreSQL 16.11 under pgrx, on `integration/s1-batch`).
Found while establishing the exposure scope of
`2026-07-28_swap_flattens_subpartitioned_child_then_sync_empties_imv.md`; filed separately per the
`untreated_bugs/` hygiene rule — different defect, different fix location.

Severity: **medium**. No data loss and no silent wrong result — the IMV is refused at create time.
The cost is (a) a documented capability that does not exist for aggregates, and (b) an error whose
prescribed fix structurally cannot clear it, which is the CLAUDE.md "don't print a remedy that can't
clear its own finding" rule.

---

## Reproduction

```sql
CREATE TABLE ag (k TEXT NOT NULL, d DATE NOT NULL, amt NUMERIC) PARTITION BY LIST (k);
CREATE TABLE ag_a PARTITION OF ag FOR VALUES IN ('A') PARTITION BY RANGE (d);
CREATE TABLE ag_a_m1 PARTITION OF ag_a FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

SELECT create_reflex_ivm('agv',
    'SELECT k, d, SUM(amt) AS total FROM ag GROUP BY k, d',
    'k,d',                       -- unique_columns names BOTH levels
    NULL, NULL, NULL,
    ARRAY['k','d']);             -- explicit depth-2 partition_by
```

```
ERROR: [reflex-unsupported] partition_by validation failed — partition key column 'd'
(level 2 of source 'ag') is not a bare projected output column in the IMV's unique key.
Add it to the SELECT list and unique_columns, or declare a shallower partition_by.
```

`d` **is** in the SELECT list, **is** a bare GROUP BY column, and **is** named in
`unique_columns`. The suggested fix has already been applied and the error still fires.

## Mechanism

`resolve_partitioning` validates each declared level `i >= 1` against
`ctx.resolved_unique_columns` (`src/create_ivm/mod.rs:597-601`, check at `:632`):

```rust
let unique_key_cols: std::collections::HashSet<String> =
    ctx.resolved_unique_columns.iter().map(|c| c.to_lowercase()).collect();
...
if i > 0 && !unique_key_cols.contains(&dl) { return Err(...) }
```

But `resolve_unique_columns` returns **immediately** when the plan is an aggregate
(`src/create_ivm/mod.rs:213-216`):

```rust
fn resolve_unique_columns(ctx: &mut BuildContext) {
    if !ctx.plan.is_passthrough {
        return;
    }
    ...
```

so `ctx.resolved_unique_columns` is **always empty for an aggregate**, whatever the caller passed
as `unique_columns`. `unique_key_cols` is therefore the empty set, `contains` is always false, and
**every** level `i >= 1` is rejected. There is no input that satisfies the check.

## Consequences

1. **Depth->=2 aggregate IMVs can only be created through the AUTO-mirror path**
   (`src/create_ivm/mod.rs:664-740`), which derives the depth from the bare-projected GROUP BY
   prefix and stores `partition_columns` of length **one** with `partition_depth = 2`. This is not
   a hypothetical: it is the shape `pg_subpart_reconcile_then_sync_keeps_depth2_aggregate_imv_data`
   builds, and it is why any operator exposure query must key off `partition_depth`, never off
   `array_length(partition_columns, 1)`.
2. **The error message is unactionable.** It names `unique_columns`, which the aggregate path does
   not read. An operator following it retries forever — the exact failure mode the CLAUDE.md
   remedy-convergence rule exists to prevent.

## What was ruled out

* **"The column really isn't bare-projected."** No — `d` is a bare GROUP BY column and a bare
  SELECT item. The auto-mirror path, whose predicate for a mirrorable level is precisely
  "bare-projected GROUP BY column" (`:697-709`), accepts the same column and reaches depth 2 on the
  same fixture. The two paths disagree about the same query.
* **"`unique_columns` was passed wrongly."** No — the parameter is parsed only inside
  `resolve_unique_columns`, which returns before parsing it for aggregates. Any value fails
  identically, including the correct one.
* **"It is a deliberate restriction."** Possible but undocumented, and if so the message is still
  wrong: it should say aggregates cannot declare sub-levels explicitly and to omit `partition_by`
  for auto-mirroring, not prescribe an edit to `unique_columns` that has no effect.

## Fix direction

Two candidates; the second is the smaller change and probably the right one.

1. **Make the check use the aggregate's real key.** For an aggregate the effective unique key is
   the GROUP BY column set (`ctx.plan.group_by_columns` / `group_by_aliases`), which is exactly
   what the auto-mirror path already consults. Validating level `i >= 1` against that set for
   aggregates, and against `resolved_unique_columns` for passthrough, makes the explicit and
   automatic paths agree.
2. **Refuse explicitly and accurately.** If explicit multi-level `partition_by` on an aggregate is
   genuinely unsupported, say so: reject with a message that names the real constraint and points
   at the working alternative (omit `partition_by`; the auto-mirror will pick the depth up).

Either way the acceptance test is a real aggregate IMV over a real `LIST -> RANGE` source: the
declared-depth-2 create must either succeed and produce `partition_depth = 2`, or fail with a
message whose prescribed remedy demonstrably converges.
