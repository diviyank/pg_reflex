# 2026-06-15 — Costly IMV maintenance during demand-plan / supply-plan validate

Found while debugging a production incident (db_dev, pg_reflex 1.10.5): validating
a demand_planning (draft→ready_for_sop) caused multi-minute COMMITs. The DB itself
stays healthy (ms-latency, no lock contention, no pool exhaustion) — these are
pg_reflex maintenance-cost bugs, not the cause of the app serving going down (that
is being investigated separately on the forecast-factory side).

Evidence captured: registry observability (`__reflex_ivm_reference.last_flush_ms/
last_flush_rows/flush_count`) + `pg_stat_statements`.

---

## Bug 1 — Partitioned aggregate IMV target-sync is not partition-pruned (THE STORM)

### Effect
`omc.sop_incoming_stock_baseline_view`: a single incremental flush took
**541,297 ms (9 min) for 26,491 rows**. The same view is ms-fast in other schemas
only because they happened to take cheaper delta shapes. `pg_stat_statements`
(mostly yse, same code shape) shows this is **chronic, not a one-off**:

| statement | calls | mean | rows |
|---|---|---|---|
| `DO $reflex_dispatch$` (per-flush dispatcher) | 79 | 25 s | — |
| `DO $_reflex_imv_sp$` (per-IMV flush wrapper) | 33 | 49 s | — |
| `MERGE INTO __reflex_intermediate_sop_incoming_stock_baseline_view` | 51 | 11 s | 139k |
| `DELETE FROM sop_incoming_stock_baseline_view WHERE EXISTS(__reflex_affected)` | 51 | 10 s | 73k |
| `INSERT INTO sop_incoming_stock_baseline_view SELECT …` | 51 | 10 s | 74k |
| `INSERT INTO allocation_summary_view SELECT …` | 43 | 2.6 s | 9.6M |

So each flush ≈ MERGE(11s) + DELETE(10s) + re-INSERT(10s) ≈ 30–50 s, every time.

### Cause (hypothesis — confirm with full DELETE text)
The target sync `DELETE … WHERE EXISTS(SELECT 1 FROM __reflex_affected __a WHERE
<group-key match>)` taking 10 s to remove 73k rows implies it **scans the whole
partitioned target instead of pruning to the one changed `supply_plan_id`
partition**. Likely the EXISTS join matches all GROUP BY keys with
`IS NOT DISTINCT FROM` (null-safe), which defeats btree index use AND partition
pruning. The IMV is `partition_by [supply_plan_id]`, so the maintenance should
restrict the target DELETE/INSERT/MERGE to the affected partition(s).

### Things to investigate
- Dump the full untruncated DELETE/INSERT/MERGE text from pg_stat_statements;
  confirm there is no `supply_plan_id = ANY(...)` pruning predicate.
- Where the target-sync DELETE/INSERT is generated (trigger/ops.rs / trigger/merge.rs):
  add a partition-key predicate derived from `__reflex_affected` so PG prunes.
- `allocation_summary_view` re-INSERTs 9.6M rows per flush — same class? It is a
  2-level partitioned IMV; check its target sync too.
- View shape contributors: inline UNION ALL in FROM (`<subquery:st>`),
  `BOOL_OR(caav.is_active)` (caav = current_assortment_activity_view IMV),
  correlated `delivery_date >= (SELECT order_date FROM max_order_date_view)`.
  Determine whether UNION-operand scoping (`trigger/union_delta.rs::
  source_requires_recompute`) or BOOL_OR/MIN-MAX recompute
  (`trigger/merge.rs::build_min_max_recompute_sql`) is also amplifying cost.
- Why omc's single flush hit 9 min vs yse's 10–50 s: more affected partitions?
  cold cache? larger __reflex_affected? first-flush backlog (flush_count 0→1)?

### Source
`base_db/sql/views/sop_incoming_stock_baseline_view.sql` (forecast-factory base-db).

---

## Bug 2 — Missing passthrough scratch table → IMV silently stale, retried forever

### Effect
Every flush cascading to `reliability_snapshot_kinds` (unpartitioned; TRACKS
demand_planning; `ignored_sources={}`) fails fast:
```
WARNING: pg_reflex: IMV reliability_snapshot_kinds flush failed at cascade:
  relation "__reflex_pt_new_reliability_snapshot_kinds_alp_demand_planning" does not exist (SQLSTATE 42P01)
```
Caught by the per-root DO-block handler → logged as WARNING → **left pending for
retry** → re-fails on every commit. The IMV silently goes **stale** (never
maintained). Identical shape already present for `stock_kind__cte_base`
(`__reflex_pt_new_stock_kind__cte_base_alp_location_inventory`).

### Cause
The per-(IMV, source) passthrough scratch table created by
`schema_builder.rs::build_passthrough_scratch_ddls` (`__reflex_pt_new_<imv>_<source>`)
was never created for these (IMV, source) pairs, but `trigger/ops.rs::
passthrough_op_stmts` emits `TRUNCATE`/`INSERT INTO` against it. Likely the IMV
was created before passthrough-scratch DDL covered all sources, OR the create
loop skipped a source (e.g. a tracked source reached only via cascade).

### Things to investigate
- Which sources get `build_passthrough_scratch_ddls` at create time vs which the
  trigger references — find the gap (cascade-only / decomposed-source case?).
- Audit checks already reference these names (`audit/checks_a_catastrophic.rs:259`,
  `checks_c_orphan.rs:151`) — does `reflex_audit` flag this? If so, a migration /
  rebuild that recreates missing scratch tables would heal existing IMVs.
- Repro: create an IMV shape matching reliability_snapshot_kinds
  (depends on an agg IMV + a tracked base dim + `<subquery:k>`), update the dim,
  assert the scratch table exists.

---

## Reproduction harness (db_dev, healthy)
1. Snapshot `__reflex_ivm_reference(name,flush_count,last_flush_ms,last_flush_rows,
   last_update_date)` into a temp table.
2. Run the mutation inside `BEGIN … COMMIT` (DEFERRED flush fires at COMMIT;
   Ctrl-C the COMMIT rolls it back cleanly).
3. Diff the registry to see which IMVs flushed and their cost.

Gotchas: psql here uses **comma as decimal separator** (`22,322 ms` = 22.3 ms).
`yse.demand_planning` has NO reflex triggers (won't reproduce there) — use alp/omc.

---

## Bug 1 UPDATE (2026-07-25, PS-13) — not partition-pruning; the storm is write-dominated

Investigated on `main` @ 3d9a6a5 (1.11.1), PG17, EXPLAIN (ANALYZE). Full detail in
`untreated_bugs/2026-06-18_aggregate_target_sync_not_group_pruned.md` (RESOLVED /
NO-FIX section). Summary as it bears on this incident:

1. **The target sync already prunes to the affected partition at production
   scale.** With PS-5's sargable `=` and the per-partition composite group-key
   index the package creates, the DELETE/INSERT plan is a parameterized nested loop
   (or merge) whose non-affected partitions are `(never executed)`. Measured on a
   2M-group / 20-partition target with a 1-partition delta: 0.09-0.11 ms, 19/20
   partitions never scanned — including the report's nullable-gated shape. There is
   no `supply_plan_id = ANY(...)` predicate and none is needed; adding one does not
   help (PG cannot runtime-prune `= ANY(array_param)` — only scalar `= $1` / Const
   arrays prune, so a multi-partition predicate is unimplementable, and a
   per-partition scalar loop is complexity + a NULL-partition silent-wrong-data risk
   not worth a sub-ms gain).

2. **So the 9-min flush was NOT an all-partition scan.** The DELETE removed 73k
   rows, INSERT 74k, MERGE 139k — each ~10 s. That is a LARGE delta whose cost is
   dominated by *writing* those rows plus B-tree index maintenance on a partitioned
   target, which partition pruning does not reduce (pruning cuts the scan to FIND
   rows, not the writes to CHANGE them). A `validate` that recomputes ~73k groups
   legitimately rewrites ~73k rows; the fix for that cost is fewer/narrower
   recomputes upstream (view shape), not target-sync pruning.

3. **Still worth chasing on the view-shape side** (unchanged from the original
   report): the inline `UNION ALL` operand scoping
   (`trigger/union_delta.rs::source_requires_recompute`) and the `BOOL_OR` / MIN-MAX
   recompute (`trigger/merge.rs`) that can amplify how many groups a single validate
   marks affected. Those decide the 73k, and 73k rows at ~130 rows/ms is the 10 s —
   reducing the affected-set size is the lever, not the sync plan.

**No package change from PS-13.** (Bug 2 — missing passthrough scratch table —
untouched here; separate issue.)
