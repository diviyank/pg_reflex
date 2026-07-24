# 2026-06-18 — aggregate target-sync is O(num_groups), not group-pruned

**Status: untreated.** Surfaced while root-causing the pg16 CI flake in
`audit_multisource_aggregate_secondary_join_is_sublinear` (that flake is now
FIXED — see commit "test: de-flake sublinear plan-quality probes via min-of-N
sampling"; this file records the two REAL residual gaps the investigation
exposed).

## How it surfaced

The pg16 failure ("base grew 25x, flush 12ms→242ms => O(base)") was a false
positive: `auto_explain` on live pg16 proved every incremental statement scans
the 100-row aggregate, never the 500k base. The fixture uses `dim = i % 100 + 1`,
so the 20k and 500k bases both collapse to exactly 100 groups → the small/big
flushes do identical work and the wall-time ratio was pure timing noise on a
loaded runner. Fixed by min-of-N sampling.

## Residual gap 1 (product) — target sync scans the whole target

The incremental target sync emitted for an aggregate IMV is (captured on pg16,
`/tmp/reflex_repro.sql`):

```sql
DELETE FROM "ma_v_b"
  WHERE EXISTS (SELECT 1 FROM "__reflex_affected_ma_v_b" __a
                WHERE "ma_v_b".dim = __a.dim AND "ma_v_b".label IS NOT DISTINCT FROM __a.label);
INSERT INTO "ma_v_b" SELECT ... FROM "__reflex_intermediate_ma_v_b"
  WHERE __ivm_count > 0 AND EXISTS (SELECT 1 FROM "__reflex_affected_ma_v_b" __a WHERE ...);
```

Both plans are a **Seq Scan of the full target / intermediate** (`Seq Scan on
ma_v_b rows=100`, `Seq Scan on __reflex_intermediate_ma_v_b rows=100`) semi-joined
to the 1-row affected set. At 100 groups it's sub-millisecond. But the cost is
**O(total groups)**, not O(affected groups): an aggregate IMV with 50k–1M groups
pays a full-target scan on every 1-row delta flush. This is the **same family** as
`untreated_bugs/2026-06-15_dp_validate_maintenance_cost.md` Bug 1 (partitioned
target DELETE/INSERT not pruned to the changed partition) — here it's the
unpartitioned, many-groups case.

Fix direction: drive the target DELETE/INSERT from `__reflex_affected` (which
holds only the changed group keys) via a join that an index on the target's
group key can serve, instead of `Seq Scan target WHERE EXISTS(affected)`. Needs
a group-key index on the target/intermediate (or a NOT-DISTINCT-aware lookup).
Confirm with EXPLAIN on a high-cardinality-group fixture before/after.

## Residual gap 2 (test coverage) — fixed-group probes can't detect base-scaling

Because the aggregate fixtures (`multisource`, `single-source`, `inner-join`,
`cte`, `union`) use a modulo'd grouping key, the IMV has a FIXED group count
regardless of base size. The "small vs big base" sublinear probes therefore have
**no base-dependent signal** to measure — after the min-of-N de-flake they are
effectively smoke tests (does a 1-row delta flush stay cheap), not the
O(base)-vs-O(delta) gates they read as. Only the calibration passthrough
(`audit_probe_calibration_passthrough_is_sublinear`), whose IMV row count scales
with base, retains real discriminating power.

To make the aggregate probes genuinely test scaling, the group count must scale
with base (e.g. `dim = i` or `i % (n/10)`). ⚠️ Doing so will immediately FAIL on
residual gap 1 above — the un-pruned target sync becomes O(groups) and trips
`assert_sublinear`. So gap 2 (better fixture) and gap 1 (group-pruned target
sync) should be treated together: fix the sync, then strengthen the fixture to
lock it in.

## Artifacts

- Repro (auto_explain on live pg16): `/tmp/reflex_repro.sql`.
- Probes: `src/tests/pg_test_audit_gaps.rs`, `src/tests/pg_test_field_replay.rs`.
- Target-sync codegen: `src/trigger/merge.rs` (target DELETE/INSERT emit).
- Related: `untreated_bugs/2026-06-15_dp_validate_maintenance_cost.md` Bug 1.

---

## RESOLVED / NO-FIX (2026-07-25, PS-13) — planner already prunes at scale; pre-spec's predicate doesn't work

Investigated on `main` @ 3d9a6a5 (1.11.1), PG17, with EXPLAIN (ANALYZE) on a
throwaway probe (now removed). Two regimes measured.

**Gap 1 (unpartitioned, many groups) is fixed by PS-5.** With a sargable `=`
(NOT-NULL keys, or the gated fast variant for nullable keys) plus the composite
group-key index the package creates on every target/intermediate
(`schema_builder.rs:389-421`), the target DELETE/INSERT is index-driven,
O(affected), not O(total groups). Locked by
`audit_ps5_nullable_group_key_target_sync_uses_index_scan`.

**Partitioned target (the 2026-06-15 Bug 1 concern): the planner already prunes
to the affected partition at realistic scale.** Fixture: 20 LIST partitions x
100k rows each = 2M groups, 1-partition/10-group delta.
- Plain path, no predicate, NOT-NULL keys, affected in lowest AND highest region:
  Merge / parameterized Nested-Loop plan with 19 of 20 partitions
  `(never executed)`. 0.07-0.11 ms.
- Report's actual shape — nullable non-partition group key (PS-5 gated), NOT-NULL
  partition key: the sargable FAST variant is a Nested Loop over
  `HashAggregate(affected DISTINCT)` (rows=10) driving an Append whose 19
  non-matching partitions are `(never executed)` via parameterized-nestloop
  runtime pruning (`Index Cond: region = __a.region AND sub = __a.sub`); PS-5's
  gate sits above as a clean `One-Time Filter`. 0.089-0.101 ms.

The all-partition Seq Scan I feared reproduces ONLY at tiny-partition scale
(8 partitions x 625 rows): there the planner picks a Hash Semi Join scanning all
partitions — but that is sub-ms and correctly costed (scanning 5k tiny rows is
cheaper than a parameterized nested loop). It is not the pathology; it appears
exactly where an all-partition scan is cheap and vanishes exactly where it would
hurt.

**The proposed fix direction does not work.** PostgreSQL runtime partition
pruning does NOT support `ScalarArrayOpExpr` (`partcol = ANY(...)`) with a param
or InitPlan array — I confirmed both an `= ANY($1::text[])` external param (forced
generic plan) and `= ANY(ARRAY(SELECT DISTINCT ...))` InitPlan array degrade to a
per-partition `Filter` and scan every partition. Only a scalar `partcol = $1`
param (single value; `Subplans Removed: N`) and a `Const` array prune. So a
spliced multi-partition predicate cannot prune. A per-partition-value scalar loop
could, but (a) needs params threaded through the plain executor
(`trigger/mod.rs::reflex_execute_separated` = bare `Spi::run`, no params), and
(b) carries a silent-wrong-data risk on a NULL LIST partition (`= $1` skips it;
`IS NOT DISTINCT FROM $1` doesn't prune) — complexity + correctness risk far above
the sub-ms gain on plans that already prune. Not worth it under CLAUDE.md
(correctness > simplicity > performance).

**No package change.** The residual delta-flush per-statement base-scaling term is
the deliberately-deferred `ANALYZE intermediate` (see
`2026-07-24_gap2_analyze_intermediate_residual.md`), not the target sync.

## Gap 2 (fixture strengthening) still stands, unblocked

The aggregate audit fixtures still use a modulo'd group key (fixed group count);
they remain smoke tests, not O(base)-vs-O(delta) gates. Since the target sync is
now index/pruning-driven, scaling the group count with base (`dim = i`) is safe to
adopt (it will pass, not trip gap 1). Independent of the above conclusion.
