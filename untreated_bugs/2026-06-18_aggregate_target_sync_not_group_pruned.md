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
