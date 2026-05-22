# Differential Fuzz Harness Findings

This document catalogs open findings from the differential correctness fuzz harness (`src/tests/pg_test_fuzz.rs`).

## Finding #1: LEFT JOIN Unmatched Primary Insert Drops Row

**Status:** OPEN

**Title:** A two-table LEFT JOIN aggregate, after inserting a new PRIMARY-side row that matches NO secondary row, DROPS that row from the IMV instead of keeping it with the secondary columns NULL.

**Exact Repro:**

```sql
CREATE TABLE t0 (id int primary key, m numeric, d text);
CREATE TABLE t1 (id int primary key, fk int, w numeric);

CREATE MATERIALIZED VIEW v_mv AS
  WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM t1 GROUP BY fk)
  SELECT t0.id, SUM(t0.m) AS s, a.sw FROM t0 LEFT JOIN agg a ON a.g = t0.id GROUP BY t0.id, a.sw;

SELECT create_reflex_ivm('v_imv', '<same body>', 'id');

INSERT INTO t0 VALUES (0,0.0,'g0'),(1,1.1,'g1'),(2,2.2,'g2'),(3,3.0,'g3'),(4,4.1,'g0'),(5,0.2,'g1'),(6,1.0,'g2'),(7,2.1,'g3');
INSERT INTO t1 VALUES (0,0,0.0),(1,1,1.1),(2,2,2.2),(3,3,3.0),(4,4,4.1),(5,0,0.2),(6,1,1.0),(7,2,2.1);

UPDATE t1 SET w = w + 1 WHERE id % 2 = 0;
INSERT INTO t0 (id,m,d) VALUES (8,3.2,'g0');
REFRESH MATERIALIZED VIEW v_mv;
```

**Expected vs Actual:**

- **MV (correct):** Contains row `(8, 3.2, NULL)` — the new primary-side row with secondary columns NULL (no matching secondary rows).
- **IMV (bug):** MISSING row `(8, 3.2, NULL)` — the row is dropped entirely.

**Suspected Root Cause:**

The data-probe (initial population of the IMV) marks outer-join columns as NOT NULL based on the initial all-matched state. During incremental MERGE maintenance, when a new primary-side row is inserted with no secondary matches, the NULL secondary columns fail the NOT NULL constraint or are filtered by the maintenance logic, causing the entire row to be dropped instead of being inserted with NULLs.

**Affected Shapes (Parked):**

- `join_aggregate_case`: Direct LEFT JOIN with aggregate
- `carried_scalar_case`: LEFT JOIN with a carried scalar expression (COALESCE, CASE, EXISTS, or cast)
- `cte_decomposed_case`: LEFT JOIN through a CTE decomposition

All three shapes are currently parked (disabled) in the `fuzz_case()` random gate to keep the harness GREEN while this finding is open.

**How to Re-enable:**

1. Fix the LEFT JOIN unmatched-row issue in the pg_reflex codegen (likely in `src/create_ivm.rs` or trigger generation).
2. Remove the `#[allow(dead_code)]` annotations and comments from the three case constructor functions.
3. Restore the three case arms in `fuzz_case()`:
   ```rust
   two_tables().prop_map(|(a, b)| join_aggregate_case(a, b)),
   (two_tables(), any::<usize>()).prop_map(|((a, b), p)| carried_scalar_case(a, b, p)),
   two_tables().prop_map(|(a, b)| cte_decomposed_case(a, b)),
   ```
4. Remove `#[ignore]` from `generator_can_emit_join_and_cte_and_carried_shapes` in `generate_tests`.
5. Remove `#[ignore]` from the `finding_1_leftjoin_unmatched_primary_insert_drops_row` test in `findings` mod and run it to confirm the fix.

**Test Reference:**

The exact repro is encoded as an `#[ignore]`'d regression test:
`src/tests/pg_test_fuzz.rs::findings::finding_1_leftjoin_unmatched_primary_insert_drops_row`

To run it (after fix):
```bash
cargo pgrx test pg17 finding_1_leftjoin_unmatched_primary_insert_drops_row -- --ignored
```
