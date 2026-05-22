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

## Finding #2: DEFERRED Mode Duplicate Key Violation During Flush

**Status:** OPEN

**Title:** A single-table passthrough view with DEFERRED incremental maintenance, after DML mutations, fails during reflex_flush_deferred() with "duplicate key value violates unique constraint" error. This suggests the maintenance logic is attempting to insert or merge rows that would create duplicate key violations in the target IMV.

**Exact Repro:**

```sql
CREATE TABLE f2_t0 (id int primary key, m numeric, d text);

CREATE MATERIALIZED VIEW f2_mv AS
  SELECT id, m, d FROM f2_t0;

SELECT create_reflex_ivm('f2_imv', '<same body>', 'id', deferred:=true);

INSERT INTO f2_t0 VALUES (0,0.0,'g0'),(1,1.1,'g1'),(2,2.2,'g2'),(3,3.0,'g3'),(4,4.1,'g0'),(5,0.2,'g1'),(6,1.0,'g2'),(7,2.1,'g3');
UPDATE f2_t0 SET m = m + 1 WHERE id % 2 = 0;
INSERT INTO f2_t0 (id,m,d) VALUES (8,3.2,'g0');

SELECT reflex_flush_deferred('f2_imv');
REFRESH MATERIALIZED VIEW f2_mv;
```

**Expected vs Actual:**

- **Expected:** reflex_flush_deferred() succeeds; IMV rows match MV rows after flush.
- **Actual:** reflex_flush_deferred() raises "duplicate key value violates unique constraint" during the MERGE operation into the target table.

**Suspected Root Cause:**

The DEFERRED mode incremental maintenance logic in pg_reflex accumulates changes in a temporary staging/delta table during DML, then applies them via a MERGE on flush. The issue appears to be that:

1. When multiple rows are modified or inserted in the same transaction, the delta tracking may not properly coalesce them (e.g., old key value and new key value for the same logical row).
2. The MERGE statement generated for DEFERRED flush may not have correct source deduplication or handling of source row cardinality > 1 per key, leading to duplicate key violation on the target.
3. Alternatively, the incremental trigger/delta-capture logic may be capturing the same row twice or with conflicting state (both inserted and updated).

**Affected Shapes (Parked):**

The DEFERRED mode variant was excluded from the fuzzer's `fuzz_case()` random generator before a deterministic repro could be captured. Instead of keeping DEFERRED variants enabled and exposing pg_reflex bugs during fuzzing, DEFERRED is parked with a comment:

```rust
// PARKED: DEFERRED mode excluded while finding #2 (docs/fuzz-findings.md) is open.
```

To re-enable DEFERRED variants, add this to `fuzz_case()`:

```rust
// Add a strategy arm that assigns deferred=true to some cases (e.g., via `any::<bool>()`)
// Then uncomment the DEFERRED variant handling in the generator
```

**How to Re-enable:**

1. Fix the DEFERRED mode duplicate-key issue in pg_reflex's flush/MERGE code generation or delta-table logic.
2. Uncomment the DEFERRED-variant strategy in `fuzz_case()` (add `any::<bool>()` strategy and `case.deferred = deferred;` assignment).
3. Re-enable the `generator_reaches_filtered_and_deferred` unit test assertion for deferred cases.
4. Remove `#[ignore]` from `finding_2_deferred_mode_duplicate_key_violation` in `findings` mod and run it to confirm the fix.

**Test Reference:**

The exact repro is encoded as an `#[ignore]`'d regression test:
`src/tests/pg_test_fuzz.rs::findings::finding_2_deferred_mode_duplicate_key_violation`

To run it (after fix):
```bash
cargo pgrx test pg17 finding_2_deferred_mode_duplicate_key_violation -- --ignored
```
