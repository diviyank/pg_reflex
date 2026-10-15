# IMMEDIATE top-K MIN/MAX: a writable CTE INSERT + UPDATE on the same groups leaves a phantom heap entry

**Severity:** silent wrong result (MIN/MAX wrong after a later DELETE). Pre-existing (reproduced on e3105b1 / 1.12.1 code). Found 2026-10-14 during 1.12.2 B2 work.

```
ADJACENT BUG (found during B2, pre-existing on 1.12.1 c0e138f / e3105b1, NOT fixed by B2)
IMMEDIATE top-K MIN/MAX: a writable CTE that INSERTs into a source and UPDATEs the same
groups of it in one statement leaves a phantom entry in the top-K heap. A later DELETE of
the real row then leaves a WRONG MIN in the target (silent).

Severity: silent wrong result. Needs IMMEDIATE + top-K MIN/MAX + one statement with two
modifying parts on the same source (writable CTE, also likely MERGE / upserts).

Reproduction (pg16 cassert, pg_reflex 1.12.1 .so md5 3585efdd..., plain table):
  CREATE TABLE s (plan INT, d DATE NOT NULL, yr INT NOT NULL, k INT NOT NULL, key_column INT, v INT NOT NULL);
  INSERT INTO s SELECT p, d::date, EXTRACT(YEAR FROM d)::int, k, 0, k*7
    FROM generate_series(1,3) p, generate_series(DATE '2027-11-01', DATE '2028-02-28', INTERVAL '1 day') d,
         generate_series(1,3) k;
  SELECT create_reflex_ivm('v', 'SELECT plan, yr, MIN(d) AS lo, MAX(d) AS hi, COUNT(*) AS n FROM s GROUP BY plan, yr',
    NULL, NULL, 'IMMEDIATE', NULL, ARRAY[]::text[]);
  WITH i AS (INSERT INTO s (plan, d, yr, k, key_column, v)
             SELECT plan, d, yr, k + 2000, key_column, v FROM s WHERE plan = 1 AND k = 1 RETURNING 1)
  UPDATE s SET d = d + 1, k = k + 100 WHERE plan = 1;
    -> __min_d_topk of (1, 2027) = {11-01, 11-01, 11-02 x5, ...}; the source has ONE row on 2027-11-01.
  DELETE FROM s WHERE plan = 1 AND d = '2027-11-01';
    -> v: (1, 2027) lo = 2027-11-01; fresh: 2027-11-02.  Target wrong.

Mechanism (inferred): both AFTER STATEMENT triggers fire at the end of the statement, when the
source already holds both changes. The UPDATE trigger's forced top-K recompute
(build_min_max_recompute_sql_force_topk) re-reads the heap from the source, which already
contains the inserted rows; the INSERT trigger then merges its delta into that heap again.
The scalar and __ivm_count stay right (the recompute only rewrites MIN/MAX columns, and the
algebra of the counts is exact), so the target looks right until a retraction exposes the
phantom. This is the hazard schema_builder.rs ~690-693 describes for rebuilds in statement
triggers, reached by the forced recompute, which is a partial rebuild.

The reverse order (WITH u AS (UPDATE ...) INSERT ...) was correct in the same probe.
Fix direction (not attempted): in IMMEDIATE, the forced recompute must not read rows another
pending trigger of the same statement will still add; or the heap merge of a later trigger
must not re-add rows the recompute already counted. Needs its own Step 0.
Test that pins it: the third form, removed from v1122_minmax_bulk_immediate_keeps_algebraic_path
(src/tests/pg_test_v1122_minmax_bulk_replace.rs), fails the intermediate oracle on base.
```

Needs its own Step 0 (mechanism unconfirmed).
