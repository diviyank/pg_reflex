# Passthrough over `a LEFT JOIN b … JOIN c` whose three tables share the key column name raises "column reference is ambiguous" on a write to `b` — S2 (LOUD, blocks writes)

Pre-existing. Observed during the 1.12.0 bug-fold work (group G4) on pg18 while writing controls
for the LEFT JOIN filter fix; the same error is raised on the branch before that work
(`8ef9743`). The G4 changes do not touch this route.

## Reproduction (pg18)

```sql
CREATE TABLE fact(kind TEXT PRIMARY KEY, v INT);
CREATE TABLE dim(kind TEXT PRIMARY KEY, label TEXT);
CREATE TABLE other(kind TEXT PRIMARY KEY, w INT);
INSERT INTO fact VALUES ('a',1),('b',-2),('c',-3),('z',-4);
INSERT INTO dim VALUES ('a','A'),('b','B'),('c','C'),('u','U'),('e','E');
INSERT INTO other VALUES ('a',1),('b',2),('c',3),('z',4);
SELECT create_reflex_ivm('v', 'SELECT d.kind, d.label, f.v, o.w
  FROM dim d LEFT JOIN fact f ON d.kind = f.kind JOIN other o ON o.kind = f.kind');
UPDATE fact SET v = 5 WHERE kind IN ('b', 'c');
-- ERROR: column reference "kind" is ambiguous
```

Every INSERT, UPDATE and DELETE on `fact` raises it (IMMEDIATE, no unique key declared). The IMV
is correct after create.

## Expected vs actual

Expected: the write succeeds and the IMV follows. Actual: the statement is aborted.

## What is established and what is not

Established: the error, on every write to `fact`, before and after the G4 work. The two-table
form (`dim d LEFT JOIN fact f`, same columns without `o.w`) is correct through the same writes
(`v112_sq_lj_pt_admits_null`, `v112_sq_lj_pt_rejects_null`), and the aggregate over the same
three-table FROM clause is correct (`v112_sq_lj_then_inner_on_fact`,
`v112_sq_lj_then_inner_on_fact_by_label`).

Not established: which generated statement holds the ambiguous reference, whether writes to `dim`
and `other` raise too (the probe stopped at the first table), and whether distinct key column
names avoid it.

## Fix direction (hypothesis)

`fact` is the table a LEFT JOIN introduces, so its writes go through the keyed branch of
`outer_join_secondary_stmts` (`src/trigger/ops.rs`), which filters `SELECT * FROM (base_query)`
and the target by the passthrough key mapping of the source. With `kind` exposed by three tables
an unqualified `"kind"` in one of those statements has several candidates. Reproduce, read the
statements `reflex_build_delta_sql` returns for the write, and qualify the reference or fall back
to the full rebuild when the mapped column name is not unique among the joined tables.
