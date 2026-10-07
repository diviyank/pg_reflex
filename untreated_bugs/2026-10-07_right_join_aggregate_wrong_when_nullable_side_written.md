# RIGHT JOIN aggregate IMV is wrong when the nullable (left) table is written — S1 (silent wrong aggregate), highest severity of the 1.12.0 batch

Pre-existing (not introduced or changed by 1.12.0). Observed during the 1.12.0 hardening work on
the `release/1.12.0` branch (base commit 6946400), on pg15. Found by the Task 1b implementer
(Step 0 of the guard item) and independently confirmed by the Task 1b adversarial reviewer (P2).
It is plain unquoted SQL, so it needs no unusual spelling, and the mirrored LEFT JOIN is
correct, so users who write the join one way see no problem and users who write it the other
way get wrong numbers with no error.

## Reproduction (pg15)

```sql
CREATE SCHEMA s;
CREATE TABLE s.dim(kind TEXT PRIMARY KEY, label TEXT);
INSERT INTO s.dim VALUES ('a','A'),('b','B'),('u','U');
CREATE TABLE fact(id SERIAL PRIMARY KEY, kind TEXT, v INT);
INSERT INTO fact(kind, v) VALUES ('a',1),('b',2),('c',3),('a',4);

-- nullable side = fact (left of RIGHT JOIN)
SELECT create_reflex_ivm('v', 'SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s
  FROM fact f RIGHT JOIN s.dim d ON d.kind = f.kind GROUP BY d.label');
INSERT INTO fact(kind, v) VALUES ('a',10),('z',20);
```

Reviewer's recorded result after the INSERT: IMV `(B,2,2),(U,2,NULL),(A,3,15)` against a fresh
run of the query `(B,1,2),(U,1,NULL),(A,3,15)`. The Task 1b implementer's oracle run reports 6
mismatches for this INSERT, 5 mismatches after `DELETE FROM fact WHERE id = 3`, and a correct
result for an `UPDATE` of `v`.

The same defect with the nullable side being `dim` (each step starting from a reconciled IMV):

```sql
SELECT create_reflex_ivm('v2', 'SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s
  FROM s.dim d RIGHT JOIN fact f ON d.kind = f.kind GROUP BY d.label');
INSERT INTO s.dim VALUES ('c','C'),('w','W');   -- 1 oracle mismatch
DELETE FROM s.dim WHERE kind = 'b';             -- 1 oracle mismatch
```

Grouping by the other table's column is wrong too (`GROUP BY f.kind`): 2 / 2 / 1 mismatches for
the first query's `dim` insert, `fact` insert and `fact` delete; 6 / 3 for the second query's
`dim` insert and delete. DML on the preserved side, grouped by its own column, is correct in both
shapes.

## Control: the mirrored LEFT JOIN is correct

`SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM s.dim d LEFT JOIN fact f ON d.kind = f.kind
GROUP BY d.label`, same fixture, same `fact` INSERT, UPDATE and DELETE: correct (Task 1b review,
section 5 (i)).

## Expected vs actual

Expected: the IMV equals a fresh run of its query after every statement. Actual: the groups
touched by the written nullable-side rows are counted again (the unmatched `dim` rows are
null-extended and counted a second time; `B` and `U` above gain a row each) and the result stays
wrong until a reconcile.

## What is known about the mechanism

- By the implementers' reading, the outer-join guard in `src/trigger/mod.rs` (about lines
  88-108 on the Task 1b review's tree) only inspects the table that follows the join keyword.
  For a `RIGHT JOIN` that is the preserved side, whose plain delta is already exact, so the
  guard firing or not makes no difference there. The table that needs the outer-join handling is
  the one before the keyword, and the guard never examines it. This is a reading of the code, not
  a traced execution.
- The result is the same on the base code by construction (the Task 1b branch did not change
  this guard's RIGHT JOIN decision).
- Existing RIGHT JOIN aggregate coverage only mutates the preserved table
  (`src/tests/pg_test_correctness.rs:3476`, "only mutate primary (right) table"), which is why
  nothing caught it.
- Not investigated: the INNER JOIN or FULL JOIN mixes with a RIGHT JOIN, and whether
  `RIGHT OUTER JOIN` behaves differently from `RIGHT JOIN`.

## Fix direction (hypothesis)

Treat the table before a `RIGHT [OUTER] JOIN` as the nullable side, with the same handling the
`LEFT JOIN` secondary gets (reconcile or scratch-fill path that recomputes from the source), or,
as a cheaper first step, rewrite `a RIGHT JOIN b` to `b LEFT JOIN a` in the analyser before the
guards run, so only one orientation has to be correct. Either way add oracle tests that mutate
the nullable side (insert, update, delete) in both orientations, and group by a column of each
table; the P2 reproduction above is the red test.
