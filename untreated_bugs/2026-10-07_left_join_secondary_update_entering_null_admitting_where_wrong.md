# LEFT JOIN secondary: an UPDATE that brings its rows into a WHERE that also admits null-extended rows leaves the aggregate wrong — S1 (silent wrong aggregate)

Pre-existing. Observed during the 1.12.0 bug-fold work (group G1, joins) on the `release/1.12.0`
line, on pg17, while writing the LEFT JOIN control for the RIGHT JOIN fix. The G1 changes do not
touch this route (a directly joined LEFT JOIN table, read once, no RIGHT JOIN in the query). The
mirrored RIGHT JOIN is correct since G1, because a query holding a RIGHT JOIN is recomputed in
full on every write.

## Reproduction (pg17)

```sql
CREATE TABLE fact(kind TEXT PRIMARY KEY, v INT);
CREATE TABLE dim(kind TEXT PRIMARY KEY, label TEXT);
INSERT INTO fact VALUES ('a',1),('b',-2),('c',-3),('z',-4);
INSERT INTO dim VALUES ('a','A'),('b','B'),('c','C'),('u','U');
SELECT create_reflex_ivm('v', 'SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s
  FROM dim d LEFT JOIN fact f ON d.kind = f.kind
  WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind');
UPDATE fact SET v = 5 WHERE kind IN ('b', 'c');   -- 2 oracle mismatches
UPDATE fact SET v = 6 WHERE kind = 'z';           -- 6 (cumulative)
UPDATE fact SET v = -1 WHERE kind = 'a';          -- 4
UPDATE fact SET v = 9 WHERE kind = 'a';           -- 7
```

Mismatch counts are those of the bidirectional `EXCEPT ALL` oracle (IMV against a fresh run of
the query) after each statement, without reconciling in between. The IMV is correct after create.
The counts are the same with default settings and with the bulk path forced
(`ANALYZE` of the intermediate table, then `SET reflex.wipe_threshold = '0'`), so the defect is
not specific to the bulk path.

Control: the same fixture and statements over `dim d JOIN fact f` (inner join) are correct.

## Expected vs actual

Expected: after each UPDATE the IMV equals a fresh run of the query. Actual: the first UPDATE,
which moves two `fact` rows from failing the WHERE to passing it, already leaves two rows
different, and the IMV stays wrong until a reconcile.

## What is known about the mechanism

Not investigated. What distinguishes the shape: the WHERE reads only columns of the null-extended
table and is true for a null-extended row (`f.v IS NULL`), so the per-source WHERE the UPDATE
trigger evaluates on the transition rows (`imv_relevant_where`) does not describe which joined
rows pass: a `dim` row with no matching `fact` row passes, the same `dim` row matched to a
`fact` row with `v <= 0` does not. The UPDATE is then classified by that per-source WHERE (old
images filtered out, new images kept) and dispatched as a promoted INSERT. Which statement of the
generated delta is wrong was not traced.

Not investigated: INSERT and DELETE on `fact` under the same WHERE; a passthrough IMV; a WHERE
on the null-extended table that rejects NULL (which makes the join an inner join in effect).

## Fix direction (hypothesis)

Either stop classifying an UPDATE by the per-source WHERE when the source is the null-extended
side of an outer join (treat it as a plain UPDATE through the outer-join secondary path), or drop
the per-source WHERE for such a source at create. Add oracle tests for INSERT, UPDATE (entering,
leaving, staying) and DELETE on the null-extended table, with a WHERE that admits NULL and one
that rejects it.
