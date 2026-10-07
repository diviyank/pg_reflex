# Top-level ungrouped BOOL_OR over a NOT NULL column returns false instead of NULL once the source is emptied — S1 (SILENT wrong result)

Pre-existing: confirmed on the 1.11.5 base and on the 1.12.0 branch by the Task 4b reviewer
("2 mismatches at `empty`, base and head"). First recorded in the Task 4b author's concerns.

## Reproduction (pg17, from the Task 4b report)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, b BOOLEAN NOT NULL);
INSERT INTO t(b) VALUES (true),(false);
SELECT create_reflex_ivm('x', 'SELECT BOOL_OR(b) AS r FROM t');
DELETE FROM t;
-- x holds false; SELECT BOOL_OR(b) FROM t returns NULL
```

## Expected vs actual

Expected: `r` is NULL once the source is empty. Actual: `false`.

## What is established and what is not

Established: the result, and that the nested form `SELECT NOT BOOL_OR(b) AS r FROM t` is correct
after 1.12.0 (ok through an emptied source; a raw error on base). The author's reading, not
verified by a separate experiment: the flattened reading is
`"__bool_or_b_true_count" > 0`, and the ungrouped guard in `generate_end_query` covers only SUM,
AVG, MIN and MAX, so BOOL_OR is not NULL-guarded when the group is empty. The NOT NULL
qualifier is part of the reproduction as given; the source does not say whether a nullable column
behaves differently.

## Fix direction (hypothesis to falsify)

Extend the ungrouped empty-source guard to BOOL_OR (and any sibling boolean aggregate that reads
a counter), then pin through DELETE of all rows and a refill, ungrouped, with the NOT NULL column.
