# An IMV whose query holds a RIGHT JOIN is recomputed in full on every write — perf (correct, slow)

Introduced on purpose by the 1.12.0 bug-fold work (group G1, joins) as the fix for the silent
wrong aggregate on a write to the table before a `RIGHT JOIN`. Nothing wrong is stored; the cost
is one full recompute of the IMV per writing statement (IMMEDIATE) or per flush (DEFERRED), for a
write to any source of the query.

## Reproduction

```sql
CREATE TABLE dim(kind TEXT PRIMARY KEY, label TEXT);
CREATE TABLE fact(id SERIAL PRIMARY KEY, kind TEXT, v INT);
SELECT create_reflex_ivm('v', 'SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s
  FROM fact f RIGHT JOIN dim d ON d.kind = f.kind GROUP BY d.label');
INSERT INTO fact(kind, v) VALUES ('a', 1);   -- TRUNCATE + refill of the intermediate table
```

`reflex_build_delta_sql` returns the full-refresh statements for every operation on `fact` and
on `dim` (`has_no_scoped_delta` in `src/trigger/mod.rs`).

## Workaround

Write the join as a LEFT JOIN with the tables swapped. `dim d LEFT JOIN fact f ON d.kind = f.kind`
returns the same rows and is maintained incrementally (oracle-tested through INSERT, UPDATE and
DELETE on both tables: `v112_j_lj_df_by_d`, `v112_j_lj_df_by_f`, `v112_j_lj_pt_df`,
`v112_j_kp_left_join` in `src/tests/pg_test_v112_joins.rs`).

## Why it is not scoped

Two things were tried and found wrong by the oracle, so the full recompute was kept:

- Routing the table before the keyword through the outer-join secondary path and the table after
  it through the plain delta. The plain delta of the table after the keyword is exact in itself,
  but the plan is not symmetric: `source_join_keys` and the passthrough key mappings are built
  for the table a JOIN introduces, and map its key to a group or passthrough key by bare column
  name. For `fact f RIGHT JOIN dim d … GROUP BY f.kind` that maps `dim.kind` to the group key,
  which is NULL for an unmatched `dim` row; a DELETE on `dim` then removes the wrong group
  (2 oracle mismatches on `DELETE FROM dim`).
- Leaving the table after the keyword on the outer-join secondary path, where it went before:
  wrong for a join-key UPDATE when the group key is read from the other table
  (`dim d RIGHT JOIN fact f … GROUP BY d.label`, `UPDATE fact SET kind = …`: 1 mismatch).

## Fix direction (hypothesis)

Rewrite `a RIGHT JOIN b ON c` to `b LEFT JOIN a ON c` in the analyser, before the plan is built,
when the FROM clause is exactly that two-table join, so one orientation carries all the scoped
paths. `SELECT *` and `USING` / `NATURAL` need care (column order, merged column). Chains mixing
a RIGHT JOIN with other joins would stay on the full recompute. A create-time WARNING naming the
workaround would make the cost visible in the meantime.
