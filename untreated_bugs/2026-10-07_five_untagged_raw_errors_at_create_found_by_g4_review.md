# Five query shapes fail at create with a raw PostgreSQL error instead of a tagged rejection — S3 (LOUD at create, nothing left behind)

Pre-existing: same outcome on `8ef9743`. Found by the adversarial review of the G4 fold (pg18).
Tables as in `2026-10-07_deferred_cte_two_tables_written_in_one_transaction_parent_not_refreshed.md`
(`s`, `t`) and `fact(id, kind, v)`, `dim(kind, label)`.

## Reproduction

| Query | Error |
|---|---|
| `SELECT d.label, COUNT(*) AS c FROM dim d LEFT JOIN fact f ON d.kind = f.kind GROUP BY d.label HAVING SUM(f.v) IS NULL` | `missing FROM-clause entry for table "f"` (`HAVING COUNT(f.id) = 0` works) |
| `SELECT m.g, SUM(m.v) AS sv FROM s m GROUP BY m.g HAVING EXISTS (SELECT 1 FROM t x WHERE x.g = m.g)` | `missing FROM-clause entry for table "m"` |
| `SELECT SUM(v) FROM s HAVING SUM(v) > (SELECT max(v) FROM t)` | `syntax error at or near "AND"` |
| `SELECT m.id, m.g, RANK() OVER (PARTITION BY m.g ORDER BY m.v, m.id, j.id) FROM s m JOIN t j ON j.g = m.g` | `missing FROM-clause entry for table "m"` |
| `SELECT d.kind, f.kind AS fk, COUNT(*) AS c FROM dim d FULL JOIN fact f ON d.kind = f.kind GROUP BY d.kind, f.kind` | `column "kind" specified more than once` |

## Expected vs actual

Expected: the IMV is created, or `create_reflex_ivm` returns the tagged rejection naming the
unsupported construct and a remedy. Actual: an untagged PostgreSQL error from generated SQL.

## What is established and what is not

Established: the five errors at create. Not established: whether each is a missing feature or a
generation defect, and whether anything is left behind by the failed create (the review did not
check for leftover relations).

## Fix direction (hypothesis to falsify)

One cause per row, probably: a HAVING or window expression that names a source alias is copied
into `end_query`, which reads only the intermediate table (rows 1, 2, 4); an ungrouped HAVING is
appended to a WHERE that is not there (row 3); two group columns normalise to the same
intermediate column name (row 5, compare
`2026-10-07_duplicate_bare_group_by_column_names_raw_error.md`). Fix or reject each at create.
