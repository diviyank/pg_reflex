# Keyed passthrough with a WHERE subquery over another table raises a duplicate key when that table is written — S2 (LOUD, blocks writes)

Pre-existing: same outcome on the 1.11.5 base and on the 1.12.0 branch unless stated. Found by the 1.12.0 whole-branch review; fixture and steps are the probes it ran (pg17). Tables `s` and `s2` as in
`2026-10-07_max_with_in_subquery_or_predicate_wrong_after_subquery_table_delete.md`.

## Reproduction

```sql
SELECT create_reflex_ivm('v', 'SELECT id, v FROM s WHERE v >= (SELECT max(v) FROM s2)', 'id');
INSERT INTO s2(g,label,v) VALUES ('q','Q',6),('n','N',7);
-- ERROR: duplicate key value violates unique constraint "__reflex_uk_v"
```

Also `WHERE g IN (SELECT g FROM s2)` followed by `UPDATE s2 SET label = 'AB' WHERE g IN ('a','b')`.
Both were run on the 1.12.0 branch only. On 1.11.5 a variant using a user-defined aggregate in the
subquery failed the same way.

## Expected vs actual

Expected: the write to `s2` succeeds and the IMV follows. Actual: the write is aborted by the
IMV's unique key. Without a declared key the same definition is correct.

## What is established and what is not

Established: the error, and that the keyless form is correct. Not established: the cause.

## Fix direction (hypothesis to falsify)

The trigger on the subquery's table appears to re-insert rows already present instead of
replacing them. For a keyed passthrough, a write to a table referenced only inside a subquery
should take the refresh path the keyless form takes.
