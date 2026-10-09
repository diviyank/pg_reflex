# drop_reflex_ivm leaves the source triggers when their names were shortened with a hash — S2 (leftover objects, loud later)

Pre-existing. Found on pg17 while testing the 1.12.0 fold group G3 (a rejected create over a CTE left
a trigger behind; the cause is in the drop path, not in the rejection).

## Reproduction (pg17)

```sql
CREATE TABLE v112_ty_zz_long_named_source_table_for_drop(id SERIAL PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('x', 'SELECT g, SUM(v) AS r FROM v112_ty_zz_long_named_source_table_for_drop GROUP BY g');
SELECT drop_reflex_ivm('x');   -- DROP REFLEX INCREMENTAL VIEW
SELECT tgname FROM pg_trigger
 WHERE tgrelid = 'v112_ty_zz_long_named_source_table_for_drop'::regclass AND NOT tgisinternal;
-- __reflex_trigger_del_on_v112_ty_zz_long_named_source_t_659a7470
-- __reflex_trigger_ins_on_v112_ty_zz_long_named_source_t_314b7217
-- __reflex_trigger_trunc_on_v112_ty_zz_long_named_source_a2af9266
-- __reflex_trigger_upd_on_v112_ty_zz_long_named_source_t_6ca67901
```

With a 38-character source name only the `trunc` trigger is left (its prefix is two characters
longer, so it alone crosses 63 bytes): a create over
`WITH c AS (SELECT ... FROM <38-char source>) SELECT ...` that is rejected after the CTE sub-IMV was
built rolls the sub-IMV back through `drop_reflex_ivm_impl` and leaves
`__reflex_trigger_trunc_on_<prefix>_<hash>` on the source.

## Expected vs actual

Expected: no pg_reflex trigger remains on a source once no IMV depends on it. Actual: all four (or
the truncate one) remain.

## What is established and what is not

Established: the leftover names above. Create shortens an over-long trigger name to
`<prefix>_<8 hex>`; `src/drop_ivm.rs` (step 9, `DROP TRIGGER IF EXISTS "__reflex_trigger_{op}_on_{safe_source}"`)
builds the unshortened name, which PostgreSQL truncates to a different 63-byte identifier, so
`IF EXISTS` matches nothing. Not established: whether the trigger functions are dropped (the
`DROP FUNCTION IF EXISTS` beside it builds its name the same way), what a write to the source does
afterwards, and whether reconcile / doctor report the orphans.

## Fix direction (hypothesis to falsify)

Have drop derive the trigger and function names through the same helper create uses, or drop by
`pg_trigger` lookup (`tgrelid` = source, `tgname LIKE '__reflex_trigger_%'`) once no IMV depends on
the source. Pin with a source name of 38 and of 45 characters, create then drop, zero triggers left.
