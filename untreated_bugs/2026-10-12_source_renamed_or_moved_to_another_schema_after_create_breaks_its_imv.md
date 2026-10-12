# A source renamed (`ALTER TABLE … RENAME TO`) or moved (`SET SCHEMA`) after its IMV was created breaks the IMV: the next write raises `relation "…" does not exist` — S2 (write-blocking, loud; pre-existing)

Stated as pre-existing by the review of the create refusals of 1.12.0 (`p3-ac-review.md`, A2
table, PostgreSQL 18, base `d0a3e06` and head `5d856e1`) and seen by the fix-wave review
(`fix-wave-review.md`, N15, pg17: "A rename after create is loud"), both in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`. Not run on 1.11.5. No report
existed for it.

## What the sources give

No script is printed. The row of the review:

> Collision made later by `RENAME` or `SET SCHEMA` of a source — not silent: the next UPDATE of
> the renamed table raises `relation "zq_plain" does not exist`; `reflex_audit` reports
> `source-exists`. A renamed source breaks its IMV whatever its new name (pre-existing)

The shape, as described (names other than `zq_plain` are placeholders):

```sql
CREATE TABLE zq_plain(id INT PRIMARY KEY, v INT);
SELECT create_reflex_ivm('x', 'SELECT id, v FROM zq_plain');
ALTER TABLE zq_plain RENAME TO zq_other;        -- or: ALTER TABLE zq_plain SET SCHEMA s2;
UPDATE zq_other SET v = v + 1;
-- ERROR:  relation "zq_plain" does not exist
```

## Expected vs actual

Expected: either the rename is refused or announced (as `pg_reflex.alter_source_policy` does for
a column change), or the IMV follows the table, which keeps its OID and its triggers. Actual:
the triggers stay on the renamed table and their generated SQL and the registry still name the
old spelling, so every write to the table raises until the IMV is dropped and recreated;
`reflex_audit` reports `source-exists`.

## What is established and what is not

Established: the error on the next UPDATE and the audit category. Not established: what the
`alter_source_policy` event trigger says at the moment of the rename (WARNING, or an error under
`'error'`); INSERT and DELETE (only UPDATE is quoted); DEFERRED mode, where the write would be
staged and the failure could move to the flush; whether `reflex_rebuild_imv` /
`reflex_rebuild_chain` can repair it (the stored query names the old table, so presumably not);
what a rename back does; whether the remedy the audit prints for `source-exists` converges.

## Fix direction (hypothesis to falsify)

In `__reflex_on_ddl_command_end`, treat `ALTER TABLE … RENAME TO` / `SET SCHEMA` of a registered
source as the refusable event it is: under `alter_source_policy = 'error'` refuse it; otherwise
WARN with the exact remedy. Rewriting the stored query and the trigger functions to the new name
is the larger alternative (the source is spelled inside `sql_query`, `base_query`, `depends_on`
and every generated function), and a rename can create a trigger-name collision the create guard
would have refused. Test: rename and `SET SCHEMA`, both modes, writes of the three kinds, then
the printed remedy and the oracle.
