# `reflex_rebuild_triggers`

(1.4.5+) Re-emits the consolidated trigger function bodies for a **source table**, picking up the latest codegen via `CREATE OR REPLACE` without changing trigger identity.

## Signature

```sql
reflex_rebuild_triggers(source_table TEXT)
RETURNS TEXT
```

!!! note "Argument is the source table, not the IMV"
    Pass the name of a source table that IMVs read from — every consolidated trigger function attached to that table is re-emitted. To rebuild an IMV's data instead, use [`reflex_rebuild_imv`](reflex_reconcile.md).

## When to use

Use to install codegen fixes on triggers attached to IMVs created by an older version — the 1.4.4→1.4.5 migration used it to install the filter-aware spurious-skip block. `CREATE OR REPLACE` overwrites the function body in place, so trigger attachment and identity are preserved.

## Example

```sql
SELECT reflex_rebuild_triggers('sales');
```
