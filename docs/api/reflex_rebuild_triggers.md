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

Since 1.12.0 it also:

- installs the triggers of a **partitioned** source on every table of its partition tree (see [Per-source triggers](../concepts/architecture.md#per-source-triggers)). After upgrading from 1.11.5 without recreating the IMVs, this is what makes statements and TRUNCATEs addressed to a partition maintained; follow it with `reflex_reconcile` of the source's IMVs;
- drops and recreates **disabled** triggers of the source and of its partition tree. Create and the COMMIT-time partition sync only add missing triggers, so a `DISABLE TRIGGER` is undone by `reflex_rebuild_triggers` alone;
- rebuilds under the spelling the IMVs registered, whatever spelling is passed.

It is the first of the two statements the `trigger-attached` finding of [`reflex_audit`](reflex_audit.md) prescribes; the second is `reflex_reconcile('<imv>')`.

!!! warning "Not on the base table of a window IMV"
    The `<imv>__base` table under a window IMV carries no triggers by design: the window layer is a view. `reflex_rebuild_triggers('<imv>__base')` installs a set, after which every write to the IMV's source raises `aggregations for '<imv>' must be valid JSON`. Run it on sources that carry triggers already; the query in [Upgrading to 1.12.0](../getting-started/upgrading.md#if-imvs-are-not-recreated) lists them.

## Example

```sql
SELECT reflex_rebuild_triggers('sales');
```
