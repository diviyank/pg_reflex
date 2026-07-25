# `reflex_rebuild_union_mirror`

(1.11.1+) Repair primitive for a materialised UNION-ALL wrapper — the TABLE built when a CTE feeding a set-op (`UNION ALL`/`UNION`) is consumed by an aggregate. Re-installs the `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers and their trigger functions on every operand recorded in the wrapper's `depends_on`.

## Signature

```sql
reflex_rebuild_union_mirror(wrapper TEXT)
RETURNS TEXT
```

## When to use

- A mirror trigger or trigger function on an operand was dropped (manually, or by DDL that didn't go through pg_reflex).
- An operand's mirror trigger functions collided under NAMEDATALEN truncation prior to 1.11.1 (see the [changelog](../changelog.md) entry for the wrapper-function-naming fix) and need re-installing under the corrected, collision-safe names.

It refuses cleanly (an `ERROR:` string, not a raised exception) on:

- a **VIEW** wrapper — it has no operand triggers to rebuild by design, since each operand sub-IMV maintains its own target independently;
- an IMV that isn't a decomposed UNION-ALL/set-op wrapper row at all.

Restores future maintenance only — it does not backfill deltas missed while a trigger was absent or broken. If the wrapper may already be stale (e.g. an operand was mutated while its mirror trigger was missing), run [`reflex_reconcile`](reflex_reconcile.md) on the wrapper afterward.

## Example

```sql
SELECT reflex_rebuild_union_mirror('sales_by_region__union_wrapper');
```
