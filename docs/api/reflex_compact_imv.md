# `reflex_compact_imv` / `reflex_compact_all_imv`

(1.4.5+) Runs `VACUUM (FULL)` on an IMV's intermediate and target tables to materialize the `fillfactor=70` set by the 1.4.3→1.4.4 migration. HOT updates only fire once pages have been rewritten with the new fillfactor, so legacy IMVs benefit from a one-time compaction.

## Signatures

```sql
reflex_compact_imv(view_name TEXT)  RETURNS TEXT
reflex_compact_all_imv()            RETURNS TEXT
```

## Behaviour

- `reflex_compact_imv` runs `VACUUM (FULL)` on both the intermediate and target tables of one IMV.
- `reflex_compact_all_imv` is a convenience wrapper that runs `reflex_compact_imv` on every enabled IMV in `(graph_depth, name)` order. A failure on one IMV does not abort the rest; the per-IMV outcome is summarized in the return value.

`VACUUM (FULL)` takes an `ACCESS EXCLUSIVE` lock on each table and rewrites it — schedule during a maintenance window for multi-gigabyte IMVs.

## Examples

```sql
-- One IMV
SELECT reflex_compact_imv('sales_by_region');

-- Every enabled IMV
SELECT reflex_compact_all_imv();
```
