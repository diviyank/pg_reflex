# `refresh_imv_depending_on`

Reconciles every IMV whose `depends_on` list contains the given source, in `graph_depth` order (L1 before L2 before L3, …). Useful after refreshing a `MATERIALIZED VIEW` that feeds IMVs, after a bulk load run with triggers disabled, or any time a source has been mutated outside of the trigger path.

## Signature

```sql
refresh_imv_depending_on(source TEXT) RETURNS TEXT
```

`source` is the entry as recorded in `__reflex_ivm_reference.depends_on` — either the bare relation name (`'orders'`) or a schema-qualified name (`'public.orders'`), matching how the IMV was registered.

Returns `'REFRESHED N IMVs'` where `N` is the count of attempted reconciles, or `'REFRESHED 0 IMVs'` (with a `WARNING`) when no enabled IMV depends on `source`.

## Behaviour

1. Look up every IMV with `$1 = ANY(depends_on) AND enabled = TRUE`, ordered by `graph_depth`.
2. Call [`reflex_reconcile(name)`](reflex_reconcile.md) for each, sequentially.
3. A failing reconcile emits a `WARNING` and is counted in the total; the loop continues. Inspect `reflex_ivm_status().last_error` afterwards to identify failures.

Each individual reconcile follows the rebuild path documented on the [`reflex_reconcile`](reflex_reconcile.md) page.

## Example

```sql
-- Source MV refreshed manually
REFRESH MATERIALIZED VIEW orders_mv;

-- Cascade the refresh into every IMV that depends on it
SELECT refresh_imv_depending_on('orders_mv');
-- REFRESHED 4 IMVs
```

## See also

- [`reflex_reconcile`](reflex_reconcile.md) — rebuild a single IMV.
- [`reflex_scheduled_reconcile`](reflex_scheduled_reconcile.md) — periodic drift scan via pg_cron.
