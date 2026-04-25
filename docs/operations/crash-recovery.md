# Crash recovery

pg_reflex's intermediate and target tables are `UNLOGGED` by default — they are not WAL-replayed and are TRUNCATEd on crash recovery (PostgreSQL's default behaviour for UNLOGGED tables).

## Two recovery modes

### Default — `UNLOGGED`

After a crash, intermediate and target tables come up empty. Run `reflex_rebuild_imv` (or schedule it via pg_cron at startup):

```sql
SELECT * FROM reflex_scheduled_reconcile(0);  -- reconcile every IMV
```

For a startup hook, use pg_cron's `@reboot`:

```sql
SELECT cron.schedule('reflex-post-restart',
    '@reboot',
    $$ SELECT * FROM reflex_scheduled_reconcile(0) $$);
```

### `LOGGED` — crash-safe

Pass `storage='LOGGED'` at IMV creation and the intermediate + target are regular WAL-logged tables:

```sql
SELECT create_reflex_ivm('critical_view',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region',
    NULL, 'LOGGED');
```

Tradeoff: every flush writes to WAL. For high-throughput INSERT workloads this can double the write amplification of the source. Use LOGGED for IMVs whose post-crash recovery time is more expensive than the WAL overhead.

## Mixed registries

`storage` is per-IMV. You can mix LOGGED and UNLOGGED IMVs in the same registry — the `reflex_scheduled_reconcile` recipe is a no-op for LOGGED IMVs that didn't actually drift.

## Why default to UNLOGGED

The 5-table-JOIN production benchmark is dominated by the cost of WAL writes when LOGGED. UNLOGGED gives you 2-4× lower flush latency at the cost of a post-crash reconcile pass — most analytical workloads accept that tradeoff.
