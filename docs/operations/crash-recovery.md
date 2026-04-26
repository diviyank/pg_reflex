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

## Picking LOGGED vs UNLOGGED — decision guide

Use this matrix when the answer isn't obvious. Each row is one IMV.

| Question | Answer ⇒ |
|---|---|
| Is the source itself UNLOGGED? | UNLOGGED (no point being safer than the source) |
| Does post-crash drift cause a wrong answer for a downstream system that *can't* tolerate stale-and-empty for the duration of one reconcile? | LOGGED |
| Is the IMV's flush rate ≥ 1 / second sustained? | UNLOGGED (WAL amplification dominates) |
| Is the IMV ≤ 100 K rows AND backing a critical dashboard? | LOGGED (recovery is cheap, write cost is small) |
| Are reconciles ≥ 5 seconds AND the IMV serves an SLA-bound read path? | LOGGED |
| Otherwise | UNLOGGED + scheduled `reflex_scheduled_reconcile` |

### Cost shape

- **UNLOGGED flush**: in-memory + dirty buffer flush at checkpoint. No WAL records for the intermediate table writes.
- **LOGGED flush**: every MERGE / INSERT / DELETE on the intermediate emits WAL records sized proportionally to the touched row payload + index updates.

Empirically, on a 10M-row JOIN-passthrough source with 100 K-row delta batches, LOGGED adds 30–80 % to flush latency vs UNLOGGED on the same hardware. The exact ratio depends on `wal_buffers`, `checkpoint_timeout`, and storage tier.

### Mixed-mode in one registry

`storage` is per-IMV. A common pattern:

```sql
-- Cheap, frequently-rebuilt aggregate — UNLOGGED, reconcile on @reboot
SELECT create_reflex_ivm('hourly_kpi',
    'SELECT region, SUM(revenue) AS r FROM events GROUP BY region',
    NULL, 'UNLOGGED');

-- SLA-bound read path that can't afford a recompile window — LOGGED
SELECT create_reflex_ivm('billing_summary',
    'SELECT customer_id, SUM(charged) FROM invoices GROUP BY customer_id',
    'customer_id', 'LOGGED');
```

The `reflex_scheduled_reconcile` recipe is a no-op for LOGGED IMVs that didn't actually drift, so the same cron job covers both modes.
