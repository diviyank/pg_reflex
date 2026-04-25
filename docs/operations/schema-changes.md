# Schema changes

`ALTER TABLE` on a tracked source can leave an IMV stale. pg_reflex's `reflex_on_ddl_command_end` event trigger provides two policies.

## Default — `'warn'`

```sql
ALTER TABLE orders ADD COLUMN priority INT;
-- WARNING: pg_reflex: source table orders was altered; IMV daily_totals may be stale — run SELECT reflex_rebuild_imv('daily_totals') to recover
```

The ALTER proceeds. The operator's job is to monitor `WARNING` log lines and call `reflex_rebuild_imv` for affected IMVs. Find the affected list before the ALTER:

```sql
SELECT name FROM public.__reflex_ivm_reference WHERE 'orders' = ANY(depends_on);
```

## Strict — `'error'` (1.2.1+)

```sql
SET pg_reflex.alter_source_policy = 'error';
ALTER TABLE orders ADD COLUMN priority INT;
-- ERROR: pg_reflex: ALTER blocked by pg_reflex.alter_source_policy='error' on tracked source(s); affected: public.orders -> daily_totals
-- HINT: Set pg_reflex.alter_source_policy = 'warn' (default) or drop_reflex_ivm() first.
```

The ALTER rolls back. Use this in change-control deployments where stale-on-DDL is unacceptable.

## Coordinated DDL window

If you have a planned schema-change window:

```sql
BEGIN;
SET LOCAL pg_reflex.alter_source_policy = 'warn';

-- Disable the event trigger entirely if you want to bypass the warning too
ALTER EVENT TRIGGER reflex_on_ddl_command_end DISABLE;

ALTER TABLE orders ADD COLUMN priority INT;
-- ... more DDL ...

ALTER EVENT TRIGGER reflex_on_ddl_command_end ENABLE;

-- Rebuild the affected IMVs
SELECT reflex_rebuild_imv(name)
FROM public.__reflex_ivm_reference
WHERE 'orders' = ANY(depends_on);

COMMIT;
```

## Why not just block ALTER unconditionally?

Strict mode breaks legitimate operator workflows — column renames, type widenings, dropping unused columns. Defaulting to `warn` keeps the operational ergonomics; teams that need gating opt in via:

```sql
ALTER DATABASE mydb SET pg_reflex.alter_source_policy = 'error';
```
