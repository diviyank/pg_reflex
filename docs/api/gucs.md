# GUCs

pg_reflex respects one custom GUC.

## `pg_reflex.alter_source_policy`

(1.2.1+) Controls how the `reflex_on_ddl_command_end` event trigger reacts when a tracked source is `ALTER TABLE`'d.

| Value | Behaviour |
|---|---|
| `'warn'` (default) | Emits `WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(…)`'. The ALTER proceeds. |
| `'error'` | Raises an `EXCEPTION`, rolling back the ALTER. |

## Setting

Custom namespaced GUCs are session-settable without explicit registration on PG 9.2+:

```sql
-- Per-session (until reset or disconnect)
SET pg_reflex.alter_source_policy = 'error';

-- Per-transaction
BEGIN;
SET LOCAL pg_reflex.alter_source_policy = 'error';
ALTER TABLE orders ADD COLUMN x INT;
COMMIT;

-- Per-database
ALTER DATABASE mydb SET pg_reflex.alter_source_policy = 'error';

-- Reset
RESET pg_reflex.alter_source_policy;
```

## Why default to `warn`

Strict mode rolls back legitimate operator workflows — renames, type widenings, column adds. Defaulting to `warn` keeps the operational ergonomics; teams that want gating opt in via `ALTER DATABASE`.
