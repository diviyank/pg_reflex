# Event triggers

pg_reflex installs two database-wide event triggers (1.2.0+).

## `reflex_on_sql_drop`

Fires on the `sql_drop` event. For each dropped table, it looks up the IMV registry for IMVs whose `depends_on` includes that table, and calls `drop_reflex_ivm(name, true)` for each.

The cleanup drops:

- Target table
- Intermediate table
- Affected-groups table
- Delta scratch table
- Passthrough scratch tables (per source)
- Trigger functions (the per-source triggers themselves were dropped automatically with the source)
- Registry row

A `NOTICE` is emitted per IMV cleaned up. A failed cleanup falls back to a registry-row-only delete plus a `WARNING`, so a misbehaving IMV cannot block a `DROP TABLE`.

## `reflex_on_ddl_command_end`

Fires on `ddl_command_end` with `WHEN TAG IN ('ALTER TABLE')`. For each tracked source that was altered, it consults the GUC `pg_reflex.alter_source_policy` (1.2.1+):

- `'warn'` (default): emits `WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(…)`. The ALTER proceeds.
- `'error'`: raises an `EXCEPTION`, rolling back the ALTER.

```sql
-- Strict mode for a session
SET pg_reflex.alter_source_policy = 'error';
ALTER TABLE orders ADD COLUMN x INT;
-- ERROR: pg_reflex: ALTER blocked by pg_reflex.alter_source_policy='error' on tracked source(s)
```

## Disabling

Both triggers are owned by the extension and re-installed on every `ALTER EXTENSION pg_reflex UPDATE`. To temporarily disable (e.g. for a coordinated DDL window):

```sql
ALTER EVENT TRIGGER reflex_on_sql_drop DISABLE;
ALTER EVENT TRIGGER reflex_on_ddl_command_end DISABLE;

-- ... your DDL window ...

ALTER EVENT TRIGGER reflex_on_sql_drop ENABLE;
ALTER EVENT TRIGGER reflex_on_ddl_command_end ENABLE;
```
