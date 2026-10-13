# Event triggers

pg_reflex installs two database-wide event triggers (1.2.0+).

## `reflex_on_sql_drop`

Fires on the `sql_drop` event. For each dropped table it cleans up two sets of IMVs:

- **Source dropped** — IMVs whose `depends_on` includes that table. Since 1.12.0 a source registered without a schema matches only a dropped table of `public`: dropping a table of the same name in another schema leaves the IMV alone. A name PostgreSQL prints in double quotes but reads back bare (a non-ASCII letter, a `$`) is matched under its bare spelling.
- **Target dropped** (1.10.6+) — the IMV whose own registered target table *is* that table. This catches an IMV whose target is dropped directly or via `DROP SCHEMA … CASCADE`, and IMVs built on a **view** (a dropped view is `object_type = 'view'`, which the source branch's table filter never matches) — both previously left an orphaned registry row pointing at a relation that no longer exists. The match is on the exact target identity (`target_schema` + bare name), so a partition-swap maintenance cycle dropping child / swap tables is never mistaken for the registered target.

For each match it calls `drop_reflex_ivm(name, true)`.

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

Fires on `ddl_command_end` with `WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE')`.

For a partition created, attached or detached under a partitioned source that an enabled IMV reads, it queues the root in `__reflex_partition_pending` (the flush runs once at COMMIT, see [`reflex_flush_partitions`](reflex_flush_partitions.md)) and syncs the partitions of the IMVs that mirror it.

A relation is matched to the IMVs that read it through `public.__reflex_reads_relation(depends_on, relation)` (1.12.0+): a source registered without a schema is a table of `public`, so partition DDL or an `ALTER TABLE` on `tb.orders` does not reach the IMVs of `public.orders`. Before 1.12.0 the bare name matched a table of any schema.

For each tracked source that was altered, it consults the GUC `pg_reflex.alter_source_policy` (1.2.1+):

- `'warn'` (default): emits `WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(…)`. The ALTER proceeds.
- `'error'`: raises an `EXCEPTION`, rolling back the ALTER.

```sql
-- Strict mode for a session
SET pg_reflex.alter_source_policy = 'error';
ALTER TABLE orders ADD COLUMN x INT;
-- ERROR: pg_reflex: ALTER blocked by pg_reflex.alter_source_policy='error' on tracked source(s)
```

## Disabling

Both triggers are owned by the extension. An update that changes them replaces their functions (`__reflex_on_sql_drop`, `__reflex_on_ddl_command_end`) and leaves the triggers, enabled or not, as they are. To temporarily disable (e.g. for a coordinated DDL window):

```sql
ALTER EVENT TRIGGER reflex_on_sql_drop DISABLE;
ALTER EVENT TRIGGER reflex_on_ddl_command_end DISABLE;

-- ... your DDL window ...

ALTER EVENT TRIGGER reflex_on_sql_drop ENABLE;
ALTER EVENT TRIGGER reflex_on_ddl_command_end ENABLE;
```
