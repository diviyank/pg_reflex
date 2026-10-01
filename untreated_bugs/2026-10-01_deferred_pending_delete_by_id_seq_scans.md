# `__reflex_deferred_flush_fn` deletes its request row with a sequential scan — S4 (perf)

Since 1.11.5 the COMMIT-time flush function removes its own 'TRUNCATE' request row with
`DELETE FROM public.__reflex_deferred_pending WHERE id = NEW.id`
(src/schema_builder.rs:919, sql/pg_reflex--1.11.4--1.11.5.sql). `__reflex_deferred_pending`
has no index (`id BIGSERIAL` without a key, schema_builder.rs:895), so each delete is a
sequential scan:

```
Delete on __reflex_deferred_pending
  ->  Seq Scan on __reflex_deferred_pending
        Filter: (id = 1)
```

The table holds only in-flight rows (each flush deletes its own), so it is normally a few rows
and the cost is negligible; it grows with concurrent DEFERRED writers and with bloat between
vacuums. One delete per truncated DEFERRED IMV per transaction.

## Fix direction
`CREATE INDEX IF NOT EXISTS ON public.__reflex_deferred_pending (id)` in
`build_deferred_flush_ddl` and a migration (guarded by `to_regclass`, the table is created
lazily). Measure first: the index adds a write to every pending insert, which runs once per
DEFERRED statement.
