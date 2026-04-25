# `reflex_flush_deferred`

Drains the pending-delta queue for a source table. Used in `DEFERRED` mode either at COMMIT (automatically, by a deferred constraint trigger) or on demand.

## Signature

```sql
reflex_flush_deferred(source_table TEXT) RETURNS TEXT
```

Returns a status string with the count of IMVs processed.

## When to call manually

```sql
-- DEFERRED IMV
SELECT create_reflex_ivm('batch_view',
    'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    NULL, 'UNLOGGED', 'DEFERRED');

INSERT INTO orders ... ;
INSERT INTO orders ... ;

-- Force flush before reading
SELECT reflex_flush_deferred('orders');
SELECT * FROM batch_view;  -- now reflects the inserts
```

## Per-IMV SAVEPOINT

Each IMV's flush body runs in its own subtransaction (`BEGIN … EXCEPTION WHEN OTHERS … END`). A failing IMV records `last_error` in the registry and emits a `WARNING`; the cascade continues. This is the operational-safety improvement landed in 1.2.0.

## Observability

Each flush updates the registry:

- `last_flush_ms` — wall time of this IMV's flush body.
- `last_flush_rows` — staged delta row count.
- `flush_count` — total number of flushes (incremented on success and on EXCEPTION).
- `last_error` — `NULL` on success; otherwise `LEFT(SQLERRM || ' (SQLSTATE …)', 500)`.
- `flush_ms_history` (1.3.0+) — ring buffer of the last 64 `last_flush_ms` values, fed into `reflex_ivm_histogram`.
- `application_name` (1.3.0+) — set to `reflex_flush:<view>` for the duration of the body so `pg_stat_statements` rows can be filtered by IMV.
