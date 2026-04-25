# `reflex_ivm_stats`

(1.2.0+) Detailed stats for a single IMV.

## Signature

```sql
reflex_ivm_stats(view_name TEXT) RETURNS TABLE(metric TEXT, value TEXT)
```

## Example

```sql
SELECT * FROM reflex_ivm_stats('sales_by_region');
```

| metric | value |
|---|---|
| `intermediate_size` | `1024 kB` |
| `target_size` | `512 kB` |
| `last_flush_ms` | `42` |
| `last_flush_rows` | `1500` |
| `flush_count` | `108` |
| `last_error` | `NULL` |

`pg_size_pretty(pg_total_relation_size(...))` is used for the size metrics. They include the table's heap, toast, and indexes.
