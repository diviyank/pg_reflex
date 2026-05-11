# `create_reflex_ivm_if_not_exists`

Idempotent variant of [`create_reflex_ivm`](create_reflex_ivm.md). Returns silently when the IMV is already registered instead of raising an error, which makes it safe to call from migration scripts that may run more than once.

## Signature

```sql
create_reflex_ivm_if_not_exists(
    view_name        TEXT,
    sql              TEXT,
    unique_columns   TEXT  DEFAULT NULL,
    storage          TEXT  DEFAULT 'UNLOGGED',
    mode             TEXT  DEFAULT 'IMMEDIATE'
) RETURNS TEXT
```

Parameters match `create_reflex_ivm` exactly — see that page for the per-argument semantics.

## Return values

| Outcome | Return string |
|---|---|
| IMV created | `'CREATE REFLEX INCREMENTAL VIEW'` |
| `view_name` already registered | `'REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)'` |
| Validation or build error | `'ERROR: …'` |

The skip path checks only that a registry row with the same `view_name` exists. It does **not** verify that the registered `sql` matches the argument — if you change the body of the IMV, you must `drop_reflex_ivm` and recreate.

## Example

```sql
-- Migration step that's safe to re-run
SELECT create_reflex_ivm_if_not_exists('sales_by_region',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region');
```

## See also

- [`create_reflex_ivm`](create_reflex_ivm.md) — full reference, including the 6-argument `topk` overload.
- [`drop_reflex_ivm`](drop_reflex_ivm.md) — required when you need to change an IMV's definition.
