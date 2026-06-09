# `reflex_probe_not_null_columns`

(1.4.5+) Re-probes an IMV's intermediate for effectively-`NOT NULL` group-by columns and updates the stored aggregations metadata, keeping the trigger codegen index-friendly.

## Signature

```sql
reflex_probe_not_null_columns(view_name TEXT)
RETURNS TEXT
```

## Why it matters

The trigger codegen consults `not_null_columns` to choose between a sargable `=` and a NULL-safe `IS NOT DISTINCT FROM` on group-key probes. The `=` form is index-friendly; the NULL-safe form is not. Re-running this after a data-shape change — e.g. a backfill that introduces NULLs into a previously NULL-free column, or removes them — re-derives the set so the codegen stays optimal.

Idempotent: a call with no data-shape change reports zero additions.

## Example

```sql
SELECT reflex_probe_not_null_columns('sales_by_region');
```
